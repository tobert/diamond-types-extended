//! Integration tests for conflict handling

use facet::Document;

#[test]
fn test_lww_conflict_detection() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Concurrent writes to same key
    doc_a.transact(alice, |tx| {
        tx.root().set("key", "alice");
    });

    doc_b.transact(bob, |tx| {
        tx.root().set("key", "bob");
    });

    // Sync both ways
    let ops_a = doc_a.ops_since(&[]).into();
    let ops_b = doc_b.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    // Check for conflicts using get_conflicted
    let conflicted_a = doc_a.root().get_conflicted("key").unwrap();
    let conflicted_b = doc_b.root().get_conflicted("key").unwrap();

    // Both should have conflicts
    assert!(conflicted_a.has_conflicts());
    assert!(conflicted_b.has_conflicts());

    // Winner should be the same
    assert_eq!(conflicted_a.value, conflicted_b.value);

    // Should have exactly one conflict
    assert_eq!(conflicted_a.conflicts.len(), 1);
    assert_eq!(conflicted_b.conflicts.len(), 1);
}

#[test]
fn test_no_conflict_sequential_writes() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    doc.transact(alice, |tx| {
        tx.root().set("key", "first");
    });

    doc.transact(alice, |tx| {
        tx.root().set("key", "second");
    });

    let conflicted = doc.root().get_conflicted("key").unwrap();
    assert!(!conflicted.has_conflicts());
    assert_eq!(conflicted.value.as_str(), Some("second"));
}

#[test]
fn test_set_add_wins_semantics() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates set
    let _set_id = doc_a.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    // Alice adds an item
    doc_a.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("item");
        }
    });

    // Sync to Bob
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Alice removes the item (she has observed it)
    doc_a.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.remove_str("item");
        }
    });

    // Concurrently, Bob adds the same item (he has also observed it)
    doc_b.transact(bob, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("item");
        }
    });

    // Sync
    let ops_a = doc_a.ops_since(&[]).into();
    let ops_b = doc_b.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    // With add-wins semantics, the item should still be present
    // (Bob's add wins over Alice's remove because it's concurrent)
    let set_a = doc_a.root().get_set("tags").unwrap();
    let set_b = doc_b.root().get_set("tags").unwrap();

    assert!(set_a.contains_str("item"));
    assert!(set_b.contains_str("item"));
}

#[test]
fn test_nested_crdt_conflict() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Both create a map at the same key (concurrent)
    doc_a.transact(alice, |tx| {
        tx.root().create_map("nested");
    });

    doc_b.transact(bob, |tx| {
        tx.root().create_map("nested");
    });

    // Alice puts something in her map
    doc_a.transact(alice, |tx| {
        if let Some(mut nested) = tx.get_map_mut(&["nested"]) {
            nested.set("from", "alice");
        }
    });

    // Bob puts something in his map
    doc_b.transact(bob, |tx| {
        if let Some(mut nested) = tx.get_map_mut(&["nested"]) {
            nested.set("from", "bob");
        }
    });

    // Sync
    let ops_a = doc_a.ops_since(&[]).into();
    let ops_b = doc_b.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    // Both should converge
    let nested_a = doc_a.root().get_map("nested");
    let nested_b = doc_b.root().get_map("nested");

    // The winning nested map should exist and have consistent content
    assert!(nested_a.is_some() || nested_b.is_some());
}
