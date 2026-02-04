//! Integration tests for replication/sync

use diamond_types_extended::Document;

#[test]
fn test_two_peer_sync_map() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice makes changes
    doc_a.transact(alice, |tx| {
        tx.root().set("from_alice", "hello from Alice");
    });

    // Bob makes changes
    doc_b.transact(bob, |tx| {
        tx.root().set("from_bob", "hello from Bob");
    });

    // Sync A -> B
    let ops_a = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();

    // Sync B -> A
    let ops_b = doc_b.ops_since(&[]).into();
    doc_a.merge_ops(ops_b).unwrap();

    // Both should have both keys
    assert!(doc_a.root().contains_key("from_alice"));
    assert!(doc_a.root().contains_key("from_bob"));
    assert!(doc_b.root().contains_key("from_alice"));
    assert!(doc_b.root().contains_key("from_bob"));

    // Values should match
    assert_eq!(
        doc_a.root().get("from_alice").unwrap().as_str(),
        doc_b.root().get("from_alice").unwrap().as_str()
    );
}

#[test]
fn test_two_peer_sync_text() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");

    // Alice creates text
    let _text_id = doc_a.transact(alice, |tx| {
        tx.root().create_text("content")
    });

    doc_a.transact(alice, |tx| {
        if let Some(mut text) = tx.get_text_mut(&["content"]) {
            text.insert(0, "Hello");
        }
    });

    // Sync A -> B
    let ops_a = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();

    // Bob should see the text
    let text_b = doc_b.root().get_text("content").unwrap();
    assert_eq!(text_b.content(), "Hello");
}

#[test]
fn test_two_peer_sync_set() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates set
    let _set_id = doc_a.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    // Sync to Bob so he knows about the set
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Both add items
    doc_a.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("from_alice");
        }
    });

    doc_b.transact(bob, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("from_bob");
        }
    });

    // Cross-sync
    let ops_a = doc_a.ops_since(&[]).into();
    let ops_b = doc_b.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    // Both should have both items
    let set_a = doc_a.root().get_set("tags").unwrap();
    let set_b = doc_b.root().get_set("tags").unwrap();

    assert!(set_a.contains_str("from_alice"));
    assert!(set_a.contains_str("from_bob"));
    assert!(set_b.contains_str("from_alice"));
    assert!(set_b.contains_str("from_bob"));
}

#[test]
fn test_incremental_sync() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");

    // Initial sync
    doc_a.transact(alice, |tx| {
        tx.root().set("v1", "first");
    });

    let ops1 = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops1).unwrap();

    // Remember B's version
    let b_version = doc_b.version().clone();

    // Alice makes more changes
    doc_a.transact(alice, |tx| {
        tx.root().set("v2", "second");
    });

    // Only sync new changes (from B's perspective)
    let ops2 = doc_a.ops_since(b_version.as_ref()).into();
    doc_b.merge_ops(ops2).unwrap();

    // B should have both
    assert!(doc_b.root().contains_key("v1"));
    assert!(doc_b.root().contains_key("v2"));
}

#[test]
fn test_convergence_after_concurrent_edits() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Both write to the same key (concurrent)
    doc_a.transact(alice, |tx| {
        tx.root().set("key", "alice's value");
    });

    doc_b.transact(bob, |tx| {
        tx.root().set("key", "bob's value");
    });

    // Cross-sync
    let ops_a = doc_a.ops_since(&[]).into();
    let ops_b = doc_b.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    // Both should converge to the same value (LWW)
    let val_a = doc_a.root().get("key").unwrap();
    let val_b = doc_b.root().get("key").unwrap();
    assert_eq!(val_a, val_b);
}
