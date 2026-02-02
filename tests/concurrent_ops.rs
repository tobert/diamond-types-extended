//! Correctness-focused concurrent operations tests
//!
//! These tests exercise path access, concurrent editing, and merge order
//! independence using the public Document API. They would have caught
//! the panic in path navigation methods when accessing nonexistent paths.
//!
//! Test coverage informed by Gemini review recommendations.

use facet::{Document, SerializedOpsOwned};

mod helpers {
    use facet::Document;

    /// Assert documents have converged (same version)
    pub fn assert_converged(a: &Document, b: &Document) {
        assert_eq!(
            a.version(),
            b.version(),
            "Documents should have same version after sync"
        );
    }

    /// Cross-sync two documents (A↔B) and verify convergence
    pub fn cross_sync(a: &mut Document, b: &mut Document) {
        let ops_a = a.ops_since(&[]).into();
        let ops_b = b.ops_since(&[]).into();
        b.merge_ops(ops_a).unwrap();
        a.merge_ops(ops_b).unwrap();
        assert_converged(a, b);
    }
}

// =============================================================================
// Path Access Tests - would have caught the panic
// =============================================================================

#[test]
fn access_nonexistent_path_returns_none() {
    let doc = Document::new();

    // These should return None, not panic
    assert!(doc.root().get_map("missing").is_none());
    assert!(doc.root().get_text("missing").is_none());
    assert!(doc.root().get_set("missing").is_none());
    assert!(doc.root().get("missing").is_none());
}

#[test]
fn access_deep_nonexistent_path() {
    let mut doc = Document::new();
    let agent = doc.get_or_create_agent("test");

    doc.transact(agent, |tx| {
        tx.root().create_map("level1");
    });

    // level1 exists, but level2 doesn't
    assert!(doc.get_map(&["level1"]).is_some());
    assert!(doc.get_map(&["level1", "level2"]).is_none());
    assert!(doc.get_text(&["level1", "missing"]).is_none());
}

#[test]
fn access_path_before_and_after_merge() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");

    // Alice creates nested structure
    doc_a.transact(alice, |tx| {
        tx.root().create_map("data");
    });
    doc_a.transact(alice, |tx| {
        if let Some(mut m) = tx.get_map_mut(&["data"]) {
            m.set("key", "value");
        }
    });

    // Bob can't see it yet
    assert!(doc_b.get_map(&["data"]).is_none());

    // After merge, Bob can see it
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();
    assert!(doc_b.get_map(&["data"]).is_some());
    assert_eq!(
        doc_b
            .get_map(&["data"])
            .unwrap()
            .get("key")
            .unwrap()
            .as_str(),
        Some("value")
    );
}

// =============================================================================
// Text Editing Tests
// =============================================================================

#[test]
fn text_sequential_sync_roundtrip() {
    // Test text sync with sequential (non-concurrent) operations
    // This validates the basic text CRDT sync works before concurrent editing
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates text with initial content
    doc_a.transact(alice, |tx| {
        tx.root().create_text("doc");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Hello");
    });

    // Sync to Bob
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Bob appends (sequential, not concurrent)
    doc_b.transact(bob, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" world");
    });

    // Sync back to Alice
    let bob_version = doc_a.version().clone();
    let ops_b = doc_b.ops_since(bob_version.as_ref()).into();
    doc_a.merge_ops(ops_b).unwrap();

    // Both should have same text now
    let text_a = doc_a.root().get_text("doc").unwrap().content();
    let text_b = doc_b.root().get_text("doc").unwrap().content();
    assert_eq!(
        text_a, text_b,
        "Documents should have same content after sequential sync"
    );
    assert_eq!(text_a, "Hello world");
}

#[test]
fn concurrent_text_overlapping_edits() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Setup: both have "Hello world"
    doc_a.transact(alice, |tx| {
        tx.root().create_text("doc");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Hello world");
    });
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Alice deletes "world", Bob inserts in middle
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().delete(6..11); // "Hello "
    });
    doc_b.transact(bob, |tx| {
        tx.get_text_mut(&["doc"])
            .unwrap()
            .insert(6, "beautiful "); // "Hello beautiful world"
    });

    helpers::cross_sync(&mut doc_a, &mut doc_b);

    let text_a = doc_a.root().get_text("doc").unwrap().content();
    let text_b = doc_b.root().get_text("doc").unwrap().content();
    assert_eq!(text_a, text_b, "Text should converge");
}

// =============================================================================
// Merge Order Independence Tests
// =============================================================================

#[test]
fn merge_order_independence_three_peers() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let mut doc_c = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");
    let carol = doc_c.get_or_create_agent("carol");

    // Each makes independent changes
    doc_a.transact(alice, |tx| {
        tx.root().set("from", "alice");
    });
    doc_b.transact(bob, |tx| {
        tx.root().set("from", "bob");
    });
    doc_c.transact(carol, |tx| {
        tx.root().set("from", "carol");
    });

    let ops_a: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    let ops_b: SerializedOpsOwned = doc_b.ops_since(&[]).into();
    let ops_c: SerializedOpsOwned = doc_c.ops_since(&[]).into();

    // Different merge orders
    let mut test1 = Document::new();
    test1.merge_ops(ops_a.clone()).unwrap();
    test1.merge_ops(ops_b.clone()).unwrap();
    test1.merge_ops(ops_c.clone()).unwrap();

    let mut test2 = Document::new();
    test2.merge_ops(ops_c.clone()).unwrap();
    test2.merge_ops(ops_a.clone()).unwrap();
    test2.merge_ops(ops_b.clone()).unwrap();

    let mut test3 = Document::new();
    test3.merge_ops(ops_b.clone()).unwrap();
    test3.merge_ops(ops_c.clone()).unwrap();
    test3.merge_ops(ops_a.clone()).unwrap();

    // All should converge to same value
    let val1 = test1.root().get("from").unwrap();
    let val2 = test2.root().get("from").unwrap();
    let val3 = test3.root().get("from").unwrap();
    assert_eq!(val1, val2);
    assert_eq!(val2, val3);
}

#[test]
fn diamond_merge_converges() {
    //     A (origin)
    //    / \
    //   B   C  (concurrent forks)
    //    \ /
    //     D  (merged)

    let mut origin = Document::new();
    let agent = origin.get_or_create_agent("origin");

    // Use map values instead of text to avoid text CRDT merge complexity
    origin.transact(agent, |tx| {
        tx.root().set("base", "origin");
    });

    // Fork to B and C
    let ops_origin: SerializedOpsOwned = origin.ops_since(&[]).into();
    let mut doc_b = Document::new();
    let mut doc_c = Document::new();
    doc_b.merge_ops(ops_origin.clone()).unwrap();
    doc_c.merge_ops(ops_origin).unwrap();

    let bob = doc_b.get_or_create_agent("bob");
    let carol = doc_c.get_or_create_agent("carol");

    // Concurrent edits to different keys
    doc_b.transact(bob, |tx| {
        tx.root().set("from_bob", "hello from bob");
    });
    doc_c.transact(carol, |tx| {
        tx.root().set("from_carol", "hello from carol");
    });

    // Cross-merge (diamond closes)
    helpers::cross_sync(&mut doc_b, &mut doc_c);

    // Both should converge - same keys visible
    assert!(doc_b.root().contains_key("base"));
    assert!(doc_b.root().contains_key("from_bob"));
    assert!(doc_b.root().contains_key("from_carol"));
    assert!(doc_c.root().contains_key("base"));
    assert!(doc_c.root().contains_key("from_bob"));
    assert!(doc_c.root().contains_key("from_carol"));

    // Values should match
    assert_eq!(
        doc_b.root().get("from_bob").unwrap(),
        doc_c.root().get("from_bob").unwrap()
    );
    assert_eq!(
        doc_b.root().get("from_carol").unwrap(),
        doc_c.root().get("from_carol").unwrap()
    );
}

// =============================================================================
// Concurrent CRDT Creation Tests
// =============================================================================

#[test]
fn concurrent_text_and_map_at_same_key() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates Text, Bob creates Map at same key
    doc_a.transact(alice, |tx| {
        tx.root().create_text("content");
    });
    doc_b.transact(bob, |tx| {
        tx.root().create_map("content");
    });

    helpers::cross_sync(&mut doc_a, &mut doc_b);

    // One type wins, both converge, no panic
    let has_text_a = doc_a.root().get_text("content").is_some();
    let has_map_a = doc_a.root().get_map("content").is_some();
    let has_text_b = doc_b.root().get_text("content").is_some();
    let has_map_b = doc_b.root().get_map("content").is_some();

    // Exactly one type should exist on each, and they should match
    assert_eq!(has_text_a, has_text_b);
    assert_eq!(has_map_a, has_map_b);
    assert!(has_text_a ^ has_map_a, "Exactly one CRDT type should win");
}

// =============================================================================
// Idempotency Tests (Gemini recommendation)
// =============================================================================

#[test]
fn ops_are_idempotent() {
    // Applying the same ops twice should not change the document
    // This simulates network retry scenarios
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");

    doc_a.transact(alice, |tx| {
        tx.root().set("key", "value");
        tx.root().set("number", 42i64);
    });

    let ops: SerializedOpsOwned = doc_a.ops_since(&[]).into();

    // Apply once
    doc_b.merge_ops(ops.clone()).unwrap();
    let version_after_first = doc_b.version().clone();
    let value_after_first = doc_b.root().get("key").unwrap();

    // Apply twice (simulate network retry)
    doc_b.merge_ops(ops).unwrap();
    let version_after_second = doc_b.version().clone();
    let value_after_second = doc_b.root().get("key").unwrap();

    assert_eq!(
        version_after_first, version_after_second,
        "Applying ops twice should not change version"
    );
    assert_eq!(
        value_after_first, value_after_second,
        "Applying ops twice should not change values"
    );
    assert_eq!(doc_b.root().get("key").unwrap().as_str(), Some("value"));
    assert_eq!(doc_b.root().get("number").unwrap().as_int(), Some(42));
}

#[test]
fn self_merge_is_noop() {
    // Merging a document's ops back into itself should be a no-op
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    doc.transact(alice, |tx| {
        tx.root().set("key", "value");
    });

    let version_before = doc.version().clone();
    let ops: SerializedOpsOwned = doc.ops_since(&[]).into();

    // Self-merge
    doc.merge_ops(ops).unwrap();

    let version_after = doc.version().clone();
    assert_eq!(
        version_before, version_after,
        "Self-merge should not change version"
    );
    assert_eq!(doc.root().get("key").unwrap().as_str(), Some("value"));
}

// =============================================================================
// Map Conflict Tests (Gemini recommendation)
// =============================================================================

#[test]
fn map_concurrent_update_and_tombstone() {
    // Test concurrent update vs set_nil (tombstone)
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Setup initial state
    doc_a.transact(alice, |tx| {
        tx.root().set("target", "initial");
    });
    helpers::cross_sync(&mut doc_a, &mut doc_b);

    // Alice updates the value
    doc_a.transact(alice, |tx| {
        tx.root().set("target", "updated");
    });

    // Bob sets to nil (tombstone)
    doc_b.transact(bob, |tx| {
        tx.root().set_nil("target");
    });

    helpers::cross_sync(&mut doc_a, &mut doc_b);

    // Both should converge to the same value (LWW determines winner)
    let val_a = doc_a.root().get("target").unwrap();
    let val_b = doc_b.root().get("target").unwrap();
    assert_eq!(val_a, val_b, "Documents should converge after concurrent update/tombstone");

    // Key should still exist (tombstone doesn't remove key)
    assert!(doc_a.root().contains_key("target"));
    assert!(doc_b.root().contains_key("target"));
}

// =============================================================================
// Set Concurrency Tests (Gemini recommendation)
// =============================================================================

#[test]
fn set_concurrent_add_and_remove() {
    // Test add-wins semantics for concurrent add/remove
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates set with initial item
    doc_a.transact(alice, |tx| {
        tx.root().create_set("tags");
    });
    doc_a.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("item");
        }
    });

    // Sync to Bob
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Concurrent: Alice removes, Bob adds the same item
    doc_a.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.remove_str("item");
        }
    });
    doc_b.transact(bob, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("item");
        }
    });

    helpers::cross_sync(&mut doc_a, &mut doc_b);

    // With add-wins semantics, the item should still be present
    let set_a = doc_a.root().get_set("tags").unwrap();
    let set_b = doc_b.root().get_set("tags").unwrap();

    // Both should agree
    assert_eq!(
        set_a.contains_str("item"),
        set_b.contains_str("item"),
        "Sets should converge"
    );

    // OR-Set with add-wins: concurrent add beats remove
    assert!(
        set_a.contains_str("item"),
        "Add-wins: concurrent add should beat remove"
    );
}

#[test]
fn set_concurrent_different_items() {
    // Test concurrent adds of different items
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates set
    doc_a.transact(alice, |tx| {
        tx.root().create_set("tags");
    });

    // Sync to Bob
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Concurrent: Alice and Bob add different items
    doc_a.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("alice_tag");
        }
    });
    doc_b.transact(bob, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["tags"]) {
            set.add_str("bob_tag");
        }
    });

    helpers::cross_sync(&mut doc_a, &mut doc_b);

    // Both items should be present in both documents
    let set_a = doc_a.root().get_set("tags").unwrap();
    let set_b = doc_b.root().get_set("tags").unwrap();

    assert!(set_a.contains_str("alice_tag"));
    assert!(set_a.contains_str("bob_tag"));
    assert!(set_b.contains_str("alice_tag"));
    assert!(set_b.contains_str("bob_tag"));
}
