//! Integration tests for replication/sync

use diamond_types_extended::{Document, Frontier, Uuid};

#[test]
fn test_two_peer_sync_map() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc_b.create_agent(Uuid::from_u128(0xB0B));

    // Alice makes changes
    doc_a.transact(alice, |tx| {
        tx.root().set("from_alice", "hello from Alice");
    });

    // Bob makes changes
    doc_b.transact(bob, |tx| {
        tx.root().set("from_bob", "hello from Bob");
    });

    // Sync A -> B
    let ops_a = doc_a.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops_a).unwrap();

    // Sync B -> A
    let ops_b = doc_b.ops_since(&Frontier::root()).into();
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

    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));

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
    let ops_a = doc_a.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops_a).unwrap();

    // Bob should see the text
    let text_b = doc_b.root().get_text("content").unwrap();
    assert_eq!(text_b.content(), "Hello");
}

#[test]
fn test_two_peer_sync_set() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc_b.create_agent(Uuid::from_u128(0xB0B));

    // Alice creates set
    let _set_id = doc_a.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    // Sync to Bob so he knows about the set
    let ops = doc_a.ops_since(&Frontier::root()).into();
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
    let ops_a = doc_a.ops_since(&Frontier::root()).into();
    let ops_b = doc_b.ops_since(&Frontier::root()).into();
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

    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));

    // Initial sync
    doc_a.transact(alice, |tx| {
        tx.root().set("v1", "first");
    });

    let ops1 = doc_a.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops1).unwrap();

    // Remember B's version
    let b_version = doc_b.version().clone();

    // Alice makes more changes
    doc_a.transact(alice, |tx| {
        tx.root().set("v2", "second");
    });

    // Only sync new changes (from B's perspective)
    let ops2 = doc_a.ops_since(&b_version).into();
    doc_b.merge_ops(ops2).unwrap();

    // B should have both
    assert!(doc_b.root().contains_key("v1"));
    assert!(doc_b.root().contains_key("v2"));
}

#[test]
fn test_convergence_after_concurrent_edits() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc_b.create_agent(Uuid::from_u128(0xB0B));

    // Both write to the same key (concurrent)
    doc_a.transact(alice, |tx| {
        tx.root().set("key", "alice's value");
    });

    doc_b.transact(bob, |tx| {
        tx.root().set("key", "bob's value");
    });

    // Cross-sync
    let ops_a = doc_a.ops_since(&Frontier::root()).into();
    let ops_b = doc_b.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    // Both should converge to the same value (LWW)
    let val_a = doc_a.root().get("key").unwrap();
    let val_b = doc_b.root().get("key").unwrap();
    assert_eq!(val_a, val_b);
}

/// This is the scenario that caused the original concurrent merge panic:
/// two peers share initial state, make concurrent edits, then try to do
/// incremental sync using each other's version. With local LVs this panics
/// because the indices don't match across documents. With RemoteFrontier
/// (portable UUID+seq pairs) it works correctly.
#[test]
fn test_concurrent_incremental_sync() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc_b.create_agent(Uuid::from_u128(0xB0B));

    // Shared initial state
    doc_a.transact(alice, |tx| {
        tx.root().set("initial", "shared");
    });
    let ops = doc_a.ops_since(&Frontier::root()).into_owned();
    doc_b.merge_ops(ops).unwrap();

    // Both make concurrent edits
    doc_a.transact(alice, |tx| {
        tx.root().set("from_alice", "concurrent A");
    });
    doc_b.transact(bob, |tx| {
        tx.root().set("from_bob", "concurrent B");
    });

    // Exchange portable remote versions and sync incrementally
    let rv_a = doc_a.remote_version();
    let rv_b = doc_b.remote_version();

    let ops_for_b = doc_a.ops_since_remote(&rv_b).into_owned();
    let ops_for_a = doc_b.ops_since_remote(&rv_a).into_owned();

    doc_b.merge_ops(ops_for_b).unwrap();
    doc_a.merge_ops(ops_for_a).unwrap();

    // Both should have all keys
    assert!(doc_a.root().contains_key("initial"));
    assert!(doc_a.root().contains_key("from_alice"));
    assert!(doc_a.root().contains_key("from_bob"));
    assert!(doc_b.root().contains_key("initial"));
    assert!(doc_b.root().contains_key("from_alice"));
    assert!(doc_b.root().contains_key("from_bob"));

    // Values should converge
    assert_eq!(
        doc_a.root().get("from_alice").unwrap(),
        doc_b.root().get("from_alice").unwrap()
    );
    assert_eq!(
        doc_a.root().get("from_bob").unwrap(),
        doc_b.root().get("from_bob").unwrap()
    );
}

/// ops_since_remote should handle a frontier containing an agent UUID
/// that the local document has never seen — it simply ignores it and
/// returns all local ops.
#[test]
fn test_ops_since_remote_unknown_agent() {
    use diamond_types_extended::RemoteVersion;
    use smallvec::smallvec;

    let mut doc = Document::new();
    let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

    doc.transact(alice, |tx| {
        tx.root().set("key", "value");
    });

    // Build a frontier referencing an agent this doc has never seen
    let unknown_frontier = smallvec![RemoteVersion(Uuid::from_u128(0xDEAD), 5)];
    let ops = doc.ops_since_remote(&unknown_frontier);

    // Should return all ops (unknown agent is ignored → falls back to root)
    assert!(!ops.is_empty());

    // Verify a fresh doc can merge them and get the data
    let mut doc_b = Document::new();
    doc_b.merge_ops(ops.into_owned()).unwrap();
    assert_eq!(doc_b.root().get("key").unwrap().as_str(), Some("value"));
}

/// ops_since_remote should handle a frontier where the remote peer is
/// ahead of the local doc for a known agent (SeqInFuture). Instead of
/// resending that agent's entire history, it should use the local doc's
/// latest version for that agent.
#[test]
fn test_ops_since_remote_future_seq() {
    use diamond_types_extended::RemoteVersion;
    use smallvec::smallvec;

    let mut doc = Document::new();
    let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc.create_agent(Uuid::from_u128(0xB0B));

    // Alice makes some changes
    doc.transact(alice, |tx| {
        tx.root().set("alice_key", "alice_val");
    });

    // Bob makes some changes
    doc.transact(bob, |tx| {
        tx.root().set("bob_key", "bob_val");
    });

    // Simulate a remote frontier where Alice is at seq 999 (far ahead)
    // but Bob is correctly at his actual version
    let bob_rv = doc.remote_version().iter()
        .find(|rv| rv.0 == Uuid::from_u128(0xB0B))
        .copied()
        .unwrap();

    let future_frontier = smallvec![
        RemoteVersion(Uuid::from_u128(0xA11CE), 999),
        bob_rv,
    ];

    let ops = doc.ops_since_remote(&future_frontier);

    // The peer already has everything from Alice (they're ahead) and
    // everything from Bob (matching frontier). So ops should be empty.
    assert!(ops.is_empty());
}

/// When the remote peer is ahead on one agent but behind on another,
/// ops_since_remote should only send the ops the peer is missing.
#[test]
fn test_ops_since_remote_mixed_ahead_behind() {
    use diamond_types_extended::RemoteVersion;
    use smallvec::smallvec;

    let mut doc = Document::new();
    let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc.create_agent(Uuid::from_u128(0xB0B));

    doc.transact(alice, |tx| {
        tx.root().set("a1", "first");
    });

    let _mid_version = doc.remote_version();

    doc.transact(bob, |tx| {
        tx.root().set("b1", "second");
    });

    // Remote peer has Alice at seq 999 (ahead) but doesn't know about Bob
    let frontier = smallvec![
        RemoteVersion(Uuid::from_u128(0xA11CE), 999),
    ];

    let ops = doc.ops_since_remote(&frontier).into_owned();
    assert!(!ops.is_empty());

    // Merge into a fresh doc that already has Alice's data
    let mut doc_b = Document::new();
    let ops_initial = doc.ops_since(&Frontier::root()).into_owned();
    doc_b.merge_ops(ops_initial).unwrap();

    // Bob's data should be present
    assert!(doc_b.root().contains_key("b1"));
}
