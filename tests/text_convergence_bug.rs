//! Minimal reproduction of text CRDT convergence bug
//!
//! After full mesh sync, documents have different text content.

use diamond_types_extended::{Document, Frontier, SerializedOpsOwned, Uuid};

fn sync_pair(docs: &mut [Document], a: usize, b: usize) {
    let ops_a: SerializedOpsOwned = docs[a].ops_since(&Frontier::root()).into();
    let ops_b: SerializedOpsOwned = docs[b].ops_since(&Frontier::root()).into();
    docs[b].merge_ops(ops_a).unwrap();
    docs[a].merge_ops(ops_b).unwrap();
}

#[test]
fn minimal_three_way_text_divergence() {
    // Simplest case: 3 peers, concurrent appends
    let mut docs: Vec<Document> = (0..3).map(|_| Document::new()).collect();
    let uuids = [Uuid::from_u128(0xA11CE), Uuid::from_u128(0xB0B), Uuid::from_u128(0xCA201)];
    let agents: Vec<_> = uuids
        .iter()
        .enumerate()
        .map(|(i, uuid)| docs[i].create_agent(*uuid))
        .collect();

    // Setup: all have "Hello"
    docs[0].transact(agents[0], |tx| {
        tx.root().create_text("doc");
    });
    docs[0].transact(agents[0], |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Hello");
    });

    let initial_ops: SerializedOpsOwned = docs[0].ops_since(&Frontier::root()).into();
    docs[1].merge_ops(initial_ops.clone()).unwrap();
    docs[2].merge_ops(initial_ops).unwrap();

    // Verify initial state
    for (i, doc) in docs.iter().enumerate() {
        let text = doc.root().get_text("doc").unwrap().content();
        assert_eq!(text, "Hello", "Doc {} should have 'Hello'", uuids[i]);
    }

    // Concurrent appends (no overlap - all at end)
    docs[0].transact(agents[0], |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" A");
    });
    docs[1].transact(agents[1], |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" B");
    });
    docs[2].transact(agents[2], |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" C");
    });

    // Full mesh sync
    for i in 0..3 {
        for j in (i + 1)..3 {
            sync_pair(&mut docs, i, j);
        }
    }

    // Check convergence
    let reference = docs[0].root().get_text("doc").unwrap().content();
    for (i, doc) in docs.iter().enumerate().skip(1) {
        let text = doc.root().get_text("doc").unwrap().content();
        assert_eq!(
            reference, text,
            "Doc {} should match reference",
            uuids[i]
        );
    }
}

#[test]
fn two_peer_concurrent_append() {
    // Even simpler: 2 peers
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    let bob = doc_b.create_agent(Uuid::from_u128(0xB0B));

    // Setup
    doc_a.transact(alice, |tx| {
        tx.root().create_text("doc");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Hello");
    });

    let ops: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops).unwrap();

    // Concurrent appends
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" from Alice");
    });
    doc_b.transact(bob, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" from Bob");
    });

    // Cross sync
    let ops_a: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();
    let ops_b: SerializedOpsOwned = doc_b.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    let text_a = doc_a.root().get_text("doc").unwrap().content();
    let text_b = doc_b.root().get_text("doc").unwrap().content();

    assert_eq!(text_a, text_b, "Documents should converge");
}

/// Test that text deletes crossing agent-assignment boundaries serialize correctly.
///
/// When a peer merges ops from multiple agents, the internal RLE may merge adjacent
/// operations into spans that cross agent boundaries. When ops_since serializes these,
/// it must split them back correctly — especially for backward (delete) operations
/// where the loc.span adjustment differs from forward (insert) operations.
#[test]
fn text_delete_across_agent_boundary_sync() {
    let uuid_a = Uuid::from_u128(0xA);
    let uuid_b = Uuid::from_u128(0xB);
    let uuid_c = Uuid::from_u128(0xC);

    // Create initial document with text
    let mut doc_a = Document::new();
    let agent_a = doc_a.create_agent(uuid_a);
    doc_a.transact(agent_a, |tx| {
        tx.root().create_text("t");
    });
    doc_a.transact(agent_a, |tx| {
        tx.get_text_mut(&["t"]).unwrap().insert(0, "abcdefghij");
    });

    // Sync to B and C
    let initial: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();
    let mut doc_b = Document::new();
    doc_b.create_agent(uuid_a);
    let agent_b = doc_b.create_agent(uuid_b);
    doc_b.merge_ops(initial.clone()).unwrap();

    let mut doc_c = Document::new();
    doc_c.create_agent(uuid_a);
    doc_c.create_agent(uuid_b);
    let agent_c = doc_c.create_agent(uuid_c);
    doc_c.merge_ops(initial).unwrap();

    // B deletes characters 2-5 ("cdef")
    doc_b.transact(agent_b, |tx| {
        tx.get_text_mut(&["t"]).unwrap().delete(2..6);
    });

    // C deletes characters 6-9 ("ghij")
    doc_c.transact(agent_c, |tx| {
        tx.get_text_mut(&["t"]).unwrap().delete(6..10);
    });

    // Merge B→C (C now has ops from agents A, B, and C)
    let ops_b: SerializedOpsOwned = doc_b.ops_since(&Frontier::root()).into();
    doc_c.merge_ops(ops_b).unwrap();

    // Now C serializes everything to a fresh doc D (simulates crash recovery)
    let full_ops: SerializedOpsOwned = doc_c.ops_since(&Frontier::root()).into();
    let mut doc_d = Document::new();
    doc_d.create_agent(uuid_a);
    doc_d.create_agent(uuid_b);
    doc_d.create_agent(uuid_c);
    doc_d.merge_ops(full_ops).unwrap();

    // D should have same content as C
    let text_c = doc_c.root().get_text("t").unwrap().content();
    let text_d = doc_d.root().get_text("t").unwrap().content();
    assert_eq!(text_c, text_d,
        "Text should converge after crash-recovery-style sync: C={:?} D={:?}",
        text_c, text_d);

    // Also verify the expected content
    assert_eq!(text_c, "ab",
        "After deleting cdef and ghij from abcdefghij, should get 'ab'");
}

/// Test that backward deletes spanning agent boundaries serialize correctly.
///
/// This triggers the RLE merge condition: two agents both delete at the same
/// position (because the first delete shifts content). The ops get RLE-merged
/// into a single ListOpMetrics entry that spans two agents. When serialized,
/// the chunk-splitting code must correctly adjust loc.span for backward ops.
#[test]
fn text_backward_delete_cross_agent_rle_merge() {
    let uuid_a = Uuid::from_u128(0xA);
    let uuid_b = Uuid::from_u128(0xB);

    // Create text "abcdefgh" on peer A
    let mut doc_a = Document::new();
    let agent_a = doc_a.create_agent(uuid_a);
    doc_a.transact(agent_a, |tx| {
        tx.root().create_text("t");
    });
    doc_a.transact(agent_a, |tx| {
        tx.get_text_mut(&["t"]).unwrap().insert(0, "abcdefgh");
    });

    // Sync to B
    let initial: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();
    let mut doc_b = Document::new();
    doc_b.create_agent(uuid_a);
    let agent_b = doc_b.create_agent(uuid_b);
    doc_b.merge_ops(initial).unwrap();

    // A deletes "d" at position 3 — text becomes "abcefgh"
    doc_a.transact(agent_a, |tx| {
        tx.get_text_mut(&["t"]).unwrap().delete(3..4);
    });

    // B deletes "d" at position 3 too — but B still has "abcdefgh"
    // So B deletes "d" as well, but this is a concurrent delete.
    // Actually, let's make them delete DIFFERENT characters at the same position
    // after merging A's ops, so the deletes are sequential in the LV space.

    // Instead: merge A→B first, then have B delete at position 3 (now "e")
    let ops_a: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();
    doc_b.merge_ops(ops_a).unwrap();
    // Now B has "abcefgh" — delete at position 3 removes "e" → "abcfgh"
    doc_b.transact(agent_b, |tx| {
        tx.get_text_mut(&["t"]).unwrap().delete(3..4);
    });

    // Now B has ops from both agents, with sequential LVs.
    // A's delete (agent A) and B's delete (agent B) both deleted at position 3.
    // If they get RLE-merged, we have a cross-agent delete span.

    // Serialize B's full state to a fresh doc C (crash-recovery style)
    let full_ops: SerializedOpsOwned = doc_b.ops_since(&Frontier::root()).into();
    let mut doc_c = Document::new();
    doc_c.create_agent(uuid_a);
    doc_c.create_agent(uuid_b);
    doc_c.merge_ops(full_ops).unwrap();

    let text_b = doc_b.root().get_text("t").unwrap().content();
    let text_c = doc_c.root().get_text("t").unwrap().content();
    assert_eq!(text_b, "abcfgh", "B should have 'abcfgh'");
    assert_eq!(text_b, text_c,
        "C should match B after crash-recovery sync: B={:?} C={:?}", text_b, text_c);
}

/// Stress: multiple rounds of concurrent inserts and deletes, then crash-recovery sync.
#[test]
fn text_insert_delete_crash_recovery_convergence() {
    let uuids: Vec<Uuid> = (0..4).map(|i| Uuid::from_u128(i + 1)).collect();

    let mut docs: Vec<Document> = (0..4).map(|_| Document::new()).collect();
    let agents: Vec<_> = (0..4).map(|i| {
        for (_j, u) in uuids.iter().enumerate() {
            docs[i].create_agent(*u);
        }
        i as u32 // agent IDs are sequential since created in same order
    }).collect();

    // Peer 0 creates text
    docs[0].transact(agents[0], |tx| {
        tx.root().create_text("t");
    });
    docs[0].transact(agents[0], |tx| {
        tx.get_text_mut(&["t"]).unwrap().insert(0, "the quick brown fox jumps");
    });

    // Sync initial state to all
    let init: SerializedOpsOwned = docs[0].ops_since(&Frontier::root()).into();
    for doc in docs.iter_mut().skip(1) {
        doc.merge_ops(init.clone()).unwrap();
    }

    // Round 1: concurrent edits
    docs[0].transact(agents[0], |tx| {
        tx.get_text_mut(&["t"]).unwrap().insert(10, "INSERTED");
    });
    docs[1].transact(agents[1], |tx| {
        tx.get_text_mut(&["t"]).unwrap().delete(4..9); // delete "quick"
    });
    docs[2].transact(agents[2], |tx| {
        tx.get_text_mut(&["t"]).unwrap().push(" over the lazy dog");
    });
    docs[3].transact(agents[3], |tx| {
        tx.get_text_mut(&["t"]).unwrap().delete(0..4); // delete "the "
    });

    // Full mesh sync
    for _ in 0..2 {
        for i in 0..4 {
            for j in (i + 1)..4 {
                let ops_i: SerializedOpsOwned = docs[i].ops_since(&Frontier::root()).into();
                let ops_j: SerializedOpsOwned = docs[j].ops_since(&Frontier::root()).into();
                docs[j].merge_ops(ops_i).unwrap();
                docs[i].merge_ops(ops_j).unwrap();
            }
        }
    }

    // Verify convergence
    let reference = docs[0].root().get_text("t").unwrap().content();
    for (i, doc) in docs.iter().enumerate().skip(1) {
        let text = doc.root().get_text("t").unwrap().content();
        assert_eq!(reference, text, "Peer {} text should match peer 0", i);
    }

    // Simulate crash recovery: each peer serializes full state to a fresh doc
    for i in 0..4 {
        let full_ops: SerializedOpsOwned = docs[i].ops_since(&Frontier::root()).into();
        let mut fresh = Document::new();
        for u in &uuids {
            fresh.create_agent(*u);
        }
        fresh.merge_ops(full_ops).unwrap();
        let text = fresh.root().get_text("t").unwrap().content();
        assert_eq!(reference, text,
            "Peer {} crash-recovery text should match: expected {:?} got {:?}",
            i, reference, text);
    }
}
