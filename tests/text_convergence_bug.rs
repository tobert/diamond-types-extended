//! Minimal reproduction of text CRDT convergence bug
//!
//! After full mesh sync, documents have different text content.

use diamond_types_extended::{Document, SerializedOpsOwned};

fn sync_pair(docs: &mut [Document], a: usize, b: usize) {
    let ops_a: SerializedOpsOwned = docs[a].ops_since(&[]).into();
    let ops_b: SerializedOpsOwned = docs[b].ops_since(&[]).into();
    docs[b].merge_ops(ops_a).unwrap();
    docs[a].merge_ops(ops_b).unwrap();
}

#[test]
fn minimal_three_way_text_divergence() {
    // Simplest case: 3 peers, concurrent appends
    let mut docs: Vec<Document> = (0..3).map(|_| Document::new()).collect();
    let names = ["alice", "bob", "carol"];
    let agents: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, name)| docs[i].get_or_create_agent(name))
        .collect();

    // Setup: all have "Hello"
    docs[0].transact(agents[0], |tx| {
        tx.root().create_text("doc");
    });
    docs[0].transact(agents[0], |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Hello");
    });

    let initial_ops: SerializedOpsOwned = docs[0].ops_since(&[]).into();
    docs[1].merge_ops(initial_ops.clone()).unwrap();
    docs[2].merge_ops(initial_ops).unwrap();

    // Verify initial state
    for (i, doc) in docs.iter().enumerate() {
        let text = doc.root().get_text("doc").unwrap().content();
        assert_eq!(text, "Hello", "Doc {} should have 'Hello'", names[i]);
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
            names[i]
        );
    }
}

#[test]
fn two_peer_concurrent_append() {
    // Even simpler: 2 peers
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Setup
    doc_a.transact(alice, |tx| {
        tx.root().create_text("doc");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Hello");
    });

    let ops: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Concurrent appends
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" from Alice");
    });
    doc_b.transact(bob, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push(" from Bob");
    });

    // Cross sync
    let ops_a: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    let ops_b: SerializedOpsOwned = doc_b.ops_since(&[]).into();
    doc_b.merge_ops(ops_a).unwrap();
    doc_a.merge_ops(ops_b).unwrap();

    let text_a = doc_a.root().get_text("doc").unwrap().content();
    let text_b = doc_b.root().get_text("doc").unwrap().content();

    assert_eq!(text_a, text_b, "Documents should converge");
}
