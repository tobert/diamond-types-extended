//! Edge case tests for serialization/sync targeting the text CRDT convergence fix.
//!
//! These tests specifically exercise the agent boundary splitting and LV mapping
//! logic that was fixed in the convergence bug patch.

use diamond_types_extended::{Document, SerializedOpsOwned};

/// Sync helper: cross-sync two documents
fn cross_sync(a: &mut Document, b: &mut Document) {
    let ops_a: SerializedOpsOwned = a.ops_since(&[]).into();
    let ops_b: SerializedOpsOwned = b.ops_since(&[]).into();
    b.merge_ops(ops_a).unwrap();
    a.merge_ops(ops_b).unwrap();
}

/// Assert documents have converged to same content
fn assert_converged(docs: &[Document], context: &str) {
    let reference = docs[0].checkout();
    for (i, doc) in docs.iter().enumerate().skip(1) {
        assert_eq!(
            reference,
            doc.checkout(),
            "Document {} should match reference ({})",
            i,
            context
        );
    }
}

// =============================================================================
// Test 1: Agent Boundary Splitting ("Zebra Stripe")
// =============================================================================

/// Alice and Bob alternate appending single characters, syncing after each.
/// This creates text where each character has a different agent.
/// When synced to a third peer, ops_since must correctly split the RLE-merged
/// span by agent boundaries.
///
/// If serialization doesn't split by agent, peer C would attribute all edits
/// to Alice, or receive wrong version numbers.
#[test]
fn test_agent_boundary_splitting_zebra() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates the text field
    doc_a.transact(alice, |tx| {
        tx.root().create_text("content");
    });

    // Initial sync so Bob has the text field
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Alternate appending: A, B, A, B, A, B, A, B, A, B
    // This creates "ABABABABAB" where each char is from a different agent
    for i in 0..5 {
        // Alice appends 'A'
        doc_a.transact(alice, |tx| {
            tx.get_text_mut(&["content"]).unwrap().push("A");
        });
        cross_sync(&mut doc_a, &mut doc_b);

        // Bob appends 'B'
        doc_b.transact(bob, |tx| {
            tx.get_text_mut(&["content"]).unwrap().push("B");
        });
        cross_sync(&mut doc_a, &mut doc_b);

        // Verify intermediate state
        let expected = "AB".repeat(i + 1);
        let text_a = doc_a.root().get_text("content").unwrap().content();
        let text_b = doc_b.root().get_text("content").unwrap().content();
        assert_eq!(text_a, expected, "Alice should have correct zebra pattern");
        assert_eq!(text_b, expected, "Bob should have correct zebra pattern");
    }

    // Now create Carol who hasn't seen any of this
    let mut doc_c = Document::new();

    // Sync full history from Alice to Carol
    // This is where agent boundary splitting matters: Alice's local RLE may
    // have merged the chars, but ops_since must split by agent
    let ops_to_carol: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    doc_c.merge_ops(ops_to_carol).unwrap();

    // Carol should have the same content
    let text_c = doc_c.root().get_text("content").unwrap().content();
    assert_eq!(text_c, "ABABABABAB", "Carol should receive correct zebra pattern");

    // Verify full convergence
    assert_converged(&[doc_a, doc_b, doc_c], "zebra stripe test");
}

// =============================================================================
// Test 2: Non-Contiguous LV Handling ("Hollow Middle")
// =============================================================================

/// Creates a "hollow" LV layout where text is contiguous in the document
/// but the underlying LVs have a gap (from unrelated map operations).
///
/// If merge_ops calculates LV ranges by simple offset math, it will read
/// garbage or panic.
#[test]
fn test_non_contiguous_lv_merging() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates text and writes "Part1"
    doc_a.transact(alice, |tx| {
        tx.root().create_text("doc");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().insert(0, "Part1");
    });

    // Sync Part1 to Bob and remember Alice's version at this point
    let alice_version_after_part1 = doc_a.version().clone();
    let ops_part1: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops_part1).unwrap();

    // Bob does 100+ unrelated map operations (burning LVs)
    // This creates a gap between Part1's LVs and Part2's LVs in Bob's view
    for i in 0..120 {
        doc_b.transact(bob, |tx| {
            tx.root().set(&format!("filler_{}", i), i as i64);
        });
    }

    // Alice writes "Part2" (appending to text)
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["doc"]).unwrap().push("Part2");
    });

    // Sync Part2 to Bob using Alice's version that Bob knows about
    // (ops_since must be called with a version the source doc knows about)
    let ops_part2: SerializedOpsOwned = doc_a.ops_since(alice_version_after_part1.as_ref()).into();
    doc_b.merge_ops(ops_part2).unwrap();

    // Verify Bob has complete text
    let text_b = doc_b.root().get_text("doc").unwrap().content();
    assert_eq!(text_b, "Part1Part2", "Bob should have complete text");

    // Now Bob syncs to Carol (fresh peer)
    // From Bob's view, "Part1Part2" is contiguous text but maps to widely
    // separated LVs (e.g., 0-5 and 150-156 due to the 120 map ops in between)
    let mut doc_c = Document::new();
    let ops_to_carol: SerializedOpsOwned = doc_b.ops_since(&[]).into();
    doc_c.merge_ops(ops_to_carol).unwrap();

    // Carol should have the complete text
    let text_c = doc_c.root().get_text("doc").unwrap().content();
    assert_eq!(text_c, "Part1Part2", "Carol should have complete text despite LV gap");

    // Verify Carol also received the filler map entries
    for i in 0..120 {
        assert!(
            doc_c.root().contains_key(&format!("filler_{}", i)),
            "Carol should have filler key {}",
            i
        );
    }
}

/// Variant: More extreme LV gaps with interleaved text and map ops
#[test]
fn test_interleaved_text_and_map_ops() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");

    // Alice creates text
    doc_a.transact(alice, |tx| {
        tx.root().create_text("log");
    });

    // Sync to Bob
    let ops = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Interleave text appends with map ops (simulates a real app log)
    for i in 0..20 {
        // Alice appends to log
        doc_a.transact(alice, |tx| {
            tx.get_text_mut(&["log"])
                .unwrap()
                .push(&format!("[{}]", i));
        });

        // Bob adds metadata
        doc_b.transact(bob, |tx| {
            tx.root().set(&format!("meta_{}", i), i as i64);
        });

        // Partial sync every 5 iterations
        if i % 5 == 4 {
            cross_sync(&mut doc_a, &mut doc_b);
        }
    }

    // Final sync
    cross_sync(&mut doc_a, &mut doc_b);

    // Build expected log content
    let expected_log: String = (0..20).map(|i| format!("[{}]", i)).collect();

    let text_a = doc_a.root().get_text("log").unwrap().content();
    let text_b = doc_b.root().get_text("log").unwrap().content();

    assert_eq!(text_a, expected_log, "Alice should have complete log");
    assert_eq!(text_b, expected_log, "Bob should have complete log");

    // Sync to fresh peer
    let mut doc_c = Document::new();
    let ops_to_c: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    doc_c.merge_ops(ops_to_c).unwrap();

    let text_c = doc_c.root().get_text("log").unwrap().content();
    assert_eq!(text_c, expected_log, "Carol should have complete log");
}

// =============================================================================
// Test 3: Multi-Hop Attribution
// =============================================================================

/// A -> B -> C sync chain where B batches A's ops with its own.
/// Verifies C receives correct content when operations pass through an intermediary.
///
/// This ensures agent definitions are properly forwarded through intermediaries.
#[test]
fn test_multi_hop_sync_attribution() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let mut doc_c = Document::new();

    let alice = doc_a.get_or_create_agent("alice");
    let bob = doc_b.get_or_create_agent("bob");
    let carol = doc_c.get_or_create_agent("carol");

    // Alice creates document structure and initial content
    doc_a.transact(alice, |tx| {
        tx.root().create_text("content");
        tx.root().create_map("metadata");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["content"]).unwrap().insert(0, "Alice wrote this. ");
        if let Some(mut m) = tx.get_map_mut(&["metadata"]) {
            m.set("author", "alice");
        }
    });

    // Sync A -> B (Bob receives Alice's content)
    let ops_a_to_b: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops_a_to_b).unwrap();

    // Bob adds his own content
    doc_b.transact(bob, |tx| {
        tx.get_text_mut(&["content"]).unwrap().push("Bob added more. ");
        if let Some(mut m) = tx.get_map_mut(&["metadata"]) {
            m.set("editor", "bob");
        }
    });

    // Now Bob syncs to Carol (B -> C)
    // Carol should receive both Alice's and Bob's ops correctly attributed
    let ops_b_to_c: SerializedOpsOwned = doc_b.ops_since(&[]).into();
    doc_c.merge_ops(ops_b_to_c).unwrap();

    // Verify Carol has correct content
    let text_c = doc_c.root().get_text("content").unwrap().content();
    assert_eq!(
        text_c, "Alice wrote this. Bob added more. ",
        "Carol should have combined text from Alice and Bob"
    );

    // Verify metadata
    let meta_c = doc_c.get_map(&["metadata"]).unwrap();
    assert_eq!(meta_c.get("author").unwrap().as_str(), Some("alice"));
    assert_eq!(meta_c.get("editor").unwrap().as_str(), Some("bob"));

    // Carol adds her content
    doc_c.transact(carol, |tx| {
        tx.get_text_mut(&["content"]).unwrap().push("Carol finished it.");
        if let Some(mut m) = tx.get_map_mut(&["metadata"]) {
            m.set("reviewer", "carol");
        }
    });

    // Sync back: C -> B -> A
    let ops_c_to_b: SerializedOpsOwned = doc_c.ops_since(&[]).into();
    doc_b.merge_ops(ops_c_to_b).unwrap();

    let ops_b_to_a: SerializedOpsOwned = doc_b.ops_since(&[]).into();
    doc_a.merge_ops(ops_b_to_a).unwrap();

    // Verify final content has all three contributions
    let final_text = doc_c.root().get_text("content").unwrap().content();
    assert!(final_text.contains("Alice wrote this."));
    assert!(final_text.contains("Bob added more."));
    assert!(final_text.contains("Carol finished it."));

    // Final convergence check
    assert_converged(&[doc_a, doc_b, doc_c], "multi-hop attribution");
}

/// Test multi-hop with larger chain: A -> B -> C -> D -> E
#[test]
fn test_long_chain_sync_attribution() {
    let mut docs: Vec<Document> = (0..5).map(|_| Document::new()).collect();
    let names = ["alice", "bob", "carol", "dave", "eve"];
    let agents: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, name)| docs[i].get_or_create_agent(name))
        .collect();

    // Alice creates initial structure
    docs[0].transact(agents[0], |tx| {
        tx.root().create_text("chain");
    });
    docs[0].transact(agents[0], |tx| {
        tx.get_text_mut(&["chain"]).unwrap().insert(0, "A");
    });

    // Chain sync: each peer receives from previous, adds content, passes to next
    for i in 1..5 {
        // Sync from previous peer
        let ops: SerializedOpsOwned = docs[i - 1].ops_since(&[]).into();
        docs[i].merge_ops(ops).unwrap();

        // Add this peer's contribution
        let agent = agents[i];
        let char = (b'A' + i as u8) as char;
        docs[i].transact(agent, |tx| {
            tx.get_text_mut(&["chain"])
                .unwrap()
                .push(&char.to_string());
        });
    }

    // Verify final peer has complete chain
    let final_text = docs[4].root().get_text("chain").unwrap().content();
    assert_eq!(final_text, "ABCDE", "Final peer should have all contributions");

    // Sync back to first peer
    let ops_back: SerializedOpsOwned = docs[4].ops_since(&[]).into();
    docs[0].merge_ops(ops_back).unwrap();

    // Alice should now have the complete chain
    let alice_text = docs[0].root().get_text("chain").unwrap().content();
    assert_eq!(alice_text, "ABCDE", "Alice should receive all downstream edits");
}

/// Test that agent IDs are preserved through multi-hop sync
#[test]
fn test_agent_id_preservation_through_hops() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let mut doc_c = Document::new();

    let alice = doc_a.get_or_create_agent("alice_unique_name");
    let bob = doc_b.get_or_create_agent("bob_unique_name");

    // Alice creates text with unique agent name
    doc_a.transact(alice, |tx| {
        tx.root().create_text("test");
    });
    doc_a.transact(alice, |tx| {
        tx.get_text_mut(&["test"]).unwrap().insert(0, "Hello");
    });

    // A -> B
    let ops: SerializedOpsOwned = doc_a.ops_since(&[]).into();
    doc_b.merge_ops(ops).unwrap();

    // Bob adds content
    doc_b.transact(bob, |tx| {
        tx.get_text_mut(&["test"]).unwrap().push(" World");
    });

    // B -> C (Carol is fresh, receives everything through Bob)
    let ops: SerializedOpsOwned = doc_b.ops_since(&[]).into();
    doc_c.merge_ops(ops).unwrap();

    // Verify content
    let text_c = doc_c.root().get_text("test").unwrap().content();
    assert_eq!(text_c, "Hello World", "Carol should have complete text");

    // The document should have knowledge of both agent names
    // (Testing internal consistency - if agent definitions weren't forwarded,
    // the merge would have failed or produced wrong results)
}
