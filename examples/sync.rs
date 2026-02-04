//! Two-peer synchronization example
//!
//! This example demonstrates:
//! - Creating two documents (simulating two peers)
//! - Making concurrent edits
//! - Syncing changes between peers
//! - Observing convergence
//!
//! Run with: cargo run --example sync

use diamond_types_extended::Document;

fn main() {
    println!("=== Diamond Types Extended Sync Example ===\n");

    // Create two documents representing two peers
    let mut doc_alice = Document::new();
    let mut doc_bob = Document::new();

    let alice = doc_alice.get_or_create_agent("alice");
    let bob = doc_bob.get_or_create_agent("bob");

    // Alice creates the initial structure
    println!("Alice creates initial document...");
    doc_alice.transact(alice, |tx| {
        tx.root().set("title", "Shared Document");
        tx.root().create_text("content");
        tx.root().create_set("contributors");
    });

    doc_alice.transact(alice, |tx| {
        if let Some(mut text) = tx.get_text_mut(&["content"]) {
            text.insert(0, "Initial content from Alice.");
        }
        if let Some(mut set) = tx.get_set_mut(&["contributors"]) {
            set.add_str("alice");
        }
    });

    println!("Alice's document:");
    println!("  Title: {}", doc_alice.root().get("title").unwrap().as_str().unwrap());
    println!("  Content: {}", doc_alice.root().get_text("content").unwrap().content());

    // Sync Alice -> Bob (initial sync)
    println!("\nSyncing Alice -> Bob...");
    let ops_alice = doc_alice.ops_since(&[]).into();
    doc_bob.merge_ops(ops_alice).unwrap();

    println!("Bob's document after sync:");
    println!("  Title: {}", doc_bob.root().get("title").unwrap().as_str().unwrap());
    println!("  Content: {}", doc_bob.root().get_text("content").unwrap().content());

    // Now both make concurrent changes
    println!("\n=== Concurrent Edits ===\n");

    // Alice edits
    println!("Alice edits the title...");
    doc_alice.transact(alice, |tx| {
        tx.root().set("title", "Alice's Title");
    });

    // Bob edits the same key
    println!("Bob edits the title concurrently...");
    doc_bob.transact(bob, |tx| {
        tx.root().set("title", "Bob's Title");
    });

    // Both edit the text
    println!("Alice appends to text...");
    doc_alice.transact(alice, |tx| {
        if let Some(mut text) = tx.get_text_mut(&["content"]) {
            text.push(" (Alice was here)");
        }
    });

    println!("Bob prepends to text...");
    doc_bob.transact(bob, |tx| {
        if let Some(mut text) = tx.get_text_mut(&["content"]) {
            text.insert(0, "[Bob] ");
        }
    });

    // Both add to the set
    doc_alice.transact(alice, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["contributors"]) {
            set.add_str("alice_v2");
        }
    });
    doc_bob.transact(bob, |tx| {
        if let Some(mut set) = tx.get_set_mut(&["contributors"]) {
            set.add_str("bob");
        }
    });

    println!("\nBefore sync:");
    println!("  Alice's title: {}", doc_alice.root().get("title").unwrap().as_str().unwrap());
    println!("  Bob's title: {}", doc_bob.root().get("title").unwrap().as_str().unwrap());

    // Cross-sync
    println!("\n=== Cross-Sync ===\n");

    let ops_alice = doc_alice.ops_since(&[]).into();
    let ops_bob = doc_bob.ops_since(&[]).into();

    doc_bob.merge_ops(ops_alice).unwrap();
    doc_alice.merge_ops(ops_bob).unwrap();

    println!("After sync:");
    println!("  Alice's title: {}", doc_alice.root().get("title").unwrap().as_str().unwrap());
    println!("  Bob's title: {}", doc_bob.root().get("title").unwrap().as_str().unwrap());

    // Verify convergence
    let alice_title = doc_alice.root().get("title").unwrap();
    let bob_title = doc_bob.root().get("title").unwrap();
    println!("\n  Titles converged: {}", alice_title == bob_title);

    // Text should contain both edits
    let alice_text = doc_alice.root().get_text("content").unwrap().content();
    let bob_text = doc_bob.root().get_text("content").unwrap().content();
    println!("\n  Alice's text: {}", alice_text);
    println!("  Bob's text: {}", bob_text);
    println!("  Texts converged: {}", alice_text == bob_text);

    // Set should have all contributors
    let alice_contributors = doc_alice.root().get_set("contributors").unwrap();
    let bob_contributors = doc_bob.root().get_set("contributors").unwrap();
    println!("\n  Contributors count (Alice): {}", alice_contributors.len());
    println!("  Contributors count (Bob): {}", bob_contributors.len());
    println!("  Both have 'alice': {}",
        alice_contributors.contains_str("alice") && bob_contributors.contains_str("alice"));
    println!("  Both have 'bob': {}",
        alice_contributors.contains_str("bob") && bob_contributors.contains_str("bob"));

    // Show conflict information
    println!("\n=== Conflict Detection ===\n");
    let conflicted = doc_alice.root().get_conflicted("title").unwrap();
    if conflicted.has_conflicts() {
        println!("Title has conflicts!");
        println!("  Winner: {:?}", conflicted.value);
        println!("  Losers: {:?}", conflicted.conflicts);
    } else {
        println!("No conflicts on title (sequential writes)");
    }

    println!("\n=== Done! ===");
}
