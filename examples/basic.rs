//! Basic usage example for Facet
//!
//! This example demonstrates:
//! - Creating a document
//! - Using transactions for mutations
//! - Working with maps, text, and sets
//! - Reading values
//!
//! Run with: cargo run --example basic

use facet::Document;

fn main() {
    // Create a new document
    let mut doc = Document::new();

    // Create an agent (represents a user/session)
    let alice = doc.get_or_create_agent("alice");

    println!("=== Basic Map Operations ===");

    // Set primitive values in the root map
    doc.transact(alice, |tx| {
        tx.root().set("name", "Alice's Document");
        tx.root().set("version", 1i64);
        tx.root().set("published", false);
    });

    // Read values back
    println!("Name: {}", doc.root().get("name").unwrap().as_str().unwrap());
    println!("Version: {}", doc.root().get("version").unwrap().as_int().unwrap());
    println!("Published: {}", doc.root().get("published").unwrap().as_bool().unwrap());

    println!("\n=== Nested Maps ===");

    // Create nested structure
    doc.transact(alice, |tx| {
        tx.root().create_map("metadata");
    });

    doc.transact(alice, |tx| {
        if let Some(mut meta) = tx.get_map_mut(&["metadata"]) {
            meta.set("author", "Alice");
            meta.set("year", 2025i64);
        }
    });

    // Read nested values
    let meta = doc.root().get_map("metadata").unwrap();
    println!("Author: {}", meta.get("author").unwrap().as_str().unwrap());
    println!("Year: {}", meta.get("year").unwrap().as_int().unwrap());

    println!("\n=== Text CRDT ===");

    // Create a text CRDT
    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("content")
    });

    // Insert text
    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(0, "Hello, ");
        text.push("world!");
    });

    println!("Content: {}", doc.root().get_text("content").unwrap().content());

    // Edit text
    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.replace(7..12, "Rust"); // Replace "world" with "Rust"
    });

    println!("After edit: {}", doc.root().get_text("content").unwrap().content());

    println!("\n=== Set CRDT ===");

    // Create a set
    let set_id = doc.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    // Add items
    doc.transact(alice, |tx| {
        let mut tags = tx.set_by_id(set_id).unwrap();
        tags.add_str("rust");
        tags.add_str("crdt");
        tags.add_str("collaborative");
        tags.add_int(2025);
    });

    // Check contents
    let tags = doc.root().get_set("tags").unwrap();
    println!("Tags count: {}", tags.len());
    println!("Has 'rust': {}", tags.contains_str("rust"));
    println!("Has 'python': {}", tags.contains_str("python"));

    // Remove an item
    doc.transact(alice, |tx| {
        let mut tags = tx.set_by_id(set_id).unwrap();
        tags.remove_str("collaborative");
    });

    println!("Tags after removal: {}", doc.root().get_set("tags").unwrap().len());

    println!("\n=== Iteration ===");

    // Iterate over map keys
    let root = doc.root();
    println!("Root keys:");
    for key in root.keys() {
        println!("  - {}", key);
    }

    println!("\n=== Done! ===");
}
