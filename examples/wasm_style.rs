//! WASM/FFI-style usage with DocumentWriter (no closures needed)
//!
//! This example shows how FFI/WASM consumers can use the flat, closure-free
//! DocumentWriter API instead of `transact()`. Each method call is a single
//! operation — no batching, no closures.
//!
//! Run with: cargo run --example wasm_style

use diamond_types_extended::{Document, Frontier, Uuid};

fn main() {
    let mut doc = Document::new();
    let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

    println!("=== DocumentWriter: Closure-Free Mutations ===\n");

    // Create a writer — binds agent to doc, no closures needed
    {
        let mut w = doc.writer(alice);

        // Root-level shortcuts
        w.root_set("title", "My Document");
        w.root_set("version", 1i64);
        w.root_set("published", false);

        // Create nested structures
        w.root_create_map("metadata");
        w.root_create_text("content");
        w.root_create_set("tags");

        // Path-based map operations (return bool for success)
        assert!(w.set(&["metadata"], "author", "Alice"));
        assert!(w.set(&["metadata"], "year", 2025i64));

        // Bad paths return false instead of silently failing
        assert!(!w.set(&["nonexistent"], "key", "value"));

        // Path-based text operations
        assert!(w.text_insert(&["content"], 0, "Hello, "));
        assert!(w.text_push(&["content"], "world!"));

        // Path-based set operations
        assert!(w.set_add(&["tags"], "rust"));
        assert!(w.set_add(&["tags"], "crdt"));
        assert!(w.set_add(&["tags"], "wasm"));

        // create_* on paths return Option<CrdtId>
        let nested_text = w.create_text(&["metadata"], "notes");
        assert!(nested_text.is_some());
        assert!(w.create_map(&["nonexistent"], "nope").is_none());
    }
    // Writer is dropped, doc is free again

    println!("=== Path-Based Reads ===\n");

    // Convenience reads — no intermediate refs needed
    println!("title:   {}", doc.get_str(&[], "title").unwrap());
    println!("version: {}", doc.get_int(&[], "version").unwrap());
    println!("author:  {}", doc.get_str(&["metadata"], "author").unwrap());
    println!("year:    {}", doc.get_int(&["metadata"], "year").unwrap());
    println!("content: {}", doc.text_content(&["content"]).unwrap());

    // Map keys (owned, no borrow issues)
    println!("\nRoot keys: {:?}", doc.map_keys(&[]).unwrap());
    println!("Meta keys: {:?}", doc.map_keys(&["metadata"]).unwrap());

    // Also available on MapRef directly
    let root = doc.root();
    println!("Root keys (owned): {:?}", root.keys_owned());
    println!("Root entries: {:?}", root.entries_owned());

    println!("\n=== Sync with ops_since_owned ===\n");

    // ops_since_owned() saves the .into() at every call site
    let mut doc_b = Document::new();
    let ops = doc.ops_since_owned(&Frontier::root());
    doc_b.merge_ops(ops).unwrap();

    println!("Synced! doc_b title: {}", doc_b.get_str(&[], "title").unwrap());
    println!("Synced! doc_b content: {}", doc_b.text_content(&["content"]).unwrap());

    println!("\n=== Done! ===");
}
