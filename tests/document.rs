//! Integration tests for the Document API

use facet::Document;

#[test]
fn test_document_creation() {
    let doc = Document::new();
    assert!(doc.is_empty());
    assert!(doc.root().is_empty());
}

#[test]
fn test_agent_management() {
    let mut doc = Document::new();

    let alice = doc.get_or_create_agent("alice");
    let alice2 = doc.get_or_create_agent("alice");
    let bob = doc.get_or_create_agent("bob");

    assert_eq!(alice, alice2); // Same name = same ID
    assert_ne!(alice, bob);    // Different names = different IDs
}

#[test]
fn test_basic_map_operations() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    // Set various value types
    doc.transact(alice, |tx| {
        tx.root().set("string", "hello");
        tx.root().set("int", 42i64);
        tx.root().set("bool", true);
        tx.root().set("nil", ());
    });

    // Verify values
    let root = doc.root();
    assert!(root.contains_key("string"));
    assert!(root.contains_key("int"));
    assert!(root.contains_key("bool"));
    assert!(root.contains_key("nil"));

    assert_eq!(root.get("string").unwrap().as_str(), Some("hello"));
    assert_eq!(root.get("int").unwrap().as_int(), Some(42));
    assert_eq!(root.get("bool").unwrap().as_bool(), Some(true));
    assert!(root.get("nil").unwrap().is_nil());
}

#[test]
fn test_map_overwrite() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    doc.transact(alice, |tx| {
        tx.root().set("key", "first");
    });
    assert_eq!(doc.root().get("key").unwrap().as_str(), Some("first"));

    doc.transact(alice, |tx| {
        tx.root().set("key", "second");
    });
    assert_eq!(doc.root().get("key").unwrap().as_str(), Some("second"));
}

#[test]
fn test_nested_maps() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    // Create nested structure
    doc.transact(alice, |tx| {
        tx.root().create_map("user");
    });

    doc.transact(alice, |tx| {
        if let Some(mut user) = tx.get_map_mut(&["user"]) {
            user.set("name", "Alice");
            user.set("age", 30i64);
            user.create_map("settings");
        }
    });

    doc.transact(alice, |tx| {
        if let Some(mut settings) = tx.get_map_mut(&["user", "settings"]) {
            settings.set("theme", "dark");
        }
    });

    // Verify nested values
    let user = doc.root().get_map("user").unwrap();
    assert_eq!(user.get("name").unwrap().as_str(), Some("Alice"));
    assert_eq!(user.get("age").unwrap().as_int(), Some(30));

    let settings = user.get_map("settings").unwrap();
    assert_eq!(settings.get("theme").unwrap().as_str(), Some("dark"));
}

#[test]
fn test_map_keys_iteration() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    doc.transact(alice, |tx| {
        tx.root().set("c", 3);
        tx.root().set("a", 1);
        tx.root().set("b", 2);
    });

    let root = doc.root();
    let keys: Vec<_> = root.keys().collect();
    assert_eq!(keys.len(), 3);
    // Keys should be iterable (order may vary)
    assert!(keys.contains(&"a"));
    assert!(keys.contains(&"b"));
    assert!(keys.contains(&"c"));
}
