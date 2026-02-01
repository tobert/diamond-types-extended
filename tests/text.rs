//! Integration tests for Text CRDT

use facet::Document;

#[test]
fn test_text_creation() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("content")
    });

    // Text exists but is empty
    let text = doc.root().get_text("content").unwrap();
    assert!(text.is_empty());
    assert_eq!(text.len(), 0);
    assert_eq!(text.id(), text_id);
}

#[test]
fn test_text_insert() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("doc")
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(0, "Hello");
    });

    assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello");

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(5, ", world!");
    });

    assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello, world!");
}

#[test]
fn test_text_delete() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("doc")
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(0, "Hello, world!");
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.delete(5..13); // Delete ", world!"
    });

    assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello");
}

#[test]
fn test_text_replace() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("doc")
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(0, "Hello, world!");
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.replace(7..12, "Rust"); // Replace "world" with "Rust"
    });

    assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello, Rust!");
}

#[test]
fn test_text_push() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("doc")
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.push("Hello");
        text.push(", ");
        text.push("world!");
    });

    assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello, world!");
}

#[test]
fn test_text_clear() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("doc")
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(0, "Hello, world!");
    });

    assert!(!doc.root().get_text("doc").unwrap().is_empty());

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.clear();
    });

    assert!(doc.root().get_text("doc").unwrap().is_empty());
}

#[test]
fn test_text_unicode() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let text_id = doc.transact(alice, |tx| {
        tx.root().create_text("doc")
    });

    doc.transact(alice, |tx| {
        let mut text = tx.text_by_id(text_id).unwrap();
        text.insert(0, "Hello, 世界!");
    });

    let text = doc.root().get_text("doc").unwrap();
    assert_eq!(text.content(), "Hello, 世界!");
    // Length is in Unicode characters
    assert_eq!(text.len(), 10);
}
