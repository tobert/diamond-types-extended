//! Integration tests for Set CRDT

use facet::Document;

#[test]
fn test_set_creation() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let set_id = doc.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    let set = doc.root().get_set("tags").unwrap();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    assert_eq!(set.id(), set_id);
}

#[test]
fn test_set_add() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let set_id = doc.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    doc.transact(alice, |tx| {
        let mut set = tx.set_by_id(set_id).unwrap();
        set.add_str("rust");
        set.add_str("crdt");
        set.add_int(42);
    });

    let set = doc.root().get_set("tags").unwrap();
    assert!(!set.is_empty());
    assert_eq!(set.len(), 3);
    assert!(set.contains_str("rust"));
    assert!(set.contains_str("crdt"));
    assert!(set.contains_int(42));
    assert!(!set.contains_str("missing"));
}

#[test]
fn test_set_remove() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let set_id = doc.transact(alice, |tx| {
        tx.root().create_set("tags")
    });

    doc.transact(alice, |tx| {
        let mut set = tx.set_by_id(set_id).unwrap();
        set.add_str("a");
        set.add_str("b");
        set.add_str("c");
    });

    assert_eq!(doc.root().get_set("tags").unwrap().len(), 3);

    doc.transact(alice, |tx| {
        let mut set = tx.set_by_id(set_id).unwrap();
        set.remove_str("b");
    });

    let set = doc.root().get_set("tags").unwrap();
    assert_eq!(set.len(), 2);
    assert!(set.contains_str("a"));
    assert!(!set.contains_str("b"));
    assert!(set.contains_str("c"));
}

#[test]
fn test_set_iteration() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let set_id = doc.transact(alice, |tx| {
        tx.root().create_set("nums")
    });

    doc.transact(alice, |tx| {
        let mut set = tx.set_by_id(set_id).unwrap();
        set.add_int(1);
        set.add_int(2);
        set.add_int(3);
    });

    let set = doc.root().get_set("nums").unwrap();
    let values = set.to_vec();
    assert_eq!(values.len(), 3);
}

#[test]
fn test_set_mixed_types() {
    let mut doc = Document::new();
    let alice = doc.get_or_create_agent("alice");

    let set_id = doc.transact(alice, |tx| {
        tx.root().create_set("mixed")
    });

    doc.transact(alice, |tx| {
        let mut set = tx.set_by_id(set_id).unwrap();
        set.add_str("hello");
        set.add_int(42);
        set.add(true);  // Using the generic add
    });

    let set = doc.root().get_set("mixed").unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains_str("hello"));
    assert!(set.contains_int(42));
}
