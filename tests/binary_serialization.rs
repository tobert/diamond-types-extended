//! Tests that SerializedOpsOwned round-trips through binary serializers.
//!
//! `#[serde(untagged)]` requires self-describing formats (JSON, YAML).
//! After removing it, compact binary formats like postcard work.

use diamond_types_extended::{Document, Frontier, MaterializedValue, SerializedOpsOwned};

#[test]
fn postcard_round_trip() {
    let mut doc_a = Document::new();
    let alice = doc_a.get_or_create_agent("alice");

    doc_a.transact(alice, |tx| {
        let mut root = tx.root();
        root.set("name", "Alice");
        root.set("age", 30);
        root.set("active", true);
        root.set("nothing", ());
    });

    let text_id = doc_a.transact(alice, |tx| {
        tx.root().create_text("bio")
    });
    doc_a.transact(alice, |tx| {
        tx.text_by_id(text_id).unwrap().insert(0, "Hello, world!");
    });

    let set_id = doc_a.transact(alice, |tx| {
        tx.root().create_set("tags")
    });
    doc_a.transact(alice, |tx| {
        let mut set = tx.set_by_id(set_id).unwrap();
        set.add_str("rust");
        set.add_str("crdt");
        set.add_int(42);
    });

    doc_a.transact(alice, |tx| {
        let nested_id = tx.root().create_map("prefs");
        tx.map_by_id(nested_id).set("theme", "dark");
    });

    let ops: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();

    // Round-trip through postcard (binary, non-self-describing)
    let bytes = postcard::to_allocvec(&ops).expect("postcard serialize");
    let ops2: SerializedOpsOwned =
        postcard::from_bytes(&bytes).expect("postcard deserialize");

    // Merge into a fresh document and verify checkout matches
    let mut doc_b = Document::new();
    doc_b.merge_ops(ops2).expect("merge round-tripped ops");

    assert_eq!(doc_a.checkout(), doc_b.checkout());
}

#[test]
fn postcard_incremental_sync() {
    let mut doc_a = Document::new();
    let mut doc_b = Document::new();
    let alice = doc_a.get_or_create_agent("alice");

    // First batch
    doc_a.transact(alice, |tx| {
        tx.root().set("v", 1);
    });

    let ops1: SerializedOpsOwned = doc_a.ops_since(&Frontier::root()).into();
    let bytes1 = postcard::to_allocvec(&ops1).unwrap();
    let ops1_rt: SerializedOpsOwned = postcard::from_bytes(&bytes1).unwrap();
    doc_b.merge_ops(ops1_rt).unwrap();

    let frontier_after_v1 = doc_b.version();

    // Second batch — incremental
    doc_a.transact(alice, |tx| {
        tx.root().set("v", 2);
    });
    let text_id = doc_a.transact(alice, |tx| {
        tx.root().create_text("log")
    });
    doc_a.transact(alice, |tx| {
        tx.text_by_id(text_id).unwrap().insert(0, "updated");
    });

    let ops2: SerializedOpsOwned = doc_a.ops_since(&frontier_after_v1).into();
    let bytes2 = postcard::to_allocvec(&ops2).unwrap();
    let ops2_rt: SerializedOpsOwned = postcard::from_bytes(&bytes2).unwrap();
    doc_b.merge_ops(ops2_rt).unwrap();

    assert_eq!(doc_a.checkout(), doc_b.checkout());
}

#[test]
fn postcard_is_compact() {
    let mut doc = Document::new();
    let agent = doc.get_or_create_agent("a");

    doc.transact(agent, |tx| {
        tx.root().set("x", 1);
    });

    let ops: SerializedOpsOwned = doc.ops_since(&Frontier::root()).into();
    let json_bytes = serde_json::to_vec(&ops).unwrap();
    let postcard_bytes = postcard::to_allocvec(&ops).unwrap();

    // postcard should be meaningfully smaller than JSON
    assert!(
        postcard_bytes.len() < json_bytes.len(),
        "postcard ({} bytes) should be smaller than JSON ({} bytes)",
        postcard_bytes.len(),
        json_bytes.len(),
    );
}

#[test]
fn to_json_produces_natural_output() {
    let mut doc = Document::new();
    let agent = doc.get_or_create_agent("a");

    doc.transact(agent, |tx| {
        let mut root = tx.root();
        root.set("name", "Alice");
        root.set("age", 30);
        root.set("active", true);
        root.set("nothing", ());
    });
    let text_id = doc.transact(agent, |tx| {
        tx.root().create_text("bio")
    });
    doc.transact(agent, |tx| {
        tx.text_by_id(text_id).unwrap().insert(0, "hi");
    });

    let json: serde_json::Value = serde_json::from_str(&doc.to_json()).unwrap();
    assert_eq!(json["name"], "Alice");
    assert_eq!(json["age"], 30);
    assert_eq!(json["active"], true);
    assert!(json["nothing"].is_null());
    assert_eq!(json["bio"], "hi");
}

#[test]
fn materialized_value_str_vs_text_distinguished() {
    // After removing #[serde(untagged)], Str and Text are distinguishable
    let str_val = MaterializedValue::Str("hello".into());
    let text_val = MaterializedValue::Text("hello".into());

    let str_json = serde_json::to_string(&str_val).unwrap();
    let text_json = serde_json::to_string(&text_val).unwrap();

    // They should serialize differently (externally tagged)
    assert_ne!(str_json, text_json);

    // And round-trip correctly
    let str_rt: MaterializedValue = serde_json::from_str(&str_json).unwrap();
    let text_rt: MaterializedValue = serde_json::from_str(&text_json).unwrap();
    assert_eq!(str_rt, str_val);
    assert_eq!(text_rt, text_val);
}
