//! Mutable reference types for Facet CRDTs.
//!
//! These types provide write access to CRDT data within a Transaction.

use std::ops::Range;

use smartstring::alias::String as SmartString;

use crate::value::{CrdtId, Value};
use crate::list::operation::TextOperation;
use crate::{AgentId, CRDTKind, CreateValue, OpLog, Primitive, ROOT_CRDT_ID, LV};

/// Mutable reference to a Map CRDT.
///
/// Obtained through `Transaction::root()` or `Transaction::get_map_mut()`.
pub struct MapMut<'a> {
    oplog: &'a mut OpLog,
    agent: AgentId,
    crdt_id: LV,
}

impl<'a> MapMut<'a> {
    pub(crate) fn new(oplog: &'a mut OpLog, agent: AgentId, crdt_id: LV) -> Self {
        Self { oplog, agent, crdt_id }
    }

    /// Get the CRDT ID for this map.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Check if this is the root map.
    pub fn is_root(&self) -> bool {
        self.crdt_id == ROOT_CRDT_ID
    }

    // ============ Write operations ============

    /// Set a key to a value.
    ///
    /// The value can be any type that implements `Into<Value>`:
    /// - Primitives: `bool`, `i64`, `i32`, `String`, `&str`
    /// - Nil: `()`
    ///
    /// For nested CRDTs, use `create_map`, `create_text`, `create_set`, or `create_register`.
    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        let value: Value = value.into();
        let create_value = match value {
            Value::Nil => CreateValue::Primitive(Primitive::Nil),
            Value::Bool(b) => CreateValue::Primitive(Primitive::Bool(b)),
            Value::Int(n) => CreateValue::Primitive(Primitive::I64(n)),
            Value::Str(s) => CreateValue::Primitive(Primitive::Str(s.into())),
            // For CRDT values, we can't "set" them - use create_* methods instead
            _ => return,
        };
        self.oplog.local_map_set(self.agent, self.crdt_id, key, create_value);
    }

    /// Delete a key from the map.
    ///
    /// Note: In CRDT semantics, this doesn't truly delete - it just sets to nil.
    /// The key will still appear in iterations until compaction.
    pub fn delete(&mut self, key: &str) {
        self.oplog.local_map_set(
            self.agent,
            self.crdt_id,
            key,
            CreateValue::Primitive(Primitive::Nil),
        );
    }

    /// Create a nested Map at the given key.
    ///
    /// Returns the CRDT ID of the new map, which can be used with
    /// `Transaction::map_by_id()` to get a mutable reference.
    pub fn create_map(&mut self, key: &str) -> CrdtId {
        let lv = self.oplog.local_map_set(
            self.agent,
            self.crdt_id,
            key,
            CreateValue::NewCRDT(CRDTKind::Map),
        );
        CrdtId(lv)
    }

    /// Create a nested Text CRDT at the given key.
    ///
    /// Returns the CRDT ID of the new text.
    pub fn create_text(&mut self, key: &str) -> CrdtId {
        let lv = self.oplog.local_map_set(
            self.agent,
            self.crdt_id,
            key,
            CreateValue::NewCRDT(CRDTKind::Text),
        );
        CrdtId(lv)
    }

    /// Create a nested Set CRDT at the given key.
    ///
    /// Returns the CRDT ID of the new set.
    pub fn create_set(&mut self, key: &str) -> CrdtId {
        let lv = self.oplog.local_map_set(
            self.agent,
            self.crdt_id,
            key,
            CreateValue::NewCRDT(CRDTKind::Set),
        );
        CrdtId(lv)
    }

    /// Create a nested Register CRDT at the given key.
    ///
    /// Returns the CRDT ID of the new register.
    pub fn create_register(&mut self, key: &str) -> CrdtId {
        let lv = self.oplog.local_map_set(
            self.agent,
            self.crdt_id,
            key,
            CreateValue::NewCRDT(CRDTKind::Register),
        );
        CrdtId(lv)
    }
}

/// Mutable reference to a Text CRDT.
///
/// Obtained through `Transaction::get_text_mut()` or `Transaction::text_by_id()`.
pub struct TextMut<'a> {
    oplog: &'a mut OpLog,
    agent: AgentId,
    crdt_id: LV,
}

impl<'a> TextMut<'a> {
    pub(crate) fn new(oplog: &'a mut OpLog, agent: AgentId, crdt_id: LV) -> Self {
        Self { oplog, agent, crdt_id }
    }

    /// Get the CRDT ID for this text.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Insert text at a position.
    ///
    /// Position is in Unicode characters (not bytes).
    pub fn insert(&mut self, pos: usize, content: &str) {
        let op = TextOperation::new_insert(pos, content);
        self.oplog.local_text_op(self.agent, self.crdt_id, op);
    }

    /// Delete a range of text.
    ///
    /// Range is in Unicode characters (not bytes).
    pub fn delete(&mut self, range: Range<usize>) {
        if !range.is_empty() {
            let op = TextOperation::new_delete(range);
            self.oplog.local_text_op(self.agent, self.crdt_id, op);
        }
    }

    /// Replace text in a range.
    ///
    /// This is equivalent to delete + insert, but may be optimized.
    pub fn replace(&mut self, range: Range<usize>, content: &str) {
        if !range.is_empty() {
            let del_op = TextOperation::new_delete(range.clone());
            self.oplog.local_text_op(self.agent, self.crdt_id, del_op);
        }
        if !content.is_empty() {
            let ins_op = TextOperation::new_insert(range.start, content);
            self.oplog.local_text_op(self.agent, self.crdt_id, ins_op);
        }
    }

    /// Append text to the end.
    pub fn push(&mut self, content: &str) {
        let len = self.oplog.checkout_text(self.crdt_id).len_chars();
        let op = TextOperation::new_insert(len, content);
        self.oplog.local_text_op(self.agent, self.crdt_id, op);
    }

    /// Clear all text.
    pub fn clear(&mut self) {
        let len = self.oplog.checkout_text(self.crdt_id).len_chars();
        if len > 0 {
            let op = TextOperation::new_delete(0..len);
            self.oplog.local_text_op(self.agent, self.crdt_id, op);
        }
    }
}

/// Mutable reference to a Set CRDT.
///
/// Obtained through `Transaction::get_set_mut()` or `Transaction::set_by_id()`.
pub struct SetMut<'a> {
    oplog: &'a mut OpLog,
    agent: AgentId,
    crdt_id: LV,
}

impl<'a> SetMut<'a> {
    pub(crate) fn new(oplog: &'a mut OpLog, agent: AgentId, crdt_id: LV) -> Self {
        Self { oplog, agent, crdt_id }
    }

    /// Get the CRDT ID for this set.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Add a value to the set.
    ///
    /// Only primitive values can be added to sets in v0.1.
    pub fn add(&mut self, value: impl Into<Value>) {
        let value: Value = value.into();
        let primitive: Primitive = value.into();
        self.oplog.local_set_add(self.agent, self.crdt_id, primitive);
    }

    /// Add a string to the set.
    pub fn add_str(&mut self, s: &str) {
        self.oplog.local_set_add(self.agent, self.crdt_id, Primitive::Str(s.into()));
    }

    /// Add an integer to the set.
    pub fn add_int(&mut self, n: i64) {
        self.oplog.local_set_add(self.agent, self.crdt_id, Primitive::I64(n));
    }

    /// Remove a value from the set.
    ///
    /// With OR-Set semantics, this removes all observed instances of the value.
    pub fn remove(&mut self, value: impl Into<Value>) {
        let value: Value = value.into();
        let primitive: Primitive = value.into();
        self.oplog.local_set_remove(self.agent, self.crdt_id, primitive);
    }

    /// Remove a string from the set.
    pub fn remove_str(&mut self, s: &str) {
        self.oplog.local_set_remove(self.agent, self.crdt_id, Primitive::Str(s.into()));
    }

    /// Remove an integer from the set.
    pub fn remove_int(&mut self, n: i64) {
        self.oplog.local_set_remove(self.agent, self.crdt_id, Primitive::I64(n));
    }
}

/// Mutable reference to a Register CRDT.
///
/// Note: In v0.1, standalone registers are primarily accessed through map keys.
/// Each map key is effectively an LWW register. Use `MapMut::set()` to update
/// register values.
///
/// Obtained through `Transaction::get_register_mut()` or `Transaction::register_by_id()`.
pub struct RegisterMut<'a> {
    #[allow(dead_code)]
    oplog: &'a mut OpLog,
    #[allow(dead_code)]
    agent: AgentId,
    crdt_id: LV,
}

impl<'a> RegisterMut<'a> {
    pub(crate) fn new(oplog: &'a mut OpLog, agent: AgentId, crdt_id: LV) -> Self {
        Self { oplog, agent, crdt_id }
    }

    /// Get the CRDT ID for this register.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    // Note: Standalone register mutation is not yet implemented.
    // Use map keys as registers via MapMut::set().
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Document;

    #[test]
    fn test_map_mut_set() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        doc.transact(alice, |tx| {
            tx.root().set("string", "hello");
            tx.root().set("int", 42);
            tx.root().set("bool", true);
            tx.root().set("nil", ());
        });

        assert_eq!(doc.root().get("string").unwrap().as_str(), Some("hello"));
        assert_eq!(doc.root().get("int").unwrap().as_int(), Some(42));
        assert_eq!(doc.root().get("bool").unwrap().as_bool(), Some(true));
        assert!(doc.root().get("nil").unwrap().is_nil());
    }

    #[test]
    fn test_map_mut_create_nested() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        let text_id = doc.transact(alice, |tx| {
            tx.root().create_text("content")
        });

        doc.transact(alice, |tx| {
            if let Some(mut text) = tx.text_by_id(text_id) {
                text.insert(0, "Hello, world!");
            }
        });

        let text = doc.root().get_text("content").unwrap();
        assert_eq!(text.content(), "Hello, world!");
    }

    #[test]
    fn test_text_mut() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        let text_id = doc.transact(alice, |tx| {
            tx.root().create_text("doc")
        });

        doc.transact(alice, |tx| {
            let mut text = tx.text_by_id(text_id).unwrap();
            text.insert(0, "Hello");
            text.insert(5, ", world!");
        });

        assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello, world!");

        doc.transact(alice, |tx| {
            let mut text = tx.text_by_id(text_id).unwrap();
            text.delete(5..13); // Remove ", world!"
        });

        assert_eq!(doc.root().get_text("doc").unwrap().content(), "Hello");
    }

    #[test]
    fn test_set_mut() {
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
        assert!(set.contains_str("rust"));
        assert!(set.contains_str("crdt"));
        assert!(set.contains_int(42));
        assert!(!set.contains_str("missing"));

        doc.transact(alice, |tx| {
            let mut set = tx.set_by_id(set_id).unwrap();
            set.remove_str("crdt");
        });

        let set = doc.root().get_set("tags").unwrap();
        assert!(set.contains_str("rust"));
        assert!(!set.contains_str("crdt"));
    }
}
