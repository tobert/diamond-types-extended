//! Read-only reference types for Facet CRDTs.
//!
//! These types provide read-only access to CRDT data without requiring
//! an agent or transaction.

use std::collections::BTreeSet;

use smartstring::alias::String as SmartString;

use crate::branch::btree_range_for_crdt;
use crate::value::{Conflicted, CrdtId, Value};
use crate::{CRDTKind, OpLog, Primitive, RegisterValue, ROOT_CRDT_ID, LV};

/// Read-only reference to a Map CRDT.
///
/// Maps are key-value containers where each key maps to an LWW Register.
/// Values can be primitives or references to nested CRDTs.
pub struct MapRef<'a> {
    oplog: &'a OpLog,
    crdt_id: LV,
}

impl<'a> MapRef<'a> {
    pub(crate) fn new(oplog: &'a OpLog, crdt_id: LV) -> Self {
        Self { oplog, crdt_id }
    }

    /// Get the CRDT ID for this map.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Check if this is the root map.
    pub fn is_root(&self) -> bool {
        self.crdt_id == ROOT_CRDT_ID
    }

    /// Get a value by key.
    ///
    /// Returns the "winning" value according to LWW semantics.
    /// Use `get_conflicted` if you need to see concurrent conflicts.
    pub fn get(&self, key: &str) -> Option<Value> {
        let info = self.oplog.map_keys.get(&(self.crdt_id, key.into()))?;
        let state = self.oplog.get_state_for_register(info);
        Some(state.value.into())
    }

    /// Get a value by key with conflict information.
    ///
    /// If there are concurrent writes to this key, the conflicts are included.
    pub fn get_conflicted(&self, key: &str) -> Option<Conflicted<Value>> {
        let info = self.oplog.map_keys.get(&(self.crdt_id, key.into()))?;
        let state = self.oplog.get_state_for_register(info);
        Some(Conflicted {
            value: state.value.into(),
            conflicts: state.conflicts_with.into_iter().map(|rv| rv.into()).collect(),
        })
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.oplog.map_keys.contains_key(&(self.crdt_id, key.into()))
    }

    /// Get all keys in this map.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        btree_range_for_crdt(&self.oplog.map_keys, self.crdt_id)
            .map(|((_, key), _)| key.as_str())
    }

    /// Get the number of keys.
    pub fn len(&self) -> usize {
        btree_range_for_crdt(&self.oplog.map_keys, self.crdt_id).count()
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a nested map by key.
    pub fn get_map(&self, key: &str) -> Option<MapRef<'a>> {
        let value = self.get(key)?;
        match value {
            Value::Map(id) => Some(MapRef::new(self.oplog, id.0)),
            _ => None,
        }
    }

    /// Get a nested text CRDT by key.
    pub fn get_text(&self, key: &str) -> Option<TextRef<'a>> {
        let value = self.get(key)?;
        match value {
            Value::Text(id) => Some(TextRef::new(self.oplog, id.0)),
            _ => None,
        }
    }

    /// Get a nested set CRDT by key.
    pub fn get_set(&self, key: &str) -> Option<SetRef<'a>> {
        let value = self.get(key)?;
        match value {
            Value::Set(id) => Some(SetRef::new(self.oplog, id.0)),
            _ => None,
        }
    }

    /// Get a nested register CRDT by key.
    pub fn get_register(&self, key: &str) -> Option<RegisterRef<'a>> {
        let value = self.get(key)?;
        match value {
            Value::Register(id) => Some(RegisterRef::new(self.oplog, id.0)),
            _ => None,
        }
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&str, Value)> + 'a {
        btree_range_for_crdt(&self.oplog.map_keys, self.crdt_id)
            .map(|((_, key), info)| {
                let state = self.oplog.get_state_for_register(info);
                (key.as_str(), state.value.into())
            })
    }
}

/// Read-only reference to a Text CRDT.
///
/// Text is a sequence CRDT optimized for collaborative text editing.
/// For large documents, prefer `slice()` or `chars()` over `content()` to avoid
/// allocating the entire string.
pub struct TextRef<'a> {
    oplog: &'a OpLog,
    crdt_id: LV,
}

impl<'a> TextRef<'a> {
    pub(crate) fn new(oplog: &'a OpLog, crdt_id: LV) -> Self {
        Self { oplog, crdt_id }
    }

    /// Get the CRDT ID for this text.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Get the full text content as a String.
    ///
    /// For large documents, consider using `slice()` or `chars()` instead
    /// to avoid allocating the entire string.
    pub fn content(&self) -> String {
        self.oplog.checkout_text(self.crdt_id).to_string()
    }

    /// Get a slice of the text as a String.
    ///
    /// Range is in Unicode characters (not bytes). This is more efficient than
    /// `content()` for extracting portions of large documents.
    ///
    /// # Example
    ///
    /// ```
    /// use facet::Document;
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.get_or_create_agent("alice");
    ///
    /// doc.transact(alice, |tx| {
    ///     let id = tx.root().create_text("content");
    ///     tx.text_by_id(id).unwrap().insert(0, "Hello, world!");
    /// });
    ///
    /// let text = doc.root().get_text("content").unwrap();
    /// assert_eq!(text.slice(0..5), "Hello");
    /// assert_eq!(text.slice(7..12), "world");
    /// ```
    pub fn slice(&self, range: std::ops::Range<usize>) -> String {
        let rope = self.oplog.checkout_text(self.crdt_id);
        let borrowed = rope.borrow();
        borrowed.slice_chars(range).collect()
    }

    /// Iterate over characters in the text.
    ///
    /// More memory-efficient than `content()` for large documents when you
    /// only need to iterate.
    pub fn chars(&self) -> impl Iterator<Item = char> {
        // Note: We collect into a Vec because the JumpRope iterator borrows
        // the rope, and we can't return a borrowed iterator from a temporary.
        // For truly streaming access on huge documents, use slice() with ranges.
        let rope = self.oplog.checkout_text(self.crdt_id);
        let borrowed = rope.borrow();
        borrowed.chars().collect::<Vec<_>>().into_iter()
    }

    /// Get the length in Unicode characters.
    pub fn len(&self) -> usize {
        self.oplog.checkout_text(self.crdt_id).len_chars()
    }

    /// Check if the text is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Read-only reference to a Set CRDT.
///
/// Sets use OR-Set (Observed-Remove) semantics with add-wins conflict resolution.
pub struct SetRef<'a> {
    oplog: &'a OpLog,
    crdt_id: LV,
}

impl<'a> SetRef<'a> {
    pub(crate) fn new(oplog: &'a OpLog, crdt_id: LV) -> Self {
        Self { oplog, crdt_id }
    }

    /// Get the CRDT ID for this set.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Check if the set contains a value.
    pub fn contains(&self, value: &Value) -> bool {
        let primitive: Primitive = value.clone().into();
        let set_data = self.oplog.checkout_set(self.crdt_id);
        set_data.contains(&primitive)
    }

    /// Check if the set contains a primitive string.
    pub fn contains_str(&self, s: &str) -> bool {
        let primitive = Primitive::Str(s.into());
        let set_data = self.oplog.checkout_set(self.crdt_id);
        set_data.contains(&primitive)
    }

    /// Check if the set contains a primitive integer.
    pub fn contains_int(&self, n: i64) -> bool {
        let primitive = Primitive::I64(n);
        let set_data = self.oplog.checkout_set(self.crdt_id);
        set_data.contains(&primitive)
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.oplog.checkout_set(self.crdt_id).len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over all values in the set.
    pub fn iter(&self) -> impl Iterator<Item = Value> + '_ {
        self.oplog.checkout_set(self.crdt_id)
            .into_iter()
            .map(|p| p.into())
    }

    /// Get all values as a Vec.
    pub fn to_vec(&self) -> Vec<Value> {
        self.iter().collect()
    }
}

/// Read-only reference to a Register CRDT.
///
/// Registers are single-value containers with LWW semantics.
pub struct RegisterRef<'a> {
    oplog: &'a OpLog,
    crdt_id: LV,
}

impl<'a> RegisterRef<'a> {
    pub(crate) fn new(oplog: &'a OpLog, crdt_id: LV) -> Self {
        Self { oplog, crdt_id }
    }

    /// Get the CRDT ID for this register.
    pub fn id(&self) -> CrdtId {
        CrdtId(self.crdt_id)
    }

    /// Get the current value.
    ///
    /// Returns the "winning" value according to LWW semantics.
    pub fn get(&self) -> Option<Value> {
        let state = self.oplog.checkout_register(self.crdt_id);
        Some(state.value.into())
    }

    /// Get the current value with conflict information.
    pub fn get_conflicted(&self) -> Option<Conflicted<Value>> {
        let state = self.oplog.checkout_register(self.crdt_id);
        Some(Conflicted {
            value: state.value.into(),
            conflicts: state.conflicts_with.into_iter().map(|rv| rv.into()).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Document;

    #[test]
    fn test_map_ref_basic() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        doc.transact(alice, |tx| {
            tx.root().set("key", "value");
            tx.root().set("num", 42);
        });

        let root = doc.root();
        assert!(root.contains_key("key"));
        assert!(!root.contains_key("missing"));

        let val = root.get("key").unwrap();
        assert_eq!(val.as_str(), Some("value"));

        let num = root.get("num").unwrap();
        assert_eq!(num.as_int(), Some(42));
    }

    #[test]
    fn test_map_ref_nested() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        doc.transact(alice, |tx| {
            tx.root().create_map("nested");
        });

        doc.transact(alice, |tx| {
            if let Some(mut nested) = tx.get_map_mut(&["nested"]) {
                nested.set("inner", "value");
            }
        });

        let nested = doc.root().get_map("nested").unwrap();
        let inner = nested.get("inner").unwrap();
        assert_eq!(inner.as_str(), Some("value"));
    }

    #[test]
    fn test_text_ref_slice() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        doc.transact(alice, |tx| {
            let id = tx.root().create_text("content");
            tx.text_by_id(id).unwrap().insert(0, "Hello, world!");
        });

        let text = doc.root().get_text("content").unwrap();

        // Full content
        assert_eq!(text.content(), "Hello, world!");

        // Slices
        assert_eq!(text.slice(0..5), "Hello");
        assert_eq!(text.slice(7..12), "world");
        assert_eq!(text.slice(0..0), "");
        assert_eq!(text.slice(5..7), ", ");
    }

    #[test]
    fn test_text_ref_slice_unicode() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        doc.transact(alice, |tx| {
            let id = tx.root().create_text("emoji");
            // Each emoji is 1 unicode character but multiple bytes
            tx.text_by_id(id).unwrap().insert(0, "🎉🎊🎈");
        });

        let text = doc.root().get_text("emoji").unwrap();

        // Length is in unicode characters, not bytes
        assert_eq!(text.len(), 3);

        // Slice by character position
        assert_eq!(text.slice(0..1), "🎉");
        assert_eq!(text.slice(1..2), "🎊");
        assert_eq!(text.slice(2..3), "🎈");
    }

    #[test]
    fn test_text_ref_chars() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

        doc.transact(alice, |tx| {
            let id = tx.root().create_text("content");
            tx.text_by_id(id).unwrap().insert(0, "abc");
        });

        let text = doc.root().get_text("content").unwrap();
        let chars: Vec<char> = text.chars().collect();

        assert_eq!(chars, vec!['a', 'b', 'c']);
    }
}
