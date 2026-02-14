//! Document container and Transaction API.
//!
//! This module provides the main entry point for the Diamond Types Extended API:
//! - `Document`: The unified CRDT container with a Map root
//! - `Transaction`: Batch mutations with captured agent

use std::collections::BTreeMap;
use std::ops::Range;
use uuid::Uuid;

use crate::value::{CrdtId, MaterializedValue, PrimitiveValue, Value, checkout_to_materialized};
use crate::{AgentId, CRDTKind, Frontier, OpLog, ROOT_CRDT_ID, LV, SerializedOpsOwned};
use crate::refs::{MapRef, RegisterRef, SetRef, TextRef};
use crate::muts::{MapMut, SetMut, TextMut};

/// A unified CRDT document container.
///
/// The Document is the main entry point for Diamond Types Extended. It wraps an OpLog and provides:
/// - A Map root for organizing data
/// - Transaction-based mutations (solves Rust borrow issues)
/// - Consistent API across all CRDT types
/// - Clean serialization/replication
///
/// # Conflict Resolution
///
/// Diamond Types Extended uses deterministic conflict resolution that guarantees all peers
/// converge to the same state:
///
/// - **Maps**: Each key is an LWW (Last-Writer-Wins) register. Concurrent writes
///   are resolved by `(lamport_timestamp, agent_id)` ordering - higher timestamp
///   wins, with agent_id as tiebreaker. Use `get_conflicted()` to see losing values.
///
/// - **Sets**: OR-Set (Observed-Remove) with add-wins semantics. If one peer adds
///   a value while another removes it concurrently, the add wins.
///
/// - **Text**: Operations are interleaved based on causal ordering. Concurrent
///   inserts at the same position are ordered deterministically.
///
/// # Transaction Isolation
///
/// Transactions provide a consistent view of the document. All reads within a
/// transaction see the state as of transaction start - they do NOT see writes
/// made earlier in the same transaction. This matches typical CRDT semantics
/// where operations are applied to the log immediately.
///
/// ```
/// use diamond_types_extended::{Document, Uuid};
///
/// let mut doc = Document::new();
/// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
///
/// doc.transact(alice, |tx| {
///     tx.root().set("key", "value");
///     // Note: get() here WILL see "value" because we read from the oplog
///     // which has the operation applied
/// });
/// ```
///
/// # Example
///
/// ```
/// use diamond_types_extended::{Document, Uuid};
///
/// let mut doc = Document::new();
/// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
///
/// // Mutations happen in transactions
/// doc.transact(alice, |tx| {
///     tx.root().set("title", "My Document");
///     tx.root().create_text("content");
/// });
///
/// // Reading is direct
/// let title = doc.root().get("title");
/// ```
pub struct Document {
    pub(crate) oplog: OpLog,
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

impl Document {
    /// Create a new empty document.
    pub fn new() -> Self {
        Self {
            oplog: OpLog::new(),
        }
    }

    /// Create or retrieve an agent ID from a UUID.
    ///
    /// Agents represent participants in collaborative editing.
    /// Each agent should have a unique UUID per editing session.
    pub fn create_agent(&mut self, id: Uuid) -> AgentId {
        self.oplog.cg.get_or_create_agent_id(id)
    }

    /// Get the current version of the document.
    ///
    /// This can be used for:
    /// - Checking if documents are at the same version
    /// - Creating patches since a version with `encode_since`
    pub fn version(&self) -> &Frontier {
        &self.oplog.cg.version
    }

    /// Check if the document is empty (no operations).
    pub fn is_empty(&self) -> bool {
        self.oplog.cg.len() == 0
    }

    // ============ Read-only access ============

    /// Get a read-only reference to the root map.
    pub fn root(&self) -> MapRef<'_> {
        MapRef::new(&self.oplog, ROOT_CRDT_ID)
    }

    /// Get a read-only reference to a map at the given path.
    ///
    /// Path elements are keys in nested maps starting from root.
    /// Returns None if path doesn't exist or isn't a Map.
    pub fn get_map(&self, path: &[&str]) -> Option<MapRef<'_>> {
        let (kind, crdt_id) = self.oplog.try_crdt_at_path(path)?;
        if kind == CRDTKind::Map && crdt_id != LV::MAX {
            Some(MapRef::new(&self.oplog, crdt_id))
        } else {
            None
        }
    }

    /// Get a read-only reference to a text CRDT at the given path.
    /// Returns None if path doesn't exist or isn't a Text.
    pub fn get_text(&self, path: &[&str]) -> Option<TextRef<'_>> {
        let (kind, crdt_id) = self.oplog.try_crdt_at_path(path)?;
        if kind == CRDTKind::Text && crdt_id != LV::MAX {
            Some(TextRef::new(&self.oplog, crdt_id))
        } else {
            None
        }
    }

    /// Get a read-only reference to a set CRDT at the given path.
    /// Returns None if path doesn't exist or isn't a Set.
    pub fn get_set(&self, path: &[&str]) -> Option<SetRef<'_>> {
        let (kind, crdt_id) = self.oplog.try_crdt_at_path(path)?;
        if kind == CRDTKind::Set && crdt_id != LV::MAX {
            Some(SetRef::new(&self.oplog, crdt_id))
        } else {
            None
        }
    }

    /// Get a read-only reference to a text CRDT by its ID.
    pub fn get_text_by_id(&self, id: CrdtId) -> Option<TextRef<'_>> {
        if self.oplog.texts.contains_key(&id.0) {
            Some(TextRef::new(&self.oplog, id.0))
        } else {
            None
        }
    }

    /// Get a read-only reference to a set CRDT by its ID.
    pub fn get_set_by_id(&self, id: CrdtId) -> Option<SetRef<'_>> {
        if self.oplog.sets.contains_key(&id.0) {
            Some(SetRef::new(&self.oplog, id.0))
        } else {
            None
        }
    }

    /// Get a read-only reference to a register CRDT by its ID.
    pub fn get_register_by_id(&self, id: CrdtId) -> Option<RegisterRef<'_>> {
        if self.oplog.registers.contains_key(&id.0) {
            Some(RegisterRef::new(&self.oplog, id.0))
        } else {
            None
        }
    }

    // ============ Mutations via Transaction ============

    /// Execute mutations in a transaction.
    ///
    /// The transaction captures the agent ID, so you don't need to pass it
    /// to each mutation. This also solves Rust's borrow checker issues when
    /// modifying nested CRDTs.
    ///
    /// # Example
    ///
    /// ```
    /// use diamond_types_extended::{Document, Uuid};
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// doc.transact(alice, |tx| {
    ///     tx.root().set("count", 42);
    ///     tx.root().create_map("nested");
    ///
    ///     // Access nested map in same transaction
    ///     if let Some(mut nested) = tx.get_map_mut(&["nested"]) {
    ///         nested.set("inner", "value");
    ///     }
    /// });
    /// ```
    pub fn transact<F, R>(&mut self, agent: AgentId, f: F) -> R
    where
        F: FnOnce(&mut Transaction) -> R,
    {
        let mut tx = Transaction {
            oplog: &mut self.oplog,
            agent,
        };
        f(&mut tx)
    }

    // ============ Serialization / Replication ============

    /// Get operations since a version for serialization.
    ///
    /// Pass `&Frontier::root()` to get all operations (full sync).
    /// Pass `peer.version()` to get only new operations (delta sync).
    ///
    /// Returns `SerializedOps` which borrows from this document. Use `.into()` to
    /// convert to `SerializedOpsOwned` for sending across threads or network.
    ///
    /// # Example
    ///
    /// ```
    /// use diamond_types_extended::{Document, Frontier, Uuid};
    ///
    /// let mut doc_a = Document::new();
    /// let mut doc_b = Document::new();
    /// let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// // Alice makes changes
    /// doc_a.transact(alice, |tx| {
    ///     tx.root().set("key", "value");
    /// });
    ///
    /// // Full sync: get all operations
    /// let all_ops = doc_a.ops_since(&Frontier::root()).into();
    /// doc_b.merge_ops(all_ops).unwrap();
    ///
    /// // Now doc_b has Alice's changes
    /// assert!(doc_b.root().contains_key("key"));
    /// ```
    pub fn ops_since(&self, version: &Frontier) -> crate::SerializedOps<'_> {
        self.oplog.ops_since(version.as_ref())
    }

    /// Merge operations from another peer (owned version).
    ///
    /// Use this when receiving operations over a network or from another thread.
    /// The owned version (`SerializedOpsOwned`) can be sent across thread boundaries.
    ///
    /// Operations are applied causally - if an operation depends on operations
    /// you don't have yet, it will still be stored and applied correctly once
    /// dependencies arrive.
    pub fn merge_ops(&mut self, ops: crate::SerializedOpsOwned) -> Result<(), crate::encoding::parseerror::ParseError> {
        self.oplog.merge_ops_owned(ops).map(|_| ())
    }

    /// Merge operations from another peer (borrowed version).
    ///
    /// Use this when you have a `SerializedOps` reference and don't need to
    /// send it across threads. Slightly more efficient than `merge_ops` as it
    /// avoids cloning strings.
    pub fn merge_ops_borrowed(&mut self, ops: crate::SerializedOps<'_>) -> Result<(), crate::encoding::parseerror::ParseError> {
        self.oplog.merge_ops(ops).map(|_| ())
    }

    // ============ Full state access ============

    /// Get the full document state as a nested map structure.
    ///
    /// Returns a `BTreeMap<String, MaterializedValue>` representing the fully
    /// resolved state of all CRDTs rooted at the document. Useful for:
    /// - Comparing document content across peers (content convergence)
    /// - Serialization to JSON or other formats
    /// - Debugging and inspection
    ///
    /// Note: For comparing documents after sync, compare `checkout()` output
    /// rather than `version()`. Local versions (LVs) differ across peers
    /// after concurrent operations, but content will converge.
    ///
    /// # Example
    ///
    /// ```
    /// use diamond_types_extended::{Document, Uuid};
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// doc.transact(alice, |tx| {
    ///     tx.root().set("key", "value");
    ///     tx.root().create_text("content");
    /// });
    /// doc.transact(alice, |tx| {
    ///     tx.get_text_mut(&["content"]).unwrap().insert(0, "Hello!");
    /// });
    ///
    /// let state = doc.checkout();
    /// // state is a BTreeMap with the full document tree
    /// ```
    pub fn checkout(&self) -> BTreeMap<String, MaterializedValue> {
        checkout_to_materialized(self.oplog.checkout())
    }

    // ============ Path-based reads (FFI/WASM convenience) ============

    /// Get a primitive value by key from a map at path.
    ///
    /// Path points to the containing map, key is the field name.
    /// Empty path reads from the root map.
    ///
    /// ```
    /// use diamond_types_extended::{Document, Uuid};
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// doc.transact(alice, |tx| {
    ///     tx.root().set("name", "Alice");
    ///     tx.root().create_map("meta");
    /// });
    /// doc.transact(alice, |tx| {
    ///     tx.get_map_mut(&["meta"]).unwrap().set("role", "admin");
    /// });
    ///
    /// assert_eq!(doc.get(&[], "name").unwrap().as_str(), Some("Alice"));
    /// assert_eq!(doc.get(&["meta"], "role").unwrap().as_str(), Some("admin"));
    /// ```
    pub fn get(&self, path: &[&str], key: &str) -> Option<Value> {
        if path.is_empty() {
            self.root().get(key)
        } else {
            self.get_map(path)?.get(key)
        }
    }

    /// Get a string value by key from a map at path.
    ///
    /// Returns `None` if the path doesn't exist, the key doesn't exist,
    /// or the value isn't a string.
    pub fn get_str(&self, path: &[&str], key: &str) -> Option<String> {
        self.get(path, key)?.as_str().map(|s| s.to_string())
    }

    /// Get an integer value by key from a map at path.
    ///
    /// Returns `None` if the path doesn't exist, the key doesn't exist,
    /// or the value isn't an integer.
    pub fn get_int(&self, path: &[&str], key: &str) -> Option<i64> {
        self.get(path, key)?.as_int()
    }

    /// Get text content at a path (path points to the Text CRDT).
    ///
    /// ```
    /// use diamond_types_extended::{Document, Uuid};
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// doc.transact(alice, |tx| {
    ///     let id = tx.root().create_text("content");
    ///     tx.text_by_id(id).unwrap().insert(0, "Hello!");
    /// });
    ///
    /// assert_eq!(doc.text_content(&["content"]).unwrap(), "Hello!");
    /// ```
    pub fn text_content(&self, path: &[&str]) -> Option<String> {
        self.get_text(path).map(|t| t.content())
    }

    /// Get keys of a map at a path (owned, no borrow issues).
    ///
    /// Empty path returns root map keys.
    pub fn map_keys(&self, path: &[&str]) -> Option<Vec<String>> {
        let map = if path.is_empty() {
            self.root()
        } else {
            self.get_map(path)?
        };
        Some(map.keys().map(|s| s.to_string()).collect())
    }

    // ============ Convenience serialization ============

    /// Get owned operations since a version.
    ///
    /// Convenience wrapper around `ops_since()` that returns the owned type
    /// directly, saving the `.into()` call at every use site.
    ///
    /// ```
    /// use diamond_types_extended::{Document, Frontier, Uuid};
    ///
    /// let mut doc_a = Document::new();
    /// let mut doc_b = Document::new();
    /// let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// doc_a.transact(alice, |tx| { tx.root().set("key", "value"); });
    ///
    /// let ops = doc_a.ops_since_owned(&Frontier::root());
    /// doc_b.merge_ops(ops).unwrap();
    /// assert!(doc_b.root().contains_key("key"));
    /// ```
    pub fn ops_since_owned(&self, version: &Frontier) -> SerializedOpsOwned {
        self.oplog.ops_since(version.as_ref()).into()
    }

    /// Serialize the full document state as JSON.
    ///
    /// Primitives are serialized naturally (strings as `"foo"`, ints as `42`,
    /// bools as `true/false`, nil as `null`) so the output is human-friendly
    /// regardless of the tagged enum representation used by `MaterializedValue`.
    pub fn to_json(&self) -> String {
        let json_val = materialized_map_to_json(self.checkout());
        serde_json::to_string(&json_val).unwrap()
    }

    /// Serialize the full document state as pretty-printed JSON.
    ///
    /// See [`to_json`](Self::to_json) for details on the output format.
    pub fn to_json_pretty(&self) -> String {
        let json_val = materialized_map_to_json(self.checkout());
        serde_json::to_string_pretty(&json_val).unwrap()
    }

    // ============ Closure-free mutations ============

    /// Create a closure-free mutation handle for FFI/WASM consumers.
    ///
    /// `DocumentWriter` binds an agent to a document so mutations don't
    /// need closures. Each method executes a single operation. For batching
    /// multiple operations in Rust, prefer `transact()`.
    ///
    /// ```
    /// use diamond_types_extended::{Document, Uuid};
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
    ///
    /// let mut w = doc.writer(alice);
    /// w.root_set("name", "Alice");
    /// w.root_create_text("content");
    /// assert!(w.text_push(&["content"], "Hello!"));
    /// assert!(!w.text_push(&["nonexistent"], "oops"));
    ///
    /// drop(w); // release borrow before reading
    /// assert_eq!(doc.root().get("name").unwrap().as_str(), Some("Alice"));
    /// ```
    pub fn writer(&mut self, agent: AgentId) -> DocumentWriter<'_> {
        DocumentWriter { doc: self, agent }
    }

    // ============ Internal access (for advanced use) ============

    /// Get a reference to the underlying OpLog.
    #[allow(dead_code)]
    pub(crate) fn oplog(&self) -> &OpLog {
        &self.oplog
    }

    /// Get a mutable reference to the underlying OpLog.
    #[allow(dead_code)]
    pub(crate) fn oplog_mut(&mut self) -> &mut OpLog {
        &mut self.oplog
    }
}

// ============ DocumentWriter — closure-free mutations ============

/// Closure-free mutation handle for FFI/WASM consumers.
///
/// Binds an agent to a document so mutations don't need closures.
/// Each method executes a single operation via `transact()` internally.
/// For batching multiple operations in Rust, prefer `Document::transact()`.
///
/// Path-based methods return `bool` (true = success) or `Option<CrdtId>`
/// so FFI consumers can detect invalid paths without panicking.
///
/// # Example
///
/// ```
/// use diamond_types_extended::{Document, Uuid};
///
/// let mut doc = Document::new();
/// let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
///
/// {
///     let mut w = doc.writer(alice);
///     w.root_set("title", "My Document");
///     w.root_set("count", 42);
///     let text_id = w.root_create_text("body");
///     assert!(w.text_push(&["body"], "Hello, world!"));
///     assert!(!w.text_push(&["nonexistent"], "oops"));
/// }
///
/// assert_eq!(doc.root().get("title").unwrap().as_str(), Some("My Document"));
/// assert_eq!(doc.root().get_text("body").unwrap().content(), "Hello, world!");
/// ```
pub struct DocumentWriter<'a> {
    doc: &'a mut Document,
    agent: AgentId,
}

impl<'a> DocumentWriter<'a> {
    /// Get the agent ID bound to this writer.
    pub fn agent(&self) -> AgentId {
        self.agent
    }

    // ============ Map primitives (path-based) ============

    /// Set a key to a primitive value in a map at path.
    ///
    /// Path points to the containing map. Empty path = root (always succeeds).
    /// Returns `false` if the path doesn't resolve to a map.
    pub fn set(&mut self, path: &[&str], key: &str, value: impl Into<PrimitiveValue>) -> bool {
        let value = value.into();
        self.doc.transact(self.agent, |tx| {
            if path.is_empty() {
                tx.root().set(key, value);
                true
            } else if let Some(mut map) = tx.get_map_mut(path) {
                map.set(key, value);
                true
            } else {
                false
            }
        })
    }

    /// Set a key to nil in a map at path.
    ///
    /// Returns `false` if the path doesn't resolve to a map.
    pub fn set_nil(&mut self, path: &[&str], key: &str) -> bool {
        self.doc.transact(self.agent, |tx| {
            if path.is_empty() {
                tx.root().set_nil(key);
                true
            } else if let Some(mut map) = tx.get_map_mut(path) {
                map.set_nil(key);
                true
            } else {
                false
            }
        })
    }

    // ============ CRDT creation (path-based) ============

    /// Create a nested Map at key in a map at path.
    ///
    /// Returns `None` if the path doesn't resolve to a map.
    pub fn create_map(&mut self, path: &[&str], key: &str) -> Option<CrdtId> {
        self.doc.transact(self.agent, |tx| {
            if path.is_empty() {
                Some(tx.root().create_map(key))
            } else {
                tx.get_map_mut(path).map(|mut map| map.create_map(key))
            }
        })
    }

    /// Create a nested Text CRDT at key in a map at path.
    ///
    /// Returns `None` if the path doesn't resolve to a map.
    pub fn create_text(&mut self, path: &[&str], key: &str) -> Option<CrdtId> {
        self.doc.transact(self.agent, |tx| {
            if path.is_empty() {
                Some(tx.root().create_text(key))
            } else {
                tx.get_map_mut(path).map(|mut map| map.create_text(key))
            }
        })
    }

    /// Create a nested Set CRDT at key in a map at path.
    ///
    /// Returns `None` if the path doesn't resolve to a map.
    pub fn create_set(&mut self, path: &[&str], key: &str) -> Option<CrdtId> {
        self.doc.transact(self.agent, |tx| {
            if path.is_empty() {
                Some(tx.root().create_set(key))
            } else {
                tx.get_map_mut(path).map(|mut map| map.create_set(key))
            }
        })
    }

    /// Create a nested Register CRDT at key in a map at path.
    ///
    /// Returns `None` if the path doesn't resolve to a map.
    pub fn create_register(&mut self, path: &[&str], key: &str) -> Option<CrdtId> {
        self.doc.transact(self.agent, |tx| {
            if path.is_empty() {
                Some(tx.root().create_register(key))
            } else {
                tx.get_map_mut(path).map(|mut map| map.create_register(key))
            }
        })
    }

    // ============ Root-level shortcuts ============

    /// Set a key to a primitive value in the root map.
    pub fn root_set(&mut self, key: &str, value: impl Into<PrimitiveValue>) {
        let value = value.into();
        self.doc.transact(self.agent, |tx| {
            tx.root().set(key, value);
        });
    }

    /// Create a nested Map in the root map.
    pub fn root_create_map(&mut self, key: &str) -> CrdtId {
        self.doc.transact(self.agent, |tx| tx.root().create_map(key))
    }

    /// Create a nested Text CRDT in the root map.
    pub fn root_create_text(&mut self, key: &str) -> CrdtId {
        self.doc.transact(self.agent, |tx| tx.root().create_text(key))
    }

    /// Create a nested Set CRDT in the root map.
    pub fn root_create_set(&mut self, key: &str) -> CrdtId {
        self.doc.transact(self.agent, |tx| tx.root().create_set(key))
    }

    // ============ Text operations (path-based) ============

    /// Insert text at a position in a Text CRDT at path.
    ///
    /// Returns `false` if the path doesn't resolve to a Text CRDT.
    pub fn text_insert(&mut self, path: &[&str], pos: usize, content: &str) -> bool {
        self.doc.transact(self.agent, |tx| {
            if let Some(mut text) = tx.get_text_mut(path) {
                text.insert(pos, content);
                true
            } else {
                false
            }
        })
    }

    /// Delete a range of text in a Text CRDT at path.
    ///
    /// Returns `false` if the path doesn't resolve to a Text CRDT.
    pub fn text_delete(&mut self, path: &[&str], range: Range<usize>) -> bool {
        self.doc.transact(self.agent, |tx| {
            if let Some(mut text) = tx.get_text_mut(path) {
                text.delete(range);
                true
            } else {
                false
            }
        })
    }

    /// Append text to the end of a Text CRDT at path.
    ///
    /// Returns `false` if the path doesn't resolve to a Text CRDT.
    pub fn text_push(&mut self, path: &[&str], content: &str) -> bool {
        self.doc.transact(self.agent, |tx| {
            if let Some(mut text) = tx.get_text_mut(path) {
                text.push(content);
                true
            } else {
                false
            }
        })
    }

    // ============ Set operations (path-based) ============

    /// Add a value to a Set CRDT at path.
    ///
    /// Returns `false` if the path doesn't resolve to a Set CRDT.
    pub fn set_add(&mut self, path: &[&str], value: impl Into<PrimitiveValue>) -> bool {
        let value = value.into();
        self.doc.transact(self.agent, |tx| {
            if let Some(mut set) = tx.get_set_mut(path) {
                set.add(value);
                true
            } else {
                false
            }
        })
    }

    /// Remove a value from a Set CRDT at path.
    ///
    /// Returns `false` if the path doesn't resolve to a Set CRDT.
    pub fn set_remove(&mut self, path: &[&str], value: impl Into<PrimitiveValue>) -> bool {
        let value = value.into();
        self.doc.transact(self.agent, |tx| {
            if let Some(mut set) = tx.get_set_mut(path) {
                set.remove(value);
                true
            } else {
                false
            }
        })
    }
}

/// A transaction for batched mutations.
///
/// Transactions capture the agent ID so you don't need to pass it to each
/// mutation. They also provide a clean solution to Rust's borrow checker
/// issues when working with nested CRDTs.
///
/// Transactions are created by `Document::transact()` and cannot be created
/// directly.
pub struct Transaction<'a> {
    pub(crate) oplog: &'a mut OpLog,
    pub(crate) agent: AgentId,
}

impl<'a> Transaction<'a> {
    /// Get the agent ID for this transaction.
    pub fn agent(&self) -> AgentId {
        self.agent
    }

    // ============ Root access ============

    /// Get a mutable reference to the root map.
    pub fn root(&mut self) -> MapMut<'_> {
        MapMut::new(self.oplog, self.agent, ROOT_CRDT_ID)
    }

    // ============ Navigate by path ============

    /// Get a mutable reference to a map at the given path.
    /// Returns None if path doesn't exist.
    pub fn get_map_mut(&mut self, path: &[&str]) -> Option<MapMut<'_>> {
        let (kind, crdt_id) = self.oplog.try_crdt_at_path(path)?;
        if kind != CRDTKind::Map || crdt_id == LV::MAX {
            return None;
        }
        Some(MapMut::new(self.oplog, self.agent, crdt_id))
    }

    /// Get a mutable reference to a text CRDT at the given path.
    /// Returns None if path doesn't exist or isn't a Text.
    pub fn get_text_mut(&mut self, path: &[&str]) -> Option<TextMut<'_>> {
        let (kind, crdt_id) = self.oplog.try_crdt_at_path(path)?;
        if kind != CRDTKind::Text || crdt_id == LV::MAX {
            return None;
        }
        Some(TextMut::new(self.oplog, self.agent, crdt_id))
    }

    /// Get a mutable reference to a set CRDT at the given path.
    /// Returns None if path doesn't exist or isn't a Set.
    pub fn get_set_mut(&mut self, path: &[&str]) -> Option<SetMut<'_>> {
        let (kind, crdt_id) = self.oplog.try_crdt_at_path(path)?;
        if kind != CRDTKind::Set || crdt_id == LV::MAX {
            return None;
        }
        Some(SetMut::new(self.oplog, self.agent, crdt_id))
    }

    // ============ Navigate by ID ============

    /// Get a mutable reference to a map by its CRDT ID.
    pub fn map_by_id(&mut self, id: CrdtId) -> MapMut<'_> {
        MapMut::new(self.oplog, self.agent, id.0)
    }

    /// Get a mutable reference to a text CRDT by its ID.
    pub fn text_by_id(&mut self, id: CrdtId) -> Option<TextMut<'_>> {
        if self.oplog.texts.contains_key(&id.0) {
            Some(TextMut::new(self.oplog, self.agent, id.0))
        } else {
            None
        }
    }

    /// Get a mutable reference to a set CRDT by its ID.
    pub fn set_by_id(&mut self, id: CrdtId) -> Option<SetMut<'_>> {
        if self.oplog.sets.contains_key(&id.0) {
            Some(SetMut::new(self.oplog, self.agent, id.0))
        } else {
            None
        }
    }
}

/// Convert a `MaterializedValue` into a natural `serde_json::Value`.
fn materialized_to_json(mv: MaterializedValue) -> serde_json::Value {
    match mv {
        MaterializedValue::Nil => serde_json::Value::Null,
        MaterializedValue::Bool(b) => serde_json::Value::Bool(b),
        MaterializedValue::Int(n) => serde_json::Value::Number(n.into()),
        MaterializedValue::Str(s) | MaterializedValue::Text(s) => serde_json::Value::String(s),
        MaterializedValue::Map(m) => materialized_map_to_json(m),
        MaterializedValue::Set(items) => serde_json::Value::Array(
            items.into_iter().map(|pv| match pv {
                PrimitiveValue::Nil => serde_json::Value::Null,
                PrimitiveValue::Bool(b) => serde_json::Value::Bool(b),
                PrimitiveValue::Int(n) => serde_json::Value::Number(n.into()),
                PrimitiveValue::Str(s) => serde_json::Value::String(s),
            }).collect(),
        ),
    }
}

/// Convert a checkout map into a `serde_json::Value::Object`.
fn materialized_map_to_json(map: BTreeMap<String, MaterializedValue>) -> serde_json::Value {
    serde_json::Value::Object(
        map.into_iter()
            .map(|(k, v)| (k, materialized_to_json(v)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_document_new() {
        let doc = Document::new();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_document_agent() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));
        let alice2 = doc.create_agent(Uuid::from_u128(0xA11CE));
        assert_eq!(alice, alice2);
    }

    #[test]
    fn test_document_transact() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        doc.transact(alice, |tx| {
            tx.root().set("key", "value");
        });

        let val = doc.root().get("key");
        assert!(val.is_some());
        assert_eq!(val.unwrap().as_str(), Some("value"));
    }

    #[test]
    fn test_document_nested() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        doc.transact(alice, |tx| {
            tx.root().create_map("nested");
        });

        // Get the nested map and set a value
        doc.transact(alice, |tx| {
            if let Some(mut nested) = tx.get_map_mut(&["nested"]) {
                nested.set("inner", 42);
            }
        });

        // Verify through the API
        let nested = doc.root().get_map("nested");
        assert!(nested.is_some());
        let inner = nested.unwrap().get("inner");
        assert!(inner.is_some());
        assert_eq!(inner.unwrap().as_int(), Some(42));
    }

    // ============ Path-based reads ============

    #[test]
    fn test_get_from_root() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        doc.transact(alice, |tx| {
            tx.root().set("name", "Alice");
            tx.root().set("age", 30i64);
        });

        assert_eq!(doc.get(&[], "name").unwrap().as_str(), Some("Alice"));
        assert_eq!(doc.get_str(&[], "name"), Some("Alice".to_string()));
        assert_eq!(doc.get_int(&[], "age"), Some(30));
        assert!(doc.get(&[], "missing").is_none());
        assert!(doc.get_str(&[], "age").is_none()); // wrong type
    }

    #[test]
    fn test_get_from_nested_path() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        doc.transact(alice, |tx| {
            tx.root().create_map("meta");
        });
        doc.transact(alice, |tx| {
            tx.get_map_mut(&["meta"]).unwrap().set("author", "Alice");
        });

        assert_eq!(doc.get_str(&["meta"], "author"), Some("Alice".to_string()));
        assert!(doc.get_str(&["nonexistent"], "key").is_none());
    }

    #[test]
    fn test_text_content() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        let id = doc.transact(alice, |tx| tx.root().create_text("body"));
        doc.transact(alice, |tx| {
            tx.text_by_id(id).unwrap().insert(0, "Hello!");
        });

        assert_eq!(doc.text_content(&["body"]), Some("Hello!".to_string()));
        assert!(doc.text_content(&["missing"]).is_none());
    }

    #[test]
    fn test_map_keys() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        doc.transact(alice, |tx| {
            tx.root().set("a", 1i64);
            tx.root().set("b", 2i64);
            tx.root().create_map("nested");
        });

        let keys = doc.map_keys(&[]).unwrap();
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
        assert!(keys.contains(&"nested".to_string()));
        assert!(doc.map_keys(&["missing"]).is_none());
    }

    // ============ ops_since_owned ============

    #[test]
    fn test_ops_since_owned() {
        let mut doc_a = Document::new();
        let mut doc_b = Document::new();
        let alice = doc_a.create_agent(Uuid::from_u128(0xA11CE));

        doc_a.transact(alice, |tx| { tx.root().set("key", "value"); });

        let ops = doc_a.ops_since_owned(&Frontier::root());
        doc_b.merge_ops(ops).unwrap();

        assert_eq!(doc_b.root().get("key").unwrap().as_str(), Some("value"));
    }

    // ============ DocumentWriter ============

    #[test]
    fn test_writer_root_set() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        {
            let mut w = doc.writer(alice);
            w.root_set("name", "Alice");
            w.root_set("count", 42i64);
            w.root_set("active", true);
        }

        assert_eq!(doc.get_str(&[], "name"), Some("Alice".to_string()));
        assert_eq!(doc.get_int(&[], "count"), Some(42));
        assert_eq!(doc.get(&[], "active").unwrap().as_bool(), Some(true));
    }

    #[test]
    fn test_writer_nested_map() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        {
            let mut w = doc.writer(alice);
            w.root_create_map("meta");
            assert!(w.set(&["meta"], "author", "Alice"));
            assert!(w.set(&["meta"], "year", 2025i64));
        }

        assert_eq!(doc.get_str(&["meta"], "author"), Some("Alice".to_string()));
        assert_eq!(doc.get_int(&["meta"], "year"), Some(2025));
    }

    #[test]
    fn test_writer_text() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        {
            let mut w = doc.writer(alice);
            w.root_create_text("body");
            assert!(w.text_insert(&["body"], 0, "Hello"));
            assert!(w.text_push(&["body"], ", world!"));
        }

        assert_eq!(doc.text_content(&["body"]), Some("Hello, world!".to_string()));

        {
            let mut w = doc.writer(alice);
            assert!(w.text_delete(&["body"], 5..13)); // ", world!"
        }

        assert_eq!(doc.text_content(&["body"]), Some("Hello".to_string()));
    }

    #[test]
    fn test_writer_set() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        {
            let mut w = doc.writer(alice);
            w.root_create_set("tags");
            assert!(w.set_add(&["tags"], "rust"));
            assert!(w.set_add(&["tags"], "crdt"));
            assert!(w.set_add(&["tags"], 42i64));
        }

        let tags = doc.root().get_set("tags").unwrap();
        assert!(tags.contains_str("rust"));
        assert!(tags.contains_str("crdt"));
        assert!(tags.contains_int(42));

        {
            let mut w = doc.writer(alice);
            assert!(w.set_remove(&["tags"], "crdt"));
        }

        let tags = doc.root().get_set("tags").unwrap();
        assert!(tags.contains_str("rust"));
        assert!(!tags.contains_str("crdt"));
    }

    #[test]
    fn test_writer_set_nil() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        {
            let mut w = doc.writer(alice);
            w.root_set("key", "value");
        }
        assert_eq!(doc.get_str(&[], "key"), Some("value".to_string()));

        {
            let mut w = doc.writer(alice);
            assert!(w.set_nil(&[], "key"));
        }
        assert!(doc.get(&[], "key").unwrap().is_nil());
    }

    #[test]
    fn test_writer_returns_false_on_bad_path() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        let mut w = doc.writer(alice);

        // All path-based ops should return false for nonexistent paths
        assert!(!w.set(&["nope"], "k", "v"));
        assert!(!w.set_nil(&["nope"], "k"));
        assert!(!w.text_insert(&["nope"], 0, "x"));
        assert!(!w.text_delete(&["nope"], 0..1));
        assert!(!w.text_push(&["nope"], "x"));
        assert!(!w.set_add(&["nope"], "x"));
        assert!(!w.set_remove(&["nope"], "x"));

        // create_* should return None
        assert!(w.create_map(&["nope"], "k").is_none());
        assert!(w.create_text(&["nope"], "k").is_none());
        assert!(w.create_set(&["nope"], "k").is_none());
        assert!(w.create_register(&["nope"], "k").is_none());
    }

    #[test]
    fn test_writer_create_returns_valid_ids() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        let text_id;
        let set_id;
        {
            let mut w = doc.writer(alice);
            let map_id = w.root_create_map("m");
            text_id = w.root_create_text("t");
            set_id = w.root_create_set("s");

            // path-based create on the nested map
            let nested = w.create_text(&["m"], "inner");
            assert!(nested.is_some());

            // root shortcuts return CrdtId directly (always succeed)
            assert_ne!(map_id.as_lv(), LV::MAX);
        }

        assert!(doc.get_map(&["m"]).is_some());
        assert!(doc.get_text_by_id(text_id).is_some());
        assert!(doc.get_set_by_id(set_id).is_some());
    }

    #[test]
    fn test_writer_agent() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        let w = doc.writer(alice);
        assert_eq!(w.agent(), alice);
    }

    #[test]
    fn test_writer_create_register() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        {
            let mut w = doc.writer(alice);
            w.root_create_map("container");

            // path-based register creation
            let reg_id = w.create_register(&["container"], "reg");
            assert!(reg_id.is_some());

            // verify it exists
            let id = reg_id.unwrap();
            assert!(doc.get_register_by_id(id).is_some());
        }
    }

    #[test]
    fn test_to_json() {
        let mut doc = Document::new();
        let alice = doc.create_agent(Uuid::from_u128(0xA11CE));

        doc.transact(alice, |tx| {
            tx.root().set("name", "Alice");
            tx.root().set("age", 30i64);
        });

        let json = doc.to_json();
        assert!(json.contains("Alice"));
        assert!(json.contains("30"));

        let pretty = doc.to_json_pretty();
        assert!(pretty.contains('\n')); // pretty-printed has newlines
        assert!(pretty.contains("Alice"));
    }

    // ============ Edge case tests (from Gemini review) ============

    /// Verify that a path created by DocumentWriter is immediately
    /// resolvable in the next operation — no deferred indexing.
    #[test]
    fn test_writer_immediate_usage_of_created_path() {
        let mut doc = Document::new();
        let agent = doc.create_agent(Uuid::from_u128(0x7E57));
        let mut w = doc.writer(agent);

        w.root_create_map("a");

        // Create "b" inside "a" using path — relies on "a" being indexed already
        let b_id = w.create_map(&["a"], "b");
        assert!(b_id.is_some(), "child map should be creatable immediately");

        // Write into "a/b"
        let success = w.set(&["a", "b"], "val", 123i64);
        assert!(success, "should write to deeply nested map immediately");

        drop(w);
        assert_eq!(doc.get_int(&["a", "b"], "val"), Some(123));
    }

    /// Verify that path-based ops fail gracefully on type mismatches:
    /// set() on a Text path, text_insert() on a Map path, etc.
    #[test]
    fn test_writer_path_type_mismatches() {
        let mut doc = Document::new();
        let agent = doc.create_agent(Uuid::from_u128(0x7E57));
        let mut w = doc.writer(agent);

        w.root_create_text("my_text");
        w.root_create_map("my_map");
        w.root_create_set("my_set");

        // Treat Text as a Map — set key inside text object
        assert!(!w.set(&["my_text"], "key", "val"));

        // Treat Map as Text — insert text into map object
        assert!(!w.text_insert(&["my_map"], 0, "hello"));

        // Treat Map as Set — add to map as if it were a set
        assert!(!w.set_add(&["my_map"], "val"));

        // Treat Set as Text
        assert!(!w.text_push(&["my_set"], "hello"));

        // Treat Text as Set
        assert!(!w.set_add(&["my_text"], "val"));
    }

    /// Verify that overwriting a CRDT key with a primitive makes
    /// the old CRDT path unresolvable.
    #[test]
    fn test_writer_overwrite_crdt_with_primitive() {
        let mut doc = Document::new();
        let agent = doc.create_agent(Uuid::from_u128(0x7E57));
        let mut w = doc.writer(agent);

        // Create a map at "config", write into it
        w.root_create_map("config");
        assert!(w.set(&["config"], "theme", "dark"));

        drop(w);
        assert_eq!(doc.get_str(&["config"], "theme"), Some("dark".to_string()));

        // Overwrite "config" with a primitive
        let mut w = doc.writer(agent);
        w.root_set("config", 404i64);

        // Old map path should be dead
        assert!(!w.set(&["config"], "theme", "light"));

        drop(w);
        assert_eq!(doc.get_int(&[], "config"), Some(404));
        assert!(doc.get_map(&["config"]).is_none());
    }

    /// Verify that setting a parent to nil makes child paths unresolvable.
    #[test]
    fn test_writer_write_to_nil_parent_path() {
        let mut doc = Document::new();
        let agent = doc.create_agent(Uuid::from_u128(0x7E57));
        let mut w = doc.writer(agent);

        // Setup: root -> a -> b
        w.root_create_map("a");
        w.create_map(&["a"], "b");
        assert!(w.set(&["a", "b"], "val", 1i64));

        // Tombstone "a"
        assert!(w.set_nil(&[], "a"));

        // Child path should be dead
        assert!(!w.set(&["a", "b"], "val", 2i64));

        drop(w);
        assert!(doc.get(&[], "a").unwrap().is_nil());
    }

    /// Verify to_json includes all CRDT types correctly.
    #[test]
    fn test_json_all_crdt_types() {
        let mut doc = Document::new();
        let agent = doc.create_agent(Uuid::from_u128(0x7E57));

        {
            let mut w = doc.writer(agent);
            w.root_create_set("my_set");
            w.set_add(&["my_set"], "item1");
            w.set_add(&["my_set"], 42i64);
            w.root_create_text("my_text");
            w.text_push(&["my_text"], "Hello");
            w.root_create_map("my_map");
            w.set(&["my_map"], "nested_key", "nested_val");
        }

        let json = doc.to_json();
        assert!(json.contains("item1"), "set string member");
        assert!(json.contains("42"), "set int member");
        assert!(json.contains("Hello"), "text content");
        assert!(json.contains("nested_val"), "nested map value");
    }
}
