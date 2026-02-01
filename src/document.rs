//! Document container and Transaction API.
//!
//! This module provides the main entry point for the Facet API:
//! - `Document`: The unified CRDT container with a Map root
//! - `Transaction`: Batch mutations with captured agent

use std::ops::Range;

use smartstring::alias::String as SmartString;

use crate::value::{Conflicted, CrdtId, Value};
use crate::{AgentId, CRDTKind, CreateValue, Frontier, OpLog, Primitive, RegisterValue, ROOT_CRDT_ID, LV};
use crate::refs::{MapRef, RegisterRef, SetRef, TextRef};
use crate::muts::{MapMut, RegisterMut, SetMut, TextMut};

/// A unified CRDT document container.
///
/// The Document is the main entry point for Facet. It wraps an OpLog and provides:
/// - A Map root for organizing data
/// - Transaction-based mutations (solves Rust borrow issues)
/// - Consistent API across all CRDT types
/// - Clean serialization/replication
///
/// # Conflict Resolution
///
/// Facet uses deterministic conflict resolution that guarantees all peers
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
/// use facet::Document;
///
/// let mut doc = Document::new();
/// let alice = doc.get_or_create_agent("alice");
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
/// use facet::Document;
///
/// let mut doc = Document::new();
/// let alice = doc.get_or_create_agent("alice");
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

    /// Load a document from serialized bytes.
    ///
    /// Note: For v0.1, use `Document::merge()` to apply serialized operations.
    /// Full load/save is planned for a future version.
    pub fn load(_bytes: &[u8]) -> Result<Self, crate::encoding::parseerror::ParseError> {
        // TODO: Implement proper serialization for the unified OpLog
        // For now, create an empty document and use merge() to apply ops
        Ok(Self::new())
    }

    /// Get or create an agent ID by name.
    ///
    /// Agents represent participants in collaborative editing.
    /// Each agent should have a unique name per editing session.
    pub fn get_or_create_agent(&mut self, name: &str) -> AgentId {
        self.oplog.cg.get_or_create_agent_id(name)
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
    pub fn get_map(&self, path: &[&str]) -> Option<MapRef<'_>> {
        let (kind, crdt_id) = self.oplog.crdt_at_path(path);
        if kind == CRDTKind::Map && crdt_id != LV::MAX {
            Some(MapRef::new(&self.oplog, crdt_id))
        } else {
            None
        }
    }

    /// Get a read-only reference to a text CRDT at the given path.
    pub fn get_text(&self, path: &[&str]) -> Option<TextRef<'_>> {
        let (kind, crdt_id) = self.oplog.crdt_at_path(path);
        if kind == CRDTKind::Text && crdt_id != LV::MAX {
            Some(TextRef::new(&self.oplog, crdt_id))
        } else {
            None
        }
    }

    /// Get a read-only reference to a set CRDT at the given path.
    pub fn get_set(&self, path: &[&str]) -> Option<SetRef<'_>> {
        let (kind, crdt_id) = self.oplog.crdt_at_path(path);
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
    /// use facet::Document;
    ///
    /// let mut doc = Document::new();
    /// let alice = doc.get_or_create_agent("alice");
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
    /// Pass an empty slice `&[]` to get all operations (full sync).
    /// Pass `doc.version().as_ref()` from a peer to get only new operations (delta sync).
    ///
    /// Returns `SerializedOps` which borrows from this document. Use `.into()` to
    /// convert to `SerializedOpsOwned` for sending across threads or network.
    ///
    /// # Example
    ///
    /// ```
    /// use facet::Document;
    ///
    /// let mut doc_a = Document::new();
    /// let mut doc_b = Document::new();
    /// let alice = doc_a.get_or_create_agent("alice");
    ///
    /// // Alice makes changes
    /// doc_a.transact(alice, |tx| {
    ///     tx.root().set("key", "value");
    /// });
    ///
    /// // Full sync: get all operations
    /// let all_ops = doc_a.ops_since(&[]).into();
    /// doc_b.merge_ops(all_ops).unwrap();
    ///
    /// // Now doc_b has Alice's changes
    /// assert!(doc_b.root().contains_key("key"));
    /// ```
    pub fn ops_since(&self, version: &[LV]) -> crate::SerializedOps<'_> {
        self.oplog.ops_since(version)
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

    // ============ Internal access (for advanced use) ============

    /// Get a reference to the underlying OpLog.
    ///
    /// This is for advanced users who need direct access to the CRDT internals.
    #[doc(hidden)]
    pub fn oplog(&self) -> &OpLog {
        &self.oplog
    }

    /// Get a mutable reference to the underlying OpLog.
    ///
    /// This is for advanced users who need direct access to the CRDT internals.
    #[doc(hidden)]
    pub fn oplog_mut(&mut self) -> &mut OpLog {
        &mut self.oplog
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
    pub fn get_map_mut(&mut self, path: &[&str]) -> Option<MapMut<'_>> {
        let (_, crdt_id) = self.oplog.crdt_at_path(path);
        if crdt_id == LV::MAX {
            return None;
        }
        Some(MapMut::new(self.oplog, self.agent, crdt_id))
    }

    /// Get a mutable reference to a text CRDT at the given path.
    pub fn get_text_mut(&mut self, path: &[&str]) -> Option<TextMut<'_>> {
        let (kind, crdt_id) = self.oplog.crdt_at_path(path);
        if kind != CRDTKind::Text || crdt_id == LV::MAX {
            return None;
        }
        Some(TextMut::new(self.oplog, self.agent, crdt_id))
    }

    /// Get a mutable reference to a set CRDT at the given path.
    pub fn get_set_mut(&mut self, path: &[&str]) -> Option<SetMut<'_>> {
        let (kind, crdt_id) = self.oplog.crdt_at_path(path);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_new() {
        let doc = Document::new();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_document_agent() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");
        let alice2 = doc.get_or_create_agent("alice");
        assert_eq!(alice, alice2);
    }

    #[test]
    fn test_document_transact() {
        let mut doc = Document::new();
        let alice = doc.get_or_create_agent("alice");

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
        let alice = doc.get_or_create_agent("alice");

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
}
