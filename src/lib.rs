//! # Facet - Unified CRDT Library
//!
//! Facet provides high-performance CRDTs (Conflict-free Replicated Data Types) for
//! collaborative applications. It's a fork of [diamond-types](https://github.com/josephg/diamond-types)
//! with an ergonomic, unified API.
//!
//! ## Quick Start
//!
//! ```
//! use facet::{Document, Value};
//!
//! // Create a document
//! let mut doc = Document::new();
//! let alice = doc.get_or_create_agent("alice");
//!
//! // All mutations happen in transactions
//! doc.transact(alice, |tx| {
//!     tx.root().set("title", "My Document");
//!     tx.root().set("count", 42);
//!     tx.root().create_text("content");
//! });
//!
//! // Access the text CRDT
//! doc.transact(alice, |tx| {
//!     if let Some(mut text) = tx.get_text_mut(&["content"]) {
//!         text.insert(0, "Hello, world!");
//!     }
//! });
//!
//! // Read values directly
//! assert_eq!(doc.root().get("title").unwrap().as_str(), Some("My Document"));
//! assert_eq!(doc.root().get_text("content").unwrap().content(), "Hello, world!");
//! ```
//!
//! ## CRDT Types
//!
//! - **Map**: Key-value container with LWW (Last-Writer-Wins) registers per key
//! - **Text**: Sequence CRDT for collaborative text editing
//! - **Set**: OR-Set (Observed-Remove) with add-wins semantics
//! - **Register**: Single-value LWW container
//!
//! All types can be nested within Maps.
//!
//! ## Replication
//!
//! ```ignore
//! // Peer A creates some changes
//! let ops = doc_a.ops_since(&[]).to_owned();
//!
//! // Peer B merges them
//! doc_b.merge_ops(ops)?;
//! ```
//!
//! ## Legacy API
//!
//! The original diamond-types API is still available via the [`list`] module for
//! text-only use cases. See [`list::ListCRDT`] and [`list::ListOpLog`].
//!
//! ## Attribution
//!
//! Facet is built on diamond-types by Joseph Gentle. See ATTRIBUTION.md for details.

#![allow(clippy::module_inception)]
#![allow(unused_imports, dead_code)] // During dev. TODO: Take me out!

extern crate core;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use jumprope::JumpRopeBuf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;

pub use ::rle::HasLength;
use causalgraph::graph::Graph;
pub use frontier::Frontier;

use crate::causalgraph::agent_assignment::remote_ids::{RemoteFrontierOwned, RemoteVersion, RemoteVersionOwned};
use crate::causalgraph::agent_span::AgentVersion;
pub use crate::causalgraph::CausalGraph;
pub use crate::dtrange::DTRange;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};

use crate::rle::{KVPair, RleVec};
use crate::textinfo::TextInfo;

// use crate::list::internal_op::OperationInternal as TextOpInternal;

// ============ New Facet Public API ============
mod value;
mod document;
mod refs;
mod muts;

pub use value::{Value, CrdtId, Conflicted};
pub use document::{Document, Transaction};
pub use refs::{MapRef, TextRef, SetRef, RegisterRef};
pub use muts::{MapMut, TextMut, SetMut, RegisterMut};

// ============ Original diamond-types modules ============
pub mod list;
pub mod register;
pub mod map;
pub mod set;

#[doc(hidden)]
pub mod rle;
mod dtrange;
mod unicount;
mod rev_range;
pub mod frontier;
mod check;
pub(crate) mod encoding;
pub mod causalgraph;
mod wal;
mod ost;

#[cfg(feature = "serde")]
pub(crate) mod serde_helpers;

mod listmerge;

#[cfg(any(test, feature = "gen_test_data"))]
mod list_fuzzer_tools;
#[cfg(test)]
mod fuzzer;
mod branch;
mod textinfo;
mod oplog;
#[cfg(feature = "storage")]
mod storage;
mod simple_checkout;
// mod listmerge2;
mod stats;

pub type AgentId = u32;

// TODO: Consider changing this to u64 to add support for very long lived documents even on 32 bit
// systems like wasm32
/// An LV (LocalVersion) is used all over the place internally to identify a single operation.
///
/// A local version (as the name implies) is local-only. Local versions generally need to be
/// converted to RawVersions before being sent over the wire or saved to disk.
pub type LV = usize;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(untagged))]
// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Primitive {
    Nil,
    Bool(bool),
    I64(i64),
    // F64(f64),
    Str(SmartString),

    #[cfg_attr(feature = "serde", serde(skip))]
    InvalidUninitialized,
}

impl Debug for Primitive {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Primitive::Nil => f.debug_struct("Nil").finish(),
            Primitive::Bool(val) => val.fmt(f),
            // Primitive::I64(val) => f.debug_tuple("I64").field(val).finish(),
            Primitive::I64(val) => val.fmt(f),
            Primitive::Str(val) => val.fmt(f),
            Primitive::InvalidUninitialized => f.debug_tuple("InvalidUninitialized").finish()
        }
    }
}

// #[derive(Debug, Eq, PartialEq, Copy, Clone, TryFromPrimitive)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// #[repr(u16)]
pub enum CRDTKind {
    Map,        // String => Register (like a JS object)
    Register,   // Single LWW value
    Collection, // SQL table / mongo collection
    Text,       // Text/sequence CRDT
    Set,        // OR-Set (add-wins semantics)
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CreateValue {
    Primitive(Primitive),
    NewCRDT(CRDTKind),
    // Deleted, // Marks that the key / contents should be deleted.
}

// #[derive(Debug, Clone, Eq, PartialEq)]
// pub enum CollectionOp {
//     Insert(CreateValue),
//     Remove(LV),
// }

// #[derive(Debug, Clone, Eq, PartialEq)]
// pub(crate) enum OpContents {
//     RegisterSet(CreateValue),
//     MapSet(SmartString, CreateValue), // TODO: Inline the index here.
//     MapDelete(SmartString), // TODO: And here.
//     Collection(CollectionOp), // TODO: Consider just inlining this.
//     Text(ListOpMetrics),
//
//
//     // The other way to write this would be:
//
//
//     // SetInsert(CRDTKind),
//     // SetRemove(Time),
//
//     // TextInsert(..),
//     // TextRemove(..)
// }

// #[derive(Debug, Clone, Eq, PartialEq)]
// pub(crate) struct Op {
//     pub target_id: LV,
//     pub contents: OpContents,
// }

// #[derive(Debug, Clone, Eq, PartialEq, Default)]
// pub(crate) struct Ops {
//     /// Local version + op pairs
//     ops: RleVec<KVPair<Op>>,
//     list_ctx: ListOperationCtx,
// }

pub const ROOT_CRDT_ID: LV = usize::MAX;
pub const ROOT_CRDT_ID_AV: AgentVersion = (AgentId::MAX, 0);


// #[derive(Debug, Clone, Eq, PartialEq)]
// pub enum SnapshotValue {
//     Primitive(Primitive),
//     InnerCRDT(LV),
//     // Ref(LV),
// }
//
// #[derive(Debug, Clone, Eq, PartialEq)]
// struct RegisterState {
//     value: SnapshotValue,
//     version: LV,
// }

// /// Guaranteed to always have at least 1 value inside.
// type MVRegister = SmallVec<RegisterState, 1>;

// // TODO: Probably should also store a dirty flag for when we flush to disk.
// #[derive(Debug, Clone, Eq, PartialEq)]
// enum OverlayValue {
//     Register(MVRegister),
//     Map(BTreeMap<SmartString, MVRegister>),
//     Collection(BTreeMap<LV, SnapshotValue>),
//     Text(Box<JumpRope>),
// }

// type Pair<T> = (LV, T);
type ValPair = (LV, CreateValue);
// type RawPair<'a, T> = (RemoteVersion<'a>, T);
type LVKey = LV;


#[derive(Debug, Clone, Default)]
pub(crate) struct RegisterInfo {
    // I bet there's a clever way to use RLE to optimize this. Right now this contains the full
    // history of values this register has ever held.
    ops: Vec<ValPair>,

    /// Cached version(s) which together store the current HEAD for this register.
    supremum: SmallVec<usize, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterValue {
    Primitive(Primitive),
    OwnedCRDT(CRDTKind, LVKey),
}


#[derive(Debug, Clone, Default)]
pub struct OpLog {
    pub cg: CausalGraph,


    // cg_storage: Option<CGStorage>,
    // wal_storage: Option<WriteAheadLog>,

    // Information about whether the map still exists!
    // maps: BTreeMap<LVKey, MapInfo>,

    /// (CRDT ID, key) -> MVRegister.
    map_keys: BTreeMap<(LVKey, SmartString), RegisterInfo>,
    /// CRDT ID -> Text CRDT.
    texts: BTreeMap<LVKey, TextInfo>,

    // These are always inserted at the end, but items in the middle are removed. There's probably
    // a better data structure to accomplish this.
    map_index: BTreeMap<LV, (LVKey, SmartString)>,
    text_index: BTreeMap<LV, LVKey>,

    /// Standalone registers (not inside maps).
    registers: BTreeMap<LVKey, RegisterInfo>,
    /// Index from operation LV to register CRDT ID.
    register_index: BTreeMap<LV, LVKey>,

    /// OR-Sets storing Primitive values.
    sets: BTreeMap<LVKey, set::SetInfo<Primitive>>,
    /// Index from operation LV to set CRDT ID.
    set_index: BTreeMap<LV, LVKey>,

    // The set of CRDTs which have been deleted or superseded in the current version. This data is
    // pretty similar to the _index data, in that its mainly just useful for branches doing
    // checkouts.
    deleted_crdts: BTreeSet<LVKey>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Branch {
    pub frontier: Frontier,

    // Objects are always created at the highest version ID, but can be deleted anywhere in the
    // range.
    //
    // TODO: Replace BTreeMap with something more appropriate later.
    maps: BTreeMap<LVKey, BTreeMap<SmartString, RegisterState>>, // any objects.
    pub texts: BTreeMap<LVKey, JumpRopeBuf>,
    /// Standalone registers (not inside maps).
    pub registers: BTreeMap<LVKey, RegisterState>,
    /// OR-Sets storing Primitive values.
    pub sets: BTreeMap<LVKey, BTreeSet<Primitive>>,
}

/// The register stores the specified value, but if conflicts_with is not empty, it has some
/// conflicting concurrent values too. The `value` field will be consistent across all peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterState {
    /// The winning value according to LWW semantics.
    pub value: RegisterValue,
    /// Any concurrent values that lost the LWW tie-break.
    pub conflicts_with: Vec<RegisterValue>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SerializedOps<'a> {
    cg_changes: Vec<u8>,

    // The version of the op, and the name of the containing CRDT.
    #[cfg_attr(feature = "serde", serde(borrow))]
    map_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, &'a str, CreateValue)>,
    text_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, ListOpMetrics)>,
    text_context: ListOperationCtx,
    /// OR-Set operations: (crdt_name, op_version, SerializedSetOp<Primitive>)
    set_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, set::SerializedSetOp<Primitive>)>,
}

impl<'a> From<SerializedOps<'a>> for SerializedOpsOwned {
    fn from(ops: SerializedOps<'a>) -> Self {
        Self {
            cg_changes: ops.cg_changes,
            map_ops: ops.map_ops.into_iter().map(|(crdt_name, rv, key, val)| {
                (crdt_name.to_owned(), rv.to_owned(), SmartString::from(key), val)
            }).collect(),
            text_ops: ops.text_ops.into_iter().map(|(crdt_name, rv, metrics)| {
                (crdt_name.to_owned(), rv.to_owned(), metrics)
            }).collect(),
            text_context: ops.text_context,
            set_ops: ops.set_ops.into_iter().map(|(crdt_name, rv, op)| {
                (crdt_name.to_owned(), rv.to_owned(), op)
            }).collect(),
        }
    }
}

impl<'a> SerializedOps<'a> {
    fn to_owned(self) -> SerializedOpsOwned {
        self.into()
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SerializedOpsOwned {
    cg_changes: Vec<u8>,

    // The version of the op, and the name of the containing CRDT.
    map_ops: Vec<(RemoteVersionOwned, RemoteVersionOwned, SmartString, CreateValue)>,
    text_ops: Vec<(RemoteVersionOwned, RemoteVersionOwned, ListOpMetrics)>,
    text_context: ListOperationCtx,
    /// OR-Set operations: (crdt_name, op_version, SerializedSetOp<Primitive>)
    set_ops: Vec<(RemoteVersionOwned, RemoteVersionOwned, set::SerializedSetOp<Primitive>)>,
}

/// This is used for checkouts. This is a value tree.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(untagged))]
// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DTValue {
    Primitive(Primitive),
    /// A register containing a value (which could be a nested CRDT).
    Register(Box<DTValue>),
    Map(BTreeMap<SmartString, Box<DTValue>>),
    // Collection(BTreeMap<LV, Box<DTValue>>),
    Text(String),
    /// An OR-Set containing primitive values.
    Set(BTreeSet<Primitive>),
}
