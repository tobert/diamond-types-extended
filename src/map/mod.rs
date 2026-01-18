//! LWW (Last-Write-Wins) Map CRDT.
//!
//! A map is a key-value container where each key maps to an LWW Register.
//! This allows for nested CRDT structures - map values can be primitives,
//! other maps, text CRDTs, or sets.
//!
//! # Nesting Model
//!
//! Values in the map can be:
//! - **Primitives**: Nil, Bool, I64, Str
//! - **Nested CRDTs**: Maps, Text, Sets (identified by their creation LV)
//!
//! Nested CRDTs are created atomically when setting a key. The CRDT ID
//! is the LV at which it was created.
//!
//! # Conflict Resolution
//!
//! When concurrent writes occur to the same key, LWW semantics apply:
//! the write with the higher `(lamport_timestamp, peer_id)` tuple wins.
//! Conflicting values are still accessible for custom merge strategies.
//!
//! # Example
//!
//! ```ignore
//! // Creating a document with nested structure
//! let mut oplog = OpLog::new();
//! let agent = oplog.cg.get_or_create_agent_id("alice");
//!
//! // Create nested map at "user"
//! let user_map = oplog.local_map_set(agent, ROOT_CRDT_ID, "user",
//!     CreateValue::NewCRDT(CRDTKind::Map));
//!
//! // Set primitive in nested map
//! oplog.local_map_set(agent, user_map, "name",
//!     CreateValue::Primitive(Primitive::Str("Alice".into())));
//!
//! // Create text CRDT in nested map
//! let bio = oplog.local_map_set(agent, user_map, "bio",
//!     CreateValue::NewCRDT(CRDTKind::Text));
//! ```

pub mod ops;

use std::collections::BTreeMap;

use smartstring::alias::String as SmartString;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::register::{RegisterInfo, RegisterState, create_to_register_value};
use crate::{CRDTKind, CreateValue, LV, Primitive, RegisterValue};

pub use ops::MapOp;

/// A value that can be stored in a map entry.
///
/// This enum distinguishes between primitive values and references to
/// nested CRDTs. The CRDT reference stores the kind and the LV at which
/// it was created.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MapValue {
    /// A primitive value (nil, bool, i64, string).
    Primitive(Primitive),
    /// A reference to a nested CRDT, identified by kind and creation LV.
    Crdt {
        kind: CRDTKind,
        id: LV,
    },
}

impl MapValue {
    /// Create a primitive map value.
    pub fn primitive(p: Primitive) -> Self {
        Self::Primitive(p)
    }

    /// Create a nil value.
    pub fn nil() -> Self {
        Self::Primitive(Primitive::Nil)
    }

    /// Create a boolean value.
    pub fn bool(b: bool) -> Self {
        Self::Primitive(Primitive::Bool(b))
    }

    /// Create an integer value.
    pub fn i64(n: i64) -> Self {
        Self::Primitive(Primitive::I64(n))
    }

    /// Create a string value.
    pub fn str(s: impl Into<SmartString>) -> Self {
        Self::Primitive(Primitive::Str(s.into()))
    }

    /// Check if this is a primitive value.
    pub fn is_primitive(&self) -> bool {
        matches!(self, Self::Primitive(_))
    }

    /// Check if this is a CRDT reference.
    pub fn is_crdt(&self) -> bool {
        matches!(self, Self::Crdt { .. })
    }

    /// Get the CRDT kind if this is a CRDT reference.
    pub fn crdt_kind(&self) -> Option<CRDTKind> {
        match self {
            Self::Crdt { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// Get the CRDT ID if this is a CRDT reference.
    pub fn crdt_id(&self) -> Option<LV> {
        match self {
            Self::Crdt { id, .. } => Some(*id),
            _ => None,
        }
    }
}

impl From<RegisterValue> for MapValue {
    fn from(rv: RegisterValue) -> Self {
        match rv {
            RegisterValue::Primitive(p) => MapValue::Primitive(p),
            RegisterValue::OwnedCRDT(kind, id) => MapValue::Crdt { kind, id },
        }
    }
}

impl From<MapValue> for RegisterValue {
    fn from(mv: MapValue) -> Self {
        match mv {
            MapValue::Primitive(p) => RegisterValue::Primitive(p),
            MapValue::Crdt { kind, id } => RegisterValue::OwnedCRDT(kind, id),
        }
    }
}

/// Information about a map stored in the OpLog.
///
/// A map is essentially a collection of RegisterInfo entries, one per key.
/// This type provides a higher-level API for working with map data.
#[derive(Debug, Clone, Default)]
pub struct MapInfo {
    /// The CRDT ID for this map.
    pub id: LV,
    /// The keys in this map and their register states.
    entries: BTreeMap<SmartString, RegisterInfo>,
}

impl MapInfo {
    /// Create a new map with the given ID.
    pub fn new(id: LV) -> Self {
        Self {
            id,
            entries: BTreeMap::new(),
        }
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of keys in the map.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the map contains a key.
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Get the register info for a key.
    pub fn get(&self, key: &str) -> Option<&RegisterInfo> {
        self.entries.get(key)
    }

    /// Get mutable register info for a key.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut RegisterInfo> {
        self.entries.get_mut(key)
    }

    /// Get or create register info for a key.
    pub fn entry(&mut self, key: impl Into<SmartString>) -> &mut RegisterInfo {
        self.entries.entry(key.into()).or_default()
    }

    /// Iterate over all keys.
    pub fn keys(&self) -> impl Iterator<Item = &SmartString> {
        self.entries.keys()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&SmartString, &RegisterInfo)> {
        self.entries.iter()
    }

    /// Iterate over all entries mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&SmartString, &mut RegisterInfo)> {
        self.entries.iter_mut()
    }

    /// Get the current value for a key, resolving LWW conflicts.
    pub fn get_value(&self, key: &str, agent_assignment: &AgentAssignment) -> Option<MapValue> {
        let info = self.entries.get(key)?;
        let (idx, _) = info.tie_break(agent_assignment)?;
        let (lv, value) = &info.ops[idx];
        Some(create_value_to_map_value(*lv, value))
    }

    /// Get the full state for a key, including conflict information.
    pub fn get_state(&self, key: &str, agent_assignment: &AgentAssignment) -> Option<MapEntryState> {
        let info = self.entries.get(key)?;
        let state = info.get_state(agent_assignment)?;
        Some(MapEntryState {
            value: state.value.into(),
            conflicts_with: state.conflicts_with.into_iter().map(Into::into).collect(),
        })
    }

    /// Checkout all values in the map.
    pub fn checkout(&self, agent_assignment: &AgentAssignment) -> BTreeMap<SmartString, MapValue> {
        self.entries.iter()
            .filter_map(|(key, info)| {
                let (idx, _) = info.tie_break(agent_assignment)?;
                let (lv, value) = &info.ops[idx];
                Some((key.clone(), create_value_to_map_value(*lv, value)))
            })
            .collect()
    }
}

/// The state of a map entry, including conflict information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MapEntryState {
    /// The winning value according to LWW semantics.
    pub value: MapValue,
    /// Any concurrent values that lost the LWW tie-break.
    pub conflicts_with: Vec<MapValue>,
}

impl MapEntryState {
    /// Check if there are any conflicts.
    pub fn has_conflicts(&self) -> bool {
        !self.conflicts_with.is_empty()
    }
}

/// Convert a CreateValue to a MapValue with the given LV as the CRDT ID.
fn create_value_to_map_value(lv: LV, value: &CreateValue) -> MapValue {
    match value {
        CreateValue::Primitive(p) => MapValue::Primitive(p.clone()),
        CreateValue::NewCRDT(kind) => MapValue::Crdt {
            kind: *kind,
            id: lv, // The CRDT ID is the LV where it was created
        },
    }
}

/// An LWW Map CRDT with a clean API.
///
/// This wraps MapInfo and provides convenience methods for common operations.
#[derive(Debug, Clone)]
pub struct LWWMap {
    /// The underlying map info.
    pub info: MapInfo,
}

impl LWWMap {
    /// Create a new map with the given ID.
    pub fn new(id: LV) -> Self {
        Self {
            info: MapInfo::new(id),
        }
    }

    /// Get the map's CRDT ID.
    pub fn id(&self) -> LV {
        self.info.id
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.info.is_empty()
    }

    /// Get the number of keys.
    pub fn len(&self) -> usize {
        self.info.len()
    }

    /// Get a value by key.
    pub fn get(&self, key: &str, agent_assignment: &AgentAssignment) -> Option<MapValue> {
        self.info.get_value(key, agent_assignment)
    }

    /// Get the full state for a key.
    pub fn get_state(&self, key: &str, agent_assignment: &AgentAssignment) -> Option<MapEntryState> {
        self.info.get_state(key, agent_assignment)
    }

    /// Iterate over keys.
    pub fn keys(&self) -> impl Iterator<Item = &SmartString> {
        self.info.keys()
    }

    /// Checkout all values.
    pub fn checkout(&self, agent_assignment: &AgentAssignment) -> BTreeMap<SmartString, MapValue> {
        self.info.checkout(agent_assignment)
    }
}

impl Default for LWWMap {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::causalgraph::CausalGraph;

    #[test]
    fn map_value_creation() {
        let nil = MapValue::nil();
        assert!(nil.is_primitive());

        let num = MapValue::i64(42);
        assert!(num.is_primitive());

        let text = MapValue::str("hello");
        assert!(text.is_primitive());

        let crdt = MapValue::Crdt { kind: CRDTKind::Text, id: 5 };
        assert!(crdt.is_crdt());
        assert_eq!(crdt.crdt_kind(), Some(CRDTKind::Text));
        assert_eq!(crdt.crdt_id(), Some(5));
    }

    #[test]
    fn map_info_basic() {
        let mut cg = CausalGraph::new();
        let agent = cg.get_or_create_agent_id("alice");
        let lv = cg.assign_local_op(agent, 1).start;

        let mut map = MapInfo::new(0);
        let entry = map.entry("name");
        entry.local_push(lv, CreateValue::Primitive(Primitive::Str("Alice".into())));

        assert!(map.contains_key("name"));
        assert_eq!(map.len(), 1);

        let value = map.get_value("name", &cg.agent_assignment).unwrap();
        assert_eq!(value, MapValue::str("Alice"));
    }

    #[test]
    fn lww_map_api() {
        let mut cg = CausalGraph::new();
        let agent = cg.get_or_create_agent_id("bob");
        let lv = cg.assign_local_op(agent, 1).start;

        let mut map = LWWMap::new(100);
        map.info.entry("count").local_push(lv, CreateValue::Primitive(Primitive::I64(42)));

        let value = map.get("count", &cg.agent_assignment).unwrap();
        assert_eq!(value, MapValue::i64(42));
    }

    // ===== Concurrency & Edge Case Tests =====

    #[test]
    fn map_concurrent_key_writes() {
        let mut cg = CausalGraph::new();
        let alice = cg.get_or_create_agent_id("alice");
        let bob = cg.get_or_create_agent_id("bob");

        let parents = cg.version.clone();
        let lv_a = cg.assign_local_op_with_parents(parents.as_ref(), alice, 1).start;
        let lv_b = cg.assign_local_op_with_parents(parents.as_ref(), bob, 1).start;

        let mut map = MapInfo::new(0);
        map.entry("key").local_push(lv_a, CreateValue::Primitive(Primitive::Str("alice".into())));
        map.entry("key").remote_push(lv_b, CreateValue::Primitive(Primitive::Str("bob".into())), &cg.graph);

        // get_state should show conflict
        let state = map.get_state("key", &cg.agent_assignment).unwrap();
        assert!(state.has_conflicts());
        assert_eq!(state.value, MapValue::str("bob")); // "bob" > "alice"
        assert_eq!(state.conflicts_with.len(), 1);

        // checkout should only show winner
        let snapshot = map.checkout(&cg.agent_assignment);
        assert_eq!(snapshot.get("key"), Some(&MapValue::str("bob")));
    }

    #[test]
    fn map_nested_crdt_creation() {
        let mut cg = CausalGraph::new();
        let alice = cg.get_or_create_agent_id("alice");
        let lv = cg.assign_local_op(alice, 1).start;

        let mut map = MapInfo::new(0);
        map.entry("child").local_push(lv, CreateValue::NewCRDT(CRDTKind::Map));

        // get_value now correctly returns the LV as the CRDT ID
        let value = map.get_value("child", &cg.agent_assignment).unwrap();
        match value {
            MapValue::Crdt { kind, id } => {
                assert_eq!(kind, CRDTKind::Map);
                assert_eq!(id, lv); // ID should be the LV where it was created
            }
            _ => panic!("Expected CRDT value"),
        }

        // get_state should also work
        let state = map.get_state("child", &cg.agent_assignment).unwrap();
        match state.value {
            MapValue::Crdt { kind, id } => {
                assert_eq!(kind, CRDTKind::Map);
                assert_eq!(id, lv);
            }
            _ => panic!("Expected CRDT value"),
        }
    }

    #[test]
    fn map_missing_key() {
        let cg = CausalGraph::new();
        let map = MapInfo::new(0);
        assert!(map.get_value("missing", &cg.agent_assignment).is_none());
        assert!(map.get_state("missing", &cg.agent_assignment).is_none());
    }
}
