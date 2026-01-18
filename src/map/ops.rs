//! Map operations for the LWW Map CRDT.
//!
//! Operations represent atomic changes that can be made to a map.

use smartstring::alias::String as SmartString;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{CRDTKind, CreateValue, Primitive};

/// An operation on an LWW Map.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MapOp {
    /// Set a key to a primitive or new CRDT value.
    Set {
        key: SmartString,
        value: CreateValue,
    },
    /// Delete a key from the map.
    ///
    /// In LWW semantics, delete is equivalent to setting to nil/tombstone.
    /// We track it as a separate operation for clarity.
    Delete {
        key: SmartString,
    },
}

impl MapOp {
    /// Create a set operation with a primitive value.
    pub fn set_primitive(key: impl Into<SmartString>, value: Primitive) -> Self {
        Self::Set {
            key: key.into(),
            value: CreateValue::Primitive(value),
        }
    }

    /// Create a set operation that creates a nested CRDT.
    pub fn set_crdt(key: impl Into<SmartString>, kind: CRDTKind) -> Self {
        Self::Set {
            key: key.into(),
            value: CreateValue::NewCRDT(kind),
        }
    }

    /// Create a delete operation.
    pub fn delete(key: impl Into<SmartString>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// Get the key this operation affects.
    pub fn key(&self) -> &str {
        match self {
            MapOp::Set { key, .. } => key.as_str(),
            MapOp::Delete { key } => key.as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_op_creation() {
        let set_op = MapOp::set_primitive("name", Primitive::Str("Alice".into()));
        assert_eq!(set_op.key(), "name");

        let crdt_op = MapOp::set_crdt("content", CRDTKind::Text);
        assert_eq!(crdt_op.key(), "content");

        let delete_op = MapOp::delete("old_key");
        assert_eq!(delete_op.key(), "old_key");
    }
}
