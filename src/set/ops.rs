//! Set operations for the OR-Set CRDT.
//!
//! Operations represent atomic changes that can be made to a set.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::LV;

/// An operation on an OR-Set.
///
/// In an OR-Set, each add is tagged with a unique identifier (the LV when added).
/// Removes specify which add-tags to remove. This enables "add-wins" semantics:
/// if an add and remove happen concurrently, the add wins.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SetOp<T> {
    /// Add an element to the set.
    ///
    /// The add is tagged with the operation's LV, creating a unique
    /// identifier that can later be targeted by a remove.
    Add(T),

    /// Remove an element from the set.
    ///
    /// This removes all observed add-tags for the element at the time
    /// of the remove. Concurrent adds will survive (add-wins semantics).
    Remove(T),
}

impl<T> SetOp<T> {
    /// Create an add operation.
    pub fn add(value: T) -> Self {
        Self::Add(value)
    }

    /// Create a remove operation.
    pub fn remove(value: T) -> Self {
        Self::Remove(value)
    }

    /// Get a reference to the value this operation affects.
    pub fn value(&self) -> &T {
        match self {
            SetOp::Add(v) => v,
            SetOp::Remove(v) => v,
        }
    }

    /// Check if this is an add operation.
    pub fn is_add(&self) -> bool {
        matches!(self, Self::Add(_))
    }

    /// Check if this is a remove operation.
    pub fn is_remove(&self) -> bool {
        matches!(self, Self::Remove(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_op_creation() {
        let add = SetOp::add("hello");
        assert!(add.is_add());
        assert_eq!(add.value(), &"hello");

        let remove = SetOp::remove(42i64);
        assert!(remove.is_remove());
        assert_eq!(remove.value(), &42);
    }
}
