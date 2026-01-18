//! Register operations for the LWW Register CRDT.
//!
//! Operations represent the atomic changes that can be made to a register.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::CreateValue;

/// An operation on an LWW Register.
///
/// Register operations are simple - they just set a new value.
/// The causal ordering and conflict resolution is handled by the
/// CausalGraph infrastructure.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RegisterOp {
    /// The new value being set
    pub value: CreateValue,
}

impl RegisterOp {
    /// Create a new register set operation.
    pub fn new(value: CreateValue) -> Self {
        Self { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Primitive;

    #[test]
    fn register_op_creation() {
        let op = RegisterOp::new(CreateValue::Primitive(Primitive::I64(42)));
        assert_eq!(op.value, CreateValue::Primitive(Primitive::I64(42)));
    }
}
