//! LWW (Last-Write-Wins) Register CRDT.
//!
//! A register is a single-value container where concurrent writes are resolved
//! deterministically using Lamport timestamps and peer ID tie-breaking.
//!
//! # Conflict Resolution
//!
//! When multiple concurrent writes occur, the winner is determined by comparing
//! `(lamport_timestamp, peer_id)` tuples - the higher tuple wins. This ensures
//! all replicas converge to the same value deterministically.
//!
//! # Example
//!
//! ```ignore
//! use diamond_types::register::LWWRegister;
//! use diamond_types::{Primitive, CreateValue};
//!
//! let mut reg = LWWRegister::new();
//! // Operations are applied through the OpLog infrastructure
//! ```

pub mod ops;

use std::cmp::Ordering;
use smallvec::SmallVec;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::causalgraph::agent_span::AgentVersion;
use crate::causalgraph::graph::Graph;
use crate::{CreateValue, LV, Primitive, RegisterValue};

pub use ops::RegisterOp;

/// Information about a register stored in the OpLog.
///
/// This tracks all historical values and which values are currently
/// at the "supremum" (the set of values that are causally maximal).
#[derive(Debug, Clone, Default)]
pub struct RegisterInfo {
    /// All operations ever applied to this register, in (LV, value) pairs.
    /// These are sorted by LV.
    pub(crate) ops: Vec<(LV, CreateValue)>,

    /// Cached indices into `ops` representing the current supremum.
    /// These are the values that are not causally dominated by any other value.
    /// Usually just one index, but can be multiple during concurrent writes.
    pub(crate) supremum: SmallVec<usize, 2>,
}

/// The state of a register at a point in time.
///
/// Contains the winning value and any concurrent conflicting values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterState {
    /// The "winning" value according to LWW semantics.
    pub value: RegisterValue,
    /// Any concurrent values that lost the LWW tie-break.
    /// These are exposed so applications can implement custom merge strategies
    /// if needed (e.g., showing conflict UI).
    pub conflicts_with: Vec<RegisterValue>,
}

impl RegisterInfo {
    /// Create a new empty register info.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if this register has any values.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Get the number of operations in this register's history.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Add a new operation to this register (local operation).
    ///
    /// For local operations, the new value always becomes the sole supremum
    /// since it causally dominates all previous values.
    ///
    /// Returns the index of the previous supremum value (if any) for cleanup purposes.
    pub fn local_push(&mut self, lv: LV, value: CreateValue) -> Option<usize> {
        let new_idx = self.ops.len();

        // Get the old supremum index before we replace it (for cleanup)
        let old_sup_idx = if self.supremum.len() == 1 {
            Some(self.supremum[0])
        } else {
            None
        };

        self.ops.push((lv, value));
        self.supremum = smallvec::smallvec![new_idx];

        old_sup_idx
    }

    /// Get the value at a given index.
    pub fn get_op(&self, idx: usize) -> Option<&(LV, CreateValue)> {
        self.ops.get(idx)
    }

    /// Add a remote operation to this register.
    ///
    /// Returns the indices that were removed from supremum (for cleanup).
    pub fn remote_push(&mut self, lv: LV, value: CreateValue, graph: &Graph) -> Vec<usize> {
        // Check if we already have this operation
        if self.ops.binary_search_by_key(&lv, |e| e.0).is_ok() {
            return vec![];
        }

        // Verify ordering - remote ops must have higher LV than existing ops
        if let Some(last_op) = self.ops.last() {
            assert!(last_op.0 < lv, "Remote operation LV must be greater than existing ops");
        }

        let new_idx = self.ops.len();
        self.ops.push((lv, value));

        // Update supremum based on causal relationships
        let mut new_sup = smallvec::smallvec![new_idx];
        let mut removed = vec![];

        for &s_idx in &self.supremum {
            let old_lv = self.ops[s_idx].0;
            match graph.version_cmp(old_lv, lv) {
                None => {
                    // Concurrent - keep both in supremum
                    new_sup.push(s_idx);
                }
                Some(Ordering::Less) => {
                    // New value dominates old - remove old from supremum
                    removed.push(s_idx);
                }
                Some(_) => {
                    panic!("Invalid state: new value should not be dominated by existing");
                }
            }
        }

        self.supremum = new_sup;
        removed
    }

    /// Get the current value, resolving conflicts using LWW semantics.
    ///
    /// Uses agent name comparison for tie-breaking when timestamps are equal.
    pub fn get_value(&self, agent_assignment: &AgentAssignment) -> Option<&CreateValue> {
        let (idx, _) = self.tie_break(agent_assignment)?;
        Some(&self.ops[idx].1)
    }

    /// Get the full state including any conflicting values.
    pub fn get_state(&self, agent_assignment: &AgentAssignment) -> Option<RegisterState> {
        let (active_idx, other_idxes) = self.tie_break(agent_assignment)?;

        Some(RegisterState {
            value: create_to_register_value(self.ops[active_idx].0, &self.ops[active_idx].1),
            conflicts_with: other_idxes
                .map(|v| create_to_register_value(self.ops[v].0, &self.ops[v].1))
                .collect(),
        })
    }

    /// Perform LWW tie-breaking among concurrent values.
    ///
    /// Returns the winning index and an iterator over losing indices.
    pub fn tie_break(&self, agent_assignment: &AgentAssignment) -> Option<(usize, impl Iterator<Item = usize> + '_)> {
        match self.supremum.len() {
            0 => None,
            1 => Some((self.supremum[0], TieBreakIter::Empty)),
            _ => {
                // Find the winner by comparing (agent_name, seq) tuples
                let active_idx = self.supremum.iter()
                    .map(|&s| (s, agent_assignment.local_to_agent_version(self.ops[s].0)))
                    .max_by(|(_, a), (_, b)| {
                        agent_assignment.tie_break_agent_versions(*a, *b)
                    })
                    .map(|(idx, _)| idx)?;

                Some((
                    active_idx,
                    TieBreakIter::Some {
                        iter: self.supremum.iter(),
                        active_idx,
                    }
                ))
            }
        }
    }

    /// Get the local versions that are currently in the supremum.
    pub fn supremum_versions(&self) -> impl Iterator<Item = LV> + '_ {
        self.supremum.iter().map(|&idx| self.ops[idx].0)
    }
}

/// Iterator for tie-break results that filters out the winning index.
enum TieBreakIter<'a> {
    Empty,
    Some {
        iter: std::slice::Iter<'a, usize>,
        active_idx: usize,
    },
}

impl<'a> Iterator for TieBreakIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TieBreakIter::Empty => None,
            TieBreakIter::Some { iter, active_idx } => {
                for &idx in iter.by_ref() {
                    if idx != *active_idx {
                        return Some(idx);
                    }
                }
                None
            }
        }
    }
}

/// Convert a CreateValue to a RegisterValue for state representation.
pub fn create_to_register_value(lv: LV, create: &CreateValue) -> RegisterValue {
    match create {
        CreateValue::Primitive(p) => RegisterValue::Primitive(p.clone()),
        CreateValue::NewCRDT(kind) => RegisterValue::OwnedCRDT(*kind, lv),
    }
}

/// A standalone LWW Register that can exist outside of a Map.
///
/// This wraps a RegisterInfo with its CRDT ID for use as an independent CRDT.
#[derive(Debug, Clone)]
pub struct LWWRegister {
    /// The CRDT ID for this register.
    pub id: LV,
    /// The register's operation history and state.
    pub info: RegisterInfo,
}

impl LWWRegister {
    /// Create a new register with the given ID.
    pub fn new(id: LV) -> Self {
        Self {
            id,
            info: RegisterInfo::new(),
        }
    }

    /// Get the current value, resolving conflicts using LWW semantics.
    pub fn get(&self, agent_assignment: &AgentAssignment) -> Option<&CreateValue> {
        self.info.get_value(agent_assignment)
    }

    /// Get the full state including conflicts.
    pub fn get_state(&self, agent_assignment: &AgentAssignment) -> Option<RegisterState> {
        self.info.get_state(agent_assignment)
    }

    /// Check if this register has any values.
    pub fn is_empty(&self) -> bool {
        self.info.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::causalgraph::CausalGraph;

    #[test]
    fn register_info_local_push() {
        let mut info = RegisterInfo::new();

        info.local_push(0, CreateValue::Primitive(Primitive::I64(1)));
        assert_eq!(info.supremum.len(), 1);
        assert_eq!(info.supremum[0], 0);

        info.local_push(1, CreateValue::Primitive(Primitive::I64(2)));
        assert_eq!(info.supremum.len(), 1);
        assert_eq!(info.supremum[0], 1);
    }

    #[test]
    fn register_info_get_value() {
        let mut cg = CausalGraph::new();
        let agent = cg.get_or_create_agent_id("alice");
        let lv = cg.assign_local_op(agent, 1).start;

        let mut info = RegisterInfo::new();
        info.local_push(lv, CreateValue::Primitive(Primitive::I64(42)));

        let value = info.get_value(&cg.agent_assignment).unwrap();
        assert_eq!(*value, CreateValue::Primitive(Primitive::I64(42)));
    }

    #[test]
    fn lww_register_basic() {
        let mut cg = CausalGraph::new();
        let agent = cg.get_or_create_agent_id("bob");
        let lv = cg.assign_local_op(agent, 1).start;

        let mut reg = LWWRegister::new(0);
        reg.info.local_push(lv, CreateValue::Primitive(Primitive::Str("hello".into())));

        let value = reg.get(&cg.agent_assignment).unwrap();
        assert_eq!(*value, CreateValue::Primitive(Primitive::Str("hello".into())));
    }

    // ===== Concurrency & Edge Case Tests =====

    #[test]
    fn register_concurrent_writes_lww() {
        // Two agents write concurrently - higher (agent_name, seq) wins
        let mut cg = CausalGraph::new();
        let alice = cg.get_or_create_agent_id("alice");
        let bob = cg.get_or_create_agent_id("bob");

        // Both write from same parent (root) - concurrent!
        let parents = cg.version.clone();
        let lv_alice = cg.assign_local_op_with_parents(parents.as_ref(), alice, 1).start;
        let lv_bob = cg.assign_local_op_with_parents(parents.as_ref(), bob, 1).start;

        let mut info = RegisterInfo::new();
        info.local_push(lv_alice, CreateValue::Primitive(Primitive::Str("alice_val".into())));
        info.remote_push(lv_bob, CreateValue::Primitive(Primitive::Str("bob_val".into())), &cg.graph);

        // Both should be in supremum (concurrent)
        assert_eq!(info.supremum.len(), 2);

        // Tie-break: "bob" > "alice" alphabetically
        let value = info.get_value(&cg.agent_assignment).unwrap();
        assert_eq!(*value, CreateValue::Primitive(Primitive::Str("bob_val".into())));

        // Verify conflicts are accessible
        let state = info.get_state(&cg.agent_assignment).unwrap();
        assert_eq!(state.conflicts_with.len(), 1);
    }

    #[test]
    fn register_remote_push_causal_dominance() {
        // Remote op that causally dominates existing value
        let mut cg = CausalGraph::new();
        let alice = cg.get_or_create_agent_id("alice");

        let lv1 = cg.assign_local_op(alice, 1).start;
        let lv2 = cg.assign_local_op(alice, 1).start; // Causally after lv1

        let mut info = RegisterInfo::new();
        info.local_push(lv1, CreateValue::Primitive(Primitive::I64(1)));

        let removed = info.remote_push(lv2, CreateValue::Primitive(Primitive::I64(2)), &cg.graph);

        // lv2 dominates lv1, so lv1 should be removed from supremum
        assert_eq!(info.supremum.len(), 1);
        assert_eq!(removed, vec![0]); // Index 0 was removed

        let value = info.get_value(&cg.agent_assignment).unwrap();
        assert_eq!(*value, CreateValue::Primitive(Primitive::I64(2)));
    }

    #[test]
    fn register_three_way_concurrent() {
        // Three concurrent writes - all should be in supremum
        let mut cg = CausalGraph::new();
        let alice = cg.get_or_create_agent_id("alice");
        let bob = cg.get_or_create_agent_id("bob");
        let carol = cg.get_or_create_agent_id("carol");

        let parents = cg.version.clone();
        let lv_a = cg.assign_local_op_with_parents(parents.as_ref(), alice, 1).start;
        let lv_b = cg.assign_local_op_with_parents(parents.as_ref(), bob, 1).start;
        let lv_c = cg.assign_local_op_with_parents(parents.as_ref(), carol, 1).start;

        let mut info = RegisterInfo::new();
        info.local_push(lv_a, CreateValue::Primitive(Primitive::I64(1)));
        info.remote_push(lv_b, CreateValue::Primitive(Primitive::I64(2)), &cg.graph);
        info.remote_push(lv_c, CreateValue::Primitive(Primitive::I64(3)), &cg.graph);

        assert_eq!(info.supremum.len(), 3);

        // "carol" wins tie-break (alphabetically highest)
        let state = info.get_state(&cg.agent_assignment).unwrap();
        assert_eq!(state.value, RegisterValue::Primitive(Primitive::I64(3)));
        assert_eq!(state.conflicts_with.len(), 2);
    }

    #[test]
    fn register_empty_tie_break() {
        let cg = CausalGraph::new();
        let info = RegisterInfo::new();
        assert!(info.get_value(&cg.agent_assignment).is_none());
        assert!(info.get_state(&cg.agent_assignment).is_none());
    }

    #[test]
    fn register_duplicate_remote_push() {
        let mut cg = CausalGraph::new();
        let alice = cg.get_or_create_agent_id("alice");
        let lv = cg.assign_local_op(alice, 1).start;

        let mut info = RegisterInfo::new();
        info.local_push(lv, CreateValue::Primitive(Primitive::I64(1)));

        // Try to push same LV again (should be no-op)
        let removed = info.remote_push(lv, CreateValue::Primitive(Primitive::I64(2)), &cg.graph);

        assert!(removed.is_empty());
        assert_eq!(info.ops.len(), 1); // Still just one op
    }

    #[test]
    fn register_convergence_fuzz() {
        use rand::prelude::*;

        for seed in 0..50 {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut cg = CausalGraph::new();

            let agents: Vec<_> = ["alice", "bob", "carol"]
                .iter()
                .map(|name| cg.get_or_create_agent_id(name))
                .collect();

            // Two registers that should converge
            let mut reg1 = RegisterInfo::new();
            let mut reg2 = RegisterInfo::new();

            for _ in 0..20 {
                // Random agent makes a write
                let agent = agents[rng.gen_range(0..3)];
                let lv = cg.assign_local_op(agent, 1).start;
                let val = CreateValue::Primitive(Primitive::I64(rng.gen()));

                // Apply to both (simulating replication)
                if reg1.ops.is_empty() {
                    reg1.local_push(lv, val.clone());
                } else {
                    reg1.remote_push(lv, val.clone(), &cg.graph);
                }

                if reg2.ops.is_empty() {
                    reg2.local_push(lv, val);
                } else {
                    reg2.remote_push(lv, val, &cg.graph);
                }
            }

            // Both should have same winning value
            let v1 = reg1.get_value(&cg.agent_assignment);
            let v2 = reg2.get_value(&cg.agent_assignment);
            assert_eq!(v1, v2, "Registers diverged on seed {}", seed);
        }
    }
}
