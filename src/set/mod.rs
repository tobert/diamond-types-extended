//! OR-Set (Observed-Remove Set) CRDT.
//!
//! An OR-Set is a set where concurrent adds and removes are resolved with
//! "add-wins" semantics: if an add and remove happen concurrently, the
//! element remains in the set.
//!
//! # How It Works
//!
//! Each add operation is tagged with a unique identifier (the LV when added).
//! Remove operations target all *observed* add-tags at the time of remove.
//! This means:
//!
//! - If Alice adds "x" and Bob (concurrently) removes "x", the element persists
//!   because Bob's remove didn't observe Alice's add.
//! - If Alice adds "x", then Bob observes Alice's add and removes "x",
//!   the element is removed.
//!
//! # Example
//!
//! ```ignore
//! use diamond_types::set::ORSet;
//!
//! // Create an OR-Set
//! let mut set = ORSet::<String>::new(0);
//!
//! // Add operations are applied through the OpLog infrastructure
//! ```

pub mod ops;

use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::causalgraph::graph::Graph;
use crate::LV;

pub use ops::SetOp;

/// A tag representing a single "add" of an element.
///
/// Each add operation creates a unique tag (the LV at which the add occurred).
/// Remove operations target specific tags.
pub type AddTag = LV;

/// Internal representation of an element's presence in the set.
///
/// Tracks all add-tags and which ones have been removed.
#[derive(Debug, Clone, Default)]
struct ElementInfo {
    /// The LVs at which this element was added.
    adds: BTreeSet<AddTag>,
    /// For each add-tag, the LVs that removed it.
    /// An add-tag is "live" if it has no removals, or all removals
    /// are causally before adds that restore it.
    removals: BTreeMap<AddTag, BTreeSet<LV>>,
}

impl ElementInfo {
    fn new() -> Self {
        Self::default()
    }

    /// Record an add operation.
    fn add(&mut self, tag: AddTag) {
        self.adds.insert(tag);
    }

    /// Record a remove operation that removes all currently observed tags.
    ///
    /// Returns the tags that were removed.
    fn remove(&mut self, remove_lv: LV) -> Vec<AddTag> {
        let tags_to_remove: Vec<_> = self.adds.iter().copied().collect();
        for &tag in &tags_to_remove {
            self.removals.entry(tag).or_default().insert(remove_lv);
        }
        tags_to_remove
    }

    /// Remove a specific tag (used for remote operations).
    fn remove_tag(&mut self, tag: AddTag, remove_lv: LV) {
        if self.adds.contains(&tag) {
            self.removals.entry(tag).or_default().insert(remove_lv);
        }
    }

    /// Check if this element is present in the set.
    ///
    /// An element is present if at least one add-tag has not been removed.
    fn is_present(&self) -> bool {
        self.adds.iter().any(|tag| {
            self.removals.get(tag).is_none_or(|removals| removals.is_empty())
        })
    }

    /// Get the "live" add-tags (those not removed).
    fn live_tags(&self) -> impl Iterator<Item = AddTag> + '_ {
        self.adds.iter().copied().filter(|tag| {
            self.removals.get(tag).is_none_or(|removals| removals.is_empty())
        })
    }
}

/// An OR-Set (Observed-Remove Set) CRDT.
///
/// Elements can be added and removed. Concurrent add + remove = add wins.
/// This is achieved by tagging each add with a unique identifier and having
/// removes only affect observed tags.
#[derive(Debug, Clone)]
pub struct ORSet<T: Ord + Clone> {
    /// The CRDT ID for this set.
    pub id: LV,
    /// Map from element values to their add/remove information.
    elements: BTreeMap<T, ElementInfo>,
}

impl<T: Ord + Clone> ORSet<T> {
    /// Create a new empty set with the given ID.
    pub fn new(id: LV) -> Self {
        Self {
            id,
            elements: BTreeMap::new(),
        }
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        !self.elements.values().any(|info| info.is_present())
    }

    /// Get the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.values().filter(|info| info.is_present()).count()
    }

    /// Check if the set contains a value.
    pub fn contains(&self, value: &T) -> bool {
        self.elements.get(value).is_some_and(|info| info.is_present())
    }

    /// Add an element locally.
    ///
    /// Returns the add-tag for this operation.
    pub fn local_add(&mut self, value: T, tag: AddTag) -> AddTag {
        self.elements.entry(value).or_default().add(tag);
        tag
    }

    /// Remove an element locally.
    ///
    /// Returns the tags that were removed (for serialization).
    pub fn local_remove(&mut self, value: T, remove_lv: LV) -> Vec<AddTag> {
        if let Some(info) = self.elements.get_mut(&value) {
            info.remove(remove_lv)
        } else {
            vec![]
        }
    }

    /// Apply a remote add operation.
    pub fn remote_add(&mut self, value: T, tag: AddTag) {
        self.elements.entry(value).or_default().add(tag);
    }

    /// Apply a remote remove operation for specific tags.
    pub fn remote_remove(&mut self, value: T, tags: &[AddTag], remove_lv: LV) {
        if let Some(info) = self.elements.get_mut(&value) {
            for &tag in tags {
                info.remove_tag(tag, remove_lv);
            }
        }
    }

    /// Iterate over elements in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.iter()
            .filter(|(_, info)| info.is_present())
            .map(|(value, _)| value)
    }

    /// Convert to a regular BTreeSet (snapshot).
    pub fn to_btree_set(&self) -> BTreeSet<T> {
        self.iter().cloned().collect()
    }

    /// Get the live add-tags for a value (for implementing remove).
    pub fn get_tags(&self, value: &T) -> Vec<AddTag> {
        self.elements.get(value)
            .map(|info| info.live_tags().collect())
            .unwrap_or_default()
    }
}

impl<T: Ord + Clone> Default for ORSet<T> {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Stored operation for serialization/persistence.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum StoredSetOp<T> {
    /// Add with the tag (LV) at which it was added.
    Add { value: T, tag: AddTag },
    /// Remove specific tags for a value.
    Remove { value: T, tags: Vec<AddTag> },
}

/// Information about a set stored in the OpLog.
#[derive(Debug, Clone, Default)]
pub struct SetInfo<T: Ord + Clone> {
    /// The set's current state.
    pub set: ORSet<T>,
    /// Operations history for this set.
    ops: Vec<(LV, StoredSetOp<T>)>,
}

impl<T: Ord + Clone> SetInfo<T> {
    /// Create a new set info with the given ID.
    pub fn new(id: LV) -> Self {
        Self {
            set: ORSet::new(id),
            ops: Vec::new(),
        }
    }

    /// Apply a local add operation.
    pub fn local_add(&mut self, lv: LV, value: T) -> AddTag {
        let tag = self.set.local_add(value.clone(), lv);
        self.ops.push((lv, StoredSetOp::Add { value, tag }));
        tag
    }

    /// Apply a local remove operation.
    pub fn local_remove(&mut self, lv: LV, value: T) -> Vec<AddTag> {
        let tags = self.set.local_remove(value.clone(), lv);
        if !tags.is_empty() {
            self.ops.push((lv, StoredSetOp::Remove { value, tags: tags.clone() }));
        }
        tags
    }

    /// Apply a remote add operation.
    pub fn remote_add(&mut self, lv: LV, value: T, tag: AddTag) {
        self.set.remote_add(value.clone(), tag);
        self.ops.push((lv, StoredSetOp::Add { value, tag }));
    }

    /// Apply a remote remove operation.
    pub fn remote_remove(&mut self, lv: LV, value: T, tags: Vec<AddTag>) {
        self.set.remote_remove(value.clone(), &tags, lv);
        self.ops.push((lv, StoredSetOp::Remove { value, tags }));
    }

    /// Iterate over elements in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.set.iter()
    }

    /// Check if the set contains a value.
    pub fn contains(&self, value: &T) -> bool {
        self.set.contains(value)
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orset_basic_operations() {
        let mut set = ORSet::new(0);

        // Add elements
        set.local_add("apple", 1);
        set.local_add("banana", 2);
        set.local_add("cherry", 3);

        assert!(set.contains(&"apple"));
        assert!(set.contains(&"banana"));
        assert!(set.contains(&"cherry"));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn orset_remove() {
        let mut set = ORSet::new(0);

        set.local_add("apple", 1);
        set.local_add("banana", 2);

        assert_eq!(set.len(), 2);

        // Remove apple
        let removed = set.local_remove("apple", 3);
        assert_eq!(removed, vec![1]);
        assert!(!set.contains(&"apple"));
        assert!(set.contains(&"banana"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn orset_add_wins() {
        let mut set = ORSet::new(0);

        // Alice adds "x"
        set.local_add("x", 1);

        // Bob observes Alice's add and removes "x"
        let tags = set.get_tags(&"x");
        set.remote_remove("x", &tags, 2);

        // Now "x" should be gone
        assert!(!set.contains(&"x"));

        // But if Alice adds "x" again concurrently with Bob's remove
        // (simulated by adding after remove with a tag not covered by remove)
        set.remote_add("x", 3);

        // "x" should be present (add wins for the concurrent add)
        assert!(set.contains(&"x"));
    }

    #[test]
    fn orset_concurrent_add_remove() {
        let mut set = ORSet::new(0);

        // Alice adds "x" at LV 1
        set.local_add("x", 1);

        // Simulate: Bob doesn't see Alice's add, and does a remove at LV 2
        // Since Bob didn't observe any adds, the remove affects nothing
        // (empty tags list)
        set.remote_remove("x", &[], 2);

        // Alice's add should still be present (add wins)
        assert!(set.contains(&"x"));
    }

    #[test]
    fn orset_multiple_adds_partial_remove() {
        let mut set = ORSet::new(0);

        // Two concurrent adds of the same element
        set.local_add("x", 1);
        set.remote_add("x", 2);

        // Remove only sees tag 1
        set.remote_remove("x", &[1], 3);

        // Element still present because tag 2 is live
        assert!(set.contains(&"x"));
    }

    #[test]
    fn setinfo_operations() {
        let mut info = SetInfo::new(0);

        info.local_add(1, "hello".to_string());
        info.local_add(2, "world".to_string());

        assert!(info.contains(&"hello".to_string()));
        assert!(info.contains(&"world".to_string()));
        assert_eq!(info.len(), 2);

        info.local_remove(3, "hello".to_string());
        assert!(!info.contains(&"hello".to_string()));
        assert_eq!(info.len(), 1);
    }

    #[test]
    fn orset_to_btree_set() {
        let mut set = ORSet::new(0);

        set.local_add(1, 1);
        set.local_add(2, 2);
        set.local_add(3, 3);
        set.local_remove(4, 5); // Remove something not in set

        let snapshot = set.to_btree_set();
        assert_eq!(snapshot, BTreeSet::from([1, 2, 3]));
    }

    // ===== Edge Case Tests =====

    #[test]
    fn orset_remove_nonexistent() {
        let mut set = ORSet::new(0);

        // Remove something never added
        let removed = set.local_remove("phantom", 1);
        assert!(removed.is_empty());
        assert!(!set.contains(&"phantom"));
    }

    #[test]
    fn orset_readd_after_full_removal() {
        let mut set = ORSet::new(0);

        // Add, fully remove, re-add
        set.local_add("x", 1);
        let tags = set.get_tags(&"x");
        set.remote_remove("x", &tags, 2);
        assert!(!set.contains(&"x"));

        // Re-add with new tag
        set.local_add("x", 3);
        assert!(set.contains(&"x"));

        // Should have exactly one live tag now
        assert_eq!(set.get_tags(&"x"), vec![3]);
    }

    #[test]
    fn orset_multiple_concurrent_removes() {
        let mut set = ORSet::new(0);

        // Add "x"
        set.local_add("x", 1);

        // Two agents concurrently observe and remove
        let tags = set.get_tags(&"x");
        set.remote_remove("x", &tags, 2);
        set.remote_remove("x", &tags, 3); // Same tags, different remove LV

        // Should still be removed (no duplicate issues)
        assert!(!set.contains(&"x"));
    }

    #[test]
    fn orset_high_concurrency() {
        let mut set = ORSet::new(0);

        // 10 concurrent adds of same element
        for i in 1..=10 {
            set.remote_add("x", i);
        }
        assert!(set.contains(&"x"));
        assert_eq!(set.get_tags(&"x").len(), 10);

        // Remove only observes first 5 tags
        set.remote_remove("x", &[1, 2, 3, 4, 5], 20);

        // Element still present (5 live tags remain)
        assert!(set.contains(&"x"));
        assert_eq!(set.get_tags(&"x").len(), 5);
    }

    #[test]
    fn orset_empty_operations() {
        let set = ORSet::<i32>::new(0);
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert_eq!(set.to_btree_set(), BTreeSet::new());
    }
}
