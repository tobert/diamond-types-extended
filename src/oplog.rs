use std::collections::{BTreeMap, BTreeSet};
use smallvec::smallvec;
use std::cmp::Ordering;
use jumprope::JumpRopeBuf;
use smartstring::alias::String as SmartString;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer};

use rle::{HasLength, SplitableSpanCtx};
use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
use crate::{AgentId, CRDTKind, CreateValue, DTRange, DTValue, OpLog, LV, LVKey, Primitive, RegisterInfo, RegisterState, RegisterValue, ROOT_CRDT_ID, SerializedOps, SerializedOpsOwned, ValPair};
use crate::encoding::bufparser::BufParser;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, write_cg_entry_iter};
use crate::encoding::map::{ReadMap, WriteMap};
use crate::encoding::parseerror::ParseError;
use crate::branch::btree_range_for_crdt;
use crate::frontier::{is_sorted_iter_uniq, is_sorted_slice};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::TextOperation;
use crate::rle::{KVPair, RleSpanHelpers};
use crate::set::{StoredSetOp, SerializedSetOp};

#[cfg(feature = "serde")]
impl Serialize for OpLog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.ops_since(&[]).serialize(serializer)
    }
}

pub(super) fn create_to_snapshot(v: LV, create: &CreateValue) -> RegisterValue {
    match create {
        CreateValue::Primitive(p) => RegisterValue::Primitive(p.clone()),
        CreateValue::NewCRDT(kind) => RegisterValue::OwnedCRDT(*kind, v)
    }
}
// Hmmmm... If this is equivalent, could I just use ValPair() instead of RegisterValue?
impl From<&ValPair> for RegisterValue {
    fn from((version, value): &ValPair) -> Self {
        create_to_snapshot(*version, value)
    }
}

impl OpLog {
    pub(crate) fn dbg_check(&self, deep: bool) {
        self.cg.dbg_check(deep);
        let cg_len = self.cg.len();

        let mut item_type = BTreeMap::new();
        item_type.insert(ROOT_CRDT_ID, CRDTKind::Map);

        // Map operations
        let mut expected_idx_count = 0;
        for ((crdt, key), info) in self.map_keys.iter() {
            // Check the supremum is sorted
            assert!(is_sorted_slice::<true, _>(&info.supremum));

            // Record the type of all the items
            for op in &info.ops {
                match op.1 {
                    CreateValue::Primitive(_) => {}
                    CreateValue::NewCRDT(crdt_type) => {
                        item_type.insert(op.0, crdt_type);
                    }
                }

                assert!(op.0 < cg_len);
            }

            // Check the operations are sorted
            assert!(is_sorted_iter_uniq(info.ops.iter().map(|(v, _)| *v)));

            // Check the index contains the correct items
            for idx in info.supremum.iter() {
                let v = info.ops[*idx].0;
                let (idx_crdt, idx_key) = self.map_index.get(&v).unwrap();
                assert_eq!(idx_crdt, crdt);
                assert_eq!(idx_key, key);
                expected_idx_count += 1;

                if deep {
                    // Check the supremum is correct.
                    let all_versions = info.ops.iter().map(|(v, _)| *v).collect::<Vec<_>>();
                    let dominators = self.cg.graph.find_dominators(&all_versions);

                    let sup_versions = info.supremum.iter().map(|idx| info.ops[*idx].0).collect::<Vec<_>>();
                    assert_eq!(dominators.as_ref(), &sup_versions);
                }
            }
        }
        assert_eq!(self.map_index.len(), expected_idx_count);

        // And now text operations
        let mut expected_idx_count = 0;
        for (crdt, info) in self.texts.iter() {
            assert_ne!(*crdt, ROOT_CRDT_ID);
            assert_eq!(*item_type.get(crdt).unwrap(), CRDTKind::Text);

            // Check the operations are sorted
            assert!(is_sorted_iter_uniq(info.ops.iter().map(|KVPair(v, _)| *v)));

            for v in info.frontier.as_ref() {
                assert!(*v < cg_len);

                let index_crdt = self.text_index.get(v).unwrap();
                assert_eq!(index_crdt, crdt);
                expected_idx_count += 1;
            }

            if deep {
                // Also check the version is correct.
                let all_versions = info.ops.iter().map(|op| op.last()).collect::<Vec<_>>();
                let dominators = self.cg.graph.find_dominators(&all_versions);
                assert_eq!(dominators, info.frontier);
            }
        }
        assert_eq!(self.text_index.len(), expected_idx_count);

        if deep {
            // Find all the CRDTs which have been created then later overwritten or deleted.
            let mut deleted_crdts = BTreeSet::new();
            let mut directly_overwritten_maps = vec![];
            for reg_info in self.map_keys.values() {
                for (idx, (lv, val)) in reg_info.ops.iter().enumerate() {
                    if !reg_info.supremum.contains(&idx) {
                        if let CreateValue::NewCRDT(kind) = val {
                            deleted_crdts.insert(*lv);

                            if *kind == CRDTKind::Map {
                                directly_overwritten_maps.push(*lv);
                            }
                        }
                    }
                }
            }

            // Now find everything that has been removed indirectly
            let mut queue = directly_overwritten_maps;
            while let Some(crdt_id) = queue.pop() {
                for (_, info) in btree_range_for_crdt(&self.map_keys, crdt_id) {
                    for s in info.supremum.iter() {
                        let (lv, create_val) = &info.ops[*s];
                        if let CreateValue::NewCRDT(kind) = create_val {
                            assert!(deleted_crdts.insert(*lv));

                            if *kind == CRDTKind::Map {
                                // Go through this CRDT's children.
                                queue.push(*lv);
                            }
                        }
                    }
                }
            }

            assert_eq!(deleted_crdts, self.deleted_crdts);

            // // Recursively traverse the "alive" data, checking that the deleted_crdts data is
            // // correct.
            //
            // // First lets make a set of all the CRDTs which are "alive".
            // let mut all_crdts: BTreeSet<LV> = self.texts.keys().copied().collect();
            // let mut last_crdt = ROOT_CRDT_ID;
            // for (crdt, _) in self.map_keys.keys() {
            //     if *crdt != last_crdt {
            //         last_crdt = *crdt;
            //         all_crdts.insert(*crdt);
            //     }
            // }
            // dbg!(&all_crdts);
            //
            // // Now recursively walk the map CRDTs looking for items which aren't deleted.
            //
            // let mut dead_crdts = all_crdts;
            // let mut crdt_maps = vec![ROOT_CRDT_ID];
            // dead_crdts.remove(&ROOT_CRDT_ID);
            //
            // // Recursively go through all the "alive" items and remove them from dead_crdts.
            // while let Some(crdt) = crdt_maps.pop() {
            //     for (_, info) in btree_range_for_crdt(&self.map_keys, crdt) {
            //         for s in info.supremum.iter() {
            //             let (lv, create_val) = &info.ops[*s];
            //             if let CreateValue::NewCRDT(kind) = create_val {
            //                 assert!(dead_crdts.remove(lv));
            //                 if *kind == CRDTKind::Map {
            //                     // Go through this CRDT's children.
            //                     crdt_maps.push(*lv);
            //                 }
            //             }
            //         }
            //     }
            // }
        }
    }

    pub fn new() -> Self {
        Default::default()
    }

    // The way I'm using this below, it should be idempotent.
    fn create_child_crdt(&mut self, v: LV, kind: CRDTKind) {
        match kind {
            CRDTKind::Map => {}
            CRDTKind::Register => {
                self.registers.entry(v).or_default();
            }
            CRDTKind::Collection => {}
            CRDTKind::Set => {
                self.sets.entry(v).or_insert_with(|| crate::set::SetInfo::new(v));
            }
            CRDTKind::Text => {
                self.texts.entry(v).or_default();
            }
        }
    }

    fn recursive_mark_deleted_inner(&mut self, mut to_delete: Vec<LV>) {
        while let Some(crdt) = to_delete.pop() {
            for (_, info) in btree_range_for_crdt(&self.map_keys, crdt) {
                for s in info.supremum.iter() {
                    let (lv, create_val) = &info.ops[*s];
                    if let CreateValue::NewCRDT(kind) = create_val {
                        assert!(self.deleted_crdts.insert(*lv));

                        if *kind == CRDTKind::Map {
                            // Go through this CRDT's children.
                            to_delete.push(*lv);
                        }
                    }
                }
            }
        }
    }

    pub fn local_map_set(&mut self, agent: AgentId, crdt: LVKey, key: &str, value: CreateValue) -> LV {
        let v = self.cg.assign_local_op(agent, 1).start;
        if let CreateValue::NewCRDT(kind) = value {
            self.create_child_crdt(v, kind);
        }

        let entry = self.map_keys.entry((crdt, key.into()))
            .or_default();

        let new_idx = entry.ops.len();

        let mut to_delete = vec![];
        // Remove the old supremum from the index
        for idx in &entry.supremum {
            let (lv, val) = &entry.ops[*idx];
            if let CreateValue::NewCRDT(kind) = val {
                assert!(self.deleted_crdts.insert(*lv));
                if *kind == CRDTKind::Map {
                    to_delete.push(*lv);
                }
            }

            self.map_index.remove(lv);
        }

        entry.supremum = smallvec![new_idx];
        entry.ops.push((v, value));

        self.map_index.insert(v, (crdt, key.into()));

        // dbg!((crdt, key, &to_delete));
        self.recursive_mark_deleted_inner(to_delete);
        v
    }

    // This function requires that the lv has already been added to the causal graph.
    pub fn remote_map_set(&mut self, crdt: LVKey, v: LV, key: &str, value: CreateValue) {
        if let CreateValue::NewCRDT(kind) = value {
            self.create_child_crdt(v, kind);
        }

        let entry = self.map_keys.entry((crdt, key.into()))
            .or_default();

        // If the entry already contains the new op, ignore it.
        if entry.ops.binary_search_by_key(&v, |e| e.0).is_ok() {
            return;
        }

        if let Some(last_op) = entry.ops.last() {
            // The added operation must have a higher local version than the last version.
            assert!(last_op.0 < v);
        }

        let new_idx = entry.ops.len();
        entry.ops.push((v, value));

        // The normal case is that the new operation replaces the old value. A faster implementation
        // would special case that and fall back to the more complex version if need be.
        let mut new_sup = smallvec![new_idx];
        self.map_index.insert(v, (crdt, key.into()));
        let mut to_delete = vec![];

        for s_idx in &entry.supremum {
            let (old_lv, old_val) = &entry.ops[*s_idx];
            match self.cg.graph.version_cmp(*old_lv, v) {
                None => {
                    // Versions are concurrent. Leave the old entry in index.
                    new_sup.push(*s_idx);
                }
                Some(Ordering::Less) => {
                    // The most common case. The new version dominates the old version. Remove the
                    // old (version, value) pair.
                    if let CreateValue::NewCRDT(kind) = old_val {
                        assert!(self.deleted_crdts.insert(*old_lv));
                        if *kind == CRDTKind::Map {
                            to_delete.push(*old_lv);
                        }
                    }
                    self.map_index.remove(old_lv);
                }
                Some(_) => {
                    // Either the versions are equal, or the newly inserted version is earlier than
                    // the existing version. Either way, this is an invalid operation.
                    panic!("Invalid state");
                }
            }
        }
        // Sort to maintain supremum invariant (indices must be sorted)
        new_sup.sort_unstable();
        entry.supremum = new_sup;
        self.recursive_mark_deleted_inner(to_delete);
    }

    pub fn local_text_op(&mut self, agent: AgentId, crdt: LVKey, op: TextOperation) -> DTRange {
        let v_range = self.cg.assign_local_op(agent, op.len());

        let entry = self.texts.get_mut(&crdt).unwrap();

        // Remove it from the index
        for v in entry.frontier.as_ref() {
            let old_index_item = self.text_index.remove(v);
            assert!(old_index_item.is_some());
        }

        entry.local_push_op(op, v_range);

        // And add it back to the index.
        self.text_index.insert(v_range.last(), crdt);

        v_range
    }

    pub fn remote_text_op(&mut self, crdt: LVKey, v_range: DTRange, op: TextOperation) {
        // TODO: This doesn't look like it handles discarding existing data...
        debug_assert_eq!(v_range.len(), op.len());

        // What should we do here if the item is missing?
        let entry = self.texts.get_mut(&crdt).unwrap();

        // Remove it from the index
        for v in entry.frontier.as_ref() {
            let old_index_item = self.text_index.remove(v);
            assert!(old_index_item.is_some());
        }

        entry.remote_push_op_unknown_parents(op, v_range, &self.cg.graph);

        // And add it back to the index.
        for v in entry.frontier.as_ref() {
            self.text_index.insert(*v, crdt);
        }
    }

    // ===== OR-Set Operations =====

    /// Add an element to an OR-Set locally.
    ///
    /// Returns the LV (which is also the add-tag) for this operation.
    pub fn local_set_add(&mut self, agent: AgentId, set_id: LVKey, elem: Primitive) -> LV {
        let lv = self.cg.assign_local_op(agent, 1).start;

        let info = self.sets.get_mut(&set_id)
            .expect("Set CRDT not found");
        info.local_add(lv, elem);

        self.set_index.insert(lv, set_id);
        lv
    }

    /// Apply a remote add operation to an OR-Set.
    pub fn remote_set_add(&mut self, set_id: LVKey, lv: LV, elem: Primitive, tag: LV) {
        let info = self.sets.get_mut(&set_id)
            .expect("Set CRDT not found");
        info.remote_add(lv, elem, tag);

        self.set_index.insert(lv, set_id);
    }

    /// Remove an element from an OR-Set locally.
    ///
    /// Returns the tags that were removed (for serialization/replication).
    pub fn local_set_remove(&mut self, agent: AgentId, set_id: LVKey, elem: Primitive) -> (LV, Vec<LV>) {
        let lv = self.cg.assign_local_op(agent, 1).start;

        let info = self.sets.get_mut(&set_id)
            .expect("Set CRDT not found");
        let removed_tags = info.local_remove(lv, elem);

        self.set_index.insert(lv, set_id);
        (lv, removed_tags)
    }

    /// Apply a remote remove operation to an OR-Set.
    pub fn remote_set_remove(&mut self, set_id: LVKey, lv: LV, elem: Primitive, tags: Vec<LV>) {
        let info = self.sets.get_mut(&set_id)
            .expect("Set CRDT not found");
        info.remote_remove(lv, elem, tags);

        self.set_index.insert(lv, set_id);
    }

    // Its quite annoying, but RegisterInfo objects store the supremum as an array of indexes. This
    // returns the active index and (if necessary) the set of indexes of conflicting values.
    pub(crate) fn tie_break_mv<'a>(&self, reg: &'a RegisterInfo) -> (usize, Option<impl Iterator<Item = usize> + 'a>) {
        match reg.supremum.len() {
            0 => panic!("Internal consistency violation"),
            1 => (reg.supremum[0], None),
            _ => {
                let active_idx = reg.supremum.iter()
                    .map(|s| (*s, self.cg.agent_assignment.local_to_agent_version(reg.ops[*s].0)))
                    .max_by(|(_, a), (_, b)| {
                        self.cg.agent_assignment.tie_break_agent_versions(*a, *b)
                    })
                    .unwrap().0;

                (
                    active_idx,
                    Some(reg.supremum.iter().copied().filter(move |i| *i != active_idx))
                )
            }
        }
    }

    fn resolve_mv(&self, reg: &RegisterInfo) -> RegisterValue {
        let (active_idx, _) = self.tie_break_mv(reg);

        let (v, value) = &reg.ops[active_idx];
        create_to_snapshot(*v, value)
    }

    pub fn checkout_text(&self, crdt: LVKey) -> JumpRopeBuf {
        let info = self.texts.get(&crdt).unwrap();

        let mut result = JumpRopeBuf::new();
        info.merge_into(&mut result, &self.cg, &[], self.cg.version.as_ref());
        result
    }

    /// Checkout a standalone register, returning its current state including conflicts.
    pub fn checkout_register(&self, crdt: LVKey) -> RegisterState {
        let info = self.registers.get(&crdt)
            .expect("Register CRDT not found");
        self.get_state_for_register(info)
    }

    /// Checkout an OR-Set, returning the current set of elements.
    pub fn checkout_set(&self, crdt: LVKey) -> BTreeSet<Primitive> {
        let info = self.sets.get(&crdt)
            .expect("Set CRDT not found");
        info.set.to_btree_set()
    }

    pub fn checkout_map(&self, crdt: LVKey) -> BTreeMap<SmartString, Box<DTValue>> {
        let empty_str: SmartString = "".into();
        // dbg!((crdt, empty_str.clone())..(crdt, empty_str));
        let iter = if crdt == ROOT_CRDT_ID {
            // For the root CRDT we can't use the crdt+1 trick because the range wraps around.
            self.map_keys.range((crdt, empty_str)..)
        } else {
            self.map_keys.range((crdt, empty_str.clone())..(crdt + 1, empty_str))
        };

        iter.map(|((_, key), info)| {
            let inner = match self.resolve_mv(info) {
                RegisterValue::Primitive(p) => DTValue::Primitive(p),
                RegisterValue::OwnedCRDT(kind, child_crdt) => {
                    match kind {
                        CRDTKind::Map => DTValue::Map(self.checkout_map(child_crdt)),
                        CRDTKind::Text => DTValue::Text(self.checkout_text(child_crdt).to_string()),
                        CRDTKind::Register => {
                            let reg_state = self.checkout_register(child_crdt);
                            // Convert the register's value to DTValue
                            let inner = match reg_state.value {
                                RegisterValue::Primitive(p) => DTValue::Primitive(p),
                                RegisterValue::OwnedCRDT(inner_kind, inner_id) => {
                                    match inner_kind {
                                        CRDTKind::Map => DTValue::Map(self.checkout_map(inner_id)),
                                        CRDTKind::Text => DTValue::Text(self.checkout_text(inner_id).to_string()),
                                        CRDTKind::Set => DTValue::Set(self.checkout_set(inner_id)),
                                        _ => DTValue::Primitive(Primitive::Nil), // Fallback
                                    }
                                }
                            };
                            DTValue::Register(Box::new(inner))
                        }
                        CRDTKind::Set => DTValue::Set(self.checkout_set(child_crdt)),
                        CRDTKind::Collection => unimplemented!("Collection checkout"),
                    }
                }
            };
            (key.clone(), Box::new(inner))
        }).collect()
    }

    pub fn checkout(&self) -> BTreeMap<SmartString, Box<DTValue>> {
        self.checkout_map(ROOT_CRDT_ID)
    }

    /// Try to navigate to a CRDT at the given path. Returns None if path doesn't exist.
    pub fn try_crdt_at_path(&self, path: &[&str]) -> Option<(CRDTKind, LVKey)> {
        let mut kind = CRDTKind::Map;
        let mut key = ROOT_CRDT_ID;

        for p in path {
            match kind {
                CRDTKind::Map => {
                    let container = self.map_keys.get(&(key, (*p).into()))?;
                    match self.resolve_mv(container) {
                        RegisterValue::Primitive(_) => {
                            return None; // Found primitive, not CRDT
                        }
                        RegisterValue::OwnedCRDT(new_kind, new_key) => {
                            kind = new_kind;
                            key = new_key;
                        }
                    }
                }
                _ => {
                    return None; // Can't navigate deeper into non-map
                }
            }
        }

        Some((kind, key))
    }

    /// Navigate to a CRDT at the given path. Panics if path doesn't exist.
    pub fn crdt_at_path(&self, path: &[&str]) -> (CRDTKind, LVKey) {
        self.try_crdt_at_path(path)
            .expect("Path should exist in document")
    }

    /// Try to navigate to a text CRDT at the given path. Returns None if path doesn't exist or isn't Text.
    pub fn try_text_at_path(&self, path: &[&str]) -> Option<LVKey> {
        let (kind, key) = self.try_crdt_at_path(path)?;
        if kind == CRDTKind::Text { Some(key) } else { None }
    }

    /// Navigate to a text CRDT at the given path. Panics if path doesn't exist or isn't Text.
    pub fn text_at_path(&self, path: &[&str]) -> LVKey {
        self.try_text_at_path(path)
            .expect("Path should exist and be a Text CRDT")
    }

    pub fn text_changes_since(&self, text: LVKey, since_frontier: &[LV]) -> Vec<(DTRange, Option<TextOperation>)> {
        let info = self.texts.get(&text).unwrap();
        info.xf_operations_from(&self.cg, since_frontier, self.cg.version.as_ref())
    }
}

impl OpLog {
    fn crdt_name_to_remote(&self, crdt: LVKey) -> RemoteVersion<'_> {
        if crdt == ROOT_CRDT_ID {
            RemoteVersion("ROOT", 0)
        } else {
            self.cg.agent_assignment.local_to_remote_version(crdt)
        }
    }

    fn remote_to_crdt_name(&self, crdt_rv: RemoteVersion) -> LVKey {
        if crdt_rv.0 == "ROOT" { ROOT_CRDT_ID }
        else { self.cg.agent_assignment.remote_to_local_version(crdt_rv) }
    }

    // pub fn xf_text_changes_since(&self, text_item: LVKey, since_frontier: &[LV]) {
    //     let crdt = self.texts.get(&text_item).unwrap();
    //
    //     crdt.iter_xf_operations_from(&sel)
    // }

    pub fn ops_since(&self, since_frontier: &[LV]) -> SerializedOps<'_> {
        let mut write_map = WriteMap::with_capacity_from(&self.cg.agent_assignment.client_data);

        let diff_rev = self.cg.diff_since_rev(since_frontier);
        // let bump = Bump::new();
        // let mut result = bumpalo::collections::Vec::new_in(&bump);
        let mut cg_changes = Vec::new();
        let mut text_crdts_to_send = BTreeSet::new();
        let mut map_crdts_to_send = BTreeSet::new();
        let mut set_crdts_to_send = BTreeSet::new();
        for range_rev in diff_rev.iter() {
            let iter = self.cg.iter_range(*range_rev);
            write_cg_entry_iter(&mut cg_changes, iter, &mut write_map, &self.cg);

            for (_, text_crdt) in self.text_index.range(*range_rev) {
                text_crdts_to_send.insert(*text_crdt);
            }

            for (_, (map_crdt, key)) in self.map_index.range(*range_rev) {
                // dbg!(map_crdt, key);
                map_crdts_to_send.insert((*map_crdt, key));
            }

            for (_, set_crdt) in self.set_index.range(*range_rev) {
                set_crdts_to_send.insert(*set_crdt);
            }
        }

        // Serialize map operations
        let mut map_ops = Vec::new();
        for (crdt, key) in map_crdts_to_send {
            let crdt_name = self.crdt_name_to_remote(crdt);
            let entry = self.map_keys.get(&(crdt, key.clone()))
                .unwrap();
            for r in diff_rev.iter() {
                // Find all the unknown ops.
                // TODO: Add a flag to trim this to only the most recent ops.
                let start_idx = entry.ops
                    .binary_search_by_key(&r.start, |e| e.0)
                    .unwrap_or_else(|idx| idx);

                for pair in &entry.ops[start_idx..] {
                    if pair.0 >= r.end { break; }

                    // dbg!(pair);
                    let rv = self.cg.agent_assignment.local_to_remote_version(pair.0);
                    map_ops.push((crdt_name, rv, key.as_str(), pair.1.clone()));
                }
            }
        }

        // Serialize text operations
        let mut text_context = ListOperationCtx::new();
        let mut text_ops = Vec::new();
        for crdt in text_crdts_to_send {
            let crdt_name = self.crdt_name_to_remote(crdt);
            let info = &self.texts[&crdt];
            for r in diff_rev.iter() {
                for KVPair(lv, op) in info.ops.iter_range_ctx(*r, &info.ctx) {
                    // dbg!(&op);

                    let op_out = ListOpMetrics {
                        loc: op.loc,
                        kind: op.kind,
                        content_pos: op.content_pos.map(|content_pos| {
                            let content = info.ctx.get_str(op.kind, content_pos);
                            text_context.push_str(op.kind, content)
                        }),
                    };
                    let rv = self.cg.agent_assignment.local_to_remote_version(lv);
                    text_ops.push((crdt_name, rv, op_out));
                }
            }
        }

        // Serialize set operations, converting LV tags to RemoteVersions
        let mut set_ops = Vec::new();
        for crdt in set_crdts_to_send {
            let crdt_name = self.crdt_name_to_remote(crdt);
            let info = &self.sets[&crdt];
            for r in diff_rev.iter() {
                for (lv, op) in info.ops_in_range(r.start, r.end) {
                    let rv = self.cg.agent_assignment.local_to_remote_version(*lv);
                    // Convert StoredSetOp to SerializedSetOp
                    let serialized_op = match op {
                        StoredSetOp::Add { value, tag: _ } => {
                            // Tag is implicit (same as rv)
                            SerializedSetOp::Add { value: value.clone() }
                        }
                        StoredSetOp::Remove { value, tags } => {
                            // Convert LV tags to RemoteVersions
                            let remote_tags: Vec<_> = tags.iter()
                                .map(|tag| self.cg.agent_assignment.local_to_remote_version(*tag).to_owned())
                                .collect();
                            SerializedSetOp::Remove { value: value.clone(), tags: remote_tags }
                        }
                    };
                    set_ops.push((crdt_name, rv, serialized_op));
                }
            }
        }

        SerializedOps {
            cg_changes,
            map_ops,
            text_ops,
            text_context,
            set_ops,
        }
    }


    pub fn merge_ops(&mut self, changes: SerializedOps) -> Result<DTRange, ParseError> {
        let mut read_map = ReadMap::new();

        let old_end = self.cg.len();

        let mut buf = BufParser(&changes.cg_changes);
        while !buf.is_empty() {
            read_cg_entry_into_cg(&mut buf, true, &mut self.cg, &mut read_map)?;
        }

        let new_end = self.cg.len();
        let new_range: DTRange = (old_end..new_end).into();

        // The code above will discard any operations we already know about. The new range could be empty, could
        // contain all of the new changes, or have some subset of them. We need to respect that in the code below
        // and only append new changes.
        if new_range.is_empty() { return Ok(new_range); }

        for (crdt_r_name, rv, key, val) in changes.map_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version(rv);
            if new_range.contains(lv) {
                let crdt_id = self.remote_to_crdt_name(crdt_r_name);
                // dbg!(crdt_id, lv, key, val);
                self.remote_map_set(crdt_id, lv, key, val);
            }
        }

        for (crdt_r_name, rv, mut op_metrics) in changes.text_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version(rv);
            let mut v_range: DTRange = (lv..lv + op_metrics.len()).into();

            if v_range.end <= new_range.start { continue; }
            else if v_range.start < new_range.start {
                // Trim the new operation.
                op_metrics.truncate_keeping_right_ctx(new_range.start - v_range.start, &changes.text_context);
                v_range.start = new_range.start;
            }

            let crdt_id = self.remote_to_crdt_name(crdt_r_name);

            let op = op_metrics.to_operation(&changes.text_context);
            self.remote_text_op(crdt_id, v_range, op);
        }

        // Deserialize set operations, converting RemoteVersion tags back to LVs
        for (crdt_r_name, rv, set_op) in changes.set_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version(rv);
            if new_range.contains(lv) {
                let crdt_id = self.remote_to_crdt_name(crdt_r_name);
                match set_op {
                    SerializedSetOp::Add { value } => {
                        // The tag for an Add is the operation's LV
                        self.remote_set_add(crdt_id, lv, value, lv);
                    }
                    SerializedSetOp::Remove { value, tags } => {
                        // Convert RemoteVersion tags back to local LVs
                        let local_tags: Vec<_> = tags.iter()
                            .map(|rv| self.cg.agent_assignment.remote_to_local_version(RemoteVersion::from(rv)))
                            .collect();
                        self.remote_set_remove(crdt_id, lv, value, local_tags);
                    }
                }
            }
        }

        Ok(new_range)
    }

    /// Merge ops from an owned serialized ops struct (for cross-thread communication)
    pub fn merge_ops_owned(&mut self, changes: SerializedOpsOwned) -> Result<DTRange, ParseError> {
        let mut read_map = ReadMap::new();

        let old_end = self.cg.len();

        let mut buf = BufParser(&changes.cg_changes);
        while !buf.is_empty() {
            read_cg_entry_into_cg(&mut buf, true, &mut self.cg, &mut read_map)?;
        }

        let new_end = self.cg.len();
        let new_range: DTRange = (old_end..new_end).into();

        if new_range.is_empty() { return Ok(new_range); }

        for (crdt_r_name, rv, key, val) in changes.map_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version((&rv).into());
            if new_range.contains(lv) {
                let crdt_id = self.remote_to_crdt_name((&crdt_r_name).into());
                self.remote_map_set(crdt_id, lv, &key, val);
            }
        }

        for (crdt_r_name, rv, mut op_metrics) in changes.text_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version((&rv).into());
            let mut v_range: DTRange = (lv..lv + op_metrics.len()).into();

            if v_range.end <= new_range.start { continue; }
            else if v_range.start < new_range.start {
                op_metrics.truncate_keeping_right_ctx(new_range.start - v_range.start, &changes.text_context);
                v_range.start = new_range.start;
            }

            let crdt_id = self.remote_to_crdt_name((&crdt_r_name).into());

            let op = op_metrics.to_operation(&changes.text_context);
            self.remote_text_op(crdt_id, v_range, op);
        }

        for (crdt_r_name, rv, set_op) in changes.set_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version((&rv).into());
            if new_range.contains(lv) {
                let crdt_id = self.remote_to_crdt_name((&crdt_r_name).into());
                match set_op {
                    SerializedSetOp::Add { value } => {
                        self.remote_set_add(crdt_id, lv, value, lv);
                    }
                    SerializedSetOp::Remove { value, tags } => {
                        let local_tags: Vec<_> = tags.iter()
                            .map(|rv| self.cg.agent_assignment.remote_to_local_version(rv.into()))
                            .collect();
                        self.remote_set_remove(crdt_id, lv, value, local_tags);
                    }
                }
            }
        }

        Ok(new_range)
    }

    pub fn xf_text_changes_since(&self, text_crdt: LVKey, since: &[LV]) -> Vec<(DTRange, Option<TextOperation>)> {
        let textinfo = self.texts.get(&text_crdt).unwrap();
        textinfo.xf_operations_from(&self.cg, since, textinfo.frontier.as_ref())
    }
}


#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use crate::{CRDTKind, CreateValue, OpLog, Primitive, ROOT_CRDT_ID, SerializedOps};
    use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
    use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
    use crate::list::operation::TextOperation;

    #[test]
    fn smoke() {
        let mut oplog = OpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(321)));

        dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn text() {
        let mut oplog = OpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        let text = oplog.local_map_set(seph, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text, TextOperation::new_insert(0, "Oh hai!"));
        oplog.local_text_op(seph, text, TextOperation::new_delete(0..3));

        let title = oplog.local_map_set(seph, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, title, TextOperation::new_insert(0, "Please read this cool info"));

        // dbg!(&oplog);

        assert_eq!(oplog.checkout_text(text).to_string(), "hai!");
        oplog.dbg_check(true);

        // dbg!(oplog.checkout());

        // dbg!(oplog.changes_since(&[]));
        // dbg!(oplog.changes_since(&[title]));


        let c = oplog.ops_since(&[]);
        let mut oplog_2 = OpLog::new();
        oplog_2.merge_ops(c).unwrap();
        assert_eq!(oplog_2.cg, oplog.cg);
        // dbg!(oplog_2)
        // dbg!(oplog_2.checkout());
        oplog_2.dbg_check(true);

        assert_eq!(oplog.checkout(), oplog_2.checkout());
    }

    #[test]
    fn concurrent_changes() {
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();


        let seph = oplog1.cg.get_or_create_agent_id("seph");
        let text = oplog1.local_map_set(seph, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));
        oplog1.local_text_op(seph, text, TextOperation::new_insert(0, "Oh hai!"));


        let kaarina = oplog2.cg.get_or_create_agent_id("kaarina");
        let title = oplog2.local_map_set(kaarina, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        oplog2.local_text_op(kaarina, title, TextOperation::new_insert(0, "Better keep it clean"));


        // let c = oplog1.changes_since(&[]);
        // dbg!(serde_json::to_string(&c).unwrap());
        // let c = oplog2.changes_since(&[]);
        // dbg!(serde_json::to_string(&c).unwrap());

        oplog2.merge_ops(oplog1.ops_since(&[])).unwrap();
        oplog2.dbg_check(true);

        oplog1.merge_ops(oplog2.ops_since(&[])).unwrap();
        oplog1.dbg_check(true);

        // dbg!(oplog1.checkout());
        // dbg!(oplog2.checkout());
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        dbg!(oplog1.crdt_at_path(&["title"]));
    }

    #[test]
    fn checkout() {
        let mut oplog = OpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        let map = oplog.local_map_set(seph, ROOT_CRDT_ID, "yo", CreateValue::NewCRDT(CRDTKind::Map));
        oplog.local_map_set(seph, map, "yo", CreateValue::Primitive(Primitive::Str("blah".into())));

        dbg!(oplog.checkout());
        oplog.dbg_check(true);
    }

    #[test]
    fn overwrite_local() {
        let mut oplog = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id("seph");

        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Map));
        let text_item = oplog.local_map_set(seph, child_obj, "text_item", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "yooo"));
        oplog.local_map_set(seph, child_obj, "smol_embedded", CreateValue::NewCRDT(CRDTKind::Map));

        // Now overwrite the parent item.
        oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::Primitive(Primitive::I64(123)));

        // dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn overwrite_remote() {
        let mut oplog = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id("seph");

        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Map));
        let text_item = oplog.local_map_set(seph, child_obj, "text_item", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "yooo"));
        oplog.local_map_set(seph, child_obj, "smol_embedded", CreateValue::NewCRDT(CRDTKind::Map));

        // Now overwrite the parent item with a remote operation.
        let lv = oplog.cg.assign_local_op(seph, 1).start;
        oplog.remote_map_set(ROOT_CRDT_ID, lv, "overwritten", CreateValue::Primitive(Primitive::I64(123)));

        oplog.dbg_check(true);
    }

    #[test]
    fn overlapping_updates() {
        // Regression.
        let mut oplog = OpLog::new();
        let mut oplog2 = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id("seph");

        let text_item = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "a"));

        let partial_update = oplog.ops_since(&[]);
        oplog2.merge_ops(partial_update).unwrap();

        oplog.local_text_op(seph, text_item, TextOperation::new_insert(1, "b"));
        let full_update = oplog.ops_since(&[]);

        oplog2.merge_ops(full_update).unwrap();
    }



    // ===== Multi-CRDT Integration Tests =====

    #[test]
    fn multi_crdt_replication() {
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");

        // Alice: creates nested structure
        let user = oplog1.local_map_set(alice, ROOT_CRDT_ID, "user",
            CreateValue::NewCRDT(CRDTKind::Map));
        oplog1.local_map_set(alice, user, "name",
            CreateValue::Primitive(Primitive::Str("Alice".into())));
        let bio = oplog1.local_map_set(alice, user, "bio",
            CreateValue::NewCRDT(CRDTKind::Text));
        oplog1.local_text_op(alice, bio, TextOperation::new_insert(0, "Hello!"));

        // Bob: makes concurrent changes
        oplog2.cg.get_or_create_agent_id("alice"); // Know about Alice
        let settings = oplog2.local_map_set(bob, ROOT_CRDT_ID, "settings",
            CreateValue::NewCRDT(CRDTKind::Map));
        oplog2.local_map_set(bob, settings, "theme",
            CreateValue::Primitive(Primitive::Str("dark".into())));

        // Exchange changes
        oplog1.merge_ops(oplog2.ops_since(&[])).unwrap();
        oplog2.merge_ops(oplog1.ops_since(&[])).unwrap();

        // Verify convergence
        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        // Verify structure
        let checkout = oplog1.checkout();
        assert!(checkout.contains_key("user"));
        assert!(checkout.contains_key("settings"));
    }

    #[test]
    fn standalone_register_nested_in_map() {
        let mut oplog = OpLog::new();
        let alice = oplog.cg.get_or_create_agent_id("alice");

        // Create a register inside the root map
        let counter_reg = oplog.local_map_set(alice, ROOT_CRDT_ID, "counter",
            CreateValue::NewCRDT(CRDTKind::Register));

        // Verify the register CRDT was created in storage
        assert!(oplog.registers.contains_key(&counter_reg));

        // Consistency check passes
        oplog.dbg_check(true);

        // Note: checkout_register requires the register to have at least one value set.
        // This is a design decision - empty registers created as nested CRDTs
        // don't have values until an operation is applied to them.
        // For now, we just verify the storage is set up correctly.
    }

    #[test]
    fn set_crdt_creation() {
        let mut oplog = OpLog::new();
        let alice = oplog.cg.get_or_create_agent_id("alice");

        // Create a set inside the root map
        let tags_set = oplog.local_map_set(alice, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));

        // Verify the set CRDT was created
        assert!(oplog.sets.contains_key(&tags_set));

        // Checkout should work
        oplog.dbg_check(true);
        let _branch = oplog.checkout();

        // Verify checkout_set works (returns empty set initially)
        let set_contents = oplog.checkout_set(tags_set);
        assert!(set_contents.is_empty());
    }

    #[test]
    fn set_replication_basic() {
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");

        // Create a set and add elements
        let tags = oplog1.local_map_set(alice, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, tags, Primitive::Str("rust".into()));
        oplog1.local_set_add(alice, tags, Primitive::Str("crdt".into()));
        oplog1.local_set_add(alice, tags, Primitive::I64(42));

        // Verify local state
        let set1 = oplog1.checkout_set(tags);
        assert_eq!(set1.len(), 3);
        assert!(set1.contains(&Primitive::Str("rust".into())));
        assert!(set1.contains(&Primitive::Str("crdt".into())));
        assert!(set1.contains(&Primitive::I64(42)));

        // Replicate to oplog2
        let changes = oplog1.ops_since(&[]);
        oplog2.merge_ops(changes).unwrap();

        // Verify replicated state
        oplog2.dbg_check(true);
        let (_, tags2) = oplog2.crdt_at_path(&["tags"]);
        let set2 = oplog2.checkout_set(tags2);
        assert_eq!(set1, set2);
    }

    #[test]
    fn set_replication_with_removes() {
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");

        // Create a set, add elements, then remove some
        let tags = oplog1.local_map_set(alice, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, tags, Primitive::Str("a".into()));
        oplog1.local_set_add(alice, tags, Primitive::Str("b".into()));
        oplog1.local_set_add(alice, tags, Primitive::Str("c".into()));

        // Remove "b"
        oplog1.local_set_remove(alice, tags, Primitive::Str("b".into()));

        // Verify local state after remove
        let set1 = oplog1.checkout_set(tags);
        assert_eq!(set1.len(), 2);
        assert!(set1.contains(&Primitive::Str("a".into())));
        assert!(!set1.contains(&Primitive::Str("b".into())));
        assert!(set1.contains(&Primitive::Str("c".into())));

        // Replicate to oplog2
        let changes = oplog1.ops_since(&[]);
        oplog2.merge_ops(changes).unwrap();

        // Verify replicated state matches
        oplog2.dbg_check(true);
        let (_, tags2) = oplog2.crdt_at_path(&["tags"]);
        let set2 = oplog2.checkout_set(tags2);
        assert_eq!(set1, set2);
    }

    #[test]
    fn set_concurrent_adds() {
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");

        // Alice creates a set
        let tags = oplog1.local_map_set(alice, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, tags, Primitive::Str("alice-tag".into()));

        // Sync set creation to Bob before concurrent ops
        oplog2.merge_ops(oplog1.ops_since(&[])).unwrap();
        let (_, tags2) = oplog2.crdt_at_path(&["tags"]);

        // Now make concurrent additions
        oplog1.local_set_add(alice, tags, Primitive::Str("concurrent-a".into()));
        oplog2.local_set_add(bob, tags2, Primitive::Str("concurrent-b".into()));

        // Exchange changes
        oplog1.merge_ops(oplog2.ops_since(&[])).unwrap();
        oplog2.merge_ops(oplog1.ops_since(&[])).unwrap();

        // Both should converge with all elements
        oplog1.dbg_check(true);
        oplog2.dbg_check(true);

        let set1 = oplog1.checkout_set(tags);
        let set2 = oplog2.checkout_set(tags2);
        assert_eq!(set1, set2);
        assert_eq!(set1.len(), 3);
        assert!(set1.contains(&Primitive::Str("alice-tag".into())));
        assert!(set1.contains(&Primitive::Str("concurrent-a".into())));
        assert!(set1.contains(&Primitive::Str("concurrent-b".into())));
    }

    // ===== Concurrent Operations Test Suite =====
    //
    // These tests exercise CRDT semantics under concurrent operations
    // from multiple peers, verifying convergence after sync.

    use crate::DTValue;

    /// Helper to sync two oplogs bidirectionally
    fn sync_oplogs(a: &mut OpLog, b: &mut OpLog) {
        // Sync A -> B, then B -> A
        // Order matters: second merge may include changes from first
        a.merge_ops(b.ops_since(&[])).unwrap();
        b.merge_ops(a.ops_since(&[])).unwrap();
    }

    /// Helper to sync three oplogs (all pairs)
    fn sync_three_oplogs(a: &mut OpLog, b: &mut OpLog, c: &mut OpLog) {
        // Round 1: A <-> B
        a.merge_ops(b.ops_since(&[])).unwrap();
        b.merge_ops(a.ops_since(&[])).unwrap();

        // Round 2: A <-> C (A now has B's changes)
        a.merge_ops(c.ops_since(&[])).unwrap();
        c.merge_ops(a.ops_since(&[])).unwrap();

        // Round 3: B <-> C (to ensure B gets C's original changes)
        b.merge_ops(c.ops_since(&[])).unwrap();
        c.merge_ops(b.ops_since(&[])).unwrap();
    }

    #[test]
    fn concurrent_map_same_key_lww() {
        // Two peers write to the same key concurrently
        // LWW semantics: higher agent ID wins ties
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");

        // Both know about each other
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Concurrent writes to same key
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "color",
            CreateValue::Primitive(Primitive::Str("red".into())));
        oplog2.local_map_set(bob, ROOT_CRDT_ID, "color",
            CreateValue::Primitive(Primitive::Str("blue".into())));

        // Sync
        sync_oplogs(&mut oplog1, &mut oplog2);

        // Should converge
        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        // One value wins (LWW based on agent ordering)
        let checkout = oplog1.checkout();
        let color = checkout.get("color").unwrap().as_ref();
        match color {
            DTValue::Primitive(Primitive::Str(s)) => {
                assert!(s == "red" || s == "blue", "Unexpected color: {}", s);
            }
            _ => panic!("Expected Primitive::Str, got {:?}", color),
        }
    }

    #[test]
    fn concurrent_map_different_keys() {
        // Two peers write to different keys concurrently
        // Both writes should be preserved
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");

        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Concurrent writes to different keys
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "name",
            CreateValue::Primitive(Primitive::Str("Alice".into())));
        oplog2.local_map_set(bob, ROOT_CRDT_ID, "age",
            CreateValue::Primitive(Primitive::I64(30)));

        sync_oplogs(&mut oplog1, &mut oplog2);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        // Both keys present
        let checkout = oplog1.checkout();
        assert!(checkout.contains_key("name"));
        assert!(checkout.contains_key("age"));
    }

    #[test]
    fn concurrent_map_overwrite_sequence() {
        // Multiple rounds of concurrent overwrites
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Round 1: concurrent writes
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "x",
            CreateValue::Primitive(Primitive::I64(1)));
        oplog2.local_map_set(bob, ROOT_CRDT_ID, "x",
            CreateValue::Primitive(Primitive::I64(2)));
        sync_oplogs(&mut oplog1, &mut oplog2);

        // Round 2: more concurrent writes
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "x",
            CreateValue::Primitive(Primitive::I64(3)));
        oplog2.local_map_set(bob, ROOT_CRDT_ID, "x",
            CreateValue::Primitive(Primitive::I64(4)));
        sync_oplogs(&mut oplog1, &mut oplog2);

        // Round 3: one more
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "x",
            CreateValue::Primitive(Primitive::I64(5)));
        oplog2.local_map_set(bob, ROOT_CRDT_ID, "x",
            CreateValue::Primitive(Primitive::I64(6)));
        sync_oplogs(&mut oplog1, &mut oplog2);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());
    }

    #[test]
    fn concurrent_set_add_wins() {
        // OR-Set add-wins semantics: concurrent add + remove = element present
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Alice creates set and adds element
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "items",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, set_id, Primitive::Str("x".into()));

        // Sync so Bob has the set with "x"
        sync_oplogs(&mut oplog1, &mut oplog2);
        let (_, set_id2) = oplog2.crdt_at_path(&["items"]);

        // Now concurrent: Alice adds "x" again, Bob removes "x"
        oplog1.local_set_add(alice, set_id, Primitive::Str("x".into()));
        oplog2.local_set_remove(bob, set_id2, Primitive::Str("x".into()));

        sync_oplogs(&mut oplog1, &mut oplog2);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);

        // Add-wins: "x" should be present because Alice's concurrent add survives
        let set1 = oplog1.checkout_set(set_id);
        let set2 = oplog2.checkout_set(set_id2);
        assert_eq!(set1, set2);
        assert!(set1.contains(&Primitive::Str("x".into())),
            "Add-wins failed: element should be present after concurrent add+remove");
    }

    #[test]
    fn concurrent_set_remove_observed_add() {
        // Remove that observes an add should remove it
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Alice creates set and adds element
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "items",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, set_id, Primitive::Str("x".into()));

        // Sync - Bob observes the add
        sync_oplogs(&mut oplog1, &mut oplog2);
        let (_, set_id2) = oplog2.crdt_at_path(&["items"]);

        // Bob removes (having observed Alice's add)
        oplog2.local_set_remove(bob, set_id2, Primitive::Str("x".into()));

        // Sync
        sync_oplogs(&mut oplog1, &mut oplog2);

        // Element should be gone (remove observed the add)
        let set1 = oplog1.checkout_set(set_id);
        let set2 = oplog2.checkout_set(set_id2);
        assert_eq!(set1, set2);
        assert!(!set1.contains(&Primitive::Str("x".into())),
            "Element should be removed when remove observes the add");
    }

    #[test]
    fn concurrent_set_multiple_adds_same_element() {
        // Multiple peers concurrently add the same element
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();
        let mut oplog3 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        let carol = oplog3.cg.get_or_create_agent_id("carol");

        // Everyone knows everyone
        for oplog in [&mut oplog1, &mut oplog2, &mut oplog3] {
            oplog.cg.get_or_create_agent_id("alice");
            oplog.cg.get_or_create_agent_id("bob");
            oplog.cg.get_or_create_agent_id("carol");
        }

        // Alice creates set
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));

        // Sync set creation
        sync_three_oplogs(&mut oplog1, &mut oplog2, &mut oplog3);
        let (_, set_id2) = oplog2.crdt_at_path(&["tags"]);
        let (_, set_id3) = oplog3.crdt_at_path(&["tags"]);

        // All three concurrently add "important"
        oplog1.local_set_add(alice, set_id, Primitive::Str("important".into()));
        oplog2.local_set_add(bob, set_id2, Primitive::Str("important".into()));
        oplog3.local_set_add(carol, set_id3, Primitive::Str("important".into()));

        sync_three_oplogs(&mut oplog1, &mut oplog2, &mut oplog3);

        // All should converge with element present
        let set1 = oplog1.checkout_set(set_id);
        let set2 = oplog2.checkout_set(set_id2);
        let set3 = oplog3.checkout_set(set_id3);

        assert_eq!(set1, set2);
        assert_eq!(set2, set3);
        assert!(set1.contains(&Primitive::Str("important".into())));
    }

    #[test]
    fn concurrent_set_partial_remove() {
        // Remove only sees some of the concurrent adds
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();
        let mut oplog3 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        let carol = oplog3.cg.get_or_create_agent_id("carol");

        for oplog in [&mut oplog1, &mut oplog2, &mut oplog3] {
            oplog.cg.get_or_create_agent_id("alice");
            oplog.cg.get_or_create_agent_id("bob");
            oplog.cg.get_or_create_agent_id("carol");
        }

        // Alice creates set and adds element
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "items",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, set_id, Primitive::Str("x".into()));

        // Sync to Bob only (Carol doesn't see Alice's add yet)
        let alice_ops = oplog1.ops_since(&[]);
        oplog2.merge_ops(alice_ops).unwrap();
        let (_, set_id2) = oplog2.crdt_at_path(&["items"]);

        // Bob removes "x" (observing Alice's add)
        oplog2.local_set_remove(bob, set_id2, Primitive::Str("x".into()));

        // Carol gets the set and concurrently adds "x"
        let alice_ops = oplog1.ops_since(&[]);
        oplog3.merge_ops(alice_ops).unwrap();
        let (_, set_id3) = oplog3.crdt_at_path(&["items"]);
        oplog3.local_set_add(carol, set_id3, Primitive::Str("x".into()));

        // Now sync everyone
        sync_three_oplogs(&mut oplog1, &mut oplog2, &mut oplog3);

        // Element should be present (Carol's add wasn't observed by Bob's remove)
        let set1 = oplog1.checkout_set(set_id);
        let set2 = oplog2.checkout_set(set_id2);
        let set3 = oplog3.checkout_set(set_id3);

        assert_eq!(set1, set2);
        assert_eq!(set2, set3);
        assert!(set1.contains(&Primitive::Str("x".into())),
            "Element should survive: Carol's add wasn't observed by Bob's remove");
    }

    #[test]
    fn concurrent_nested_crdt_creation() {
        // Two peers concurrently create nested CRDTs at same key
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Both create a nested map at "data"
        let map1 = oplog1.local_map_set(alice, ROOT_CRDT_ID, "data",
            CreateValue::NewCRDT(CRDTKind::Map));
        let map2 = oplog2.local_map_set(bob, ROOT_CRDT_ID, "data",
            CreateValue::NewCRDT(CRDTKind::Map));

        // Each writes to their local nested map
        oplog1.local_map_set(alice, map1, "alice_key",
            CreateValue::Primitive(Primitive::I64(1)));
        oplog2.local_map_set(bob, map2, "bob_key",
            CreateValue::Primitive(Primitive::I64(2)));

        sync_oplogs(&mut oplog1, &mut oplog2);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());
    }

    #[test]
    fn concurrent_mixed_crdt_operations() {
        // Concurrent operations across different CRDT types
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Alice creates a set
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, set_id, Primitive::Str("a".into()));

        // Bob creates a text
        let text_id = oplog2.local_map_set(bob, ROOT_CRDT_ID, "content",
            CreateValue::NewCRDT(CRDTKind::Text));
        oplog2.local_text_op(bob, text_id, TextOperation::new_insert(0, "Hello"));

        // Sync
        sync_oplogs(&mut oplog1, &mut oplog2);

        // Both add more
        oplog1.local_set_add(alice, set_id, Primitive::Str("b".into()));
        let (_, text_id2) = oplog2.crdt_at_path(&["content"]);
        oplog2.local_text_op(bob, text_id2, TextOperation::new_insert(5, " World"));

        sync_oplogs(&mut oplog1, &mut oplog2);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        // Verify contents
        let checkout = oplog1.checkout();
        assert!(checkout.contains_key("tags"));
        assert!(checkout.contains_key("content"));
    }

    #[test]
    fn three_way_concurrent_map_writes() {
        // Three peers all write to same key concurrently
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();
        let mut oplog3 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        let carol = oplog3.cg.get_or_create_agent_id("carol");

        for oplog in [&mut oplog1, &mut oplog2, &mut oplog3] {
            oplog.cg.get_or_create_agent_id("alice");
            oplog.cg.get_or_create_agent_id("bob");
            oplog.cg.get_or_create_agent_id("carol");
        }

        // All three write to "winner" concurrently
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "winner",
            CreateValue::Primitive(Primitive::Str("alice".into())));
        oplog2.local_map_set(bob, ROOT_CRDT_ID, "winner",
            CreateValue::Primitive(Primitive::Str("bob".into())));
        oplog3.local_map_set(carol, ROOT_CRDT_ID, "winner",
            CreateValue::Primitive(Primitive::Str("carol".into())));

        sync_three_oplogs(&mut oplog1, &mut oplog2, &mut oplog3);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        oplog3.dbg_check(true);

        // All must converge to same value
        assert_eq!(oplog1.checkout(), oplog2.checkout());
        assert_eq!(oplog2.checkout(), oplog3.checkout());
    }

    #[test]
    fn concurrent_operations_with_delayed_sync() {
        // Operations accumulate before sync
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Alice does many operations
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "numbers",
            CreateValue::NewCRDT(CRDTKind::Set));
        for i in 0..10 {
            oplog1.local_set_add(alice, set_id, Primitive::I64(i));
        }
        oplog1.local_map_set(alice, ROOT_CRDT_ID, "count",
            CreateValue::Primitive(Primitive::I64(10)));

        // Bob does many operations independently
        let text_id = oplog2.local_map_set(bob, ROOT_CRDT_ID, "log",
            CreateValue::NewCRDT(CRDTKind::Text));
        oplog2.local_text_op(bob, text_id, TextOperation::new_insert(0, "Line 1\n"));
        oplog2.local_text_op(bob, text_id, TextOperation::new_insert(7, "Line 2\n"));
        oplog2.local_text_op(bob, text_id, TextOperation::new_insert(14, "Line 3\n"));

        // Big sync
        sync_oplogs(&mut oplog1, &mut oplog2);

        oplog1.dbg_check(true);
        oplog2.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        // Verify all data present
        let checkout = oplog1.checkout();
        assert!(checkout.contains_key("numbers"));
        assert!(checkout.contains_key("count"));
        assert!(checkout.contains_key("log"));
    }

    #[test]
    fn serialization_roundtrip_concurrent_ops() {
        // Verify ops_since/merge_ops works correctly with concurrent operations
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();
        let mut oplog3 = OpLog::new(); // Fresh peer joins late

        let alice = oplog1.cg.get_or_create_agent_id("alice");
        let bob = oplog2.cg.get_or_create_agent_id("bob");
        oplog1.cg.get_or_create_agent_id("bob");
        oplog2.cg.get_or_create_agent_id("alice");

        // Create structure
        let set_id = oplog1.local_map_set(alice, ROOT_CRDT_ID, "items",
            CreateValue::NewCRDT(CRDTKind::Set));
        oplog1.local_set_add(alice, set_id, Primitive::I64(1));

        sync_oplogs(&mut oplog1, &mut oplog2);
        let (_, set_id2) = oplog2.crdt_at_path(&["items"]);

        // Concurrent modifications
        oplog1.local_set_add(alice, set_id, Primitive::I64(2));
        oplog1.local_set_add(alice, set_id, Primitive::I64(3));
        oplog2.local_set_add(bob, set_id2, Primitive::I64(4));
        oplog2.local_set_remove(bob, set_id2, Primitive::I64(1));

        sync_oplogs(&mut oplog1, &mut oplog2);

        // Now oplog3 joins and gets everything via serialization
        let full_ops = oplog1.ops_since(&[]);
        oplog3.merge_ops(full_ops).unwrap();

        oplog3.dbg_check(true);
        assert_eq!(oplog1.checkout(), oplog3.checkout());
    }

    // ===== Extreme Concurrency Stress Tests =====
    //
    // These tests push concurrency to extremes: many writers, many operations,
    // randomized sync patterns. They verify convergence under stress.

    /// Create N oplogs with registered agents
    fn create_oplogs(n: usize) -> Vec<OpLog> {
        let agent_names: Vec<String> = (0..n).map(|i| format!("agent_{}", i)).collect();
        let mut oplogs: Vec<OpLog> = (0..n).map(|_| OpLog::new()).collect();

        // Register all agents in all oplogs
        for oplog in &mut oplogs {
            for name in &agent_names {
                oplog.cg.get_or_create_agent_id(name);
            }
        }
        oplogs
    }

    /// Sync all oplogs with each other (full mesh)
    fn sync_all_oplogs(oplogs: &mut [OpLog]) {
        let n = oplogs.len();
        // Multiple rounds to ensure full propagation
        for _ in 0..2 {
            for i in 0..n {
                for j in (i + 1)..n {
                    let (left, right) = oplogs.split_at_mut(j);
                    let a = &mut left[i];
                    let b = &mut right[0];
                    a.merge_ops(b.ops_since(&[])).unwrap();
                    b.merge_ops(a.ops_since(&[])).unwrap();
                }
            }
        }
    }

    #[test]
    fn stress_many_writers_same_map_key() {
        // 8 writers all write to the same key concurrently
        const N: usize = 8;
        let mut oplogs = create_oplogs(N);

        // Each writer writes to "counter"
        for (i, oplog) in oplogs.iter_mut().enumerate() {
            let agent = oplog.cg.get_or_create_agent_id(&format!("agent_{}", i));
            oplog.local_map_set(agent, ROOT_CRDT_ID, "counter",
                CreateValue::Primitive(Primitive::I64(i as i64)));
        }

        // Sync all
        sync_all_oplogs(&mut oplogs);

        // All must converge
        for oplog in &oplogs {
            oplog.dbg_check(true);
        }
        let first = oplogs[0].checkout();
        for oplog in &oplogs[1..] {
            assert_eq!(first, oplog.checkout());
        }
    }

    #[test]
    fn stress_many_writers_map_rapid_overwrites() {
        // 8 writers each do 10 rapid overwrites to the same key
        const N: usize = 8;
        const OPS_PER_WRITER: usize = 10;

        let mut oplogs = create_oplogs(N);

        // Each writer does multiple writes before sync
        for (i, oplog) in oplogs.iter_mut().enumerate() {
            let agent = oplog.cg.get_or_create_agent_id(&format!("agent_{}", i));
            for j in 0..OPS_PER_WRITER {
                oplog.local_map_set(agent, ROOT_CRDT_ID, "value",
                    CreateValue::Primitive(Primitive::I64((i * 100 + j) as i64)));
            }
        }

        // Sync all
        sync_all_oplogs(&mut oplogs);

        // Verify convergence
        for oplog in &oplogs {
            oplog.dbg_check(true);
        }
        let first = oplogs[0].checkout();
        for oplog in &oplogs[1..] {
            assert_eq!(first, oplog.checkout());
        }
    }

    #[test]
    fn stress_many_writers_set_concurrent_adds() {
        // 8 writers all add different elements to the same set
        const N: usize = 8;
        let mut oplogs = create_oplogs(N);

        // First writer creates the set
        let agent0 = oplogs[0].cg.get_or_create_agent_id("agent_0");
        let _set_id = oplogs[0].local_map_set(agent0, ROOT_CRDT_ID, "tags",
            CreateValue::NewCRDT(CRDTKind::Set));

        // Sync set creation to all
        sync_all_oplogs(&mut oplogs);

        // Each writer adds their own tags
        for (i, oplog) in oplogs.iter_mut().enumerate() {
            let agent = oplog.cg.get_or_create_agent_id(&format!("agent_{}", i));
            let (_, set_id) = oplog.crdt_at_path(&["tags"]);
            for j in 0..5 {
                oplog.local_set_add(agent, set_id,
                    Primitive::Str(format!("tag_{}_{}", i, j).into()));
            }
        }

        // Sync all
        sync_all_oplogs(&mut oplogs);

        // Verify convergence - all 8 * 5 = 40 tags should be present
        for oplog in &oplogs {
            oplog.dbg_check(true);
        }
        let (_, set_id) = oplogs[0].crdt_at_path(&["tags"]);
        let set = oplogs[0].checkout_set(set_id);
        assert_eq!(set.len(), N * 5, "Expected {} elements, got {}", N * 5, set.len());

        // All must have same set
        for oplog in &oplogs[1..] {
            let (_, sid) = oplog.crdt_at_path(&["tags"]);
            assert_eq!(set, oplog.checkout_set(sid));
        }
    }

    #[test]
    fn stress_set_add_remove_chaos() {
        // 8 writers: half add, half remove, same elements
        const N: usize = 8;
        let mut oplogs = create_oplogs(N);

        // Create set and add initial elements
        let agent0 = oplogs[0].cg.get_or_create_agent_id("agent_0");
        let set_id = oplogs[0].local_map_set(agent0, ROOT_CRDT_ID, "items",
            CreateValue::NewCRDT(CRDTKind::Set));
        for i in 0..10 {
            oplogs[0].local_set_add(agent0, set_id, Primitive::I64(i));
        }

        // Sync to all
        sync_all_oplogs(&mut oplogs);

        // Half add new elements, half try to remove existing ones
        for (i, oplog) in oplogs.iter_mut().enumerate() {
            let agent = oplog.cg.get_or_create_agent_id(&format!("agent_{}", i));
            let (_, set_id) = oplog.crdt_at_path(&["items"]);

            if i % 2 == 0 {
                // Adders: add new elements
                for j in 0..5 {
                    oplog.local_set_add(agent, set_id, Primitive::I64(100 + i as i64 * 10 + j));
                }
            } else {
                // Removers: try to remove original elements
                for j in 0..5 {
                    oplog.local_set_remove(agent, set_id, Primitive::I64(j * 2));
                }
            }
        }

        // Sync all
        sync_all_oplogs(&mut oplogs);

        // Verify convergence
        for oplog in &oplogs {
            oplog.dbg_check(true);
        }
        let first = oplogs[0].checkout();
        for oplog in &oplogs[1..] {
            assert_eq!(first, oplog.checkout());
        }
    }

    #[test]
    fn stress_16_writers_map_convergence() {
        // 16 writers, each writes to their own key and a shared key
        const N: usize = 16;
        let mut oplogs = create_oplogs(N);

        // Each writer writes to their own key AND a shared key
        for (i, oplog) in oplogs.iter_mut().enumerate() {
            let agent = oplog.cg.get_or_create_agent_id(&format!("agent_{}", i));
            // Own key
            oplog.local_map_set(agent, ROOT_CRDT_ID, &format!("writer_{}", i),
                CreateValue::Primitive(Primitive::I64(i as i64)));
            // Shared key - concurrent writes
            oplog.local_map_set(agent, ROOT_CRDT_ID, "shared",
                CreateValue::Primitive(Primitive::Str(format!("from_{}", i).into())));
        }

        // Sync all
        sync_all_oplogs(&mut oplogs);

        // Verify convergence
        for oplog in &oplogs {
            oplog.dbg_check(true);
        }

        let first = oplogs[0].checkout();

        // Should have 16 individual keys + 1 shared key = 17 keys
        assert_eq!(first.len(), N + 1);

        for oplog in &oplogs[1..] {
            assert_eq!(first, oplog.checkout());
        }
    }

    #[test]
    fn stress_interleaved_sync_patterns() {
        // 8 writers with interleaved sync - not everyone syncs with everyone
        const N: usize = 8;
        let mut oplogs = create_oplogs(N);

        // Create shared set
        let agent0 = oplogs[0].cg.get_or_create_agent_id("agent_0");
        let _ = oplogs[0].local_map_set(agent0, ROOT_CRDT_ID, "data",
            CreateValue::NewCRDT(CRDTKind::Set));

        // Partial sync - only adjacent pairs
        for i in 0..(N - 1) {
            let (left, right) = oplogs.split_at_mut(i + 1);
            let a = &mut left[i];
            let b = &mut right[0];
            a.merge_ops(b.ops_since(&[])).unwrap();
            b.merge_ops(a.ops_since(&[])).unwrap();
        }

        // Everyone adds to the set
        for (i, oplog) in oplogs.iter_mut().enumerate() {
            let agent = oplog.cg.get_or_create_agent_id(&format!("agent_{}", i));
            let (_, set_id) = oplog.crdt_at_path(&["data"]);
            oplog.local_set_add(agent, set_id, Primitive::I64(i as i64));
        }

        // Another round of partial sync (ring topology)
        for i in 0..N {
            let j = (i + 1) % N;
            if i < j {
                let (left, right) = oplogs.split_at_mut(j);
                let a = &mut left[i];
                let b = &mut right[0];
                a.merge_ops(b.ops_since(&[])).unwrap();
                b.merge_ops(a.ops_since(&[])).unwrap();
            }
        }

        // Final full sync to ensure convergence
        sync_all_oplogs(&mut oplogs);

        // Verify all converge
        for oplog in &oplogs {
            oplog.dbg_check(true);
        }
        let first = oplogs[0].checkout();
        for oplog in &oplogs[1..] {
            assert_eq!(first, oplog.checkout());
        }
    }

    #[cfg(feature = "gen_test_data")]
    #[test]
    fn serde_stuff() {
        // let line = r##"{"type":"DocsDelta","deltas":[[["RUWYEZu",0],{"cg_changes":[1,6,83,67,72,69,77,65,10,1],"map_ops":[[["ROOT",0],["SCHEMA",9],"content",{"NewCRDT":"Text"}],[["ROOT",0],["SCHEMA",0],"title",{"NewCRDT":"Text"}]],"text_ops":[[["SCHEMA",0],["SCHEMA",1],{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]],"text_context":{"ins_content":[85,110,116,105,116,108,101,100],"del_content":[]}}]]}"##;
        // let line = r##"{"cg_changes":[1,6,83,67,72,69,77,65,10,1],"map_ops":[[["ROOT",0],["SCHEMA",9],"content",{"NewCRDT":"Text"}],[["ROOT",0],["SCHEMA",0],"title",{"NewCRDT":"Text"}]],"text_ops":[[["SCHEMA",0],["SCHEMA",1],{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]],"text_context":{"ins_content":[85,110,116,105,116,108,101,100],"del_content":[]}}"##;
        //
        // let msg: SerializedOps = serde_json::from_str(&line).unwrap();

        #[derive(Debug, Clone)]
        #[derive(Serialize, Deserialize)]
        pub struct SS {
            // cg_changes: Vec<u8>,

            // The version of the op, and the name of the containing CRDT.
            // map_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, &'a str, CreateValue)>,
            // text_ops: Vec<ListOpMetrics>,
            // text_context: ListOperationCtx,
        }

        // let line = r#"{"cg_changes":[1,6,83,67,72,69,77,65,10,1],"map_ops":[[["ROOT",0],["SCHEMA",9],"content",{"NewCRDT":"Text"}],[["ROOT",0],["SCHEMA",0],"title",{"NewCRDT":"Text"}]],"text_ops":[[["SCHEMA",0],["SCHEMA",1],{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]],"text_context":{"ins_content":[85,110,116,105,116,108,101,100],"del_content":[]}}"#;
        // let x: SS = serde_json::from_str(&line).unwrap();
        // let line = r#"{"text_ops":[{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]}"#;
        // let x: SS = serde_json::from_str(&line).unwrap();


        let line = r#"{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}"#;
        let _x: ListOpMetrics = serde_json::from_str(&line).unwrap();

    }
}