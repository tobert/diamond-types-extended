//! This file contains utilities to convert remote IDs to local version and back.

use uuid::Uuid;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use crate::rle::{HasLength, MergableSpan, SplitableSpanHelpers};
use crate::dtrange::DTRange;
use crate::{Frontier, LV};
use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::causalgraph::agent_span::{AgentVersion, AgentSpan};

/// Remote IDs are IDs you can pass to a remote peer.
/// With UUID agent IDs, these are Copy and have no lifetime.
///
/// The sequence number is `u64` for cross-platform wire compatibility
/// (consistent size on 32-bit WASM and 64-bit native).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct RemoteVersion(pub Uuid, pub u64);

/// Alias for backwards compatibility — RemoteVersion is already owned (Copy).
pub type RemoteVersionOwned = RemoteVersion;

impl From<(Uuid, u64)> for RemoteVersion {
    fn from(r: (Uuid, u64)) -> Self {
        Self(r.0, r.1)
    }
}

/// A range of sequence numbers, using `u64` for wire safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct RemoteSeqRange {
    pub start: u64,
    pub end: u64,
}

impl RemoteSeqRange {
    pub fn len(&self) -> u64 { self.end - self.start }
    pub fn is_empty(&self) -> bool { self.start == self.end }
}

impl From<DTRange> for RemoteSeqRange {
    fn from(r: DTRange) -> Self {
        Self { start: r.start as u64, end: r.end as u64 }
    }
}

impl From<RemoteSeqRange> for DTRange {
    fn from(r: RemoteSeqRange) -> Self {
        Self { start: r.start as usize, end: r.end as usize }
    }
}

/// External equivalent of CRDTSpan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct RemoteVersionSpan(pub Uuid, pub RemoteSeqRange);

/// Alias for backwards compatibility.
pub type RemoteVersionSpanOwned = RemoteVersionSpan;

impl HasLength for RemoteVersionSpan {
    fn len(&self) -> usize {
        self.1.len() as usize
    }
}

impl SplitableSpanHelpers for RemoteVersionSpan {
    fn truncate_h(&mut self, at: usize) -> Self {
        let at64 = at as u64;
        let rest = RemoteSeqRange { start: self.1.start + at64, end: self.1.end };
        self.1.end = self.1.start + at64;
        Self(self.0, rest)
    }
}

impl MergableSpan for RemoteVersionSpan {
    fn can_append(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1.end == other.1.start
    }

    fn append(&mut self, other: Self) {
        self.1.end = other.1.end;
    }
}

pub type RemoteFrontier = SmallVec<RemoteVersion, 2>;

/// Alias for backwards compatibility.
pub type RemoteFrontierOwned = RemoteFrontier;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
#[derive(Serialize)]
pub enum VersionConversionError {
    UnknownAgent,
    SeqInFuture,
}

impl AgentAssignment {
    pub fn try_remote_to_local_version(&self, rv: RemoteVersion) -> Result<LV, VersionConversionError> {
        let agent = self.get_agent_id(rv.0)
            .ok_or(VersionConversionError::UnknownAgent)?;

        self.client_data[agent as usize]
            .try_seq_to_lv(rv.1 as usize)
            .ok_or(VersionConversionError::SeqInFuture)
    }

    /// This panics if the ID isn't known to the document.
    pub fn remote_to_local_version(&self, RemoteVersion(uuid, seq): RemoteVersion) -> LV {
        let agent = self.get_agent_id(uuid).unwrap();
        self.client_data[agent as usize].seq_to_lv(seq as usize)
    }

    pub(crate) fn agent_version_to_remote(&self, (agent, seq): AgentVersion) -> RemoteVersion {
        RemoteVersion(
            self.get_agent_uuid(agent),
            seq as u64
        )
    }

    pub(crate) fn agent_span_to_remote(&self, loc: AgentSpan) -> RemoteVersionSpan {
        RemoteVersionSpan(
            self.get_agent_uuid(loc.agent),
            loc.seq_range.into()
        )
    }

    pub(crate) fn remote_to_agent_version_unknown(&mut self, RemoteVersion(uuid, seq): RemoteVersion) -> AgentVersion {
        let agent = self.get_or_create_agent_id(uuid);
        (agent, seq as usize)
    }
    pub(crate) fn remote_to_agent_version_known(&self, RemoteVersion(uuid, seq): RemoteVersion) -> AgentVersion {
        let agent = self.get_agent_id(uuid).unwrap();
        (agent, seq as usize)
    }

    pub fn local_to_remote_version(&self, v: LV) -> RemoteVersion {
        let agent_v = self.local_to_agent_version(v);
        self.agent_version_to_remote(agent_v)
    }

    /// **NOTE:** This method will return a version span with length min(lv, agent_v). The
    /// resulting length will NOT be guaranteed to be the same as the input.
    pub fn local_to_remote_version_span(&self, v: DTRange) -> RemoteVersionSpan {
        let agent_span = self.local_span_to_agent_span(v);
        self.agent_span_to_remote(agent_span)
    }

    pub fn try_remote_to_local_frontier<I>(&self, ids_iter: I) -> Result<Frontier, VersionConversionError>
        where I: Iterator<Item=RemoteVersion>
    {
        let frontier: Frontier = ids_iter
            .map(|rv| self.try_remote_to_local_version(rv))
            .collect::<Result<Frontier, VersionConversionError>>()?;

        Ok(frontier)
    }

    pub fn remote_to_local_frontier<I>(&self, ids_iter: I) -> Frontier
        where I: Iterator<Item=RemoteVersion>
    {
        let frontier: Frontier = ids_iter
            .map(|rv| self.remote_to_local_version(rv))
            .collect();

        frontier
    }

    pub fn local_to_remote_frontier(&self, local_frontier: &[LV]) -> RemoteFrontier {
        local_frontier
            .iter()
            .map(|lv| self.local_to_remote_version(*lv))
            .collect()
    }

    pub fn iter_remote_mappings(&self) -> impl Iterator<Item = RemoteVersionSpan> + '_ {
        self.client_with_lv
            .iter()
            .map(|item| self.agent_span_to_remote(item.1))
    }

    pub fn iter_remote_mappings_range(&self, range: DTRange) -> impl Iterator<Item = RemoteVersionSpan> + '_ {
        self.client_with_lv
            .iter_range(range)
            .map(|item| self.agent_span_to_remote(item.1))
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;
    use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
    use crate::CausalGraph;

    #[test]
    fn id_smoke_test() {
        let mut cg = CausalGraph::new();
        cg.get_or_create_agent_id(Uuid::from_u128(0x5E98));
        cg.get_or_create_agent_id(Uuid::from_u128(0x341CE));
        cg.assign_local_op_with_parents(&[], 0, 2);
        cg.assign_local_op_with_parents(&[], 1, 4);

        assert_eq!(0, cg.agent_assignment.remote_to_local_version(RemoteVersion(Uuid::from_u128(0x5E98), 0)));
        assert_eq!(1, cg.agent_assignment.remote_to_local_version(RemoteVersion(Uuid::from_u128(0x5E98), 1)));
        assert_eq!(2, cg.agent_assignment.remote_to_local_version(RemoteVersion(Uuid::from_u128(0x341CE), 0)));

        for lv in 0..cg.len() {
            let rv = cg.agent_assignment.local_to_remote_version(lv);
            let expect_lv = cg.agent_assignment.remote_to_local_version(rv);
            assert_eq!(lv, expect_lv);
        }
    }

    #[test]
    fn remote_versions_can_be_empty() {
        let cg = CausalGraph::new();
        assert!(cg.agent_assignment.remote_to_local_frontier(std::iter::empty::<RemoteVersion>()).is_root());
    }
}
