//! Transaction-local logical history bounds. Physical pointer order is not
//! progress.
//!
//! Ordinary stream rows use ordered item IDs. Logical history adds lineage
//! bounds and exact pointer observations; both use `StreamReadKind` at the
//! physical attempt boundary.
use std::{borrow::Cow, collections::BTreeMap, num::NonZeroU64};

use storage_types::{
    StorageError, StorageResult, StreamItemId, SystemStreamCheckpoint, SystemStreamContractError,
    SystemStreamLineageId, SystemStreamOffset, SystemStreamPosition,
};

use super::{
    SystemStreamLineageCatalog, SystemStreamSplitReceipt, SystemStreamSplitRequest,
    SystemStreamSplitResult, SystemStreamTrimPageRequest,
};

/// Immutable allocation baseline and committed upper bound for one lineage.
#[derive(Debug)]
pub struct SystemStreamHistoryBounds {
    pub lineage_id: SystemStreamLineageId,
    pub initial_sequence: NonZeroU64,
    pub next_sequence: NonZeroU64,
    pub sealed_after: Option<NonZeroU64>,
}

/// One shared writer decision. SQL and KV compare `expected_offset` against
/// their native sequence row and persist `next_offset` on the same attempt.
pub struct SystemStreamWriteAllocation {
    pub position: SystemStreamPosition,
    pub expected_offset: NonZeroU64,
    pub next_offset: NonZeroU64,
}

/// Catalog and sequence facts read in one protected native transaction.
#[derive(Debug)]
pub struct SystemStreamHistory {
    catalog: SystemStreamLineageCatalog,
    bounds: Vec<SystemStreamHistoryBounds>,
    ordering: SystemStreamHistoryOrdering,
    commit_frontiers: Vec<SystemStreamPosition>,
}

impl AsRef<SystemStreamLineageCatalog> for SystemStreamHistory {
    fn as_ref(&self) -> &SystemStreamLineageCatalog {
        &self.catalog
    }
}

/// Physical allocation and pagination contract for one history snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SystemStreamHistoryOrdering {
    #[default]
    Contiguous,
    SparseNumeric,
    Versionstamped,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SystemStreamHistoryRead {
    pub after: SystemStreamPosition,
    pub through: u64,
    pub initial_sequence: NonZeroU64,
    pub limit: u32,
    pub ordering: SystemStreamHistoryOrdering,
}

/// Native logical row and its exact physical-pointer observation.
#[derive(Debug)]
pub struct SystemStreamHistoryRecord {
    pub position: SystemStreamPosition,
    pub expected_pointer: StreamItemId,
    pub actual_pointer: Option<StreamItemId>,
}

/// A native response tied to the complete read that produced it.
#[derive(Debug)]
pub struct SystemStreamHistoryObservation<'a> {
    pub retired_through: Option<SystemStreamPosition>,
    pub read: &'a SystemStreamHistoryRead,
    pub records: Vec<SystemStreamHistoryRecord>,
}

/// A fixed-high page plan. Unselected lineages retain their current positions.
#[derive(Debug)]
pub struct SystemStreamHistoryPage {
    current: SystemStreamCheckpoint,
    high: SystemStreamCheckpoint,
    reads: Vec<SystemStreamHistoryRead>,
}

impl SystemStreamHistory {
    pub fn uses_commit_versionstamps(&self) -> bool {
        self.ordering == SystemStreamHistoryOrdering::Versionstamped
    }

    pub fn with_commit_ordering(mut self, frontiers: Vec<SystemStreamPosition>) -> Self {
        self.ordering = SystemStreamHistoryOrdering::Versionstamped;
        self.commit_frontiers = frontiers;
        self
    }

    pub fn with_sparse_numeric_ordering(mut self, frontiers: Vec<SystemStreamPosition>) -> Self {
        self.ordering = SystemStreamHistoryOrdering::SparseNumeric;
        self.commit_frontiers = frontiers;
        self
    }

    pub fn native_numeric_position(
        &self,
        routing_key: &[u8],
        offset: u64,
    ) -> StorageResult<SystemStreamPosition> {
        self.catalog
            .writer_lineage(routing_key)?
            .position(format!("{offset:020}-ns"))
            .map_err(|_| StorageError::internal("Invalid native numeric stream position"))
    }

    pub fn pending_commit_position(
        &self,
        routing_key: &[u8],
        ordinal: u16,
    ) -> StorageResult<SystemStreamPosition> {
        let lineage = self.catalog.writer_lineage(routing_key)?;
        lineage
            .position(format!("{:020}-vsffff{ordinal:04x}", u64::MAX))
            .map_err(|_| StorageError::internal("Invalid pending native stream position"))
    }

    fn frontier_position(
        &self,
        lineage: &super::SystemStreamLineage,
        version: u64,
    ) -> Result<SystemStreamPosition, SystemStreamContractError> {
        if self.ordering == SystemStreamHistoryOrdering::Versionstamped {
            lineage.position(format!("{version:020}-vsffffffff"))
        } else {
            lineage.position(format!("{version:020}"))
        }
    }

    /// Decide a fresh split from the protected catalog and sequence snapshot.
    /// The physical owner persists this receipt with the lineage changes in
    /// the same native attempt.
    pub fn plan_split(
        &self,
        request: &SystemStreamSplitRequest,
    ) -> Result<SystemStreamSplitReceipt, SystemStreamContractError> {
        request.validate_against(&self.catalog)?;
        let parent = self.find_bounds(request.parent().lineage_id())?;
        SystemStreamSplitReceipt::try_new(request, parent.next_sequence.get())
    }

    /// A receipt can replay after its children have accepted writes or have
    /// themselves split. Their immutable identities and initial sequences
    /// still have to match the original publication.
    pub fn replay_split(
        &self,
        request: &SystemStreamSplitRequest,
        receipt: &SystemStreamSplitReceipt,
    ) -> Result<SystemStreamSplitResult, SystemStreamContractError> {
        self.validate_scope(request.source_scope())?;
        let result = receipt.replay(request)?;
        let parent = self
            .catalog
            .lineage(request.parent().lineage_id())
            .ok_or(SystemStreamContractError::UnknownLineage)?;
        let parent_bounds = self.find_bounds(parent.lineage_id())?;
        if parent != result.sealed_parent()
            || parent_bounds.sealed_after.map(NonZeroU64::get) != Some(receipt.sealed_after_offset)
            || parent_bounds.next_sequence.get() != receipt.sealed_after_offset
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        for child in request.children() {
            let stored = self
                .catalog
                .lineage(child.lineage().lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            let bounds = self.find_bounds(stored.lineage_id())?;
            if stored.shard_id() != child.lineage().shard_id()
                || stored.parent_lineage_id() != child.lineage().parent_lineage_id()
                || bounds.initial_sequence != child.initial_sequence()
            {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        Ok(result)
    }

    /// Select and validate the next logical position from one protected
    /// catalog and sequence snapshot. The caller advances that sequence in
    /// the same native attempt before persisting the record.
    pub fn plan_write(&self, routing_key: &[u8]) -> StorageResult<SystemStreamWriteAllocation> {
        let lineage = self.catalog.writer_lineage(routing_key)?;
        let expected_offset = self
            .bounds
            .iter()
            .find(|entry| &entry.lineage_id == lineage.lineage_id())
            .ok_or_else(|| StorageError::internal("SystemStream writer has no sequence"))?
            .next_sequence;
        let next_offset = expected_offset
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| StorageError::internal("SystemStream sequence overflow"))?;
        let position = lineage
            .position(format!("{:020}", expected_offset.get()))
            .map_err(|_| StorageError::internal("SystemStream write position is invalid"))?;
        Ok(SystemStreamWriteAllocation {
            position,
            expected_offset,
            next_offset,
        })
    }

    /// Reserve another position in this attempt without rereading a sequence
    /// that has only been staged locally. The physical owner persists the
    /// aggregate first-to-final sequence advance before commit.
    pub fn reserve_write(
        &mut self,
        routing_key: &[u8],
    ) -> StorageResult<SystemStreamWriteAllocation> {
        let allocation = self.plan_write(routing_key)?;
        let bound = self
            .bounds
            .iter_mut()
            .find(|entry| &entry.lineage_id == allocation.position.lineage_id())
            .ok_or_else(|| StorageError::internal("SystemStream writer has no sequence"))?;
        bound.next_sequence = allocation.next_offset;
        Ok(allocation)
    }

    pub fn try_new(
        catalog: SystemStreamLineageCatalog,
        bounds: Vec<SystemStreamHistoryBounds>,
    ) -> Result<Self, SystemStreamContractError> {
        if bounds.len() != catalog.entries().len() {
            return Err(SystemStreamContractError::BackendContract);
        }
        for lineage in catalog.entries() {
            let mut matches = bounds
                .iter()
                .filter(|entry| &entry.lineage_id == lineage.lineage_id());
            let entry = matches
                .next()
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            if matches.next().is_some()
                || entry.next_sequence < entry.initial_sequence
                || lineage.sealed() != entry.sealed_after.is_some()
                || entry
                    .sealed_after
                    .is_some_and(|end| end != entry.next_sequence)
            {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        Ok(Self {
            catalog,
            bounds,
            ordering: SystemStreamHistoryOrdering::Contiguous,
            commit_frontiers: Vec::new(),
        })
    }

    pub fn validate_scope(
        &self,
        scope: &storage_types::SystemStreamScope,
    ) -> Result<(), SystemStreamContractError> {
        self.catalog.validate_scope(scope)
    }

    pub fn initial_checkpoint(&self) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        let positions = self
            .catalog
            .entries()
            .iter()
            .filter(|lineage| lineage.parent_lineage_id().is_none())
            .map(|lineage| {
                let bound = self.find_bounds(lineage.lineage_id())?;
                self.frontier_position(lineage, bound.initial_sequence.get() - 1)
            })
            .collect::<Result<Vec<_>, _>>()?;
        SystemStreamCheckpoint::try_new(self.catalog.source_scope().clone(), positions)
    }

    pub fn capture_high(&self) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        if self.ordering == SystemStreamHistoryOrdering::SparseNumeric {
            let positions = self
                .commit_frontiers
                .iter()
                .filter(|position| {
                    self.catalog
                        .lineage(position.lineage_id())
                        .is_some_and(|lineage| !lineage.sealed())
                })
                .cloned()
                .collect();
            return SystemStreamCheckpoint::try_new(self.catalog.source_scope().clone(), positions);
        }
        let positions = self
            .catalog
            .entries()
            .iter()
            .filter(|lineage| !lineage.sealed())
            .map(|lineage| {
                let bound = self.find_bounds(lineage.lineage_id())?;
                if self.uses_commit_versionstamps() && bound.next_sequence > bound.initial_sequence
                {
                    return self
                        .commit_frontiers
                        .iter()
                        .find(|position| position.lineage_id() == lineage.lineage_id())
                        .cloned()
                        .ok_or(SystemStreamContractError::BackendContract);
                }
                self.frontier_position(lineage, bound.next_sequence.get() - 1)
            })
            .collect::<Result<Vec<_>, _>>()?;
        SystemStreamCheckpoint::try_new(self.catalog.source_scope().clone(), positions)
    }

    pub fn plan_page(
        &self,
        current: &SystemStreamCheckpoint,
        high: &SystemStreamCheckpoint,
        limit: u32,
    ) -> Result<SystemStreamHistoryPage, SystemStreamContractError> {
        if limit == 0 {
            return Err(SystemStreamContractError::BackendContract);
        }
        self.validate_frontier(current)?;
        self.validate_frontier(high)?;
        let current = self.expand_frontier(current, high)?;
        let mut reads = Vec::new();
        let mut remaining = limit.min(storage_types::SYSTEM_STREAM_RECORDS_LIMIT_MAX);
        for (index, position) in current.frontier().iter().enumerate() {
            let boundary = self.read_boundary(position, high)?;
            let through = boundary.order()?;
            let offset = Self::parse_offset(position)?;
            let count = if position.offset().compare_order(&boundary)?.is_eq() {
                0
            } else if self.ordering != SystemStreamHistoryOrdering::Contiguous {
                remaining.div_ceil((current.frontier().len() - index) as u32)
            } else {
                u32::try_from(through - offset)
                    .unwrap_or(u32::MAX)
                    .min(remaining)
            };
            if count != 0 {
                reads.push(SystemStreamHistoryRead {
                    after: position.clone(),
                    ordering: self.ordering,
                    through,
                    limit: count,
                    initial_sequence: self.find_bounds(position.lineage_id())?.initial_sequence,
                });
                remaining -= count;
            }
        }
        Ok(SystemStreamHistoryPage {
            current,
            high: high.clone(),
            reads,
        })
    }

    /// Convert private consumed positions to exclusive trim limits. A hold on
    /// any ancestor protects every record in each descendant until handoff.
    pub fn private_trim_boundaries(
        &self,
        minima: &[SystemStreamPosition],
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        if minima.len() > self.bounds.len() {
            return Err(SystemStreamContractError::BackendContract);
        }
        for (ordinal, position) in minima.iter().enumerate() {
            let lineage = self
                .catalog
                .lineage(position.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            let bounds = self.find_bounds(position.lineage_id())?;
            let offset = position.offset().order()?;
            if lineage.shard_id() != position.shard_id()
                || offset < bounds.initial_sequence.get() - 1
                || offset >= bounds.next_sequence.get()
                || minima[..ordinal]
                    .iter()
                    .any(|previous| previous.lineage_id() == position.lineage_id())
            {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        if self.ordering == SystemStreamHistoryOrdering::Versionstamped {
            return self.native_trim_boundaries(minima);
        }
        let mut limits = Vec::new();
        for lineage in self.catalog.entries() {
            let mut limit = None;
            for position in minima {
                let bound = if position.lineage_id() == lineage.lineage_id() {
                    Some(
                        position
                            .offset()
                            .order()?
                            .checked_add(1)
                            .ok_or(SystemStreamContractError::BackendContract)?,
                    )
                } else if self.is_ancestor(position.lineage_id(), lineage.lineage_id()) {
                    Some(
                        self.find_bounds(lineage.lineage_id())?
                            .initial_sequence
                            .get(),
                    )
                } else {
                    None
                };
                if let Some(bound) = bound {
                    limit = Some(limit.map_or(bound, |current: u64| current.min(bound)));
                }
            }
            if let Some(limit) = limit {
                let offset = format!("{limit:020}");
                limits.push(lineage.position(offset)?);
            }
        }
        Ok(limits)
    }

    fn native_trim_boundaries(
        &self,
        minima: &[SystemStreamPosition],
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        let mut limits = Vec::new();
        for lineage in self.catalog.entries() {
            let mut limit: Option<SystemStreamPosition> = None;
            for position in minima {
                let boundary = if position.lineage_id() == lineage.lineage_id() {
                    position.next_versionstamp()?
                } else if self.is_ancestor(position.lineage_id(), lineage.lineage_id()) {
                    let initial = self
                        .find_bounds(lineage.lineage_id())?
                        .initial_sequence
                        .get();
                    lineage.position(format!("{initial:020}-vs00000000"))?
                } else {
                    continue;
                };
                if limit
                    .as_ref()
                    .map(|current| {
                        boundary
                            .offset()
                            .compare_order(current.offset())
                            .map(|order| order.is_lt())
                    })
                    .transpose()?
                    .unwrap_or(true)
                {
                    limit = Some(boundary);
                }
            }
            limits.extend(limit);
        }
        Ok(limits)
    }

    /// Bounded lineage identities used for one indexed minimum-hold probe per
    /// lineage. The zero offset is only a probe key; it is not a frontier.
    pub fn trim_hold_probes(&self) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        self.catalog
            .entries()
            .iter()
            .map(|lineage| lineage.position("0"))
            .collect()
    }

    /// Combine durable checkpoint frontiers with GSI private holds into one
    /// exclusive boundary per lineage. An absent checkpoint boundary retains
    /// the lineage even when no GSI build is active.
    pub fn trim_boundaries(
        &self,
        request: &SystemStreamTrimPageRequest,
        checkpoint_frontiers: &[SystemStreamPosition],
        private_minima: &[SystemStreamPosition],
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        self.catalog.validate_scope(request.scope())?;
        if checkpoint_frontiers.len() > self.catalog.entries().len() {
            return Err(SystemStreamContractError::BackendContract);
        }
        for position in checkpoint_frontiers {
            let lineage = self
                .catalog
                .lineage(position.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            let bounds = self.find_bounds(position.lineage_id())?;
            let offset = position.offset().order()?;
            if lineage.shard_id() != position.shard_id()
                || offset < bounds.initial_sequence.get() - 1
                || offset >= bounds.next_sequence.get()
            {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        let mut boundaries = BTreeMap::<SystemStreamLineageId, SystemStreamPosition>::new();
        for position in self
            .catalog
            .expand_protected_positions(checkpoint_frontiers)?
        {
            let position = if self.ordering == SystemStreamHistoryOrdering::Versionstamped
                && position.offset().versionstamp().is_none()
            {
                SystemStreamPosition::try_new(
                    position.shard_id().as_str(),
                    position.lineage_id().as_str(),
                    format!("{:020}-vs00000000", position.offset().order()?),
                )?
            } else {
                position
            };
            boundaries.insert(position.lineage_id().clone(), position);
        }
        for position in self.private_trim_boundaries(private_minima)? {
            if let Some(current) = boundaries.get_mut(position.lineage_id())
                && position.offset().compare_order(current.offset())?.is_lt()
            {
                *current = position;
            }
        }
        Ok(boundaries.into_values().collect())
    }

    fn find_bounds(
        &self,
        lineage: &SystemStreamLineageId,
    ) -> Result<&SystemStreamHistoryBounds, SystemStreamContractError> {
        self.bounds
            .iter()
            .find(|bound| &bound.lineage_id == lineage)
            .ok_or(SystemStreamContractError::UnknownLineage)
    }

    fn parse_offset(position: &SystemStreamPosition) -> Result<u64, SystemStreamContractError> {
        position.offset().order()
    }

    fn is_ancestor(
        &self,
        ancestor: &SystemStreamLineageId,
        descendant: &SystemStreamLineageId,
    ) -> bool {
        let mut current = Some(descendant);
        while let Some(id) = current {
            if id == ancestor {
                return true;
            }
            current = self
                .catalog
                .lineage(id)
                .and_then(|entry| entry.parent_lineage_id());
        }
        false
    }

    fn validate_frontier(
        &self,
        checkpoint: &SystemStreamCheckpoint,
    ) -> Result<(), SystemStreamContractError> {
        checkpoint.validate_scope(self.catalog.source_scope())?;
        for position in checkpoint.frontier() {
            let lineage = self
                .catalog
                .lineage(position.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            let bounds = self.find_bounds(position.lineage_id())?;
            let offset = Self::parse_offset(position)?;
            if position.shard_id() != lineage.shard_id()
                || offset < bounds.initial_sequence.get() - 1
                || offset >= bounds.next_sequence.get()
            {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        for leaf in self
            .catalog
            .entries()
            .iter()
            .filter(|entry| !entry.sealed())
        {
            let coverage = checkpoint
                .frontier()
                .iter()
                .filter(|position| self.is_ancestor(position.lineage_id(), leaf.lineage_id()))
                .count();
            if coverage != 1 {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        Ok(())
    }

    fn read_boundary<'a>(
        &self,
        position: &SystemStreamPosition,
        high: &'a SystemStreamCheckpoint,
    ) -> Result<Cow<'a, SystemStreamOffset>, SystemStreamContractError> {
        let exact = high
            .frontier()
            .iter()
            .find(|end| end.lineage_id() == position.lineage_id());
        // Keep the complete boundary until the native range is planned. A
        // numeric version alone cannot identify a consumed commit ordinal.
        let through = if let Some(end) = exact {
            Cow::Borrowed(end.offset())
        } else {
            if !high
                .frontier()
                .iter()
                .any(|end| self.is_ancestor(position.lineage_id(), end.lineage_id()))
            {
                return Err(SystemStreamContractError::BackendContract);
            }
            let version = self
                .find_bounds(position.lineage_id())?
                .sealed_after
                .ok_or(SystemStreamContractError::BackendContract)?
                .get()
                - 1;
            // Include every commit ordinal at the last version before the
            // sealed boundary.
            let offset = if self.ordering == SystemStreamHistoryOrdering::Versionstamped {
                format!("{version:020}-vsffffffff")
            } else {
                format!("{version:020}")
            };
            Cow::Owned(SystemStreamOffset::try_new(offset)?)
        };
        if position.offset().compare_order(&through)?.is_gt() {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(through)
    }

    fn expand_frontier(
        &self,
        checkpoint: &SystemStreamCheckpoint,
        high: &SystemStreamCheckpoint,
    ) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        let mut positions = checkpoint.frontier().to_vec();
        let mut cursor = 0;
        while cursor < positions.len() {
            let position = &positions[cursor];
            let bounds = self.find_bounds(position.lineage_id())?;
            let at_seal = bounds.sealed_after.is_some_and(|end| {
                Self::parse_offset(position).is_ok_and(|offset| offset == end.get() - 1)
                    && (!(self.ordering == SystemStreamHistoryOrdering::Versionstamped)
                        || self
                            .commit_frontiers
                            .iter()
                            .find(|high| high.lineage_id() == position.lineage_id())
                            .is_some_and(|high| {
                                position
                                    .offset()
                                    .compare_order(high.offset())
                                    .is_ok_and(|order| !order.is_lt())
                            }))
            });
            let high_has_parent = high
                .frontier()
                .iter()
                .any(|end| end.lineage_id() == position.lineage_id());
            if !at_seal || high_has_parent {
                cursor += 1;
                continue;
            }
            let mut children = Vec::with_capacity(2);
            for child in self
                .catalog
                .entries()
                .iter()
                .filter(|entry| entry.parent_lineage_id() == Some(position.lineage_id()))
            {
                let child_bounds = self.find_bounds(child.lineage_id())?;
                children
                    .push(self.frontier_position(child, child_bounds.initial_sequence.get() - 1)?);
            }
            if children.len() != 2 {
                return Err(SystemStreamContractError::BackendContract);
            }
            positions.splice(cursor..=cursor, children);
        }
        for position in &mut positions {
            if !(self.ordering == SystemStreamHistoryOrdering::Versionstamped)
                || position.offset().versionstamp().is_none()
            {
                let lineage = self
                    .catalog
                    .lineage(position.lineage_id())
                    .ok_or(SystemStreamContractError::UnknownLineage)?;
                *position = self.frontier_position(lineage, position.offset().order()?)?;
            }
        }
        SystemStreamCheckpoint::try_new(self.catalog.source_scope().clone(), positions)
    }
}

impl SystemStreamHistoryPage {
    /// Sealed lineages are replaced by their children before any physical
    /// record read. A public page begins from this expanded frontier.
    #[must_use]
    pub fn expanded_checkpoint(&self) -> &SystemStreamCheckpoint {
        &self.current
    }

    /// Reject records from another persisted source incarnation.
    pub fn validate_scope(
        &self,
        scope: &storage_types::SystemStreamScope,
    ) -> Result<(), SystemStreamContractError> {
        self.current.validate_scope(scope)
    }

    /// Validate complete native observations before any consumer filters by
    /// table.
    pub fn finish(
        &self,
        observations: &[SystemStreamHistoryObservation<'_>],
    ) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        self.finish_positions(observations, None)
    }

    /// Validate a byte-bounded prefix. A short physical read is valid only
    /// when its provider reports another row, and no later lineage may be
    /// consumed after that short read. Omitted later reads keep their frontier.
    pub fn finish_bounded(
        &self,
        observations: &[SystemStreamHistoryObservation<'_>],
        more: &[bool],
    ) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        self.finish_positions(observations, Some(more))
    }

    fn finish_positions(
        &self,
        observations: &[SystemStreamHistoryObservation<'_>],
        more: Option<&[bool]>,
    ) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        if observations.len() > self.reads.len()
            || more.is_none() && observations.len() != self.reads.len()
            || more.is_some_and(|more| more.len() != observations.len())
            || more.is_some() && !self.reads.is_empty() && observations.is_empty()
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        let mut positions = self.current.frontier().to_vec();
        for (index, (read, observation)) in self.reads.iter().zip(observations).enumerate() {
            let provider_more = more.map(|flags| flags[index]);
            let consumed = Self::validate_observation(read, observation, provider_more)?;
            if consumed < read.limit
                && index + 1 != observations.len()
                && (read.ordering == SystemStreamHistoryOrdering::Contiguous
                    || provider_more == Some(true))
            {
                return Err(SystemStreamContractError::BackendContract);
            }
            let position = positions
                .iter_mut()
                .find(|position| position.lineage_id() == read.after.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            // Private progress records the consumed sequence. Public record
            // suffixes must not prevent equality with a captured numeric high.
            let after = SystemStreamHistory::parse_offset(&read.after)?;
            *position = if read.ordering == SystemStreamHistoryOrdering::Contiguous {
                Self::sequence_position(position, after + u64::from(consumed))?
            } else if provider_more == Some(true) {
                observation
                    .records
                    .last()
                    .ok_or(SystemStreamContractError::BackendContract)?
                    .position
                    .clone()
            } else if read.ordering == SystemStreamHistoryOrdering::Versionstamped {
                if let Some(end) = self
                    .high
                    .frontier()
                    .iter()
                    .find(|end| end.lineage_id() == position.lineage_id())
                {
                    end.clone()
                } else {
                    SystemStreamPosition::try_new(
                        position.shard_id().as_str(),
                        position.lineage_id().as_str(),
                        format!("{:020}-vsffffffff", read.through),
                    )?
                }
            } else {
                Self::sequence_position(position, read.through)?
            };
        }
        SystemStreamCheckpoint::try_new(self.current.scope().clone(), positions)
    }

    fn validate_observation(
        read: &SystemStreamHistoryRead,
        observation: &SystemStreamHistoryObservation<'_>,
        more: Option<bool>,
    ) -> Result<u32, SystemStreamContractError> {
        if let Some(retired) = &observation.retired_through
            && read.after.offset().compare_order(retired.offset())?.is_lt()
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        let rows = &observation.records;
        let count =
            u32::try_from(rows.len()).map_err(|_| SystemStreamContractError::BackendContract)?;
        let is_commit_ordered = read.ordering != SystemStreamHistoryOrdering::Contiguous;
        if observation.read != read
            || count > read.limit
            || !is_commit_ordered && count < read.limit && (more != Some(true) || count == 0)
            || is_commit_ordered && more == Some(true) && count == 0
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        let after = SystemStreamHistory::parse_offset(&read.after)?;
        let mut previous = read.after.offset().as_str();
        let mut previous_order = after;
        for (ordinal, row) in rows.iter().enumerate() {
            if row.position.shard_id() != read.after.shard_id()
                || row.position.lineage_id() != read.after.lineage_id()
                || if is_commit_ordered {
                    (read.ordering == SystemStreamHistoryOrdering::Versionstamped
                        && row.position.offset().versionstamp().is_none())
                        || (read.ordering == SystemStreamHistoryOrdering::Versionstamped
                            && row.position.offset().as_str() <= previous)
                        || (read.ordering == SystemStreamHistoryOrdering::SparseNumeric
                            && SystemStreamHistory::parse_offset(&row.position)? <= previous_order)
                        || SystemStreamHistory::parse_offset(&row.position)? > read.through
                } else {
                    SystemStreamHistory::parse_offset(&row.position)? != after + ordinal as u64 + 1
                }
                || row.actual_pointer != Some(row.expected_pointer)
            {
                return Err(SystemStreamContractError::BackendContract);
            }
            previous = row.position.offset().as_str();
            previous_order = SystemStreamHistory::parse_offset(&row.position)?;
        }
        Ok(count)
    }

    fn sequence_position(
        position: &SystemStreamPosition,
        sequence: u64,
    ) -> Result<SystemStreamPosition, SystemStreamContractError> {
        SystemStreamPosition::try_new(
            position.shard_id().as_str(),
            position.lineage_id().as_str(),
            format!("{sequence:020}"),
        )
    }
}

impl AsRef<[SystemStreamHistoryRead]> for SystemStreamHistoryPage {
    fn as_ref(&self) -> &[SystemStreamHistoryRead] {
        &self.reads
    }
}
