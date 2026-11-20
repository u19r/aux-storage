//! Transaction-local logical history bounds. Physical pointer order is not
//! progress.
//!
//! Ordinary stream rows use ordered item IDs. Logical history adds lineage
//! bounds and exact pointer observations; both use `StreamReadKind` at the
//! physical attempt boundary.
use std::{borrow::Cow, num::NonZeroU64};

use storage_types::{
    StorageEnum, StorageError, StorageResult, SystemStreamCheckpoint, SystemStreamContractError,
    SystemStreamLineageId, SystemStreamOffset, SystemStreamPosition,
};

use super::{
    SystemStreamLineageCatalog, SystemStreamSplitPlan, SystemStreamSplitReceipt,
    SystemStreamSplitRequest, SystemStreamSplitResult,
};

/// The highest stable position of one lineage in a history snapshot.
/// `visible_upper` is the first position the snapshot cannot yet see (KV:
/// `next`; Postgres: the snapshot's xmin position). A live lineage is stable
/// up to `visible_upper - 1`, clamped to its own range; a sealed lineage ends
/// at its stored end, which must lie in `[initial, next]`.
pub fn lineage_frontier_high(
    initial_sequence: NonZeroU64,
    next_sequence: NonZeroU64,
    sealed_after: Option<u64>,
    visible_upper: u64,
) -> StorageResult<u64> {
    let (initial, next) = (initial_sequence.get(), next_sequence.get());
    if initial > next {
        return Err(StorageEnum::HistoryUnavailable.into());
    }
    let end = match sealed_after {
        Some(end) if end > next || end < initial => {
            return Err(StorageEnum::HistoryUnavailable.into());
        }
        Some(end) => end,
        None => visible_upper.clamp(initial, next),
    };
    Ok(end - 1)
}

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
    minimum_new_position: Option<NonZeroU64>,
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

    pub fn with_commit_ordering(
        mut self,
        frontiers: Vec<SystemStreamPosition>,
        minimum_new_position: NonZeroU64,
    ) -> Self {
        self.ordering = SystemStreamHistoryOrdering::Versionstamped;
        self.commit_frontiers = frontiers;
        self.minimum_new_position = Some(minimum_new_position);
        self
    }

    pub fn with_sparse_numeric_ordering(
        mut self,
        frontiers: Vec<SystemStreamPosition>,
        minimum_new_position: NonZeroU64,
    ) -> Self {
        self.ordering = SystemStreamHistoryOrdering::SparseNumeric;
        self.commit_frontiers = frontiers;
        self.minimum_new_position = Some(minimum_new_position);
        self
    }

    /// Lowest position a native event can receive after this history snapshot.
    pub fn minimum_new_position(&self) -> Option<NonZeroU64> {
        self.minimum_new_position
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
    ) -> Result<SystemStreamSplitPlan, SystemStreamContractError> {
        request.validate_against(&self.catalog)?;
        if self.minimum_new_position.is_some_and(|minimum| {
            request
                .children()
                .iter()
                .any(|child| child.initial_sequence() > minimum)
        }) {
            return Err(SystemStreamContractError::InvalidChildStart);
        }
        let parent = self.find_bounds(request.parent().lineage_id())?;
        if self.uses_commit_versionstamps() {
            let parent = request.parent();
            let sealed_parent = super::SystemStreamLineage::try_new(
                parent.shard_id().as_str(),
                parent.lineage_id().as_str(),
                parent.parent_lineage_id().map(|id| id.as_str().to_owned()),
                true,
            )?;
            Ok(SystemStreamSplitPlan::EndAtCommit(
                SystemStreamSplitResult::try_new(
                    request,
                    sealed_parent,
                    super::SystemStreamSplitDisposition::Applied,
                )?,
            ))
        } else {
            Ok(SystemStreamSplitPlan::KnownEnd(
                SystemStreamSplitReceipt::try_new(request, parent.next_sequence.get())?,
            ))
        }
    }

    pub fn sealed_end(
        &self,
        lineage: &SystemStreamLineageId,
    ) -> Result<NonZeroU64, SystemStreamContractError> {
        self.find_bounds(lineage)?
            .sealed_after
            .ok_or(SystemStreamContractError::BackendContract)
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
        // Counter publication and entries commit atomically. Its closed high
        // remains authoritative after retention or table deletion leaves gaps.
        let commit_frontiers = catalog
            .entries()
            .iter()
            .map(|lineage| {
                let bound = bounds
                    .iter()
                    .find(|bound| &bound.lineage_id == lineage.lineage_id())
                    .ok_or(SystemStreamContractError::UnknownLineage)?;
                let high = bound.sealed_after.unwrap_or(bound.next_sequence).get() - 1;
                SystemStreamPosition::try_new(
                    lineage.shard_id().as_str(),
                    lineage.lineage_id().as_str(),
                    format!("{high:020}"),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            catalog,
            bounds,
            ordering: SystemStreamHistoryOrdering::SparseNumeric,
            commit_frontiers,
            minimum_new_position: None,
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
        let mut positions = current
            .frontier()
            .iter()
            .map(|position| {
                let mut order = [0; 12];
                if self.minimum_new_position.is_some() {
                    if let Some(versionstamp) = position.offset().versionstamp() {
                        order = versionstamp;
                    } else {
                        order[..8].copy_from_slice(&position.offset().order()?.to_be_bytes());
                    }
                }
                Ok((position, order))
            })
            .collect::<Result<Vec<_>, SystemStreamContractError>>()?;
        if self.minimum_new_position.is_some() {
            // Native clocks advance empty lineages too. Read the least advanced
            // checkpoint first so unrelated writes cannot starve later
            // lineages.
            positions.sort_by_key(|(_, order)| *order);
        }
        for (index, (position, _)) in positions.into_iter().enumerate() {
            let boundary = self.read_boundary(position, high)?;
            let through = boundary.order()?;
            let count = if position.offset().compare_order(&boundary)?.is_eq() {
                0
            } else {
                remaining.div_ceil((current.frontier().len() - index) as u32)
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

    pub fn retirement_origins(
        &self,
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        self.catalog
            .entries()
            .iter()
            .map(|lineage| {
                let bound = self.find_bounds(lineage.lineage_id())?;
                self.frontier_position(lineage, bound.initial_sequence.get() - 1)
            })
            .collect()
    }

    /// These are publication bounds, independent of consumer checkpoints.
    /// PostgreSQL transaction IDs can be allocated before an older attempt
    /// commits.
    pub fn retirement_boundaries(
        &self,
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        let high = self.capture_high()?;
        self.retirement_origins()?
            .into_iter()
            .map(|origin| {
                let through = self.read_boundary(&origin, &high)?;
                SystemStreamPosition::try_new(
                    origin.shard_id().as_str(),
                    origin.lineage_id().as_str(),
                    through.as_str(),
                )
            })
            .collect()
    }

    /// A fresh reader starts at durable retained prefixes. Issued checkpoints
    /// remain strict: missing history behind a retirement frontier is a gap.
    pub fn retained_checkpoint(
        &self,
        retired: &[SystemStreamPosition],
    ) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        let high = self.capture_high()?;
        for position in retired {
            let lineage = self
                .catalog
                .lineage(position.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            if lineage.shard_id() != position.shard_id() {
                return Err(SystemStreamContractError::BackendContract);
            }
            self.read_boundary(position, &high)?;
        }
        let mut current = self.initial_checkpoint()?;
        loop {
            let mut positions = current.frontier().to_vec();
            for position in &mut positions {
                if let Some(floor) = retired
                    .iter()
                    .find(|floor| floor.lineage_id() == position.lineage_id())
                    && position.offset().compare_order(floor.offset())?.is_lt()
                {
                    *position = floor.clone();
                }
            }
            let floored = SystemStreamCheckpoint::try_new(current.scope().clone(), positions)?;
            let expanded = self.expand_frontier(&floored, &high)?;
            if expanded == floored {
                return Ok(expanded);
            }
            current = expanded;
        }
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
                && provider_more == Some(true)
            {
                return Err(SystemStreamContractError::BackendContract);
            }
            let position = positions
                .iter_mut()
                .find(|position| position.lineage_id() == read.after.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            // Private progress records the consumed sequence. Public record
            // suffixes must not prevent equality with a captured numeric high.
            *position = if provider_more == Some(true) {
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
        if observation.read != read || count > read.limit || more == Some(true) && count == 0 {
            return Err(SystemStreamContractError::BackendContract);
        }
        let after = SystemStreamHistory::parse_offset(&read.after)?;
        let mut previous = read.after.offset().as_str();
        let mut previous_order = after;
        for row in rows {
            if row.position.shard_id() != read.after.shard_id()
                || row.position.lineage_id() != read.after.lineage_id()
                || {
                    (read.ordering == SystemStreamHistoryOrdering::Versionstamped
                        && row.position.offset().versionstamp().is_none())
                        || (read.ordering == SystemStreamHistoryOrdering::Versionstamped
                            && row.position.offset().as_str() <= previous)
                        || (read.ordering == SystemStreamHistoryOrdering::SparseNumeric
                            && SystemStreamHistory::parse_offset(&row.position)? <= previous_order)
                        || SystemStreamHistory::parse_offset(&row.position)? > read.through
                }
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
