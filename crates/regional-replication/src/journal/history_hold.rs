//! Shared operation-scoped original history retention authority.

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange},
};
use replication_types::{
    configuration::proposal::ControlProposal,
    identity::origin::{OriginId, OriginPosition},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::JournalStore;
use crate::{constants::SOURCE_HOLD_SUFFIX, native_record::corrupt_record};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct SourceHold {
    pub origin: OriginId,
    pub lineage: replication_types::identity::resource::TableLineage,
    pub low: OriginPosition,
    operation: replication_types::identity::resource::ControlOperationId,
    content: CanonicalDigest,
    state: SourceHoldState,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum SourceHoldState {
    Capturing,
    Captured(OriginPosition),
    Released(OriginPosition),
}

pub(in crate::journal) enum SourceHoldMutation {
    Create,
    CaptureHigh(OriginPosition),
    Release,
    Cancel,
}

pub(in crate::journal) struct SourceHoldWrite<'a> {
    pub origin: &'a OriginId,
    pub proposal: &'a ControlProposal,
    pub mutation: SourceHoldMutation,
}

impl JournalStore {
    pub(in crate::journal) async fn store_source_hold_changes<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        lineage: replication_types::identity::resource::TableLineage,
        changes: &[RecordChange],
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if changes.is_empty() {
                return Ok(());
            }
            let indexes = self
                .prepare_source_hold_indexes(attempt, lineage, changes)
                .await?;
            let mut retained = Vec::with_capacity(changes.len() + indexes.len());
            retained.extend_from_slice(changes);
            retained.extend(indexes);
            self.retain_history_epoch(attempt, lineage).await?;
            self.records.store(attempt, &retained).await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn prepare_source_hold_change(
        &self,
        row: (DurableRecordKey, Option<DurableRecord>),
        input: SourceHoldWrite<'_>,
    ) -> StorageResult<Option<RecordChange>> {
        let (key, row) = row;
        if key != self.source_hold_key(input.origin, input.proposal)? {
            return Err(corrupt_record());
        }
        if matches!(input.mutation, SourceHoldMutation::Create) {
            if row.is_some() {
                return Err(corrupt_record());
            }
            return self
                .records
                .prepare(
                    key,
                    None,
                    &SourceHold::new(input.origin.clone(), input.proposal),
                )
                .map(Some);
        }
        let Some(row) = row else {
            return if matches!(input.mutation, SourceHoldMutation::Cancel) {
                Ok(None)
            } else {
                Err(corrupt_record())
            };
        };
        let mut hold = self.decode_source_hold(&row)?;
        hold.require_scope(input.origin, input.proposal)?;
        if hold.is_released() {
            return Err(corrupt_record());
        }
        if matches!(input.mutation, SourceHoldMutation::Cancel) {
            return Ok(Some(RecordChange::Delete {
                key,
                expected: row.revision,
            }));
        }
        match input.mutation {
            SourceHoldMutation::CaptureHigh(through) => hold.capture_high(through)?,
            SourceHoldMutation::Release => hold.release()?,
            SourceHoldMutation::Create | SourceHoldMutation::Cancel => return Err(corrupt_record()),
        }
        self.records
            .prepare(key, Some(row.revision), &hold)
            .map(Some)
    }

    pub(in crate::journal) fn source_hold_key(
        &self,
        origin: &OriginId,
        proposal: &ControlProposal,
    ) -> StorageResult<DurableRecordKey> {
        self.source_hold_operation_key(origin, proposal.operation().id)
    }

    pub(in crate::journal) fn decode_source_hold(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<SourceHold> {
        let hold: SourceHold = self.records.decode(row)?;
        let initial = storage_types::Revision::INITIAL;
        let expected = match hold.state {
            SourceHoldState::Capturing => initial,
            SourceHoldState::Captured(_) => initial.checked_next()?,
            SourceHoldState::Released(_) => initial.checked_next()?.checked_next()?,
        };
        if row.revision != expected {
            return Err(corrupt_record());
        }
        if row.key != self.source_hold_operation_key(&hold.origin, hold.operation)?
            || self.origin(hold.origin.journal_partition)? != hold.origin
            || u64::from(hold.low) != 0
        {
            return Err(corrupt_record());
        }
        Ok(hold)
    }

    fn source_hold_operation_key(
        &self,
        origin: &OriginId,
        operation: replication_types::identity::resource::ControlOperationId,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self.identity_key(SOURCE_HOLD_SUFFIX, origin)?.into();
        key.suffix
            .extend_from_slice(CanonicalDigest::of(operation.as_ref()).as_ref());
        key.try_into()
    }
}

impl SourceHold {
    fn new(origin: OriginId, proposal: &ControlProposal) -> Self {
        let operation = proposal.operation();
        Self {
            origin,
            lineage: operation.lineage,
            low: 0.into(),
            operation: operation.id,
            content: operation.content,
            state: SourceHoldState::Capturing,
        }
    }

    pub(in crate::journal) fn require_scope(
        &self,
        origin: &OriginId,
        proposal: &ControlProposal,
    ) -> StorageResult<()> {
        if &self.origin != origin
            || self.lineage != proposal.operation().lineage
            || self.operation != proposal.operation().id
            || self.content != proposal.operation().content
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn require_captured_through(&self) -> StorageResult<OriginPosition> {
        match self.state {
            SourceHoldState::Captured(through) | SourceHoldState::Released(through) => Ok(through),
            SourceHoldState::Capturing => Err(corrupt_record()),
        }
    }

    pub(in crate::journal) fn observe_live_interval(
        &self,
    ) -> StorageResult<crate::history::hold_discovery::CaptureHoldRange> {
        let through = match self.state {
            SourceHoldState::Capturing => None,
            SourceHoldState::Captured(through) => Some(through),
            SourceHoldState::Released(_) => return Err(corrupt_record()),
        };
        Ok(crate::history::hold_discovery::CaptureHoldRange {
            lineage: self.lineage,
            origin: self.origin.clone(),
            after: self.low,
            through,
        })
    }

    pub(in crate::journal) fn is_released(&self) -> bool {
        matches!(self.state, SourceHoldState::Released(_))
    }

    fn capture_high(&mut self, through: OriginPosition) -> StorageResult<()> {
        if !matches!(self.state, SourceHoldState::Capturing) || through < self.low {
            return Err(corrupt_record());
        }
        self.state = SourceHoldState::Captured(through);
        Ok(())
    }

    fn release(&mut self) -> StorageResult<()> {
        let SourceHoldState::Captured(through) = self.state else {
            return Err(corrupt_record());
        };
        self.state = SourceHoldState::Released(through);
        Ok(())
    }
}
