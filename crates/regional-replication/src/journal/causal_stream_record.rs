//! Exact stream, stack and visited-address binding for native traversal state.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::{event::identity::CanonicalEventId, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::JournalStore;
use crate::{
    history::{
        causal_stream::{CanonicalCausalStreamId, CanonicalCausalStreamRequest},
        checkpoint::CanonicalCheckpointId,
        selection::CanonicalViewCursor,
        view::DeliveryPosition,
    },
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CausalStreamRecord {
    pub operation: CanonicalCausalStreamId,
    pub scope: CanonicalViewCursor,
    pub portable: CanonicalCheckpointId,
    pub ancestry: CanonicalDigest,
    pub head: DeliveryPosition,
    pub position: DeliveryPosition,
    pub depth: u64,
    pub step: u64,
    pub last: Option<CanonicalEventId>,
    pub finished: bool,
}
impl CausalStreamRecord {
    pub(super) fn try_new(
        request: &CanonicalCausalStreamRequest,
        bounds: &super::history_checkpoint::ReceiverCheckpointBounds,
    ) -> StorageResult<Self> {
        Ok(Self {
            operation: request.causal_stream,
            scope: bounds.scope.clone(),
            portable: request.portable_checkpoint,
            ancestry: request.ancestry_scopes.binding()?,
            head: bounds.high,
            position: 0u64.into(),
            depth: 0,
            step: 0,
            last: None,
            finished: bounds.high == DeliveryPosition::from(0),
        })
    }
    pub(super) fn require_request(
        &self,
        request: &CanonicalCausalStreamRequest,
        bounds: &super::history_checkpoint::ReceiverCheckpointBounds,
    ) -> StorageResult<()> {
        if self.operation != request.causal_stream
            || self.scope != bounds.scope
            || self.portable != request.portable_checkpoint
            || self.head != bounds.high
            || self.ancestry != request.ancestry_scopes.binding()?
        {
            return Err(StorageError::validation(
                "Causal stream operation does not match its held selection",
            ));
        }
        if self.position > self.head
            || self.depth > self.step
            || u64::from_be_bytes(self.position.bytes()) > self.step
            || (self.depth != 0 && self.position == DeliveryPosition::from(0))
            || (self.step == 0 && self.last.is_some())
            || (self.finished
                && (self.depth != 0 || self.position != self.head || self.last.is_some()))
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CausalStackFrame {
    pub operation: CanonicalCausalStreamId,
    pub depth: u64,
    pub event: CanonicalEventId,
    pub next_parent: usize,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum CausalVisitState {
    Visiting,
    Complete,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CausalVisit {
    pub operation: CanonicalCausalStreamId,
    pub event: CanonicalEventId,
    pub digest: CanonicalDigest,
    pub state: CausalVisitState,
}

impl JournalStore {
    pub(super) fn causal_stack_key(
        &self,
        stream: CanonicalCausalStreamId,
        depth: u64,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(crate::constants::CAUSAL_STACK_SUFFIX, &(stream, depth))
    }
    pub(super) fn causal_visit_key(
        &self,
        stream: CanonicalCausalStreamId,
        event: &CanonicalEventId,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(crate::constants::CAUSAL_VISITED_SUFFIX, &(stream, event))
    }
    pub(super) async fn load_causal_visit<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: (CanonicalCausalStreamId, &CanonicalEventId, CanonicalDigest),
    ) -> StorageResult<Option<(DurableRecord, CausalVisit)>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.causal_visit_key(scope.0, scope.1)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let Some(row) = row else {
                return Ok(None);
            };
            let visit: CausalVisit = self.records.decode(&row)?;
            if row.key != key
                || visit.operation != scope.0
                || visit.event != *scope.1
                || visit.digest != scope.2
            {
                return Err(corrupt_record());
            }
            Ok(Some((row, visit)))
        }
        .await;
        settlement.complete(result)
    }
    pub(super) async fn load_causal_stack<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: (CanonicalCausalStreamId, u64),
    ) -> StorageResult<(DurableRecord, CausalStackFrame)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.causal_stack_key(scope.0, scope.1)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let frame: CausalStackFrame = self.records.decode(&row)?;
            if scope.1 == 0
                || row.key != key
                || frame.operation != scope.0
                || frame.depth != scope.1
            {
                return Err(corrupt_record());
            }
            Ok((row, frame))
        }
        .await;
        settlement.complete(result)
    }
    pub(super) fn prepare_causal_push(
        &self,
        stream: &CausalStreamRecord,
        event: &replication_types::event::content::CanonicalEvent,
    ) -> StorageResult<[RecordChange; 2]> {
        let depth = stream.depth.checked_add(1).ok_or_else(corrupt_record)?;
        let visit = CausalVisit {
            operation: stream.operation,
            event: event.as_ref().id.clone(),
            digest: event.as_ref().digest,
            state: CausalVisitState::Visiting,
        };
        let frame = CausalStackFrame {
            operation: stream.operation,
            depth,
            event: visit.event.clone(),
            next_parent: 0,
        };
        Ok([
            self.records.prepare(
                self.causal_visit_key(stream.operation, &visit.event)?,
                None,
                &visit,
            )?,
            self.records.prepare(
                self.causal_stack_key(stream.operation, depth)?,
                None,
                &frame,
            )?,
        ])
    }
}
