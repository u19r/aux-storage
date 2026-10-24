//! Durable DFS performs one parent selection or one completed-event emission.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use replication_types::event::content::CanonicalEvent;
use storage_types::StorageResult;

use super::{
    JournalStore,
    causal_ancestry::RecordedAncestry,
    causal_stream::CausalStreamScope,
    causal_stream_record::{CausalStackFrame, CausalStreamRecord, CausalVisit, CausalVisitState},
};
use crate::{history::causal_stream::CanonicalCausalStreamEvent, native_record::corrupt_record};

impl JournalStore {
    pub(super) async fn advance_causal_step<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &CausalStreamScope<'_>,
        stream: &mut CausalStreamRecord,
    ) -> StorageResult<Option<CanonicalCausalStreamEvent>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if stream.depth == 0 {
                return self.advance_causal_root(attempt, scope, stream).await;
            }
            let (row, frame) = self
                .load_causal_stack(attempt, (stream.operation, stream.depth))
                .await?;
            let event = self
                .load_causal_stream_event(attempt, scope, &frame.event)
                .await?;
            let visit = self
                .load_causal_visit(
                    attempt,
                    (stream.operation, &event.as_ref().id, event.as_ref().digest),
                )
                .await?
                .ok_or_else(corrupt_record)?;
            if visit.1.state != CausalVisitState::Visiting {
                return Err(corrupt_record());
            }
            let ancestry = RecordedAncestry::new(&event);
            if frame.next_parent > ancestry.count() {
                return Err(corrupt_record());
            }
            if let Some(parent) = ancestry.parent(frame.next_parent) {
                let parent = self
                    .load_causal_stream_event(attempt, scope, &parent)
                    .await?;
                ancestry.require_parent(&parent)?;
                self.advance_causal_parent(attempt, stream, (row, frame), &parent)
                    .await?;
                return Ok(None);
            }
            self.complete_causal_node(attempt, stream, row, visit)
                .await?;
            self.render_causal_stream_event(event).map(Some)
        }
        .await;
        settlement.complete(result)
    }
    async fn advance_causal_root<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        scope: &CausalStreamScope<'_>,
        stream: &mut CausalStreamRecord,
    ) -> StorageResult<Option<CanonicalCausalStreamEvent>> {
        if stream.position == stream.head {
            stream.finished = true;
            return Ok(None);
        }
        let through = stream.position.checked_next()?;
        let root = self
            .load_view_record(
                attempt,
                (&scope.request.view, through, &scope.receiver_authority),
                (scope.limits, scope.codec),
            )
            .await?;
        stream.position = through;
        let Some(root) = root else {
            return Ok(None);
        };
        let event = self
            .load_causal_stream_event(attempt, scope, &root.pointer.event)
            .await?;
        let visited = self
            .load_causal_visit(
                attempt,
                (stream.operation, &event.as_ref().id, event.as_ref().digest),
            )
            .await?;
        if let Some((_, visit)) = visited {
            return if visit.state == CausalVisitState::Complete {
                Ok(None)
            } else {
                Err(corrupt_record())
            };
        }
        self.records
            .store(attempt, &self.prepare_causal_push(stream, &event)?)
            .await?;
        stream.depth = 1;
        Ok(None)
    }
    async fn advance_causal_parent<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        stream: &mut CausalStreamRecord,
        frame: (DurableRecord, CausalStackFrame),
        parent: &CanonicalEvent,
    ) -> StorageResult<()> {
        let (row, mut frame) = frame;
        let visited = self
            .load_causal_visit(
                attempt,
                (
                    stream.operation,
                    &parent.as_ref().id,
                    parent.as_ref().digest,
                ),
            )
            .await?;
        if visited
            .as_ref()
            .is_some_and(|(_, visit)| visit.state == CausalVisitState::Visiting)
        {
            return Err(corrupt_record());
        }
        frame.next_parent = frame
            .next_parent
            .checked_add(1)
            .ok_or_else(corrupt_record)?;
        let mut changes = vec![self.records.prepare(row.key, Some(row.revision), &frame)?];
        if visited.is_none() {
            changes.extend(self.prepare_causal_push(stream, parent)?);
            stream.depth = stream.depth.checked_add(1).ok_or_else(corrupt_record)?;
        }
        self.records.store(attempt, &changes).await
    }
    async fn complete_causal_node<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        stream: &mut CausalStreamRecord,
        frame: DurableRecord,
        visit: (DurableRecord, CausalVisit),
    ) -> StorageResult<()> {
        let (row, mut visit) = visit;
        visit.state = CausalVisitState::Complete;
        self.records
            .store(
                attempt,
                &[
                    self.records.prepare(row.key, Some(row.revision), &visit)?,
                    RecordChange::Delete {
                        key: frame.key,
                        expected: frame.revision,
                    },
                ],
            )
            .await?;
        stream.depth = stream.depth.checked_sub(1).ok_or_else(corrupt_record)?;
        Ok(())
    }
}
