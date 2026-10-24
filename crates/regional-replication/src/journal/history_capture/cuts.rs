use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{HistoryCapturePhase, HistoryCaptureRecord};
use crate::{
    bootstrap::SourceCaptureCut,
    history_capture::{HistoryCaptureRequest, HistoryCaptureVectorPage},
    journal::{
        JournalStore,
        history_hold::{SourceHoldMutation, SourceHoldWrite},
    },
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn advance_history_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut HistoryCaptureRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let next = match record.phase {
                HistoryCapturePhase::Holding(next)
                | HistoryCapturePhase::High(next)
                | HistoryCapturePhase::Releasing(next)
                | HistoryCapturePhase::Cancelling(next) => next,
                _ => return Err(corrupt_record()),
            };
            let count = u32::from(self.partitions);
            let width = u32::try_from(
                (self.records.limits.max_records().get().saturating_sub(1) / 2)
                    .clamp(1, crate::constants::HISTORY_CUT_PAGE_WIDTH),
            )
            .map_err(|_| corrupt_record())?;
            if next >= count {
                return Err(corrupt_record());
            }
            let end = next.saturating_add(width).min(count);
            let origins = (next..end)
                .map(|partition| {
                    self.origin(
                        u16::try_from(partition)
                            .map_err(|_| corrupt_record())?
                            .into(),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let keys = origins
                .iter()
                .map(|origin| self.source_hold_key(origin, &record.request.proposal))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if matches!(record.phase, HistoryCapturePhase::Cancelling(_)) {
                self.require_absent_capture_indexes(
                    attempt,
                    &record.request.proposal,
                    &keys,
                    &rows,
                )
                .await?;
            }
            let heads = if matches!(record.phase, HistoryCapturePhase::High(_)) {
                self.load_source_heads(
                    attempt,
                    &origins
                        .iter()
                        .map(|origin| origin.journal_partition)
                        .collect::<Vec<_>>(),
                    ReadProtection::Conflict,
                )
                .await?
            } else {
                Vec::new()
            };
            let mut changes = Vec::with_capacity(keys.len());
            for (index, ((key, row), origin)) in
                keys.into_iter().zip(rows).zip(&origins).enumerate()
            {
                if let Some(change) = self
                    .prepare_history_hold((key, row), (record, origin, heads.get(index).copied()))?
                {
                    changes.push(change);
                }
            }
            self.store_source_hold_changes(
                attempt,
                record.request.proposal.operation().lineage,
                &changes,
            )
            .await?;
            record.phase = next_history_phase(record.phase, end, count);
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_history_hold(
        &self,
        row: (
            infrastructure_provider::record::DurableRecordKey,
            Option<infrastructure_provider::record::DurableRecord>,
        ),
        input: (
            &mut HistoryCaptureRecord,
            &replication_types::identity::origin::OriginId,
            Option<replication_types::identity::origin::OriginPosition>,
        ),
    ) -> StorageResult<Option<infrastructure_provider::record::RecordChange>> {
        let (record, origin, high) = input;
        let mutation = match record.phase {
            HistoryCapturePhase::Holding(_) => SourceHoldMutation::Create,
            HistoryCapturePhase::High(_) => {
                SourceHoldMutation::CaptureHigh(high.ok_or_else(corrupt_record)?)
            }
            HistoryCapturePhase::Releasing(_) => SourceHoldMutation::Release,
            HistoryCapturePhase::Cancelling(_) => SourceHoldMutation::Cancel,
            _ => return Err(corrupt_record()),
        };
        let change = self.prepare_source_hold_change(
            row,
            SourceHoldWrite {
                origin,
                proposal: &record.request.proposal,
                mutation,
            },
        )?;
        if matches!(record.phase, HistoryCapturePhase::High(_)) {
            let cut = SourceCaptureCut {
                origin: origin.clone(),
                through: high.ok_or_else(corrupt_record)?,
            };
            record.vector = cut.fold_vector(record.vector)?;
        }
        Ok(change)
    }

    pub(crate) async fn load_history_capture_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        after: u32,
    ) -> StorageResult<HistoryCaptureVectorPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, record) = self
                .load_history_capture(attempt, request, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                record.phase,
                HistoryCapturePhase::Ready
                    | HistoryCapturePhase::Releasing(_)
                    | HistoryCapturePhase::Released
            ) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::PublicationHistory,
                }
                .into());
            }
            let count = u32::from(self.partitions);
            if after >= count {
                return Err(corrupt_record());
            }
            let width = u32::try_from(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .min(crate::constants::HISTORY_CUT_PAGE_WIDTH),
            )
            .map_err(|_| corrupt_record())?;
            let end = after.saturating_add(width).min(count);
            let origins = (after..end)
                .map(|partition| {
                    self.origin(
                        u16::try_from(partition)
                            .map_err(|_| corrupt_record())?
                            .into(),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let holds = self
                .load_indexed_source_holds(
                    attempt,
                    (&request.proposal, &origins),
                    ReadProtection::Snapshot,
                )
                .await?;
            let mut cuts = Vec::with_capacity(holds.len());
            for (hold, origin) in holds.into_iter().zip(origins) {
                cuts.push(SourceCaptureCut {
                    origin,
                    through: hold.require_captured_through()?,
                });
            }
            let schema = replication_types::protocol::CanonicalDigest::of_json(&record.schema)
                .map_err(|_| corrupt_record())?;
            Ok(HistoryCaptureVectorPage {
                content: request.content()?,
                schema,
                vector: record.vector,
                cuts,
                next_partition: (end < count).then_some(end),
            })
        }
        .await;
        settlement_guard.complete(result)
    }
}

fn next_history_phase(phase: HistoryCapturePhase, next: u32, count: u32) -> HistoryCapturePhase {
    match (phase, next == count) {
        (HistoryCapturePhase::Holding(_), true) => HistoryCapturePhase::High(0),
        (HistoryCapturePhase::Holding(_), false) => HistoryCapturePhase::Holding(next),
        (HistoryCapturePhase::High(_), true) => HistoryCapturePhase::Ready,
        (HistoryCapturePhase::High(_), false) => HistoryCapturePhase::High(next),
        (HistoryCapturePhase::Releasing(_), true) => HistoryCapturePhase::Released,
        (HistoryCapturePhase::Releasing(_), false) => HistoryCapturePhase::Releasing(next),
        (HistoryCapturePhase::Cancelling(_), true) => HistoryCapturePhase::Cancelled,
        _ => HistoryCapturePhase::Cancelling(next),
    }
}
