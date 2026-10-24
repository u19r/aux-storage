use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::{
    configuration::proposal::ControlProposal,
    identity::origin::{OriginId, OriginPosition},
};
use storage_types::{ReadProtection, StorageResult};

use super::record::{CapturePhase, CaptureRecord};
use crate::{
    journal::{
        JournalStore,
        history_hold::{SourceHoldMutation, SourceHoldWrite},
    },
    native_record::corrupt_record,
};

#[derive(Clone, Copy)]
enum CutAction {
    Hold,
    High,
    Release,
    Finish,
}

impl JournalStore {
    pub(in crate::journal) async fn require_source_capture_cut<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        input: (&crate::bootstrap::SourceCaptureCut, ReadProtection),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (cut, protection) = input;
            if self.origin(cut.origin.journal_partition)? != cut.origin {
                return Err(corrupt_record());
            }
            let hold = self
                .load_indexed_source_holds(
                    attempt,
                    (proposal, std::slice::from_ref(&cut.origin)),
                    protection,
                )
                .await?
                .pop()
                .ok_or_else(corrupt_record)?;
            if hold.require_captured_through()? != cut.through {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_source_capture_cut<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        partition: u32,
    ) -> StorageResult<crate::bootstrap::SourceCaptureCut> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, record) = self
                .load_source_capture(attempt, proposal, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                record.phase,
                CapturePhase::BaselineReady | CapturePhase::Releasing(_) | CapturePhase::Released
            ) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::PublicationHistory,
                }
                .into());
            }
            if partition >= u32::from(self.partitions) {
                return Err(corrupt_record());
            }
            let origin = self.origin(
                u16::try_from(partition)
                    .map_err(|_| corrupt_record())?
                    .into(),
            )?;
            let hold = self
                .load_indexed_source_holds(
                    attempt,
                    (proposal, std::slice::from_ref(&origin)),
                    ReadProtection::Snapshot,
                )
                .await?
                .pop()
                .ok_or_else(corrupt_record)?;
            Ok(crate::bootstrap::SourceCaptureCut {
                origin,
                through: hold.require_captured_through()?,
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn advance_source_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut CaptureRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (next, action) = match record.phase {
                CapturePhase::Holding(next) => (next, CutAction::Hold),
                CapturePhase::High(next) => (next, CutAction::High),
                CapturePhase::Cancelling(next) => (next, CutAction::Release),
                CapturePhase::Releasing(next) => (next, CutAction::Finish),
                _ => return Err(corrupt_record()),
            };
            let count = u32::from(self.partitions);
            if next >= count {
                return Err(corrupt_record());
            }
            let width = u32::try_from(
                (self.records.limits.max_records().get().saturating_sub(1) / 2).clamp(1, 64),
            )
            .map_err(|_| corrupt_record())?;
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
                .map(|origin| self.source_hold_key(origin, &record.proposal))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if matches!(action, CutAction::Release) {
                self.require_absent_capture_indexes(attempt, &record.proposal, &keys, &rows)
                    .await?;
            }
            let heads = if matches!(action, CutAction::High) {
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
                if matches!(action, CutAction::High) {
                    let cut = crate::bootstrap::SourceCaptureCut {
                        origin: origin.clone(),
                        through: heads.get(index).copied().ok_or_else(corrupt_record)?,
                    };
                    record.vector = cut.fold_vector(record.vector)?;
                }
                if let Some(change) = self.prepare_capture_hold(
                    (key, row),
                    (origin, &record.proposal, action, heads.get(index).copied()),
                )? {
                    changes.push(change);
                }
            }
            self.store_source_hold_changes(attempt, record.proposal.operation().lineage, &changes)
                .await?;
            record.phase = next_cut_phase(end, count, action);
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_capture_hold(
        &self,
        row: (DurableRecordKey, Option<DurableRecord>),
        input: (
            &OriginId,
            &ControlProposal,
            CutAction,
            Option<OriginPosition>,
        ),
    ) -> StorageResult<Option<RecordChange>> {
        let (origin, proposal, action, high) = input;
        let mutation = match action {
            CutAction::Hold => SourceHoldMutation::Create,
            CutAction::High => SourceHoldMutation::CaptureHigh(high.ok_or_else(corrupt_record)?),
            CutAction::Release => SourceHoldMutation::Cancel,
            CutAction::Finish => SourceHoldMutation::Release,
        };
        self.prepare_source_hold_change(
            row,
            SourceHoldWrite {
                origin,
                proposal,
                mutation,
            },
        )
    }
}

fn next_cut_phase(next: u32, count: u32, action: CutAction) -> CapturePhase {
    match (action, next == count) {
        (CutAction::Hold, true) => CapturePhase::Live(storage_backfill::ScanProgress {
            low: (),
            after: None,
        }),
        (CutAction::High, true) => CapturePhase::History,
        (CutAction::Release, true) => CapturePhase::Cancelled,
        (CutAction::Finish, true) => CapturePhase::Released,
        (CutAction::Hold, false) => CapturePhase::Holding(next),
        (CutAction::High, false) => CapturePhase::High(next),
        (CutAction::Release, false) => CapturePhase::Cancelling(next),
        (CutAction::Finish, false) => CapturePhase::Releasing(next),
    }
}
