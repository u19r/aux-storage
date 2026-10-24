//! An active inventory pins its native original facts across recovery and
//! cleanup.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordChange},
};
use replication_types::identity::resource::ControlOperationId;
use storage_types::{ReadProtection, StorageResult};

use super::record::{InventoryPhase, InventoryRecord, InventorySettlement};
use crate::{
    history::hold_discovery::ReaderHoldObservation,
    journal::{JournalStore, reader_hold::RetentionHoldReference},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) fn reenable_custody_key(
        &self,
        operation: ControlOperationId,
    ) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::READER_HOLD_SUFFIX.to_vec();
        suffix.push(4);
        suffix.extend_from_slice(operation.as_ref());
        self.record_key(suffix)
    }

    pub(super) async fn retain_reenable_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &InventoryRecord,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_reservation_admission(
                attempt,
                crate::journal::trim_reservation::ReservationAdmission::Incomplete,
            )
            .await?;
            self.retain_history_epoch(attempt, root.proposal.operation().lineage)
                .await?;
            let key = self.reenable_custody_key(root.proposal.operation().id)?;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(
                        key,
                        None,
                        &RetentionHoldReference::Reenable(root.proposal.operation().id),
                    )?],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(super) async fn require_reenable_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &InventoryRecord,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let operation = root.proposal.operation().id;
            let row = self
                .records
                .load(
                    attempt,
                    &[self.reenable_custody_key(operation)?],
                    protection,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            if self.decode_retention_hold(&row)? != RetentionHoldReference::Reenable(operation) {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn observe_reenable_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        protection: ReadProtection,
    ) -> StorageResult<ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, root) = self
                .load_reenable_inventory(attempt, operation, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if matches!(
                root.phase,
                InventoryPhase::Released | InventoryPhase::Cancelled
            ) {
                return Err(corrupt_record());
            }
            self.require_reenable_custody(attempt, &root, protection)
                .await?;
            Ok(ReaderHoldObservation::RetainedInventory {
                operation,
                table: root.proposal.operation().table,
                captured_cuts: root.cuts,
                status: if matches!(root.phase, InventoryPhase::Ready) {
                    crate::history::hold_discovery::RetainedInventoryStatus::Ready
                } else {
                    crate::history::hold_discovery::RetainedInventoryStatus::Collecting
                },
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn settle_reenable_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
        settlement: InventorySettlement,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some((row, mut root)) = self
                .load_reenable_inventory(attempt, proposal.operation().id, ReadProtection::Conflict)
                .await?
            else {
                let before = proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .before
                    .as_ref()
                    .ok_or_else(corrupt_record)?;
                return if u64::from(before.configuration.config) == 0 {
                    Ok(())
                } else {
                    Err(corrupt_record())
                };
            };
            if root.proposal != *proposal {
                return Err(corrupt_record());
            }
            let terminal = if matches!(settlement, InventorySettlement::Cancel) {
                InventoryPhase::Cancelled
            } else {
                InventoryPhase::Released
            };
            if matches!(
                (&root.phase, &terminal),
                (InventoryPhase::Cancelled, InventoryPhase::Cancelled)
                    | (InventoryPhase::Released, InventoryPhase::Released)
            ) {
                return Ok(());
            }
            if matches!(
                root.phase,
                InventoryPhase::Cancelled | InventoryPhase::Released
            ) || (matches!(settlement, InventorySettlement::Release)
                && !matches!(root.phase, InventoryPhase::Ready))
            {
                return Err(corrupt_record());
            }
            self.require_reenable_custody(attempt, &root, ReadProtection::Shared)
                .await?;
            let key = self.reenable_custody_key(proposal.operation().id)?;
            let hold = self
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
            root.phase = terminal;
            self.retain_history_epoch(attempt, proposal.operation().lineage)
                .await?;
            self.records
                .store(
                    attempt,
                    &[
                        self.records.prepare(row.key, Some(row.revision), &root)?,
                        RecordChange::Delete {
                            key,
                            expected: hold.revision,
                        },
                    ],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }
}
