//! Retire old-incarnation scheduling pointers without changing shared proofs.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::JournalStore;
use crate::{constants, control::pending::PendingControlReference, native_record::corrupt_record};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum PendingTableFamily {
    Enable,
    Addition,
    Control,
}

impl PendingTableFamily {
    pub(super) fn next(self) -> Option<Self> {
        match self {
            Self::Enable => Some(Self::Addition),
            Self::Addition => Some(Self::Control),
            Self::Control => None,
        }
    }

    fn prefix(self) -> &'static [u8] {
        match self {
            Self::Enable => constants::ENABLE_PENDING_SUFFIX,
            Self::Addition => constants::ADDITION_PENDING_SUFFIX,
            Self::Control => constants::CONTROL_PENDING_SUFFIX,
        }
    }
}

impl JournalStore {
    pub(super) async fn reclaim_table_trim_reservation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let barrier = self
            .load_reservation_barrier(attempt, ReadProtection::Conflict)
            .await?;
        let (_, count) = barrier.reservation_state();
        let Some(reservation) = self
            .load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
            .await?
        else {
            return Ok(());
        };
        if reservation
            .proposal
            .participant(&self.region)?
            .state
            .physical
            != history
        {
            return Ok(());
        }
        let changes = self
            .prepare_trim_reservation_release(attempt, &reservation.proposal, reservation.grant)
            .await?;
        self.records.store(attempt, &changes).await
    }

    pub(super) async fn reclaim_pending_table_work<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        cursor: (PendingTableFamily, Option<DurableRecordKey>),
    ) -> StorageResult<Option<DurableRecordKey>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let page = self
                .scan_reclaim_family(operation.as_mut(), cursor.0.prefix(), cursor.1)
                .await?;
            let mut changes = Vec::new();
            for row in &page.records {
                if self
                    .load_pending_table_owner(operation.as_mut(), row, cursor.0)
                    .await?
                    == history
                {
                    changes.push(RecordChange::Delete {
                        key: row.key.clone(),
                        expected: row.revision,
                    });
                }
            }
            self.records.store(operation.as_mut(), &changes).await?;
            Ok(page.continuation)
        }
        .await;
        operation.complete(result)
    }

    async fn load_pending_table_owner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        family: PendingTableFamily,
    ) -> StorageResult<TableHistoryId> {
        if let PendingTableFamily::Control = family {
            let reference = self.decode_pending_control_reference(row)?;
            return self.load_pending_control_owner(attempt, reference).await;
        }
        let id: replication_types::identity::resource::ControlOperationId =
            self.records.decode(row)?;
        if row.revision != storage_types::Revision::INITIAL
            || row.key != self.identity_key(family.prefix(), &id)?
        {
            return Err(corrupt_record());
        }
        let prefix = match family {
            PendingTableFamily::Enable => constants::ENABLE_OPERATION_SUFFIX,
            PendingTableFamily::Addition => constants::ADDITION_OPERATION_SUFFIX,
            PendingTableFamily::Control => return Err(corrupt_record()),
        };
        let key = self.identity_key(prefix, &id)?;
        let root = self
            .records
            .load(attempt, &[key], ReadProtection::Conflict)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        match family {
            PendingTableFamily::Enable => {
                Ok(self.decode_enable_record(&root)?.plan.source.physical)
            }
            PendingTableFamily::Addition => Ok(self
                .decode_addition_record(&root)?
                .plan
                .target
                .source
                .physical),
            PendingTableFamily::Control => Err(corrupt_record()),
        }
    }

    async fn load_pending_control_owner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: PendingControlReference,
    ) -> StorageResult<TableHistoryId> {
        let prefix = match reference {
            PendingControlReference::Configuration(_) => constants::CONTROL_OPERATION_SUFFIX,
            PendingControlReference::Trim(_) => constants::PUBLIC_TRIM_PROPOSAL_SUFFIX,
        };
        let key = self.identity_key(prefix, &reference.operation())?;
        let row = self
            .records
            .load(attempt, &[key], ReadProtection::Conflict)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        match reference {
            PendingControlReference::Configuration(_) => {
                let control = self.decode_control_record(&row)?;
                Ok(control
                    .proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    .physical)
            }
            PendingControlReference::Trim(id) => {
                let proposal: crate::trim::proposal::PublicTrimProposal =
                    self.records.decode(&row)?;
                proposal.validate()?;
                if proposal.operation != id || row.revision != storage_types::Revision::INITIAL {
                    return Err(corrupt_record());
                }
                Ok(proposal.participant(&self.region)?.state.physical)
            }
        }
    }
}
