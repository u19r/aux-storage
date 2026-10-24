use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::{CatalogAttempt, DescribeTimeToLiveAttempt};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{CONTROL_OPERATION_SUFFIX, CONTROL_SLOT_SUFFIX},
    control::{ControlDecision, ControlRecord},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn prepare_control<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        operation: &ControlProposal,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<(ControlRecord, Option<storage_provider::TableUpdateChange>)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if let Some(row) = self
                .load_control(attempt, operation, ReadProtection::Conflict)
                .await?
            {
                return self
                    .decode_control(&row, operation)
                    .map(|record| (record, None));
            }
            let (_, bootstrap) = self.require_control_before(attempt, operation).await?;
            if matches!(
                operation.operation().change,
                replication_types::configuration::control::ConfigurationChange::TtlPolicy { .. }
            ) {
                self.prepare_ttl_policy(attempt, operation).await?;
            }
            let metadata = if matches!(
                operation.operation().change,
                replication_types::configuration::control::ConfigurationChange::TableMetadata { .. }
            ) {
                Some(
                    self.prepare_table_metadata(attempt, operation, observed_at)
                        .await?
                        .1,
                )
            } else {
                None
            };
            let slot = self.reserve_control(attempt, operation).await?;
            let record = ControlRecord {
                proposal: operation.clone(),
                decision: ControlDecision::Prepared,
                bootstrap,
                retirement: Vec::new(),
            };
            let key = self.identity_key(CONTROL_OPERATION_SUFFIX, &operation.operation().id)?;
            let change = self.records.prepare(key, None, &record)?;
            self.records.store(attempt, &[slot, change]).await?;
            Ok((record, metadata))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_control_revision<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        operation: &ControlProposal,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let target = operation
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            let state = self
                .observe_table_state(attempt, target.proposed.configuration.table, protection)
                .await?;
            if state.as_ref().map(|state| state.state()) != target.before.as_ref() {
                return Err(corrupt_record());
            }
            self.require_local_source(&target.proposed)?;
            let stamp = attempt
                .read_catalog_stamp(&target.proposed.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != target.proposed.physical {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn reserve_control<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        operation: &ControlProposal,
    ) -> StorageResult<infrastructure_provider::record::RecordChange> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let table = operation
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                .configuration
                .table;
            let key = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let revision = if let Some(row) = &rows[0] {
                let active = self.decode_control_slot(row, table)?;
                if active != ControlSlotOwner::Idle
                    && active != ControlSlotOwner::Configuration(operation.operation().id)
                {
                    return Err(corrupt_record());
                }
                Some(row.revision)
            } else {
                None
            };
            self.prepare_control_slot(
                table,
                revision,
                ControlSlotOwner::Configuration(operation.operation().id),
            )
        }
        .await;
        settlement.complete(result)
    }
}
