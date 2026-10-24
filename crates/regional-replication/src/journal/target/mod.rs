mod cancellation;
mod copy;
mod materialization;
mod record;
mod reservation;

use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use record::TargetRecord;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{
    bootstrap::{HiddenTargetRequest, HiddenTargetReservation},
    constants::TARGET_BOOTSTRAP_SUFFIX,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn observe_hidden_target<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
    ) -> StorageResult<Option<HiddenTargetReservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some((_, record)) = self
                .load_hidden_target(attempt, request, ReadProtection::Snapshot)
                .await?
            else {
                return Ok(None);
            };
            self.require_reservation_receipt(attempt, &record, ReadProtection::Snapshot)
                .await?;
            Ok(Some(record.reservation()))
        }
        .await;
        settlement.complete(result)
    }

    async fn load_hidden_target<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        protection: ReadProtection,
    ) -> StorageResult<Option<(DurableRecord, TargetRecord)>> {
        replication_provider::control::validate_joining_definition(
            &request.operation,
            (&request.source, &request.target),
            &self.region,
        )
        .map_err(|_| corrupt_record())?;
        request.schema.validate(&request.target.name)?;
        let key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &request.operation.id)?;
        let mut rows = self.records.load(attempt, &[key], protection).await?;
        let Some(row) = rows.pop().flatten() else {
            return Ok(None);
        };
        let record: TargetRecord = self.records.decode(&row)?;
        if record.content != request.content()?
            || record.content != record.request.content()?
            || record.state != request.target.bind_physical(record.state.physical)
        {
            return Err(corrupt_record());
        }
        Ok(Some((row, record)))
    }

    async fn require_hidden_table<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        record: &TargetRecord,
        protection: ReadProtection,
    ) -> StorageResult<std::sync::Arc<storage_types::BoundTableInfo>> {
        if record.cancelled {
            return Err(storage_types::StorageError::unsupported(
                "hidden target reservation was cancelled",
            ));
        }
        let observed = self
            .observe_table_state(attempt, record.state.configuration.table, protection)
            .await?
            .ok_or_else(corrupt_record)?;
        if observed.state() != &record.state {
            return Err(corrupt_record());
        }
        self.require_local_source(&record.state)?;
        let stamp = attempt
            .read_catalog_stamp(&record.state.name)
            .await?
            .ok_or_else(corrupt_record)?;
        if stamp.history_id != record.state.physical {
            return Err(corrupt_record());
        }
        let table = attempt
            .read_table_at_stamp(&record.state.name, stamp)
            .await?;
        record.request.schema.require_table(&table.info)?;
        Ok(table)
    }
}

mod cuts;
mod primary_verification;
mod reconstruction;
mod verification;

mod projection_verification;

mod ttl_verification;

mod projection_history;
mod projection_metadata;
mod projection_owners;

mod certification;

mod cleanup;

mod retained_definition;

mod adoption;

mod adoption_cuts;

mod adoption_verification;

mod tombstone;

mod reenable;

mod retained_baseline;
