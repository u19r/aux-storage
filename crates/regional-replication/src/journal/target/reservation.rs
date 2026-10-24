use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{DynamoAttempt, create_table_in_attempt};
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use super::record::TargetRecord;
use crate::{
    bootstrap::{HiddenTargetRequest, HiddenTargetReservation},
    constants::{CONTROL_OPERATION_SUFFIX, CONTROL_SLOT_SUFFIX, TARGET_BOOTSTRAP_SUFFIX},
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn reserve_hidden_target<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        now: TimestampMillis,
    ) -> StorageResult<HiddenTargetReservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self
                .load_target_tombstone(attempt, request, ReadProtection::Conflict)
                .await?
            {
                return Err(storage_types::StorageError::validation(
                    "hidden target reservation was cancelled",
                ));
            }
            if let Some((_, record)) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
            {
                self.require_reservation_receipt(attempt, &record, ReadProtection::Shared)
                    .await?;
                return Ok(record.reservation());
            }
            let table = request.target.configuration.table;
            if self
                .observe_table_state(attempt, table, ReadProtection::Conflict)
                .await?
                .is_some()
            {
                return Err(corrupt_record());
            }
            let operation_key =
                self.identity_key(CONTROL_OPERATION_SUFFIX, &request.operation.id)?;
            let capture_key = self.identity_key(
                crate::constants::SOURCE_CAPTURE_SUFFIX,
                &request.operation.id,
            )?;
            if self
                .records
                .load(
                    attempt,
                    &[operation_key, capture_key],
                    ReadProtection::Conflict,
                )
                .await?
                .iter()
                .any(Option::is_some)
            {
                return Err(corrupt_record());
            }
            let slot_key = self.identity_key(CONTROL_SLOT_SUFFIX, &table)?;
            let slots = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&slot_key),
                    ReadProtection::Conflict,
                )
                .await?;
            if slots[0].is_some() {
                return Err(corrupt_record());
            }
            let mut table_info = request.schema.table.clone();
            table_info.created_at = now;
            create_table_in_attempt(attempt, table_info)
                .await?
                .into_result()?;
            let stamp = attempt
                .read_catalog_stamp(&request.target.name)
                .await?
                .ok_or_else(corrupt_record)?;
            let state = request.target.bind_physical(stamp.history_id);
            self.require_local_source(&state)?;
            self.initialize_hidden_ttl(attempt, (request, &state), now)
                .await?;
            self.persist_table_state(attempt, (None, state.clone()))
                .await?;
            let record = TargetRecord {
                request: request.clone(),
                content: request.content()?,
                state,
                definition: None,
                source_vector: None,
                next_page: 0,
                baselines_complete: false,
                last_baseline: None,
                cancelled: false,
                adoption: None,
                reenable: None,
                verification: None,
            };
            let root_key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &request.operation.id)?;
            let changes = [
                self.prepare_control_slot(
                    table,
                    None,
                    ControlSlotOwner::Configuration(request.operation.id),
                )?,
                self.records.prepare(root_key, None, &record)?,
            ];
            self.records.store(attempt, &changes).await?;
            Ok(record.reservation())
        }
        .await;
        settlement.complete(result)
    }

    async fn initialize_hidden_ttl<A: DynamoAttempt>(
        &self,
        attempt: &mut A,
        input: (
            &HiddenTargetRequest,
            &replication_types::configuration::installed::InstalledTableState,
        ),
        now: TimestampMillis,
    ) -> StorageResult<()> {
        let (request, state) = input;
        let Some(policy) = &request.schema.ttl else {
            return Ok(());
        };
        let stamp = attempt
            .read_catalog_stamp(&state.name)
            .await?
            .ok_or_else(corrupt_record)?;
        let table = attempt.read_table_at_stamp(&state.name, stamp).await?;
        let policy = storage_common::TtlConfigRecord::try_new_at(
            policy.attribute.clone(),
            &state.name,
            policy.status.clone(),
            now,
        )?;
        attempt.write_ttl_config(&table, policy).await
    }
}
