//! Freeze old baseline dependencies with the same native inventory cursor.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::identity::resource::ControlOperationId;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::record::{InventoryPhase, InventoryRecord};
use crate::{
    bootstrap::{ReenableBaselineDefinition, copy_scope::BaselineCopyBudget},
    constants::REENABLE_BASELINE_INVENTORY_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineInventoryRecord {
    operation: ControlOperationId,
    definition: ReenableBaselineDefinition,
}

impl JournalStore {
    pub(super) async fn capture_retained_baselines<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let InventoryPhase::Baselines {
                lineage,
                initial,
                after,
                mut vector,
                mut count,
                next,
            } = root.phase.clone()
            else {
                return Err(corrupt_record());
            };
            let native_initial = self
                .load_lineage_baseline_definition(
                    attempt,
                    &lineage.survivor,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            if native_initial.content() != initial.content() {
                return Err(corrupt_record());
            }
            let budget = BaselineCopyBudget {
                records: encoding
                    .1
                    .import_events
                    .min(self.records.limits.max_records()),
                bytes: encoding.1.import_bytes.min(self.records.limits.max_bytes()),
            };
            let page = self
                .load_baseline_page(
                    attempt,
                    (lineage.survivor.configuration.lineage, after),
                    (encoding.0, budget),
                )
                .await?;
            for authority in page.authorities {
                initial.require_baseline_scope(&authority)?;
                vector = ReenableBaselineDefinition::fold_bytes(vector, authority.bytes())?;
                count = count.checked_add(1).ok_or_else(corrupt_record)?;
            }
            if let Some(after) = page.continuation {
                root.phase = InventoryPhase::Baselines {
                    lineage,
                    initial,
                    after: Some(after),
                    vector,
                    count,
                    next,
                };
                return Ok(());
            }
            let definition = ReenableBaselineDefinition::from_native(*initial, (count, vector))?;
            definition.require_lineage(&lineage.survivor)?;
            self.store_reenable_baseline_inventory(
                attempt,
                root.proposal.operation().id,
                &definition,
            )
            .await?;
            root.phase = InventoryPhase::Survivor {
                lineage,
                partition: 0,
                next,
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_reenable_baseline_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: replication_types::identity::resource::ControlOperationId,
        lineage: &replication_types::configuration::installed::InstalledTableState,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<ReenableBaselineDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.identity_key(
                REENABLE_BASELINE_INVENTORY_SUFFIX,
                &(operation, lineage.configuration.lineage),
            )?;
            let row = self
                .load_immutable_waves(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let definition = self.decode_reenable_baseline_inventory(&row)?;
            definition.require_lineage(lineage)?;
            Ok(definition)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn decode_reenable_baseline_inventory(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<ReenableBaselineDefinition> {
        let record: BaselineInventoryRecord = self.records.decode(row)?;
        record.definition.validate()?;
        let proposal = record.definition.initial.proposal();
        let home = &proposal
            .participant(&proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .proposed;
        if row.revision != Revision::INITIAL
            || home.configuration.tenant != self.tenant
            || row.key
                != self.identity_key(
                    REENABLE_BASELINE_INVENTORY_SUFFIX,
                    &(record.operation, proposal.operation().lineage),
                )?
        {
            return Err(corrupt_record());
        }
        Ok(record.definition)
    }

    pub(super) async fn store_reenable_baseline_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        definition: &ReenableBaselineDefinition,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            definition.validate()?;
            let proposal = definition.initial.proposal();
            let home = &proposal
                .participant(&proposal.operation().home_region)
                .map_err(|_| corrupt_record())?
                .proposed;
            if home.configuration.tenant != self.tenant {
                return Err(corrupt_record());
            }
            let key = self.identity_key(
                REENABLE_BASELINE_INVENTORY_SUFFIX,
                &(operation, definition.initial.proposal().operation().lineage),
            )?;
            let row = self
                .load_immutable_waves(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(row) = row {
                let old = self.decode_reenable_baseline_inventory(&row)?;
                if old.content != definition.content {
                    return Err(corrupt_record());
                }
                return Ok(());
            }
            let record = BaselineInventoryRecord {
                operation,
                definition: definition.clone(),
            };
            let change = self.records.prepare(key, None, &record)?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement_guard.complete(result)
    }
}
