use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::{CatalogAttempt, DescribeTimeToLiveAttempt};
use storage_types::{ReadProtection, StorageResult};

use super::{
    record::{CapturePhase, CaptureRecord},
    status,
};
use crate::{bootstrap::SourceCaptureStatus, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(crate) async fn begin_source_capture<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<SourceCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let capture = replication_provider::control::source_capture_state(proposal)
                .map_err(|_| corrupt_record())?;
            if proposal.operation().home_region != self.region {
                return Err(corrupt_record());
            }
            if let Some((_, record)) = self
                .load_source_capture(attempt, proposal, ReadProtection::Conflict)
                .await?
            {
                return Ok(status(&record.phase));
            }
            self.require_enable_capture_plan(attempt, proposal).await?;
            if self
                .load_control(attempt, proposal, ReadProtection::Conflict)
                .await?
                .is_some()
            {
                return Err(corrupt_record());
            }
            let target_key = self.identity_key(
                crate::constants::TARGET_BOOTSTRAP_SUFFIX,
                &proposal.operation().id,
            )?;
            if self
                .records
                .load(attempt, &[target_key], ReadProtection::Conflict)
                .await?[0]
                .is_some()
            {
                return Err(corrupt_record());
            }
            self.require_control_revision(attempt, proposal, storage_types::ReadProtection::Shared)
                .await?;
            let before = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            self.require_enable_source_history(attempt, before).await?;
            let slot = self.reserve_control(attempt, proposal).await?;
            let observed = self
                .observe_table_state(
                    attempt,
                    capture.configuration.table,
                    ReadProtection::Conflict,
                )
                .await?;
            self.require_local_source(&capture)?;
            let schema = self.freeze_capture_schema(attempt, &capture).await?;
            self.begin_reenable_inventory(attempt, proposal).await?;
            self.records.store(attempt, &[slot]).await?;
            let vector = crate::bootstrap::SourceCaptureDefinition::vector_seed(proposal, &schema)?;
            let status = self
                .store_source_capture(
                    attempt,
                    &CaptureRecord {
                        proposal: proposal.clone(),
                        schema,
                        vector,
                        phase: CapturePhase::Holding(0),
                    },
                    None,
                )
                .await?;
            self.persist_table_state(attempt, (observed, capture))
                .await?;
            Ok(status)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn freeze_capture_schema<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        state: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<crate::bootstrap::CaptureSchema> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let stamp = attempt
                .read_catalog_stamp(&state.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != state.physical {
                return Err(corrupt_record());
            }
            let physical = attempt.read_table_at_stamp(&state.name, stamp).await?;
            self.require_table_gsi_ready(attempt, &physical).await?;
            let ttl = attempt.read_ttl_config(&physical).await?;
            if let Some(policy) = &ttl {
                policy.validate()?;
            }
            let mut table = physical.info.as_ref().clone();
            table.table_size_bytes = 0;
            table.item_count = 0;
            table.table_status = storage_types::TableStatus::Active;
            Ok(crate::bootstrap::CaptureSchema {
                table,
                ttl: ttl.map(|policy| crate::bootstrap::CaptureTtlPolicy {
                    attribute: policy.attribute_name,
                    status: policy.status,
                }),
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_source_schema<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        table: replication_types::identity::resource::TableIdentity,
    ) -> StorageResult<crate::bootstrap::SourceSchemaObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let source = self
                .observe_table_state(attempt, table, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            let source = source.state();
            self.require_local_source(source)?;
            if source.phase
                != replication_types::configuration::installed::RegionalTablePhase::Active
                || source.configuration.regional
                    != replication_types::configuration::table::RegionalMode::Local
            {
                return Err(corrupt_record());
            }
            let schema = self.freeze_capture_schema(attempt, source).await?;
            Ok(crate::bootstrap::SourceSchemaObservation {
                source: source.clone(),
                schema,
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn require_capture_table<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<replication_types::configuration::installed::InstalledTableState> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let expected = replication_provider::control::source_capture_state(proposal)
                .map_err(|_| corrupt_record())?;
            let observed = self
                .observe_table_state(attempt, expected.configuration.table, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if observed.state() != &expected {
                return Err(corrupt_record());
            }
            self.require_local_source(&expected)?;
            let stamp = attempt
                .read_catalog_stamp(&expected.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != expected.physical {
                return Err(corrupt_record());
            }
            Ok(expected)
        }
        .await;
        settlement.complete(result)
    }
}
