use replication_types::configuration::{control::ConfigurationChange, proposal::ControlProposal};
use storage_provider::{TableUpdateChange, TableUpdateTransaction};
use storage_types::{BoundTableInfo, StorageResult};

use crate::{
    control::{ControlDecision, ControlRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn prepare_table_metadata<
        A: TableUpdateTransaction<Catalog = BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<(BoundTableInfo, TableUpdateChange)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let ConfigurationChange::TableMetadata {
                definition,
                requested_at,
            } = &proposal.operation().change
            else {
                return Err(corrupt_record());
            };
            let state = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            let request = definition.to_request(state.name.clone());
            let (catalog, change) = attempt
                .prepare_table_update(
                    &request,
                    storage_provider::TableUpdateTime::for_control(*requested_at, observed_at),
                )
                .await?
                .into_result()?;
            if catalog.history_id != state.physical {
                return Err(corrupt_record());
            }
            Ok((catalog, change))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn install_table_metadata<
        A: TableUpdateTransaction<Catalog = BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                proposal.operation().change,
                ConfigurationChange::TableMetadata { .. }
            ) {
                return Ok(());
            }
            let (catalog, change) = self
                .prepare_table_metadata(attempt, proposal, observed_at)
                .await?;
            attempt.persist_table_update(catalog, change).await?;
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    /// Installation effects settle before build readiness is queried. An
    /// unready installed control stays durable without issuing a ready receipt.
    pub(crate) async fn require_control_gsi_ready<
        A: TableUpdateTransaction<Catalog = BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        record: &ControlRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if record.decision != ControlDecision::Installed {
                return Ok(());
            }
            let ConfigurationChange::TableMetadata { definition, .. } =
                &record.proposal.operation().change
            else {
                return Ok(());
            };
            let names = definition.created_indexes().cloned().collect::<Vec<_>>();
            if names.is_empty() {
                return Ok(());
            }
            let state = &record
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed;
            let catalog = attempt
                .observe_table_update(&state.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if catalog.history_id != state.physical {
                return Err(corrupt_record());
            }
            for create in definition
                .indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter_map(|update| update.create.as_ref())
            {
                if !catalog
                    .info
                    .global_secondary_indexes
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .any(|index| {
                        index.index_name == create.index_name
                            && index.key_schema == create.key_schema
                            && index.projection == create.projection
                    })
                {
                    return Err(corrupt_record());
                }
            }
            self.require_native_gsi_ready(attempt, &catalog, &names)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_native_gsi_ready<
        A: TableUpdateTransaction<Catalog = BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        catalog: &BoundTableInfo,
        names: &[storage_types::IndexName],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if names.is_empty() {
                return Ok(());
            }
            let readiness = attempt
                .observe_table_update_readiness(catalog)
                .await?
                .select(names)?;
            if readiness.into_iter().any(|(_, ready)| {
                !matches!(ready.index_status, storage_types::IndexStatus::Active)
                    || ready.backfilling
            }) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSchema,
                }
                .into());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_table_gsi_ready<
        A: TableUpdateTransaction<Catalog = BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        catalog: &BoundTableInfo,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let names = catalog
                .info
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|index| index.index_name.clone())
                .collect::<Vec<_>>();
            self.require_native_gsi_ready(attempt, catalog, &names)
                .await
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) async fn require_shared_index_ready<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &replication_types::configuration::installed::InstalledTableState,
        index: &storage_types::IndexName,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(control) = self.load_described_control(attempt, state).await? else {
                return Ok(());
            };
            if matches!(
                control.decision,
                ControlDecision::Completed | ControlDecision::Cancelled
            ) {
                return Ok(());
            }
            if let ConfigurationChange::TableMetadata { definition, .. } =
                &control.proposal.operation().change
                && definition.created_indexes().any(|name| name == index)
            {
                return Err(storage_types::StorageError::validation(format!(
                    "Global secondary index {index} is not ready for queries"
                )));
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
