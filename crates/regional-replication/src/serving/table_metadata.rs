use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{
        control::ConfigurationChange, installed::RegionalTablePhase, metadata::TableMetadataChange,
        table::RegionalMode,
    },
    identity::resource::ControlOperationId,
};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, TableUpdateTransaction, resolve_table_in_attempt,
};
use storage_types::{
    ReadProtection, StorageError, StorageResult, TableDescription, TableStatus, UpdateTableRequest,
    UpdateTableResponse,
};

use super::runtime::RegionalDynamo;
use crate::native_record::corrupt_record;

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn execute_metadata_update(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let operation: ControlOperationId = uuid::Uuid::new_v4()
            .into_bytes()
            .try_into()
            .map_err(|_| corrupt_record())?;
        let requested_at = self.port.now();
        let request = Arc::new(request);
        let cache = self.port.metadata_cache();
        let response = retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let observed_at = self.port.now();
            let request = Arc::clone(&request);
            let cache = Arc::clone(&cache);
            let journal = Arc::clone(&self.journal);
            let configuration = Arc::clone(&self.configuration);
            async move {
                self.port
                    .with_dynamo_schema_attempt(move |attempt| {
                        Box::pin(async move {
                            let (table, _) =
                                resolve_table_in_attempt(attempt, &cache, &request.table_name)
                                    .await?;
                            let state = journal.load_capture_state(attempt, &table).await?;
                            let Some(state) = state.as_ref().filter(|state| {
                                matches!(
                                    state.state().configuration.regional,
                                    RegionalMode::Global { .. }
                                )
                            }) else {
                                journal.require_local_metadata(attempt, &table).await?;
                                return Ok(attempt
                                    .execute_table_update(&request, requested_at)
                                    .await?
                                    .into_result()?
                                    .1);
                            };
                            journal.require_readable(attempt, &table).await?;
                            let RegionalMode::Global { primary, .. } =
                                &state.state().configuration.regional
                            else {
                                return Err(corrupt_record());
                            };
                            if state.state().phase != RegionalTablePhase::Active
                                || primary != &configuration.region
                            {
                                return Err(StorageError::validation(
                                    "table metadata control requires the active configuration \
                                     primary",
                                ));
                            }
                            let current = journal
                                .load_configuration_control(
                                    attempt,
                                    state.state(),
                                    ReadProtection::Shared,
                                )
                                .await?;
                            let definition = TableMetadataChange::from(request.as_ref());
                            let proposal = replication_provider::control::bind_metadata_control(
                                operation,
                                &current.proposal,
                                ConfigurationChange::TableMetadata {
                                    definition,
                                    requested_at,
                                },
                            )
                            .map_err(|_| corrupt_record())?;
                            crate::lifecycle::deployment::require_control_deployment(
                                &proposal,
                                &configuration,
                            )?;
                            let preview = journal
                                .accept_pending_control(attempt, &proposal, observed_at)
                                .await?
                                .ok_or_else(corrupt_record)?;
                            let mut description = TableDescription::from_stored_table_info(
                                preview.into_parts().0.next_table,
                                TableStatus::Active,
                            );
                            journal.project_control_description(
                                &mut description,
                                &proposal,
                                crate::control::ControlDecision::Prepared,
                            )?;
                            Ok(UpdateTableResponse {
                                table_description: description,
                            })
                        })
                    })
                    .await
            }
        })
        .await?;
        cache.invalidate(&request.table_name);
        Ok(response)
    }
}
