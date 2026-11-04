use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, RequestTableBindings, describe_table_from_table,
};
use storage_types::{
    AttemptMode, StorageError, StorageResult, UpdateTableRequest, UpdateTableResponse,
};

use super::runtime::RegionalDynamo;
use crate::lifecycle::update::LifecycleUpdate;

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn execute_lifecycle_update(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let request = Arc::new(LifecycleUpdate::try_from_update(request)?);
        let cache = self.port.metadata_cache();
        let bindings = RequestTableBindings::default();
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let observed_at = self.port.now();
            let bindings = bindings.clone();
            let request = Arc::clone(&request);
            let cache = Arc::clone(&cache);
            let journal = Arc::clone(&self.journal);
            let configuration = Arc::clone(&self.configuration);
            async move {
                self.port
                    .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                        Box::pin(async move {
                            let (table, _) = bindings
                                .resolve_table(attempt, &cache, request.table_name())
                                .await?;
                            let proposal = journal
                                .bind_lifecycle_update(attempt, &table, &request, &configuration)
                                .await?;
                            journal
                                .accept_pending_control(attempt, &proposal, observed_at)
                                .await?;
                            let mut response = describe_table_from_table(attempt, &table).await?;
                            journal.project_control_description(
                                &mut response.table,
                                &proposal,
                                crate::control::ControlDecision::Prepared,
                            )?;
                            Ok(UpdateTableResponse {
                                table_description: response.table,
                            })
                        })
                    })
                    .await
            }
        })
        .await
    }
}

impl crate::journal::JournalStore {
    async fn bind_lifecycle_update<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &storage_types::BoundTableInfo,
        request: &LifecycleUpdate,
        configuration: &replication_provider::runtime_configuration::deployment::DeploymentConfiguration,
    ) -> StorageResult<replication_types::configuration::proposal::ControlProposal> {
        self.require_readable(attempt, table).await?;
        let state = self
            .load_capture_state(attempt, table)
            .await?
            .ok_or_else(|| {
                StorageError::validation("regional lifecycle control requires a global table")
            })?;
        let current = self
            .load_configuration_control(
                attempt,
                state.state(),
                storage_types::ReadProtection::Shared,
            )
            .await?;
        request.bind(state.state(), &current.proposal, configuration)
    }
}
