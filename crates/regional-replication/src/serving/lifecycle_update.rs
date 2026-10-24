use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, describe_table_from_table, resolve_table_in_attempt,
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
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let observed_at = self.port.now();
            let request = Arc::clone(&request);
            let cache = Arc::clone(&cache);
            let journal = Arc::clone(&self.journal);
            let configuration = Arc::clone(&self.configuration);
            async move {
                self.port
                    .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                        Box::pin(async move {
                            let (table, _) =
                                resolve_table_in_attempt(attempt, &cache, request.table_name())
                                    .await?;
                            journal.require_readable(attempt, &table).await?;
                            let state = journal
                                .load_capture_state(attempt, &table)
                                .await?
                                .ok_or_else(|| {
                                    StorageError::validation(
                                        "regional lifecycle control requires a global table",
                                    )
                                })?;
                            let current = journal
                                .load_configuration_control(
                                    attempt,
                                    state.state(),
                                    storage_types::ReadProtection::Shared,
                                )
                                .await?;
                            let proposal =
                                request.bind(state.state(), &current.proposal, &configuration)?;
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
