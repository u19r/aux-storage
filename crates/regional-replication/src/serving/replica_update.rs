use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort, RequestTableBindings};
use storage_types::{AttemptMode, StorageResult, UpdateTableRequest, UpdateTableResponse};

use super::runtime::RegionalDynamo;

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn execute_replica_update(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        if super::replica_request::validate_replica_request(&request)?
            == super::replica_request::ReplicaActionKind::Delete
        {
            return self.execute_lifecycle_update(request).await;
        }
        let request = Arc::new(
            super::replica_create_request::ReplicaCreateRequest::try_from_update(
                request,
                &self.configuration,
            )?,
        );
        let cache = self.port.metadata_cache();
        let bindings = RequestTableBindings::default();
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let request = Arc::clone(&request);
            let bindings = bindings.clone();
            let cache = Arc::clone(&cache);
            let journal = Arc::clone(&self.journal);
            let configuration = Arc::clone(&self.configuration);
            async move {
                self.port
                    .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                        Box::pin(async move {
                            let (table, _) = bindings
                                .resolve_table(attempt, &cache, &request.table)
                                .await?;
                            journal
                                .accept_replica_create(attempt, &table, &request, &configuration)
                                .await
                        })
                    })
                    .await
            }
        })
        .await
    }
}
