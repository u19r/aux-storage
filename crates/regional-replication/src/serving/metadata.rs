use std::{future::Future, sync::Arc};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoAttempt, DynamoPort, TableMetadataCache, resolve_table_in_attempt,
};
use storage_types::{StorageResult, TableName, TimestampMillis};

use super::runtime::RegionalDynamo;

pub(super) trait LocalCatalogOperation: Clone + Send + 'static {
    type Response: Send + 'static;
    fn table_name(&self) -> &TableName;
    fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        context: (&TableMetadataCache, TimestampMillis),
    ) -> impl Future<Output = StorageResult<Self::Response>> + Send;
}

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn execute_local_catalog<O: LocalCatalogOperation>(
        &self,
        operation: O,
    ) -> StorageResult<O::Response> {
        let name = operation.table_name().clone();
        let cache = self.port.metadata_cache();
        let now = self.port.now();
        let response = retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let operation = operation.clone();
            let cache = Arc::clone(&cache);
            let journal = Arc::clone(&self.journal);
            async move {
                self.port
                    .with_dynamo_schema_attempt(move |attempt| {
                        Box::pin(async move {
                            let (table, _) =
                                resolve_table_in_attempt(attempt, &cache, operation.table_name())
                                    .await?;
                            journal.require_local_metadata(attempt, &table).await?;
                            operation.execute(attempt, (&cache, now)).await
                        })
                    })
                    .await
            }
        })
        .await?;
        cache.invalidate(&name);
        Ok(response)
    }
}
