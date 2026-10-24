use storage_provider::{DynamoAttempt, delete_table_in_attempt};
use storage_types::{DeleteTableRequest, DeleteTableResponse, StorageResult, TableName};

use super::metadata::LocalCatalogOperation;

impl LocalCatalogOperation for DeleteTableRequest {
    type Response = DeleteTableResponse;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        context: (
            &storage_provider::TableMetadataCache,
            storage_types::TimestampMillis,
        ),
    ) -> StorageResult<Self::Response> {
        delete_table_in_attempt(attempt, context.0, &self, context.1)
            .await?
            .into_result()
    }
}
