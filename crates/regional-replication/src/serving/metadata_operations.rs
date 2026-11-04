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
        table: &storage_types::BoundTableInfo,
        _now: storage_types::TimestampMillis,
    ) -> StorageResult<Self::Response> {
        delete_table_in_attempt(attempt, table).await?.into_result()
    }
}
