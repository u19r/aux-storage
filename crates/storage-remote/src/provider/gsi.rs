use async_trait::async_trait;
use storage_provider::{GsiReadinessRecord, maintenance::GsiLifecycleRepository};
use storage_types::{
    DescribeTableRequest, DescribeTableResponse, IndexName, StorageResult, TableName,
};

use super::RemoteStorageProvider;

#[async_trait]
impl GsiLifecycleRepository for RemoteStorageProvider {
    async fn read_gsi_readiness_records(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<GsiReadinessRecord>> {
        let response: DescribeTableResponse = self
            .invoke(
                "DynamoDB_20120810.DescribeTable",
                &DescribeTableRequest {
                    table_name: table_name.clone(),
                },
            )
            .await?;
        let indexes = response.table.global_secondary_indexes.unwrap_or_default();
        Ok(
            super::provider_helpers::gsi_index_readiness(&indexes, index_names)?
                .into_iter()
                .map(|(index_name, readiness)| GsiReadinessRecord {
                    table_name: table_name.clone(),
                    index_name,
                    readiness,
                })
                .collect(),
        )
    }
}
