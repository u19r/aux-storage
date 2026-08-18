use http_error::HttpApiError;
use storage_sync::SyncWriteRequest;
use storage_types::{
    BillingMode, BillingModeSummary, DeleteTableRequest, DeleteTableResponse,
    GlobalSecondaryIndexDescription, StoredTableInfo, TableDescription, TableStatus,
};

use crate::{
    manager::{
        StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::required_sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn delete_table_internal(
        &self,
        request: DeleteTableRequest,
    ) -> Result<Response, HttpApiError> {
        if let Some(response) = self
            .propose_sync_write_if_configured(|| SyncWriteRequest::DeleteTable(request.clone()))
            .await?
        {
            return Ok(Response::DeleteTable(required_sync_response_at(
                &response,
                0,
                "DeleteTable",
            )?));
        }

        let table_info = self.db().get_table_info(&request.table_name).await?;
        let (latest_stream_arn, latest_stream_label) = Self::latest_stream_metadata(
            &table_info.table_name,
            table_info.created_at,
            table_info.stream_specification.as_ref(),
        );

        self.db().delete_table(&request.table_name).await?;

        let response =
            Self::build_delete_table_response(table_info, latest_stream_arn, latest_stream_label);

        Ok(Response::DeleteTable(response))
    }

    pub(super) fn build_delete_table_response(
        table_info: StoredTableInfo,
        latest_stream_arn: Option<String>,
        latest_stream_label: Option<String>,
    ) -> DeleteTableResponse {
        DeleteTableResponse {
            table_description: TableDescription {
                table_name: table_info.table_name.clone(),
                table_status: TableStatus::Deleting,
                created_at: table_info.created_at.into(),
                attribute_definitions: table_info.attribute_definitions,
                key_schema: table_info.key_schema,
                table_size_bytes: table_info.table_size_bytes,
                item_count: table_info.item_count,
                max_indexers: table_info.max_indexers,
                table_arn: Self::table_arn(&table_info.table_name),
                replicas: None,
                multi_region_consistency: None,
                billing_mode_summary: Some(BillingModeSummary {
                    billing_mode: Some(BillingMode::PayPerRequest),
                    last_update_to_pay_per_request_date_time: None,
                }),
                global_secondary_indexes: table_info.global_secondary_indexes.map(|indexes| {
                    indexes
                        .into_iter()
                        .map(|index| GlobalSecondaryIndexDescription {
                            index_name: index.index_name,
                            key_schema: index.key_schema,
                            projection: index.projection,
                            index_status: None,
                            backfilling: None,
                            provisioned_throughput: None,
                            index_size_bytes: None,
                            item_count: None,
                            index_arn: None,
                        })
                        .collect()
                }),
                local_secondary_indexes: None,
                provisioned_throughput: None,
                stream_specification: table_info.stream_specification,
                latest_stream_arn,
                latest_stream_label,
                deletion_protection_enabled: table_info.deletion_protection_enabled,
            },
        }
    }
}
