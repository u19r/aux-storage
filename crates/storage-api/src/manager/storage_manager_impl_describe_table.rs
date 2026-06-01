use http_error::HttpApiError;
use storage_types::{
    BillingMode, BillingModeSummary, DescribeTableRequest, DescribeTableResponse,
    GlobalSecondaryIndexDescription, HIDDEN_TTL_INDEX_PREFIX, TableDescription,
};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn describe_table_internal(
        &self,
        request: DescribeTableRequest,
    ) -> Result<Response, HttpApiError> {
        let table_info = self.db().get_table_info(&request.table_name).await?;

        let filtered_gsis = table_info
            .global_secondary_indexes
            .as_ref()
            .map(|gsis| {
                gsis.iter()
                    .filter(|g| !g.index_name.as_ref().starts_with(HIDDEN_TTL_INDEX_PREFIX))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .filter(|indexes| !indexes.is_empty());
        let (latest_stream_arn, latest_stream_label) = Self::latest_stream_metadata(
            &table_info.table_name,
            table_info.created_at,
            table_info.stream_specification.as_ref(),
        );

        let mut table = TableDescription {
            table_name: table_info.table_name.clone(),
            table_status: table_info.table_status,
            created_at: table_info.created_at.into(),
            attribute_definitions: table_info.attribute_definitions,
            key_schema: table_info.key_schema,
            table_size_bytes: table_info.table_size_bytes,
            item_count: table_info.item_count,
            table_arn: Self::table_arn(&table_info.table_name),
            replicas: None,
            multi_region_consistency: None,
            billing_mode_summary: Some(BillingModeSummary {
                billing_mode: Some(BillingMode::PayPerRequest),
                last_update_to_pay_per_request_date_time: None,
            }),
            global_secondary_indexes: filtered_gsis.map(|indexes| {
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
        };
        self.apply_multi_region_state(&mut table).await?;

        let response = DescribeTableResponse { table };

        Ok(Response::DescribeTable(response))
    }
}
