use storage_sync::{ResolvedSyncMutation, SyncMutationResponse};
use storage_types::{
    BillingMode, BillingModeSummary, CreateTableResponse, DeleteTableResponse,
    GlobalSecondaryIndexDescription, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    TableDescription, TableStatus, TimeToLiveDescription, TimeToLiveSpecification,
    TimeToLiveStatus, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
    context::WrappedError as _,
};

use crate::DatabaseManager;

impl DatabaseManager {
    pub(crate) async fn apply_lifecycle_sync_mutation(
        &self,
        mutation: ResolvedSyncMutation,
    ) -> StorageResult<SyncMutationResponse> {
        match mutation {
            ResolvedSyncMutation::CreateTable(mutation) => {
                let request = serde_json::from_str::<storage_types::CreateTableRequest>(
                    &mutation.request_json,
                )?;
                if self.table_exists(&request.table_name).await? {
                    let table_info = self.get_table_info(&request.table_name).await?;
                    return sync_response_json(&create_table_response(table_info));
                }
                self.create_table(&request).await?;
                let table_info = self.get_table_info(&request.table_name).await?;
                sync_response_json(&create_table_response(table_info))
            }
            ResolvedSyncMutation::UpdateTable(mutation) => {
                let request = serde_json::from_str::<storage_types::UpdateTableRequest>(
                    &mutation.request_json,
                )?;
                let response = self.update_table(request).await?;
                sync_response_json(&response)
            }
            ResolvedSyncMutation::DeleteTable(mutation) => {
                let request = serde_json::from_str::<storage_types::DeleteTableRequest>(
                    &mutation.request_json,
                )?;
                let table_info = self.get_table_info(&request.table_name).await?;
                self.delete_table(&request.table_name).await?;
                sync_response_json(&delete_table_response(table_info))
            }
            ResolvedSyncMutation::UpdateTimeToLive(mutation) => {
                let request =
                    serde_json::from_str::<UpdateTimeToLiveRequest>(&mutation.request_json)?;
                let response = match self.update_time_to_live(request.clone()).await {
                    Ok(response) => response,
                    Err(error) if ttl_replay_in_progress(&error) => {
                        self.idempotent_ttl_replay_response(request).await?
                    }
                    Err(error) => return Err(error),
                };
                sync_response_json(&response)
            }
            ResolvedSyncMutation::Put(_) | ResolvedSyncMutation::Delete(_) => Err(
                StorageError::internal("item sync mutation reached lifecycle apply path"),
            ),
        }
    }
}

impl DatabaseManager {
    async fn idempotent_ttl_replay_response(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        let description = self
            .describe_time_to_live(&request.table_name)
            .await?
            .time_to_live_description
            .ok_or_else(|| StorageError::validation(TTL_UPDATE_IN_PROGRESS_MESSAGE))?;
        ttl_replay_response_from_description(&request.time_to_live_specification, description)
    }
}

const TTL_UPDATE_IN_PROGRESS_MESSAGE: &str =
    "Time to live configuration update in progress; retry later";

fn ttl_replay_in_progress(error: &StorageError) -> bool {
    matches!(
        error.to_enum(),
        StorageEnum::Validation { message } if message == TTL_UPDATE_IN_PROGRESS_MESSAGE
    )
}

fn ttl_replay_response_from_description(
    requested: &TimeToLiveSpecification,
    description: TimeToLiveDescription,
) -> StorageResult<UpdateTimeToLiveResponse> {
    if requested.enabled {
        let Some(attribute_name) = description.attribute_name.as_deref() else {
            return Err(StorageError::validation(TTL_UPDATE_IN_PROGRESS_MESSAGE));
        };
        if attribute_name == requested.attribute_name
            && matches!(
                description.time_to_live_status,
                TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
            )
        {
            return Ok(UpdateTimeToLiveResponse {
                time_to_live_specification: requested.clone(),
            });
        }
    }

    if !requested.enabled
        && matches!(
            description.time_to_live_status,
            TimeToLiveStatus::Disabling | TimeToLiveStatus::Disabled
        )
    {
        let mut time_to_live_specification = requested.clone();
        if let Some(attribute_name) = description.attribute_name {
            time_to_live_specification.attribute_name = attribute_name;
        }
        time_to_live_specification.enabled = false;
        return Ok(UpdateTimeToLiveResponse {
            time_to_live_specification,
        });
    }

    Err(StorageError::validation(TTL_UPDATE_IN_PROGRESS_MESSAGE))
}

fn sync_response_json<T>(response: &T) -> StorageResult<SyncMutationResponse>
where T: serde::Serialize {
    Ok(SyncMutationResponse {
        response_json: Some(serde_json::to_string(response)?),
    })
}

fn create_table_response(table_info: StoredTableInfo) -> CreateTableResponse {
    CreateTableResponse {
        table_description: table_description(table_info, None),
    }
}

fn delete_table_response(table_info: StoredTableInfo) -> DeleteTableResponse {
    DeleteTableResponse {
        table_description: table_description(table_info, Some(TableStatus::Deleting)),
    }
}

fn table_description(
    table_info: StoredTableInfo,
    status_override: Option<TableStatus>,
) -> TableDescription {
    TableDescription {
        table_name: table_info.table_name.clone(),
        table_status: status_override.unwrap_or(table_info.table_status),
        created_at: table_info.created_at.into(),
        attribute_definitions: table_info.attribute_definitions,
        key_schema: table_info.key_schema,
        max_indexers: table_info.max_indexers,
        table_size_bytes: table_info.table_size_bytes,
        item_count: table_info.item_count,
        table_arn: format!(
            "arn:aws:dynamodb:us-east-1:123456789012:table/{}",
            table_info.table_name
        ),
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
        latest_stream_arn: None,
        latest_stream_label: None,
        deletion_protection_enabled: table_info.deletion_protection_enabled,
    }
}
