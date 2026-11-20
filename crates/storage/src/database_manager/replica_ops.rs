use storage_types::{
    DescribeTableRequest, ReplicaUpdate, StorageError, StorageResult, UpdateTableRequest,
    UpdateTableResponse,
};

use crate::{
    admission::AdmissionClass,
    database_manager::{constants::ROUTED_DEFAULT_CONNECTION_ID, core::DatabaseManager},
};

impl DatabaseManager {
    pub(super) async fn update_table_replicas(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let update = single_replica_update(&request)?;
        let service = self
            .regional_service(ROUTED_DEFAULT_CONNECTION_ID)
            .ok_or_else(|| {
                StorageError::unsupported("multi-region replication is not available")
            })?;
        match (&update.create, &update.update, &update.delete) {
            (Some(create), None, None) => {
                service
                    .create_replica(&request.table_name, &create.region_name)
                    .await?;
            }
            (None, None, Some(delete)) => {
                service
                    .delete_replica(&request.table_name, &delete.region_name)
                    .await?
            }
            _ => {
                return Err(StorageError::validation(
                    "a replica update names exactly one Create or Delete",
                ));
            }
        }
        let table_name = request.table_name.clone();
        let described = self
            .run_dynamo_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::PointRead,
                |dynamo| async move {
                    dynamo
                        .describe_table(DescribeTableRequest { table_name })
                        .await
                },
            )
            .await?;
        Ok(UpdateTableResponse {
            table_description: described.table,
        })
    }
}

/// Replica changes are a request of their own: exactly one update, no other
/// field.
pub(crate) fn single_replica_update(request: &UpdateTableRequest) -> StorageResult<&ReplicaUpdate> {
    let UpdateTableRequest {
        table_name: _,
        max_indexers,
        attribute_definitions,
        billing_mode,
        provisioned_throughput,
        on_demand_throughput,
        deletion_protection_enabled,
        global_secondary_index_updates,
        replica_updates,
        sse_specification,
        stream_specification,
        aux_stream_duration_hours,
        table_class,
    } = request;
    let combined = max_indexers.is_some()
        || attribute_definitions.is_some()
        || billing_mode.is_some()
        || provisioned_throughput.is_some()
        || on_demand_throughput.is_some()
        || deletion_protection_enabled.is_some()
        || global_secondary_index_updates.is_some()
        || sse_specification.is_some()
        || stream_specification.is_some()
        || aux_stream_duration_hours.is_some()
        || table_class.is_some();
    match replica_updates.as_deref() {
        Some([update]) if !combined => Ok(update),
        Some([_]) => Err(StorageError::validation(
            "ReplicaUpdates cannot be combined with other table updates",
        )),
        _ => Err(StorageError::validation(
            "ReplicaUpdates must contain exactly one update",
        )),
    }
}

/// Test-support reads and requests of the regional service (two-store tests,
/// REG-12 section 5.2).
#[cfg(feature = "test-support")]
impl DatabaseManager {
    /// The table's regional binding, whatever its pair.
    pub async fn regional_binding_for_test(
        &self,
        table: &storage_types::TableName,
    ) -> StorageResult<Option<storage_types::RegionalBinding>> {
        let service = self
            .regional_service(ROUTED_DEFAULT_CONNECTION_ID)
            .ok_or_else(|| {
                StorageError::unsupported("multi-region replication is not available")
            })?;
        service.pair.ops.current_binding(table).await
    }

    /// DescribeTable `Replicas` of the table.
    pub async fn replicas_for_test(
        &self,
        table_name: storage_types::TableName,
    ) -> StorageResult<Option<Vec<storage_types::ReplicaDescription>>> {
        let described = self
            .run_dynamo_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::PointRead,
                |dynamo| async move {
                    dynamo
                        .describe_table(DescribeTableRequest { table_name })
                        .await
                },
            )
            .await?;
        Ok(described.table.replicas)
    }

    /// Answer a peer request as the local regional service would (the request
    /// comes from the peer).
    pub async fn regional_request_for_test(
        &self,
        request: regional_replication::message::RegionalRequest,
    ) -> StorageResult<regional_replication::message::RegionalResponse> {
        use regional_replication::RegionalRequestHandler as _;
        let service = self
            .regional_service(ROUTED_DEFAULT_CONNECTION_ID)
            .ok_or_else(|| {
                StorageError::unsupported("multi-region replication is not available")
            })?;
        Ok(service.handle(service.pair.config.peer.id, request).await)
    }
}
