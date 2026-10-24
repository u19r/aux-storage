use std::sync::Arc;

use storage_provider::{GsiIndexReadiness, TableReclaimStep};
use storage_types::{
    CreateTableRequest, DeleteTableRequest, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse,
    IndexName, ListTablesRequest, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    TableName, TimestampMillis, UpdateTableRequest, UpdateTableResponse, UpdateTimeToLiveRequest,
    UpdateTimeToLiveResponse, context::WrappedError as _,
};
use stream::StreamError;
use stream_provider::{SystemStreamPage, SystemStreamPageRequest};

use crate::{
    admission::AdmissionClass,
    database_manager::{
        constants::ROUTED_DEFAULT_CONNECTION_ID, core::DatabaseManager,
        operation_metrics::record_storage_operation,
    },
};

impl DatabaseManager {
    pub async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        let owned_request = request.clone();
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| async move {
                record_storage_operation("create_table", dynamo.create_table(owned_request)).await
            },
        )
        .await?;
        Ok(())
    }

    pub async fn create_table_on_connection(
        &self,
        connection_id: &str,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        let owned_request = request.clone();
        self.run_dynamo_admitted(connection_id, AdmissionClass::Write, |dynamo| async move {
            record_storage_operation("create_table", dynamo.create_table(owned_request)).await
        })
        .await?;
        Ok(())
    }

    pub async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        Ok(self.resolve_table_info(table_name).await?.as_ref().clone())
    }

    pub async fn get_gsi_index_readiness(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<(IndexName, GsiIndexReadiness)>> {
        let repository = self.gsi_lifecycle_repository.clone().ok_or_else(|| {
            StorageError::unsupported("GSI lifecycle reads are unavailable for this manager")
        })?;
        let index_names = index_names.to_vec();
        let table_name = table_name.clone();
        self.admission_controller_for_connection(ROUTED_DEFAULT_CONNECTION_ID)?
            .settle_foreground(AdmissionClass::PointRead, || async move {
                record_storage_operation(
                    "read_gsi_index_readiness",
                    repository.read_gsi_index_readiness(&table_name, &index_names),
                )
                .await
            })
            .await
    }

    /// Read physical table metadata through the reserved control lane.
    ///
    /// This is for bounded control-plane work which must address a physical
    /// table directly (for example, a shared tenant table) and therefore
    /// cannot use the logical namespace-routing path used by
    /// [`Self::get_table_info`].  Foreground request code must use
    /// [`Self::get_table_info`] instead.
    pub async fn get_table_info_for_control(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        let table_name = table_name.clone();
        self.run_dynamo_control(ROUTED_DEFAULT_CONNECTION_ID, |dynamo| async move {
            record_storage_operation(
                "get_table_info_for_control",
                dynamo.get_table_info(table_name),
            )
            .await
            .map(Arc::unwrap_or_clone)
        })
        .await
    }

    /// Read the provider-owned logical stream scope through the reserved
    /// control lane for internal inspection without consuming a public
    /// range-read slot.
    pub async fn get_system_stream_scope_for_control(
        &self,
    ) -> StorageResult<storage_types::SystemStreamScope> {
        self.run_stream_control(ROUTED_DEFAULT_CONNECTION_ID, |stream| async move {
            record_storage_operation("get_system_stream_scope_for_control", async move {
                stream
                    .system_stream_scope()
                    .await
                    .map_err(|error| StorageError::from(StreamError::into_storage_enum(error)))
            })
            .await
        })
        .await
    }

    /// Read one provider-owned system-stream page through the reserved
    /// control lane for internal reclamation inspection. Public stream APIs
    /// retain their foreground admission semantics.
    pub async fn get_system_stream_page_for_control(
        &self,
        request: SystemStreamPageRequest,
    ) -> StorageResult<SystemStreamPage> {
        self.run_stream_control(ROUTED_DEFAULT_CONNECTION_ID, |stream| async move {
            record_storage_operation("get_system_stream_page_for_control", async move {
                stream
                    .get_system_stream_page(request)
                    .await
                    .map_err(|error| StorageError::from(StreamError::into_storage_enum(error)))
            })
            .await
        })
        .await
    }

    pub(crate) async fn resolve_table_info(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        Ok(self
            .resolve_storage_operation(table_name.clone())
            .await?
            .table_info)
    }

    pub(super) async fn load_physical_table_info(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        let table_name = table_name.clone();
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            |dynamo| async move {
                record_storage_operation("get_table_info", dynamo.get_table_info(table_name)).await
            },
        )
        .await
    }

    pub async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        let table_name = table_name.clone();
        match self
            .run_dynamo_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::PointRead,
                |dynamo| async move {
                    record_storage_operation("table_exists", dynamo.get_table_info(table_name))
                        .await
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if matches!(
                    error.to_enum(),
                    StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| async move {
                record_storage_operation("update_time_to_live", dynamo.update_time_to_live(request))
                    .await
            },
        )
        .await
    }

    pub async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        let request = DescribeTimeToLiveRequest {
            table_name: table_name.clone(),
        };
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            |dynamo| async move {
                record_storage_operation(
                    "describe_time_to_live",
                    dynamo.describe_time_to_live(request),
                )
                .await
            },
        )
        .await
    }

    /// Run the lightweight process-readiness probe through the reserved
    /// control lane.  Readiness must remain observable while foreground work
    /// is being shed and must not consume an adaptive foreground permit.
    pub async fn check_ready(&self) -> StorageResult<()> {
        self.check_background_services()?;
        self.run_dynamo_control(ROUTED_DEFAULT_CONNECTION_ID, |dynamo| async move {
            dynamo
                .list_tables(ListTablesRequest {
                    exclusive_start_table_name: None,
                    limit: Some(1),
                })
                .await
                .map(|_| ())
        })
        .await?;
        self.check_background_services()
    }

    pub async fn clear_all_tables(&self) -> StorageResult<()> {
        loop {
            let tables = self
                .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                    dynamo
                        .list_tables(ListTablesRequest {
                            exclusive_start_table_name: None,
                            limit: Some(storage_types::MAX_LIST_TABLES_LIMIT),
                        })
                        .await
                })
                .await?;
            if tables.table_names.is_empty() {
                break;
            }
            for table_name in tables.table_names {
                self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
                    dynamo.delete_table(DeleteTableRequest { table_name }).await
                })
                .await?;
            }
        }
        let reclaim = self.table_reclaim_repository.as_ref().ok_or_else(|| {
            StorageError::unsupported("test table cleanup requires local table reclaim")
        })?;
        while reclaim
            .reclaim_table_page_at(TimestampMillis::from_timestamp(i64::MAX))
            .await?
            == TableReclaimStep::Advanced
        {}
        Ok(())
    }

    pub async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        let request = DeleteTableRequest {
            table_name: table_name.clone(),
        };
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| async move {
                record_storage_operation("delete_table", dynamo.delete_table(request)).await
            },
        )
        .await?;
        Ok(())
    }

    pub async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            |dynamo| async move {
                record_storage_operation("update_table", dynamo.update_table(request)).await
            },
        )
        .await
    }

    /// Update a physical table through the reserved control lane.
    ///
    /// This deliberately does not perform logical namespace routing or
    /// replica-control-plane bookkeeping. It is restricted to direct
    /// physical table maintenance such as enabling streams on a shared table;
    /// callers performing ordinary table mutations must use
    /// [`Self::update_table`].
    pub async fn update_table_for_control(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        self.run_dynamo_control(ROUTED_DEFAULT_CONNECTION_ID, |dynamo| async move {
            record_storage_operation("update_table_for_control", dynamo.update_table(request)).await
        })
        .await
    }
}
