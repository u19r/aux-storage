use std::sync::Arc;

use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn table_exists_impl(&self, table_name: &TableName) -> StorageResult<bool> {
        Ok(self
            .get_table_identity_from_name(table_name)
            .await?
            .is_some())
    }

    pub(super) async fn get_table_info_impl(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        self.get_table_metadata_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))
    }

    pub(super) async fn create_table_storage_impl(
        &self,
        _table_name: &TableName,
        _request: &CreateTableRequest,
    ) -> StorageResult<()> {
        Ok(())
    }

    pub(super) async fn create_table_impl(
        &self,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        let table_name = &request.table_name;

        storage_common::validate_create_table(request)?;
        self.invalidate_table_metadata_cache(table_name);

        if self.table_exists(table_name).await? {
            return Err(StorageError::table_already_exists(table_name));
        }

        for attempt in 0..CREATE_TABLE_CONFLICT_RETRY_ATTEMPTS {
            let created_at = TimestampMillis::now();
            let table_info = table_info_for_create_request(request, created_at);
            let allocator_value = self.kv_store.get(TABLE_ID_ALLOCATOR_KEY, true).await?;
            let table_id = match allocator_value.as_deref() {
                Some(bytes) => decode_table_storage_id(bytes)?,
                None => TableStorageId::new(1),
            };
            let metadata = table_metadata_for_create_request(table_id, table_name, &table_info);
            let operations = create_table_operations(
                table_name,
                table_id,
                &table_info,
                &metadata,
                allocator_value,
                created_at,
            )?;

            match self.kv_store.transact_write_unchecked(operations).await {
                Ok(()) => {
                    self.cache_table_identity(Arc::new(metadata));
                    return Ok(());
                }
                Err(error) if matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed) => {
                    if self.table_exists(table_name).await? {
                        return Err(StorageError::table_already_exists(table_name));
                    }
                    if attempt + 1 < CREATE_TABLE_CONFLICT_RETRY_ATTEMPTS {
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageError::internal("create table retry loop exhausted"))
    }

    pub(super) async fn update_table_status_impl(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        let metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let mut table_info = metadata.table_info.clone();
        table_info.table_status = status.clone();

        let updated_metadata =
            StoredTableMetadata::active(metadata.identity.clone(), table_info.clone());
        let key = compact::table_metadata_key(metadata.identity.table_id);
        let updated_value = storage_types::storage_serde::to_bytes(&updated_metadata)?;
        self.kv_store.put(&key, &updated_value, None).await?;
        self.cache_table_identity(Arc::new(updated_metadata));

        Ok(())
    }

    pub(super) async fn list_tables_impl(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        let metadata = self.load_visible_table_metadata().await?;
        let mut tables = metadata
            .into_iter()
            .filter(|metadata| !metadata.identity.deleted)
            .map(|metadata| metadata.table_info)
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.table_name.as_ref().cmp(right.table_name.as_ref()));
        if let Some(start) = exclusive_start_table_name {
            tables.retain(|table| table.table_name.as_ref() > start.as_ref());
        }
        tables.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(tables)
    }

    pub(super) async fn delete_table_impl(&self, table_name: &TableName) -> StorageResult<()> {
        let table_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        if table_metadata.table_info.deletion_protection_enabled {
            return Err(StorageError::deletion_protection_enabled(table_name));
        }

        self.tombstone_table_metadata(table_name, &table_metadata)
            .await?;
        self.delete_table_data(&table_metadata).await?;
        self.delete_table_stream_storage(table_name).await?;
        self.kv_store
            .transact_write_unchecked(vec![delete_range(ttl::compact_ttl_index_table_range(
                &table_metadata.identity,
            ))])
            .await?;
        self.delete_ttl_config(table_name).await?;

        Ok(())
    }

    async fn load_visible_table_metadata(&self) -> StorageResult<Vec<StoredTableMetadata>> {
        let range = compact::table_metadata_prefix();
        let mut metadata = Vec::new();
        let mut page_token = None;

        loop {
            let page = self
                .kv_store
                .get_range(
                    &range.start,
                    &range.end,
                    Some(MAX_GENERIC_LIMIT),
                    page_token.clone(),
                    true,
                )
                .await?;
            let has_more = page.has_more;
            let mut last_key = None;

            for (key, value) in page.items {
                last_key = Some(RawKey(key.into_vec()));
                metadata.push(storage_types::storage_serde::from_bytes::<
                    StoredTableMetadata,
                >(&value)?);
            }

            if !has_more {
                break;
            }
            let Some(next_page_token) = last_key else {
                break;
            };
            page_token = Some(next_page_token);
        }

        Ok(metadata)
    }

    async fn tombstone_table_metadata(
        &self,
        table_name: &TableName,
        table_metadata: &StoredTableMetadata,
    ) -> StorageResult<()> {
        let deleted_metadata = StoredTableMetadata::tombstone(
            table_metadata.identity.clone(),
            table_metadata.table_info.clone(),
        );
        let metadata_key = compact::table_metadata_key(table_metadata.identity.table_id);
        let name_lookup_key = compact::table_name_lookup_key(table_name.as_ref().as_bytes());
        let deleted_value = storage_types::storage_serde::to_bytes(&deleted_metadata)?;
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::Put {
                    key: metadata_key,
                    value: deleted_value,
                },
                DirectWriteOperation::Delete {
                    key: name_lookup_key,
                },
            ])
            .await?;

        self.cache_table_identity(Arc::new(deleted_metadata));
        Ok(())
    }

    async fn delete_table_data(&self, table_metadata: &StoredTableMetadata) -> StorageResult<()> {
        let mut data_deletes = vec![delete_range(table_keys::primary_item_prefix(
            table_metadata.identity.table_id,
        ))];
        for index in &table_metadata.identity.indexes {
            data_deletes.push(delete_range(compact::gsi_prefix(
                table_metadata.identity.table_id,
                index.index_id,
            )));
            data_deletes.push(delete_range(compact::gsi_tombstone_prefix(
                table_metadata.identity.table_id,
                index.index_id,
            )));
            data_deletes.push(DirectWriteOperation::Delete {
                key: compact::gsi_backfill_key(table_metadata.identity.table_id, index.index_id),
            });
        }

        self.kv_store.transact_write_unchecked(data_deletes).await
    }
}

fn table_info_for_create_request(
    request: &CreateTableRequest,
    created_at: TimestampMillis,
) -> StoredTableInfo {
    let global_secondary_indexes = request
        .global_secondary_indexes
        .clone()
        .map(|indexes| indexes.into_iter().map(Into::into).collect());

    StoredTableInfo {
        table_name: request.table_name.clone(),
        table_status: TableStatus::Active,
        created_at,
        attribute_definitions: request.attribute_definitions.clone(),
        key_schema: request.key_schema.clone(),
        global_secondary_indexes,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: request.stream_specification.clone(),
        table_stream_duration: request.aux_stream_duration_hours.unwrap_or_default(),
        default_item_stream_duration: request
            .aux_default_item_stream_duration_hours
            .unwrap_or_default(),
        deletion_protection_enabled: request.deletion_protection_enabled.unwrap_or(false),
    }
}

fn table_metadata_for_create_request(
    table_id: TableStorageId,
    table_name: &TableName,
    table_info: &StoredTableInfo,
) -> StoredTableMetadata {
    let identity = TableIdentity::user_indexes_for_table(
        table_id,
        table_name,
        table_info.global_secondary_indexes.as_deref(),
    );
    StoredTableMetadata::active(identity, table_info.clone())
}

fn create_table_operations(
    table_name: &TableName,
    table_id: TableStorageId,
    table_info: &StoredTableInfo,
    metadata: &StoredTableMetadata,
    allocator_value: Option<Vec<u8>>,
    created_at: TimestampMillis,
) -> StorageResult<Vec<DirectWriteOperation>> {
    let next_table_id = TableStorageId::new(table_id.get().saturating_add(1));
    let metadata_key = compact::table_metadata_key(table_id);
    let metadata_value = storage_types::storage_serde::to_bytes(metadata)?;
    let name_lookup_key = compact::table_name_lookup_key(table_name.as_ref().as_bytes());
    let plan = plan_table_stream_duration(
        table_name.clone(),
        kv_table_scope_id(table_name),
        crate::storage_ops::stream_duration::table_stream_policy_version(
            table_info.table_stream_duration,
            table_info.default_item_stream_duration,
        ),
        table_info.table_stream_duration,
        table_info.default_item_stream_duration,
        created_at,
    );
    let mut operations = vec![
        DirectWriteOperation::CheckValue {
            key: TABLE_ID_ALLOCATOR_KEY.to_vec(),
            expected_value: allocator_value,
        },
        DirectWriteOperation::CheckValue {
            key: name_lookup_key.clone(),
            expected_value: None,
        },
        DirectWriteOperation::Put {
            key: TABLE_ID_ALLOCATOR_KEY.to_vec(),
            value: encode_table_storage_id(next_table_id),
        },
        DirectWriteOperation::Put {
            key: name_lookup_key,
            value: encode_table_storage_id(table_id),
        },
        DirectWriteOperation::Put {
            key: metadata_key,
            value: metadata_value,
        },
    ];
    operations.extend(stream_trim_state_write_ops_for_identity(
        &metadata.identity,
        storage_provider::StreamTrimStateWrite {
            state: plan.trim_state,
            next_marker: plan.due_marker,
        },
    )?);
    Ok(operations)
}
