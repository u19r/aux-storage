use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn mutate_table_info<T>(
        &self,
        table_name: &TableName,
        mutator: impl FnMut(&mut StoredTableInfo, &TableIdentity) -> StorageResult<T>,
    ) -> StorageResult<(StoredTableMetadata, T)> {
        self.mutate_table_info_with_operations(table_name, mutator, |_, _| Ok(Vec::new()))
            .await
    }

    pub(crate) async fn mutate_table_info_with_operations<T>(
        &self,
        table_name: &TableName,
        mut mutator: impl FnMut(&mut StoredTableInfo, &TableIdentity) -> StorageResult<T>,
        mut additional_operations: impl FnMut(
            &StoredTableMetadata,
            &T,
        ) -> StorageResult<Vec<DirectWriteOperation>>,
    ) -> StorageResult<(StoredTableMetadata, T)> {
        for _ in 0..TABLE_METADATA_CONFLICT_RETRY_ATTEMPTS {
            let lookup_key = compact::table_name_lookup_key(table_name.as_ref().as_bytes());
            let Some(table_id_bytes) = self.kv_store.get(&lookup_key, true).await? else {
                return Err(StorageError::table_not_found(table_name));
            };
            let table_id = decode_table_storage_id(&table_id_bytes)?;
            let key = compact::table_metadata_key(table_id);
            let Some(expected_value) = self.kv_store.get(&key, true).await? else {
                return Err(StorageError::table_not_found(table_name));
            };
            let current: StoredTableMetadata =
                storage_types::storage_serde::from_bytes(&expected_value)?;
            if current.identity.deleted || current.identity.table_name != *table_name {
                return Err(StorageError::table_not_found(table_name));
            }

            let mut table_info = current.table_info.clone();
            let result = mutator(&mut table_info, &current.identity)?;
            let identity = TableIdentity::user_indexes_for_table_with_tenant(
                current.identity.table_id,
                table_name,
                table_info.global_secondary_indexes.as_deref(),
                current.identity.tenant_keyspace.clone(),
            );
            let updated = StoredTableMetadata::active(identity, table_info);
            let value = storage_types::storage_serde::to_bytes(&updated)?;
            let mut operations = vec![
                DirectWriteOperation::CheckValue {
                    key: key.clone(),
                    expected_value: Some(expected_value),
                },
                DirectWriteOperation::Put { key, value },
            ];
            operations.extend(additional_operations(&updated, &result)?);
            match self.kv_store.transact_write_unchecked(operations).await {
                Ok(()) => {
                    self.cache_table_identity(Arc::new(updated.clone()));
                    return Ok((updated, result));
                }
                Err(error) if matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed) => {}
                Err(error) => return Err(error),
            }
        }

        Err(StorageError::internal(
            "table metadata update conflicted too many times",
        ))
    }

    pub(crate) async fn capture_stream_tail(&self) -> StorageResult<Option<String>> {
        let prefix = self
            .kv_store
            .get_prefix(&StreamName::system_table_stream(), false, Some(1), true)
            .await?;
        Ok(prefix
            .items
            .first()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned()))
    }

    pub(crate) async fn load_ttl_config(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(entry) = self.ttl_config_cache_lru.get(table_name) {
            return Ok(entry.config());
        }

        let Some(metadata) = self.get_table_identity_from_name(table_name).await? else {
            return Ok(None);
        };
        let key = compact::ttl_config_key(metadata.identity.table_id);
        let config = match self.kv_store.get(&key, true).await? {
            Some(bytes) => Some(storage_types::storage_serde::from_bytes(&bytes)?),
            None => None,
        };
        self.ttl_config_cache_lru.insert(
            table_name.clone(),
            Arc::new(crate::sorted_kv::TtlConfigCacheEntry::new(config.clone())),
        );
        Ok(config)
    }

    pub(crate) async fn save_ttl_config(
        &self,
        table_name: &TableName,
        config: &TtlConfigRecord,
    ) -> StorageResult<()> {
        let metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let key = compact::ttl_config_key(metadata.identity.table_id);
        let value = storage_types::storage_serde::to_bytes(config)?;
        self.kv_store.put(&key, &value, None).await?;
        self.ttl_config_cache_lru.insert(
            table_name.clone(),
            Arc::new(crate::sorted_kv::TtlConfigCacheEntry::new(Some(
                config.clone(),
            ))),
        );
        Ok(())
    }

    pub(crate) async fn delete_ttl_config(&self, table_name: &TableName) -> StorageResult<()> {
        let Some(metadata) = self.get_table_identity_from_name(table_name).await? else {
            return Ok(());
        };
        let key = compact::ttl_config_key(metadata.identity.table_id);
        let _ = self.kv_store.delete(&key).await;
        self.ttl_config_cache_lru.insert(
            table_name.clone(),
            Arc::new(crate::sorted_kv::TtlConfigCacheEntry::new(None)),
        );
        Ok(())
    }

    pub(crate) async fn list_ttl_configs(
        &self,
    ) -> StorageResult<Vec<(TableName, TtlConfigRecord)>> {
        let range = compact::table_metadata_prefix();
        let scan_result = self
            .kv_store
            .get_range(&range.start, &range.end, None, None::<RawKey>, true)
            .await?;

        let mut configs = Vec::new();
        for (_raw_key, raw_value) in scan_result.items {
            let metadata =
                match storage_types::storage_serde::from_bytes::<StoredTableMetadata>(&raw_value) {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        warn!(error = %err, "ttl.table_metadata.decode_failed");
                        continue;
                    }
                };
            if metadata.identity.deleted {
                continue;
            }
            let key = compact::ttl_config_key(metadata.identity.table_id);
            let Some(raw_config) = self.kv_store.get(&key, true).await? else {
                continue;
            };
            match storage_types::storage_serde::from_bytes::<TtlConfigRecord>(&raw_config) {
                Ok(config) => configs.push((metadata.identity.table_name, config)),
                Err(err) => {
                    let table_name = metadata.identity.table_name;
                    warn!(table=%table_name, error = %err, "ttl.config.decode_failed");
                }
            }
        }

        Ok(configs)
    }
}

pub(in crate::storage_ops) fn kv_table_scope_id(table_name: &TableName) -> String {
    format!("kv-table:{table_name}")
}
