use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn save_table_info(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
    ) -> StorageResult<()> {
        let current_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let identity = TableIdentity::user_indexes_for_table(
            current_metadata.identity.table_id,
            table_name,
            table_info.global_secondary_indexes.as_deref(),
        );
        let metadata = StoredTableMetadata::active(identity, table_info.clone());
        let key = compact::table_metadata_key(current_metadata.identity.table_id);
        let value = storage_types::storage_serde::to_bytes(&metadata)?;
        self.kv_store.put(&key, &value, None).await?;
        self.cache_table_identity(Arc::new(metadata));
        Ok(())
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
