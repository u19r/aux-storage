use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(in crate::storage_ops) async fn get_table_identity_cached(
        &self,
        cache: &mut HashMap<TableName, Arc<StoredTableMetadata>>,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableMetadata>> {
        if let Some(metadata) = cache.get(table_name) {
            return Ok(Arc::clone(metadata));
        }

        let metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or(StorageError::table_not_found(table_name))?;
        cache.insert(table_name.clone(), Arc::clone(&metadata));
        Ok(metadata)
    }

    pub(in crate::storage_ops) async fn load_ttl_config_cached(
        &self,
        cache: &mut HashMap<TableName, Option<TtlConfigRecord>>,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(config) = cache.get(table_name) {
            return Ok(config.clone());
        }

        let config = self.load_ttl_config(table_name).await?;
        cache.insert(table_name.clone(), config.clone());
        Ok(config)
    }
}
