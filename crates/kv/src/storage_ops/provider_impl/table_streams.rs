use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn delete_table_stream_storage(
        &self,
        table_name: &TableName,
    ) -> StorageResult<()> {
        let table_stream = StreamName::table_stream(table_name);
        if let Some(family) = self
            .load_ordered_log_family_state(&table_stream)
            .await
            .map_err(stream_provider::StreamError::into_storage_enum)?
        {
            for prefix in crate::partition_family::ordered_log_partition_prefixes_for_infos(
                &table_stream,
                &family.partitions,
            ) {
                self.kv_store.delete_prefix(prefix).await?;
            }
            self.delete_partition_family_state(
                crate::partition_family::PartitionFamilyKind::OrderedLog,
                &crate::partition_family::ordered_log_family_component(&table_stream),
            )
            .await?;
            let marker_key = crate::partition_family::stream_partition_marker_key(&table_stream);
            let _ = self.kv_store.delete(&marker_key).await;
        }

        self.kv_store
            .delete_prefix(stream_storage_prefix(&table_stream))
            .await?;
        self.kv_store
            .delete_prefix(table_item_stream_storage_prefix(table_name))
            .await?;
        Ok(())
    }
}

fn stream_storage_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut prefix: Vec<u8> = stream_name.into();
    prefix.push(b'/');
    prefix
}

fn table_item_stream_storage_prefix(table_name: &TableName) -> Vec<u8> {
    let mut prefix = table_name.sanitized_name().as_bytes().to_vec();
    prefix.extend_from_slice(b"/stream-item/");
    prefix
}
