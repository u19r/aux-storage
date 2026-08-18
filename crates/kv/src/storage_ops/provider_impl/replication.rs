use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn apply_replication_mutation_impl(
        &self,
        mutation: ReplicationMutation,
    ) -> StorageResult<()> {
        let table_name = mutation.table_name.clone();
        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let replication = Some(mutation.metadata);

        if let Some(mut new_image) = mutation.new_image {
            normalize_attribute_map_numbers_for_write(&mut new_image);
            self.kv_store
                .transact_write_table(
                    vec![TransactWriteTableOperation::Put {
                        table_identity: table_metadata.identity.clone(),
                        table_info,
                        item: new_image,
                        indexers: None,
                        item_stream_ttl_hours: None,
                        condition: None,
                        return_values_on_condition_check_failure: None,
                        replication,
                        ttl_config,
                    }],
                    self.immediate_gsi_consistency,
                )
                .await?;
            return Ok(());
        }

        self.kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Delete {
                    table_identity: table_metadata.identity.clone(),
                    table_info,
                    key: mutation.key,
                    item_stream_ttl_hours: None,
                    use_key_attributes_for_missing_item_condition: false,
                    condition: None,
                    return_values_on_condition_check_failure: None,
                    replication,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await?;
        Ok(())
    }
}
