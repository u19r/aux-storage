use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn atomic_item_read_modify_write_impl(
        &self,
        request: AtomicItemReadModifyWriteRequest,
    ) -> StorageResult<Vec<u8>> {
        apply_gsi_write_pressure(self).await?;
        let metadata = self
            .get_table_identity_from_name(&request.table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&request.table_name))?;
        validate_key_attributes_for_schema(&metadata.table_info.key_schema, &request.key)?;
        let item_key = ItemKey::from_key_schema(
            request.table_name.clone(),
            &metadata.table_info.key_schema,
            &request.key,
        )?;
        let read_key = table_keys::item_key(&metadata.identity, &item_key)?;
        let ttl_config = self.load_ttl_config(&request.table_name).await?;
        let transform = request.transform;
        let metadata_for_transform = Arc::clone(&metadata);
        let read_key_for_transform = read_key.clone();
        let adapter: AtomicTableWriteTransform = Arc::new(move |current_bytes| {
            let current = current_bytes
                .map(deserialize_item_from_bytes)
                .transpose()?;
            match transform(current.as_ref())? {
                AtomicItemWriteDecision::NoWrite { output } => {
                    Ok(AtomicTableWriteDecision::NoWrite { output })
                }
                AtomicItemWriteDecision::Write {
                    item,
                    additional_items,
                    output,
                } => {
                    let mut items = Vec::with_capacity(additional_items.len().saturating_add(1));
                    items.push(item);
                    items.extend(additional_items);
                    let mut operations = Vec::with_capacity(items.len());
                    for mut item in items {
                        normalize_attribute_map_numbers_for_write(&mut item);
                        validate_item_key_attributes_for_schema(
                            &metadata_for_transform.table_info.key_schema,
                            &item,
                        )?;
                        if operations.is_empty() {
                            let item_key = ItemKey::from_key_schema(
                                metadata_for_transform.table_info.table_name.clone(),
                                &metadata_for_transform.table_info.key_schema,
                                &item,
                            )?;
                            let transformed_key = table_keys::item_key(
                                &metadata_for_transform.identity,
                                &item_key,
                            )?;
                            if transformed_key != read_key_for_transform {
                                return Err(StorageError::validation(
                                    "atomic item transform changed the primary item key",
                                ));
                            }
                        }
                        operations.push(TransactWriteTableOperation::Put {
                            table_identity: metadata_for_transform.identity.clone(),
                            table_info: metadata_for_transform.table_info.clone(),
                            item,
                            item_stream_ttl_hours: None,
                            condition: None,
                            return_values_on_condition_check_failure: None,
                            replication: None,
                            ttl_config: ttl_config.clone(),
                        });
                    }
                    Ok(AtomicTableWriteDecision::Write { operations, output })
                }
            }
        });
        self.kv_store
            .atomic_read_modify_write_table(read_key, adapter, self.immediate_gsi_consistency)
            .await
    }
}
