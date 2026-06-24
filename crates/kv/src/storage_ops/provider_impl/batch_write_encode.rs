use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn apply_batch_encode_put_item(
        &self,
        table_name: &TableName,
        item: &WireItem,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<usize> {
        if aux_item_stream_ttl_hours.is_some() {
            let payload_len = item.payload_len();
            self.put_item_with_stream_ttl(
                table_name.clone(),
                item.clone().into_attribute_map()?,
                None,
                None,
                None,
                None,
                aux_item_stream_ttl_hours,
            )
            .await?;
            return Ok(payload_len);
        }
        let table_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let table_info = table_metadata.table_info.clone();

        let ttl_config = self.load_ttl_config(table_name).await?;
        let should_write_stream = crate::backends::common::should_write_stream_entries(
            &table_info,
            self.requires_immediate_gsi_updates(&table_info),
        );
        let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
        let should_write_gsi_immediately = self.requires_immediate_gsi_updates(&table_info);
        let ttl_attribute = if should_track_ttl {
            ttl_config
                .as_ref()
                .map(|config| config.attribute_name.as_str())
        } else {
            None
        };
        let item = normalized_wire_item_for_write(item)?;
        let item = item.as_ref();
        let (item_key, projected_ttl_value) =
            project_wire_item_table_key_and_ttl(item, &table_info, ttl_attribute)?;
        let item_key_bytes = table_keys::item_key(&table_metadata.identity, &item_key)?;
        let item_key_token = if should_track_ttl {
            Some(wire_item_key_token_from_item_key(&item_key)?)
        } else {
            None
        };
        let value = encode_wire_item_storage_bytes(item)?;

        if should_write_gsi_immediately {
            self.apply_batch_encode_put_item_with_immediate_gsi(
                table_name,
                &table_metadata,
                &table_info,
                ttl_config.as_ref(),
                item,
                &item_key_bytes,
                &value,
                should_write_stream,
            )
            .await?;
        } else if should_write_stream || should_track_ttl {
            self.apply_batch_encode_put_item_with_side_effects(
                table_name,
                &table_metadata,
                &table_info,
                ttl_config.as_ref(),
                item,
                &item_key,
                item_key_bytes,
                item_key_token.as_deref(),
                projected_ttl_value,
                value,
                should_write_stream,
                should_track_ttl,
            )
            .await?;
        } else {
            self.kv_store.put(&item_key_bytes, &value, None).await?;
        }

        Ok(item.payload_len())
    }

    pub(super) async fn apply_batch_encode_put_items_immediate_gsi(
        &self,
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        write_requests: &[EncodeWriteRequest],
    ) -> StorageResult<(usize, usize)> {
        let mut planned_items = Vec::with_capacity(write_requests.len());
        let mut planned_values = Vec::with_capacity(write_requests.len());
        let mut keys = Vec::with_capacity(write_requests.len());
        let mut total_bytes_written = 0usize;

        for write_request in write_requests {
            let EncodeWriteRequest {
                put_request: Some(put_request),
                delete_request: None,
            } = write_request
            else {
                return Err(StorageError::validation(
                    "Each WriteRequest must contain exactly one PutRequest",
                ));
            };

            let item = normalized_wire_item_for_write(&put_request.item)?;
            let mapped_item = item.to_attribute_map()?;
            let item_key = ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                &mapped_item,
            )?;
            let item_key = table_keys::item_key(table_identity, &item_key)?;
            total_bytes_written += item.payload_len();
            keys.push(item_key);
            planned_values.push(encode_wire_item_storage_bytes(item.as_ref())?);
            planned_items.push(mapped_item);
        }

        let old_values = self.kv_store.multi_get(keys, true).await?;
        let should_write_stream = crate::backends::common::should_write_stream_entries(
            table_info,
            self.requires_immediate_gsi_updates(table_info),
        );
        let mut operations = Vec::with_capacity(write_requests.len() * 7);
        for ((mapped_item, value), old_value) in
            planned_items.iter().zip(planned_values).zip(old_values)
        {
            let old_item = old_value
                .as_deref()
                .map(decode_wire_item_from_storage_bytes)
                .transpose()?;
            let old_item_map = old_item
                .as_ref()
                .map(WireItem::to_attribute_map)
                .transpose()?;
            if mapped_item.is_empty() {
                return Err(StorageError::validation(
                    "Item must have at least one attribute",
                ));
            }
            let item_key = ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                mapped_item,
            )?;
            let item_key_bytes = table_keys::item_key(table_identity, &item_key)?;
            operations.push(DirectWriteOperation::Put {
                key: item_key_bytes,
                value: value.clone(),
            });
            if should_write_stream {
                let stream_item_id = next_stream_item_id();
                let stream_entries =
                    crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
                        crate::stream::helpers::StreamEntryContext {
                            table_identity,
                            table_name: &table_info.table_name,
                            item_key: &item_key,
                        },
                        value.as_slice(),
                        old_value.as_deref(),
                        stream_item_id,
                        false,
                        None,
                    )?;
                operations.extend(stream_entries.into_iter().map(|(template, value)| {
                    DirectWriteOperation::PutTemplate { template, value }
                }));
            }
            operations.extend(
                Self::gsi_batch_mutations_for_items(
                    table_identity,
                    table_info,
                    old_item_map.as_ref(),
                    Some(mapped_item),
                )?
                .into_iter()
                .map(|item| match item.value {
                    Some(value) => DirectWriteOperation::Put {
                        key: item.key,
                        value,
                    },
                    None => DirectWriteOperation::Delete { key: item.key },
                }),
            );
        }

        self.kv_store.transact_write_unchecked(operations).await?;
        Ok((write_requests.len(), total_bytes_written))
    }

    pub(super) async fn apply_batch_delete_item(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<()> {
        if key.is_empty() {
            return Ok(());
        }

        let table_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(table_name).await?;

        self.kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Delete {
                    table_identity: table_metadata.identity.clone(),
                    table_info,
                    key: key.clone(),
                    item_stream_ttl_hours: aux_item_stream_ttl_hours,
                    use_key_attributes_for_missing_item_condition: false,
                    condition: None,
                    return_values_on_condition_check_failure: None,
                    replication: None,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        Ok(())
    }

    pub(super) fn can_fast_encode_batch(
        &self,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        write_requests: &[EncodeWriteRequest],
    ) -> bool {
        self.requires_immediate_gsi_updates(table_info)
            && !ttl_tracking_enabled(ttl_config)
            && write_requests.iter().all(|request| {
                matches!(
                    request,
                    EncodeWriteRequest {
                        put_request: Some(put_request),
                        delete_request: None
                    } if put_request.aux_item_stream_ttl_hours.is_none()
                )
            })
    }

    #[expect(clippy::too_many_arguments)]
    async fn apply_batch_encode_put_item_with_immediate_gsi(
        &self,
        table_name: &TableName,
        table_metadata: &StoredTableMetadata,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        item: &WireItem,
        item_key_bytes: &[u8],
        value: &[u8],
        should_write_stream: bool,
    ) -> StorageResult<()> {
        let old_item = self
            .kv_store
            .get(item_key_bytes, true)
            .await?
            .as_deref()
            .map(decode_wire_item_from_storage_bytes)
            .transpose()?;
        let old_item_map = old_item
            .as_ref()
            .map(WireItem::to_attribute_map)
            .transpose()?;
        let mapped_item = item.to_attribute_map()?;
        let mut batch_items = Self::prepare_batch_put_item(
            table_name,
            &table_metadata.identity,
            table_info,
            &mapped_item,
            should_write_stream,
            old_item_map.as_ref(),
            true,
        )?;
        batch_items.extend(Self::ttl_index_mutations_for_items(
            table_name,
            &table_metadata.identity,
            table_info,
            ttl_config,
            old_item_map.as_ref(),
            Some(&mapped_item),
        )?);
        if batch_items.is_empty() {
            self.kv_store.put(item_key_bytes, value, None).await?;
            return Ok(());
        }
        self.kv_store.batch_write(batch_items).await
    }

    #[expect(clippy::too_many_arguments)]
    async fn apply_batch_encode_put_item_with_side_effects(
        &self,
        table_name: &TableName,
        table_metadata: &StoredTableMetadata,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        item: &WireItem,
        item_key: &ItemKey,
        item_key_bytes: Vec<u8>,
        item_key_token: Option<&str>,
        projected_ttl_value: Option<i64>,
        value: Vec<u8>,
        should_write_stream: bool,
        should_track_ttl: bool,
    ) -> StorageResult<()> {
        let old_bytes = self.kv_store.get(&item_key_bytes, true).await?;
        let old_item = if should_track_ttl {
            old_bytes
                .as_deref()
                .map(decode_wire_item_from_storage_bytes)
                .transpose()?
        } else {
            None
        };

        let mut operations = Vec::with_capacity(6);
        if should_write_stream {
            let stream_item_id = next_stream_item_id();
            let stream_entries =
                crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
                    crate::stream::helpers::StreamEntryContext {
                        table_identity: &table_metadata.identity,
                        table_name,
                        item_key,
                    },
                    value.as_slice(),
                    old_bytes.as_deref(),
                    stream_item_id,
                    false,
                    None,
                )?;
            operations.extend(stream_entries.into_iter().map(|(template, value)| {
                TransactWriteOperation::PutTemplate {
                    template,
                    value,
                    condition: None,
                }
            }));
        }

        if should_track_ttl {
            operations.extend(ttl_index_direct_operations_for_wire_items(
                &table_metadata.identity,
                table_info,
                ttl_config,
                old_item.as_ref(),
                Some(item),
                item_key_token,
                projected_ttl_value,
            )?);
        }

        operations.push(TransactWriteOperation::Put {
            key: item_key_bytes,
            value,
            condition: None,
        });

        let direct_operations = operations
            .into_iter()
            .map(to_direct_write_operation)
            .collect::<StorageResult<Vec<_>>>()?;
        self.kv_store
            .transact_write_unchecked(direct_operations)
            .await
    }
}
