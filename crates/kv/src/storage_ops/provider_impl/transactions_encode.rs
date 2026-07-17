use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn transact_write_items_encode_impl(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items_encode_with_retry_impl(request, WriteRetryPolicy::no_retry())
            .await
    }

    pub(super) async fn transact_write_items_encode_with_retry_impl(
        &self,
        request: TransactWriteItemsEncodeRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<TransactWriteItemsResponse> {
        validate_encode_transaction_request(&request)?;

        for attempt in 0..policy.max_attempts() {
            match apply_gsi_write_pressure(self).await {
                Ok(()) => break,
                Err(error) if error.is_retryable_write() && attempt + 1 < policy.max_attempts() => {
                    tokio::time::sleep(policy.delay()).await;
                }
                Err(error) => return Err(error),
            }
        }

        if request.client_request_token.is_none()
            && let Some(response) = self.try_apply_fast_encode_transaction(&request).await?
        {
            return Ok(response);
        }

        let mapped = TransactWriteItemsRequest::try_from(request)?;
        self.transact_write_items_after_pressure(mapped).await
    }

    async fn try_apply_fast_encode_transaction(
        &self,
        request: &TransactWriteItemsEncodeRequest,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        let mut billed_tally = WriteCostTally::default();
        for item in &request.transact_items {
            billed_tally.record_transact_encode_item(item);
        }

        let mut operations = Vec::with_capacity(request.transact_items.len() * 4);
        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;

        for item in &request.transact_items {
            let Some(put_request) = fast_encode_put_request(item) else {
                return Ok(None);
            };
            let Some(mut prepared) = self
                .prepare_fast_encode_transaction_put(put_request)
                .await?
            else {
                return Ok(None);
            };

            total_items_updated += 1;
            total_bytes_written += prepared.bytes_written;
            operations.append(&mut prepared.operations);
        }

        let direct_operations = operations
            .into_iter()
            .map(to_direct_write_operation)
            .collect::<StorageResult<Vec<_>>>()?;
        self.kv_store
            .transact_write_unchecked(direct_operations)
            .await?;
        let response = TransactWriteItemsResponse {
            consumed_capacity: None,
            item_collection_metrics: None,
        };
        record_write(total_items_updated, total_bytes_written);
        billed_tally.emit("transact_write_items");
        Ok(Some(response))
    }

    async fn prepare_fast_encode_transaction_put(
        &self,
        put_request: &storage_types::TransactEncodePutRequest,
    ) -> StorageResult<Option<PreparedFastEncodePut>> {
        let table_metadata = self
            .get_table_identity_from_name(&put_request.table_name)
            .await?
            .ok_or(StorageError::table_not_found(&put_request.table_name))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(&put_request.table_name).await?;

        if self.requires_immediate_gsi_updates(&table_info) {
            return Ok(None);
        }

        let should_write_stream = crate::backends::common::should_write_stream_entries(
            &table_info,
            self.requires_immediate_gsi_updates(&table_info),
        );
        let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
        let ttl_attribute = should_track_ttl
            .then(|| {
                ttl_config
                    .as_ref()
                    .map(|config| config.attribute_name.as_str())
            })
            .flatten();
        let (item_key, projected_ttl_value) =
            project_wire_item_table_key_and_ttl(&put_request.item, &table_info, ttl_attribute)?;
        let item_key_bytes = table_keys::item_key(&table_metadata.identity, &item_key)?;
        let value = encode_wire_item_storage_bytes(&put_request.item)?;
        let old_bytes = self
            .load_previous_fast_encode_item(should_write_stream, should_track_ttl, &item_key_bytes)
            .await?;
        let old_item = should_track_ttl
            .then(|| {
                old_bytes
                    .as_deref()
                    .map(decode_wire_item_from_storage_bytes)
                    .transpose()
            })
            .transpose()?
            .flatten();

        let mut operations = Vec::new();
        if should_write_stream {
            operations.extend(fast_encode_stream_operations(
                &table_metadata.identity,
                &put_request.table_name,
                &item_key,
                value.as_slice(),
                old_bytes.as_deref(),
            )?);
        }

        if should_track_ttl {
            let item_key_token = wire_item_key_token_from_item_key(&item_key)?;
            operations.extend(ttl_index_direct_operations_for_wire_items(
                &table_metadata.identity,
                &table_info,
                ttl_config.as_ref(),
                old_item.as_ref(),
                Some(&put_request.item),
                Some(item_key_token.as_str()),
                projected_ttl_value,
            )?);
        }

        let bytes_written = value.len();
        operations.push(TransactWriteOperation::Put {
            key: item_key_bytes,
            value,
            condition: None,
        });
        Ok(Some(PreparedFastEncodePut {
            bytes_written,
            operations,
        }))
    }

    async fn load_previous_fast_encode_item(
        &self,
        should_write_stream: bool,
        should_track_ttl: bool,
        item_key_bytes: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        if !should_write_stream && !should_track_ttl {
            return Ok(None);
        }

        self.kv_store.get(item_key_bytes, true).await
    }
}

struct PreparedFastEncodePut {
    bytes_written: usize,
    operations: Vec<TransactWriteOperation>,
}

fn validate_encode_transaction_request(
    request: &TransactWriteItemsEncodeRequest,
) -> StorageResult<()> {
    if request.transact_items.is_empty() {
        return Err(StorageError::validation(
            "Transaction request must contain at least one item",
        ));
    }
    if request.transact_items.len() > 100 {
        return Err(StorageError::validation(
            "Transaction request cannot contain more than 100 items",
        ));
    }

    Ok(())
}

fn fast_encode_put_request(
    item: &storage_types::TransactEncodeItem,
) -> Option<&storage_types::TransactEncodePutRequest> {
    let storage_types::TransactEncodeItem {
        put,
        update,
        delete,
        condition_check,
    } = item;

    if update.is_some() || delete.is_some() || condition_check.is_some() {
        return None;
    }

    let put_request = put.as_ref()?;
    if put_request.condition_expression.is_some()
        || put_request.expression_attribute_names.is_some()
        || put_request.expression_attribute_values.is_some()
        || put_request.aux_item_stream_ttl_hours.is_some()
    {
        return None;
    }

    Some(put_request)
}

fn fast_encode_stream_operations(
    table_identity: &TableIdentity,
    table_name: &TableName,
    item_key: &ItemKey,
    value: &[u8],
    old_bytes: Option<&[u8]>,
) -> StorageResult<Vec<TransactWriteOperation>> {
    let stream_item_id = next_stream_item_id();
    let stream_entries = crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
        crate::stream::helpers::StreamEntryContext {
            table_identity,
            table_name,
            item_key,
        },
        value,
        old_bytes,
        stream_item_id,
        false,
        None,
    )?;

    Ok(stream_entries
        .into_iter()
        .map(|(template, value)| TransactWriteOperation::PutTemplate {
            template,
            value,
            condition: None,
        })
        .collect())
}
