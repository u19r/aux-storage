use super::{
    batch_write_support::{
        FastEncodeBatchOutcome, PreparedBatchWriteItem, TableBatchWriteResult,
        batch_write_response, is_terminal_batch_item_error, requested_encode_write_tally,
        requested_write_tally, unprocessed_write_tally,
    },
    *,
};

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn batch_write_item_impl(
        &self,
        request: BatchWriteItemRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        if should_write_to_stream {
            let mapped = BatchWriteItemEncodeRequest::try_from(request)?;
            return self.batch_write_item_encode_stream_impl(mapped).await;
        }
        self.batch_write_item_plain_impl(request).await
    }

    pub(super) async fn batch_write_item_encode_impl(
        &self,
        request: BatchWriteItemEncodeRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        if should_write_to_stream {
            return self.batch_write_item_encode_stream_impl(request).await;
        }
        let mapped = BatchWriteItemRequest::try_from(request)?;
        self.batch_write_item_plain_impl(mapped).await
    }

    async fn batch_write_item_plain_impl(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let requested_tally = requested_write_tally(&request);

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;
        let mut unprocessed_items: HashMap<TableName, Vec<WriteRequest>> = HashMap::new();

        for (table_name, write_requests) in &request.request_items {
            let result = self
                .batch_write_table_items(table_name, write_requests, false)
                .await;
            match result {
                Ok(table_result) => {
                    total_items_updated += table_result.items_updated;
                    total_bytes_written += table_result.bytes_written;
                    Self::collect_unprocessed_batch_items(
                        table_result.unprocessed_items,
                        table_name,
                        &mut unprocessed_items,
                    );
                }
                Err(error)
                    if matches!(error.to_enum(), StorageEnum::Validation { .. })
                        || matches!(error.to_enum(), StorageEnum::KeyValidation(_)) =>
                {
                    return Err(error);
                }
                Err(error) if matches!(error.to_enum(), StorageEnum::TableNotFound { .. }) => {
                    return Err(error);
                }
                Err(_error) => {
                    self.handle_batch_write_error(
                        table_name,
                        write_requests,
                        &mut unprocessed_items,
                    )?;
                }
            }
        }

        let response = batch_write_response(unprocessed_items);
        record_write(total_items_updated, total_bytes_written);
        requested_tally
            .subtract(&unprocessed_write_tally(&response))
            .emit("batch_write_item");

        Ok(response)
    }

    async fn batch_write_item_encode_stream_impl(
        &self,
        request: BatchWriteItemEncodeRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let requested_tally = requested_encode_write_tally(&request);

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;
        let mut unprocessed_items: HashMap<TableName, Vec<WriteRequest>> = HashMap::new();

        for (table_name, write_requests) in &request.request_items {
            let table_metadata = self
                .get_table_identity_from_name(table_name)
                .await?
                .ok_or_else(|| StorageError::table_not_found(table_name))?;
            let table_info = table_metadata.table_info.clone();
            let ttl_config = self.load_ttl_config(table_name).await?;

            if self
                .try_apply_fast_encode_batch(
                    table_name,
                    &table_metadata,
                    &table_info,
                    ttl_config.as_ref(),
                    write_requests,
                    &mut unprocessed_items,
                )
                .await?
                .record(&mut total_items_updated, &mut total_bytes_written)
            {
                continue;
            }

            let table_result = self
                .batch_write_encoded_table_items(table_name, write_requests)
                .await?;
            total_items_updated += table_result.items_updated;
            total_bytes_written += table_result.bytes_written;
            Self::collect_unprocessed_batch_items(
                table_result.unprocessed_items,
                table_name,
                &mut unprocessed_items,
            );
        }

        let response = batch_write_response(unprocessed_items);
        record_write(total_items_updated, total_bytes_written);
        requested_tally
            .subtract(&unprocessed_write_tally(&response))
            .emit("batch_write_item");
        Ok(response)
    }

    async fn batch_write_table_items(
        &self,
        table_name: &TableName,
        write_requests: &[WriteRequest],
        should_write_to_stream: bool,
    ) -> StorageResult<TableBatchWriteResult> {
        let table_identity_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let table_info = table_identity_metadata.table_info.clone();
        let ttl_config = self
            .load_ttl_config(table_name)
            .await?
            .filter(|config| ttl_tracking_enabled(Some(config)));
        let requires_immediate_gsi_updates = self.requires_immediate_gsi_updates(&table_info);
        let context = BatchWriteTableContext {
            table_name,
            table_metadata: &table_identity_metadata,
            table_info: &table_info,
            ttl_config: ttl_config.as_ref(),
            should_write_to_stream,
            requires_immediate_gsi_updates,
        };
        let existing_items = self
            .load_existing_batch_items_when_needed(&context, write_requests)
            .await?;

        let mut batch_items = Vec::new();
        let mut unprocessed_items = Vec::new();
        let mut items_updated = 0usize;
        let mut bytes_written = 0usize;

        for (index, write_request) in write_requests.iter().enumerate() {
            match self.prepare_batch_write_item(
                &context,
                existing_items[index].as_ref(),
                write_request,
            ) {
                Ok(Some(mut prepared)) => {
                    items_updated += 1;
                    bytes_written += prepared.bytes_written;
                    batch_items.append(&mut prepared.items);
                }
                Ok(None) => unprocessed_items.push(write_request.clone()),
                Err(error) if is_terminal_batch_item_error(&error) => return Err(error),
                Err(_error) => unprocessed_items.push(write_request.clone()),
            }
        }

        if batch_items.is_empty() {
            return Ok(TableBatchWriteResult {
                items_updated,
                bytes_written,
                unprocessed_items,
            });
        }

        if self.kv_store.batch_write(batch_items).await.is_err() {
            unprocessed_items.extend(write_requests.iter().cloned());
            return Ok(TableBatchWriteResult {
                items_updated: 0,
                bytes_written: 0,
                unprocessed_items,
            });
        }

        Ok(TableBatchWriteResult {
            items_updated,
            bytes_written,
            unprocessed_items,
        })
    }

    async fn try_apply_fast_encode_batch(
        &self,
        table_name: &TableName,
        table_metadata: &StoredTableMetadata,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        write_requests: &[EncodeWriteRequest],
        unprocessed_items: &mut HashMap<TableName, Vec<WriteRequest>>,
    ) -> StorageResult<FastEncodeBatchOutcome> {
        if !self.can_fast_encode_batch(table_info, ttl_config, write_requests) {
            return Ok(FastEncodeBatchOutcome::NotAttempted);
        }

        match self
            .apply_batch_encode_put_items_immediate_gsi(
                &table_metadata.identity,
                table_info,
                write_requests,
            )
            .await
        {
            Ok((items_updated, bytes_written)) => Ok(FastEncodeBatchOutcome::Applied {
                items_updated,
                bytes_written,
            }),
            Err(error) if is_terminal_batch_item_error(&error) => Err(error),
            Err(_error) => {
                unprocessed_items.insert(
                    table_name.clone(),
                    encode_requests_to_write_requests(write_requests)?,
                );
                Ok(FastEncodeBatchOutcome::Applied {
                    items_updated: 0,
                    bytes_written: 0,
                })
            }
        }
    }

    async fn batch_write_encoded_table_items(
        &self,
        table_name: &TableName,
        write_requests: &[EncodeWriteRequest],
    ) -> StorageResult<TableBatchWriteResult> {
        let mut result = TableBatchWriteResult::default();

        for write_request in write_requests {
            match write_request {
                EncodeWriteRequest {
                    put_request: Some(put_request),
                    delete_request: None,
                } => match self
                    .apply_batch_encode_put_item(
                        table_name,
                        &put_request.item,
                        put_request.aux_item_stream_ttl_hours,
                    )
                    .await
                {
                    Ok(item_bytes) => {
                        result.items_updated += 1;
                        result.bytes_written += item_bytes;
                    }
                    Err(error) if is_terminal_batch_item_error(&error) => return Err(error),
                    Err(_error) => result.unprocessed_items.push(WriteRequest {
                        put_request: Some(PutRequest {
                            item: put_request.item.clone().into_attribute_map()?,
                            aux_item_stream_ttl_hours: put_request.aux_item_stream_ttl_hours,
                        }),
                        delete_request: None,
                    }),
                },
                EncodeWriteRequest {
                    put_request: None,
                    delete_request:
                        Some(DeleteRequest {
                            key,
                            aux_item_stream_ttl_hours,
                        }),
                } => match self
                    .apply_batch_delete_item(table_name, key, *aux_item_stream_ttl_hours)
                    .await
                {
                    Ok(()) => {
                        result.items_updated += 1;
                    }
                    Err(error) if is_terminal_batch_item_error(&error) => return Err(error),
                    Err(_error) => result.unprocessed_items.push(WriteRequest {
                        put_request: None,
                        delete_request: Some(DeleteRequest {
                            key: key.clone(),
                            aux_item_stream_ttl_hours: *aux_item_stream_ttl_hours,
                        }),
                    }),
                },
                _ => {
                    return Err(StorageError::validation(
                        "Each WriteRequest must contain exactly one of PutRequest or DeleteRequest",
                    ));
                }
            }
        }

        Ok(result)
    }

    async fn load_existing_batch_items_when_needed(
        &self,
        context: &BatchWriteTableContext<'_>,
        write_requests: &[WriteRequest],
    ) -> StorageResult<Vec<Option<HashMap<String, AttributeValue>>>> {
        if !context.must_load_existing_items() {
            return Ok(vec![None; write_requests.len()]);
        }

        self.batch_existing_items_for_write_requests(
            context.table_name,
            &context.table_metadata.identity,
            context.table_info,
            write_requests,
        )
        .await
    }

    fn prepare_batch_write_item(
        &self,
        context: &BatchWriteTableContext<'_>,
        existing_item: Option<&HashMap<String, AttributeValue>>,
        write_request: &WriteRequest,
    ) -> StorageResult<Option<PreparedBatchWriteItem>> {
        match write_request {
            WriteRequest {
                put_request:
                    Some(PutRequest {
                        item,
                        aux_item_stream_ttl_hours,
                    }),
                delete_request: None,
            } => self.prepare_batch_put_write_item(
                context,
                existing_item,
                item,
                *aux_item_stream_ttl_hours,
            ),
            WriteRequest {
                put_request: None,
                delete_request:
                    Some(DeleteRequest {
                        key,
                        aux_item_stream_ttl_hours,
                    }),
            } => self.prepare_batch_delete_write_item(
                context,
                existing_item,
                key,
                *aux_item_stream_ttl_hours,
            ),
            _ => Ok(None),
        }
    }

    fn prepare_batch_put_write_item(
        &self,
        context: &BatchWriteTableContext<'_>,
        existing_item: Option<&HashMap<String, AttributeValue>>,
        item: &HashMap<String, AttributeValue>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<Option<PreparedBatchWriteItem>> {
        let item = normalized_attribute_map_for_write(item);
        let mut items = Self::prepare_batch_put_item(
            context.table_name,
            &context.table_metadata.identity,
            context.table_info,
            item.as_ref(),
            context.should_write_to_stream,
            existing_item,
            context.requires_immediate_gsi_updates,
        )?;
        items.extend(Self::ttl_index_mutations_for_items(
            context.table_name,
            &context.table_metadata.identity,
            context.table_info,
            context.ttl_config,
            existing_item,
            Some(item.as_ref()),
        )?);
        items.extend(
            crate::storage_ops::stream_duration::item_stream_duration_write_items(
                &context.table_metadata.identity,
                context.table_info,
                &key_attributes_for_item(context.table_info, item.as_ref())?,
                TimestampMillis::now().timestamp_millis().unsigned_abs(),
                aux_item_stream_ttl_hours,
            )?,
        );

        Ok(Some(PreparedBatchWriteItem {
            bytes_written: compute_items_bytes(std::slice::from_ref(item.as_ref()))?,
            items,
        }))
    }

    fn prepare_batch_delete_write_item(
        &self,
        context: &BatchWriteTableContext<'_>,
        existing_item: Option<&HashMap<String, AttributeValue>>,
        key: &KeyAttributes,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<Option<PreparedBatchWriteItem>> {
        let mut items = Self::prepare_batch_delete_item(
            context.table_name,
            &context.table_metadata.identity,
            context.table_info,
            key,
            context.should_write_to_stream,
            existing_item,
            context.requires_immediate_gsi_updates,
        )?;
        items.extend(Self::ttl_index_mutations_for_items(
            context.table_name,
            &context.table_metadata.identity,
            context.table_info,
            context.ttl_config,
            existing_item,
            None,
        )?);
        items.extend(
            crate::storage_ops::stream_duration::item_stream_duration_write_items(
                &context.table_metadata.identity,
                context.table_info,
                key,
                TimestampMillis::now().timestamp_millis().unsigned_abs(),
                aux_item_stream_ttl_hours,
            )?,
        );

        Ok(Some(PreparedBatchWriteItem {
            bytes_written: 0,
            items,
        }))
    }
}

struct BatchWriteTableContext<'a> {
    table_name: &'a TableName,
    table_metadata: &'a StoredTableMetadata,
    table_info: &'a StoredTableInfo,
    ttl_config: Option<&'a TtlConfigRecord>,
    should_write_to_stream: bool,
    requires_immediate_gsi_updates: bool,
}

impl BatchWriteTableContext<'_> {
    fn must_load_existing_items(&self) -> bool {
        self.ttl_config.is_some()
            || self.should_write_to_stream
            || self.requires_immediate_gsi_updates
    }
}
