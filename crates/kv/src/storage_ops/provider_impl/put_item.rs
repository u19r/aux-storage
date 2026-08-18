use crate::storage_ops::provider_impl::*;

pub(super) struct PutItemInput<T> {
    pub(super) table_name: TableName,
    pub(super) item: T,
    pub(super) indexers: Option<Vec<String>>,
    pub(super) condition_expression: Option<String>,
    pub(super) expression_attribute_names: Option<HashMap<String, String>>,
    pub(super) expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub(super) return_values: Option<AllOld>,
    pub(super) return_old_on_condition_failure: bool,
    pub(super) aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

struct PutItemTransactionInput {
    table_name: TableName,
    item: HashMap<String, AttributeValue>,
    indexers: Option<Vec<String>>,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    return_old_on_condition_failure: bool,
    aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

struct WireNativePutInput<'a> {
    table_metadata: &'a StoredTableMetadata,
    table_info: &'a StoredTableInfo,
    ttl_config: Option<&'a TtlConfigRecord>,
    should_write_stream: bool,
    should_track_ttl: bool,
    table_name: &'a TableName,
    item: &'a WireItem,
    indexers: Option<&'a [String]>,
}

struct WireNativePutEffectsInput<'a> {
    table_metadata: &'a StoredTableMetadata,
    table_info: &'a StoredTableInfo,
    ttl_config: Option<&'a TtlConfigRecord>,
    should_write_stream: bool,
    should_track_ttl: bool,
    table_name: &'a TableName,
    item: &'a WireItem,
    indexers: Option<&'a [String]>,
    item_key: ItemKey,
    item_key_bytes: Vec<u8>,
    item_key_token: Option<String>,
    projected_ttl_value: Option<i64>,
    bytes: Vec<u8>,
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    async fn put_item_after_pressure(
        &self,
        request: PutItemInput<HashMap<String, AttributeValue>>,
    ) -> StorageResult<PutItemResponse> {
        let PutItemInput {
            table_name,
            mut item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = request;
        if item.is_empty() {
            return Err(StorageError::validation(
                "Item must have at least one attribute",
            ));
        }
        normalize_attribute_map_numbers_for_write(&mut item);
        let billed_bytes = attr_map_payload_bytes(&item);
        let bytes_written = compute_items_bytes(std::slice::from_ref(&item))?;
        let old_item = self
            .execute_put_item(PutItemTransactionInput {
                table_name,
                item,
                indexers,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                return_old_on_condition_failure,
                aux_item_stream_ttl_hours,
            })
            .await?;

        record_write(1, bytes_written);
        record_write_cost("put_item", "put", 1, billed_bytes);
        Ok(put_item_response(old_item, return_values))
    }

    pub(super) async fn put_item_with_stream_ttl_impl(
        &self,
        request: PutItemInput<HashMap<String, AttributeValue>>,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        self.put_item_with_stream_ttl_after_pressure(request).await
    }

    async fn put_item_with_stream_ttl_after_pressure(
        &self,
        request: PutItemInput<HashMap<String, AttributeValue>>,
    ) -> StorageResult<PutItemResponse> {
        let PutItemInput {
            table_name,
            mut item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = request;

        if aux_item_stream_ttl_hours.is_none() {
            return self
                .put_item_after_pressure(PutItemInput {
                    table_name,
                    item,
                    indexers,
                    condition_expression,
                    expression_attribute_names,
                    expression_attribute_values,
                    return_values,
                    return_old_on_condition_failure,
                    aux_item_stream_ttl_hours,
                })
                .await;
        }
        if item.is_empty() {
            return Err(StorageError::validation(
                "Item must have at least one attribute",
            ));
        }
        normalize_attribute_map_numbers_for_write(&mut item);
        let billed_bytes = attr_map_payload_bytes(&item);
        let bytes_written = compute_items_bytes(std::slice::from_ref(&item))?;
        let old_item = self
            .execute_put_item(PutItemTransactionInput {
                table_name,
                item,
                indexers,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                return_old_on_condition_failure,
                aux_item_stream_ttl_hours,
            })
            .await?;

        record_write(1, bytes_written);
        record_write_cost("put_item", "put", 1, billed_bytes);
        Ok(put_item_response(old_item, return_values))
    }

    pub(super) async fn put_item_encode_impl(
        &self,
        request: PutItemInput<WireItem>,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        self.put_item_encode_after_pressure(request).await
    }

    async fn put_item_encode_after_pressure(
        &self,
        request: PutItemInput<WireItem>,
    ) -> StorageResult<PutItemResponse> {
        let PutItemInput {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = request;
        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let should_write_stream = crate::backends::common::should_write_stream_entries(
            &table_info,
            self.requires_immediate_gsi_updates(&table_info),
        );
        let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
        let should_write_gsi_immediately = self.requires_immediate_gsi_updates(&table_info);
        let can_wire_native_path = condition_expression.is_none()
            && expression_attribute_names.is_none()
            && expression_attribute_values.is_none()
            && !matches!(return_values, Some(AllOld::AllOld))
            && !return_old_on_condition_failure
            && !should_write_gsi_immediately;

        if !can_wire_native_path {
            return self
                .put_item_after_pressure(PutItemInput {
                    table_name,
                    item: item.into_attribute_map()?,
                    indexers,
                    condition_expression,
                    expression_attribute_names,
                    expression_attribute_values,
                    return_values,
                    return_old_on_condition_failure,
                    aux_item_stream_ttl_hours,
                })
                .await;
        }

        let bytes_written = self
            .execute_wire_native_put_item(WireNativePutInput {
                table_metadata: &table_metadata,
                table_info: &table_info,
                ttl_config: ttl_config.as_ref(),
                should_write_stream,
                should_track_ttl,
                table_name: &table_name,
                item: &item,
                indexers: indexers.as_deref(),
            })
            .await?;
        record_write(1, bytes_written);
        record_write_cost("put_item", "put", 1, item.payload_len() as u64);
        Ok(PutItemResponse { attributes: None })
    }

    pub(super) async fn put_item_encode_with_stream_ttl_impl(
        &self,
        request: PutItemInput<WireItem>,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        self.put_item_encode_with_stream_ttl_after_pressure(request)
            .await
    }

    async fn put_item_encode_with_stream_ttl_after_pressure(
        &self,
        request: PutItemInput<WireItem>,
    ) -> StorageResult<PutItemResponse> {
        let PutItemInput {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = request;

        if aux_item_stream_ttl_hours.is_none() {
            return self
                .put_item_encode_after_pressure(PutItemInput {
                    table_name,
                    item,
                    indexers,
                    condition_expression,
                    expression_attribute_names,
                    expression_attribute_values,
                    return_values,
                    return_old_on_condition_failure,
                    aux_item_stream_ttl_hours,
                })
                .await;
        }
        self.put_item_with_stream_ttl_after_pressure(PutItemInput {
            table_name,
            item: item.into_attribute_map()?,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        })
        .await
    }

    pub(super) async fn put_item_request_with_retry_impl(
        &self,
        request: PutItemRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        self.retry_put_pressure(policy).await?;
        self.put_item_with_stream_ttl_after_pressure(PutItemInput {
            table_name: request.table_name,
            item: request.item,
            indexers: request.indexers,
            condition_expression: request.condition_expression,
            expression_attribute_names: request.expression_attribute_names,
            expression_attribute_values: request.expression_attribute_values,
            return_values: request.return_values,
            return_old_on_condition_failure: return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
            aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        })
        .await
    }

    pub(super) async fn put_item_encode_with_retry_impl(
        &self,
        request: PutItemEncodeRequest,
        policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        self.retry_put_pressure(policy).await?;
        self.put_item_encode_with_stream_ttl_after_pressure(PutItemInput {
            table_name: request.table_name,
            item: request.item,
            indexers: request.indexers,
            condition_expression: request.condition_expression,
            expression_attribute_names: request.expression_attribute_names,
            expression_attribute_values: request.expression_attribute_values,
            return_values: request.return_values,
            return_old_on_condition_failure: request.return_old_on_condition_failure,
            aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        })
        .await
    }

    async fn retry_put_pressure(&self, policy: WriteRetryPolicy) -> StorageResult<()> {
        for attempt in 0..policy.max_attempts() {
            match apply_gsi_write_pressure(self).await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retryable_write() && attempt + 1 < policy.max_attempts() => {
                    tokio::time::sleep(policy.delay()).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("write retry policy always has an attempt")
    }

    async fn execute_put_item(
        &self,
        input: PutItemTransactionInput,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let PutItemTransactionInput {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = input;
        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let condition = condition_binding(
            condition_expression,
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;

        let mut old_new_items = self
            .kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Put {
                    table_identity: table_metadata.identity.clone(),
                    table_info,
                    item,
                    indexers,
                    item_stream_ttl_hours: aux_item_stream_ttl_hours,
                    condition,
                    return_values_on_condition_check_failure: return_old_on_condition_failure
                        .then(|| "ALL_OLD".to_string()),
                    replication: None,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        Ok(old_new_items.pop().unwrap_or((None, None)).0)
    }

    async fn execute_wire_native_put_item(
        &self,
        input: WireNativePutInput<'_>,
    ) -> StorageResult<usize> {
        let WireNativePutInput {
            table_metadata,
            table_info,
            ttl_config,
            should_write_stream,
            should_track_ttl,
            table_name,
            item,
            indexers,
        } = input;
        let ttl_attribute = should_track_ttl
            .then(|| ttl_config.map(|config| config.attribute_name.as_str()))
            .flatten();
        let (item_key, projected_ttl_value) =
            project_wire_item_table_key_and_ttl(item, table_info, ttl_attribute)?;
        let item_key_bytes = table_keys::item_key(&table_metadata.identity, &item_key)?;
        let item_key_token = should_track_ttl
            .then(|| wire_item_key_token_from_item_key(&item_key))
            .transpose()?;
        let bytes = encode_wire_item_storage_bytes(
            self.kv_store.item_value_codec(),
            item,
            indexers,
            table_info.max_indexers,
        )?;
        let bytes_written = bytes.len();

        if should_write_stream || should_track_ttl {
            self.write_wire_native_put_with_side_effects(WireNativePutEffectsInput {
                table_metadata,
                table_info,
                ttl_config,
                should_write_stream,
                should_track_ttl,
                table_name,
                item,
                indexers,
                item_key,
                item_key_bytes,
                item_key_token,
                projected_ttl_value,
                bytes,
            })
            .await?;
        } else {
            self.kv_store.put(&item_key_bytes, &bytes, None).await?;
        }

        Ok(bytes_written)
    }

    async fn write_wire_native_put_with_side_effects(
        &self,
        input: WireNativePutEffectsInput<'_>,
    ) -> StorageResult<()> {
        let WireNativePutEffectsInput {
            table_metadata,
            table_info,
            ttl_config,
            should_write_stream,
            should_track_ttl,
            table_name,
            item,
            indexers,
            item_key,
            item_key_bytes,
            item_key_token,
            projected_ttl_value,
            bytes,
        } = input;
        let old_bytes = self.kv_store.get(&item_key_bytes, true).await?;
        let old_item = if should_write_stream || should_track_ttl {
            old_bytes
                .as_deref()
                .map(|bytes| {
                    decode_wire_item_with_indexers_from_storage_bytes(
                        self.kv_store.item_value_codec(),
                        bytes,
                        table_info.max_indexers,
                    )
                })
                .transpose()?
        } else {
            None
        };
        let item_map = item.to_attribute_map()?;
        let old_item_map = old_item
            .as_ref()
            .map(|(item, _)| item.to_attribute_map())
            .transpose()?;

        let mut operations = Vec::with_capacity(6);
        if should_write_stream {
            let stream_item_id = next_stream_item_id();
            let entries = crate::stream::helpers::create_item_update_stream_entries(
                crate::stream::helpers::StreamEntryContext {
                    table_identity: &table_metadata.identity,
                    table_name,
                    item_key: &item_key,
                    indexers: indexers.unwrap_or_default(),
                    old_indexers: old_item.as_ref().map(|(_, indexers)| indexers.as_slice()),
                },
                &item_map,
                old_item_map.as_ref(),
                stream_item_id,
                false,
                None,
            )?;
            operations.extend(entries.into_iter().map(|(template, value)| {
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
                old_item.as_ref().map(|(item, _)| item),
                Some(item),
                item_key_token.as_deref(),
                projected_ttl_value,
            )?);
        }
        operations.push(TransactWriteOperation::Put {
            key: item_key_bytes,
            value: bytes,
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

fn put_item_response(
    old_item: Option<HashMap<String, AttributeValue>>,
    return_values: Option<AllOld>,
) -> PutItemResponse {
    let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
        old_item
    } else {
        None
    };

    PutItemResponse {
        attributes: attributes.map(Into::into),
    }
}

fn condition_binding(
    condition_expression: Option<String>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Option<storage_condition::Condition>> {
    condition_expression
        .map(|condition_expression| {
            parse_condition_expression(
                &condition_expression,
                expression_attribute_names,
                expression_attribute_values,
            )
            .map_err(|error| {
                warn!(error);
                StorageError::validation(StorageValidationKind::InvalidConditionExpression)
            })
        })
        .transpose()
}
