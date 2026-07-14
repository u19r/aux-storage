use std::time::Instant;

use crate::{
    keyspace::table_keys,
    sorted_kv_store::{RawKey, SortedKvReadContext},
    storage_ops::imports::{
        AttributeValue, ItemKey, KeyType, QueryTableRequest, SortedKvDbStorageProvider, Span,
        StorageEnum, StorageError, StorageResult, WireItem, helpers, key_schema_for_gsi,
        record_provider_stage, record_query_result,
    },
};

impl<S: crate::sorted_kv_store::SortedKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn query_table_impl(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.query_table_impl_with_context(None, request).await
    }

    pub(crate) async fn query_table_with_kv_read_context(
        &self,
        read_context: &dyn SortedKvReadContext,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.query_table_impl_with_context(Some(read_context), request)
            .await
    }

    async fn query_table_impl_with_context(
        &self,
        read_context: Option<&dyn SortedKvReadContext>,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        request.validate_for_dynamodb()?;
        let consistent_read = request.consistent_read;
        if consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        if let Some(idx) = request.index_name.as_ref() {
            Span::current().record("index_name", idx.to_string());
        }
        let _span = storage_common::start_op_span("query", request.table_name.as_ref());
        storage_common::record_limit(request.limit.unwrap_or(0), request.limit);
        let table_name = request.table_name.clone();
        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let table_info = &table_metadata.table_info;

        let key_schema = if let Some(index_name) = &request.index_name {
            key_schema_for_gsi(table_info, index_name).ok_or_else(|| {
                StorageEnum::ResourceNotFound {
                    resource_type: "Index",
                    resource_id: index_name.as_ref().to_string(),
                }
            })?
        } else {
            table_info.key_schema.clone()
        };

        #[expect(unused_variables)]
        let hash_key_schema = key_schema
            .iter()
            .find(|k| k.key_type == KeyType::Hash)
            .ok_or_else(|| StorageError::internal("Table must have a hash key"))?;

        let scan_forward = request.scan_index_forward.unwrap_or(true);

        let page_token = request
            .exclusive_start_key
            .as_ref()
            .and_then(|page_token| {
                ItemKey::item_key_from_next_page_token(page_token, table_info, &request.index_name)
                    .ok()
            })
            .flatten();
        let page_token = page_token
            .as_ref()
            .map(|token| table_keys::item_key(&table_metadata.identity, token))
            .transpose()?
            .map(RawKey);

        let encode_key = |key: &ItemKey| table_keys::item_key(&table_metadata.identity, key);
        let increment_key =
            |key: &ItemKey| table_keys::item_key_increment(&table_metadata.identity, key);
        let decrement_key =
            |key: &ItemKey| table_keys::item_key_decrement(&table_metadata.identity, key);

        let build_hash_key_prefix = |hash_value: &AttributeValue| -> ItemKey {
            Self::build_hash_key_prefix(
                table_info.table_name.clone(),
                &request.index_name,
                hash_value,
            )
        };

        let build_full_key =
            |hash_value: &AttributeValue, range_value: &AttributeValue| -> ItemKey {
                Self::build_full_key(
                    table_info.table_name.clone(),
                    &request.index_name,
                    hash_value,
                    range_value,
                )
            };

        match QueryKeyShape::from_key_count(key_schema.len())? {
            QueryKeyShape::HashRange => {
                if let Some((hash_value, range_value)) = helpers::parse_hash_range_key_query(
                    &request.key_condition_expression,
                    &request.expression_attribute_values,
                ) {
                    let key = build_full_key(hash_value, range_value);
                    let end_key = increment_key(&key)?;

                    return self
                        .read_query_range(
                            read_context,
                            encode_key(&key)?,
                            end_key,
                            request,
                            table_info,
                            page_token,
                        )
                        .await;
                }

                if let Some((hash_value, start_value, end_value)) =
                    helpers::parse_hash_between_query(
                        &request.key_condition_expression,
                        &request.expression_attribute_values,
                    )
                {
                    let start_key = build_full_key(hash_value, start_value);
                    let end_value_key = build_full_key(hash_value, end_value);
                    let end_key = end_value_key;

                    if scan_forward {
                        return self
                            .read_query_range(
                                read_context,
                                encode_key(&start_key)?,
                                increment_key(&end_key)?,
                                request,
                                table_info,
                                page_token,
                            )
                            .await;
                    }

                    return self
                        .read_query_range(
                            read_context,
                            encode_key(&end_key)?,
                            decrement_key(&start_key)?,
                            request,
                            table_info,
                            page_token,
                        )
                        .await;
                }

                if let Some((
                    hash_value,
                    lower_operator,
                    lower_value,
                    upper_operator,
                    upper_value,
                )) = helpers::parse_hash_bounded_comparison_query(
                    &request.key_condition_expression,
                    &request.expression_attribute_values,
                ) {
                    let lower_key = build_full_key(hash_value, lower_value);
                    let upper_key = build_full_key(hash_value, upper_value);

                    let (start, end) = if scan_forward {
                        let start_key = match lower_operator {
                            ">" => increment_key(&lower_key)?,
                            ">=" => encode_key(&lower_key)?,
                            _ => {
                                return Err(StorageError::validation(format!(
                                    "unsupported lower bound operator: {lower_operator}"
                                )));
                            }
                        };
                        let end_key = match upper_operator {
                            "<" => encode_key(&upper_key)?,
                            "<=" => increment_key(&upper_key)?,
                            _ => {
                                return Err(StorageError::validation(format!(
                                    "unsupported upper bound operator: {upper_operator}"
                                )));
                            }
                        };
                        (start_key, end_key)
                    } else {
                        let start_key = match upper_operator {
                            "<" => decrement_key(&upper_key)?,
                            "<=" => encode_key(&upper_key)?,
                            _ => {
                                return Err(StorageError::validation(format!(
                                    "unsupported upper bound operator: {upper_operator}"
                                )));
                            }
                        };
                        let end_key = match lower_operator {
                            ">" => encode_key(&lower_key)?,
                            ">=" => decrement_key(&lower_key)?,
                            _ => {
                                return Err(StorageError::validation(format!(
                                    "unsupported lower bound operator: {lower_operator}"
                                )));
                            }
                        };
                        (start_key, end_key)
                    };

                    return self
                        .read_query_range(read_context, start, end, request, table_info, page_token)
                        .await;
                }

                if let Some((hash_value, operator, comparison_value)) =
                    helpers::parse_hash_comparison_query(
                        &request.key_condition_expression,
                        &request.expression_attribute_values,
                    )
                {
                    let hash_prefix = build_hash_key_prefix(hash_value);

                    match operator {
                        "<" => {
                            let end_key = build_full_key(hash_value, comparison_value);
                            let start_key = {
                                let mut start_bytes = encode_key(&hash_prefix)?;
                                start_bytes.push(0x00);
                                start_bytes
                            };

                            let (start, end) = if scan_forward {
                                (start_key, encode_key(&end_key)?)
                            } else {
                                (decrement_key(&end_key)?, start_key)
                            };

                            return self
                                .read_query_range(
                                    read_context,
                                    start,
                                    end,
                                    request,
                                    table_info,
                                    page_token,
                                )
                                .await;
                        }
                        "<=" => {
                            let end_key = build_full_key(hash_value, comparison_value);
                            let mut start_key = encode_key(&hash_prefix)?;

                            start_key.push(0x00);

                            let (start, end) = if scan_forward {
                                (start_key, increment_key(&end_key)?)
                            } else {
                                (encode_key(&end_key)?, start_key)
                            };

                            return self
                                .read_query_range(
                                    read_context,
                                    start,
                                    end,
                                    request,
                                    table_info,
                                    page_token,
                                )
                                .await;
                        }
                        ">" => {
                            let start_key = build_full_key(hash_value, comparison_value);
                            let end_key = increment_key(&hash_prefix)?;

                            let (start, end) = if scan_forward {
                                (increment_key(&start_key)?, end_key)
                            } else {
                                (end_key, encode_key(&start_key)?)
                            };

                            return self
                                .read_query_range(
                                    read_context,
                                    start,
                                    end,
                                    request,
                                    table_info,
                                    page_token,
                                )
                                .await;
                        }
                        ">=" => {
                            let start_key = build_full_key(hash_value, comparison_value);
                            let end_key = increment_key(&hash_prefix)?;

                            let (start, end) = if scan_forward {
                                (encode_key(&start_key)?, end_key)
                            } else {
                                (end_key, decrement_key(&start_key)?)
                            };

                            return self
                                .read_query_range(
                                    read_context,
                                    start,
                                    end,
                                    request,
                                    table_info,
                                    page_token,
                                )
                                .await;
                        }
                        _ => {
                            return Err(StorageError::validation(format!(
                                "unsupported query operator: {operator}"
                            )));
                        }
                    }
                }

                match helpers::parse_hash_begins_with_query(
                    &request.key_condition_expression,
                    &request.expression_attribute_values,
                ) {
                    Ok(Some((hash_value, prefix_value))) => {
                        let start_key = build_full_key(hash_value, prefix_value);
                        let end_key = increment_key(&start_key)?;

                        let (start, end) = if scan_forward {
                            (encode_key(&start_key)?, end_key)
                        } else {
                            (end_key, encode_key(&start_key)?)
                        };

                        return self
                            .read_query_range(
                                read_context,
                                start,
                                end,
                                request,
                                table_info,
                                page_token,
                            )
                            .await;
                    }
                    Err(e) => return Err(e),
                    Ok(None) => {}
                }

                if let Some(hash_value) = helpers::parse_hash_key_query(
                    &request.key_condition_expression,
                    &request.expression_attribute_values,
                ) {
                    let hash_prefix = build_hash_key_prefix(hash_value);

                    let (start, end) = if scan_forward {
                        (encode_key(&hash_prefix)?, increment_key(&hash_prefix)?)
                    } else {
                        let end_bytes = encode_key(&hash_prefix)?;
                        let start_bytes = increment_key(&hash_prefix)?;
                        (start_bytes, end_bytes)
                    };

                    return self
                        .read_query_range(read_context, start, end, request, table_info, page_token)
                        .await;
                }
            }
            QueryKeyShape::HashOnly => {
                if let Some(hash_value) = helpers::parse_hash_key_query(
                    &request.key_condition_expression,
                    &request.expression_attribute_values,
                ) {
                    let hash_prefix = Self::build_hash_key_prefix(
                        table_info.table_name.clone(),
                        &request.index_name,
                        hash_value,
                    );

                    let (start, end) = if scan_forward {
                        (encode_key(&hash_prefix)?, increment_key(&hash_prefix)?)
                    } else {
                        (encode_key(&hash_prefix)?, decrement_key(&hash_prefix)?)
                    };

                    return self
                        .read_query_range(read_context, start, end, request, table_info, page_token)
                        .await;
                }
            }
        }

        Err(StorageError::validation(format!(
            "unsupported key condition expression for query: {}",
            request.key_condition_expression
        )))
    }

    async fn query_range_values(
        &self,
        read_context: Option<&dyn SortedKvReadContext>,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<RawKey>,
        consistent_read: bool,
    ) -> StorageResult<crate::sorted_kv_store::RangeValuesResult> {
        let started = Instant::now();
        let result = match read_context {
            Some(read_context) => {
                read_context
                    .get_range_values(start, exclusive_end, limit, page_token, consistent_read)
                    .await
            }
            None => {
                self.kv_store
                    .get_range_values(start, exclusive_end, limit, page_token, consistent_read)
                    .await
            }
        };
        record_provider_stage("query", "fdb_wait", started.elapsed());
        result
    }

    async fn read_query_range(
        &self,
        read_context: Option<&dyn SortedKvReadContext>,
        start: Vec<u8>,
        exclusive_end: Vec<u8>,
        request: &QueryTableRequest,
        table_info: &storage_types::StoredTableInfo,
        page_token: Option<RawKey>,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let range_result = self
            .query_range_values(
                read_context,
                &start,
                &exclusive_end,
                request.limit,
                page_token,
                request.consistent_read,
            )
            .await?;
        let query_result =
            Self::materialize_query_result(range_result, table_info, &request.index_name)?;
        let (items, last_evaluated_key) = record_query_result(query_result);
        Ok((request.project_wire_items(items)?, last_evaluated_key))
    }

    fn materialize_query_result(
        range_result: crate::sorted_kv_store::RangeValuesResult,
        table_info: &storage_types::StoredTableInfo,
        index_name: &Option<storage_types::IndexName>,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let decode_started = Instant::now();
        let mut items = Vec::with_capacity(range_result.values.len());
        for data in range_result.values {
            let json = storage_types::storage_serde::decompress_owned_bytes(data)?;
            items.push(WireItem::dynamo_json(json));
        }
        record_provider_stage("query", "decode", decode_started.elapsed());

        let materialize_started = Instant::now();
        let last_evaluated_key = if range_result.has_more {
            if let Some(last) = items.last() {
                last.last_evaluated_key(table_info, index_name)?
            } else {
                None
            }
        } else {
            None
        };
        record_provider_stage(
            "query",
            "response_materialization",
            materialize_started.elapsed(),
        );

        Ok((items, last_evaluated_key))
    }
}

enum QueryKeyShape {
    HashOnly,
    HashRange,
}

impl QueryKeyShape {
    fn from_key_count(key_count: usize) -> StorageResult<Self> {
        match key_count {
            1 => Ok(Self::HashOnly),
            2 => Ok(Self::HashRange),
            _ => Err(StorageError::internal(&format!(
                "unsupported query key schema length: {key_count}"
            ))),
        }
    }
}
