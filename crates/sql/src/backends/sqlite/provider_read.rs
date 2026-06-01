use std::collections::HashMap;

use storage_common::normalize_limit as calc_limit;
use storage_provider::{StorageProvider, split_item_into_key_and_attributes_sync};
use storage_types::{
    BatchGetItemRequest, BatchGetWireItemResponse, ItemVersionedWireItem, KeysAndAttributes,
    QueryTableRequest, ScanTableRequest, StorageEnum, StorageError, StorageResult, TableName,
    WireItem,
};
use tracing::{Span, debug, field};

use super::{
    SQLiteStorageProvider,
    storage_provider::{record_read, storage_error_to_rusqlite},
};
use crate::{
    billing_metrics::{record_read_cost, wire_items_payload_bytes},
    error_handler::map_sqlite_error,
    helpers::{
        DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT,
        decode_exclusive_start,
    },
    parse_conditions::parse_key_condition_expression,
    provider_core::read::plan_read_target,
    read_path::execute_unified_read,
    sql_builder::build_sql_query,
    utils::call_sqlite,
};

impl SQLiteStorageProvider {
    pub(crate) async fn do_scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        if request.consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        let span = Span::current();
        if let Some(index_name) = &request.index_name {
            span.record("index_name", index_name.as_ref());
        }
        span.record(
            "req_limit",
            u64::from(request.limit.unwrap_or(DEFAULT_SCAN_LIMIT)),
        );
        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;
        let read_target = plan_read_target(&request.table_name, &table_info, &request.index_name)?;
        let effective_limit = calc_limit(request.limit, DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)?;
        let table_key_schema = read_target.table_key_schema_for_index.as_deref();
        let (sql, values) = build_sql_query(
            &read_target.physical_name,
            &read_target.key_schema,
            None,
            exclusive_start_key,
            effective_limit,
            None,
            table_key_schema,
        )?;
        let start = std::time::Instant::now();
        let index_name_opt = request.index_name.clone();
        let key_schema_for_origin_cloned = read_target.key_schema;
        let origin = read_target.origin;
        let table_info_for_read = table_info; // move ownership
        let (items, last_evaluated_key) = call_sqlite(&self.connection, move |conn| {
            let res = execute_unified_read(
                conn,
                &sql,
                &values,
                &table_info_for_read,
                origin,
                &key_schema_for_origin_cloned,
                effective_limit,
                &index_name_opt,
            )?;
            Ok::<_, StorageError>((res.items, res.last_evaluated_key))
        })
        .await?;
        let elapsed_ms_u128 = start.elapsed().as_millis();
        let elapsed_ms = u64::try_from(elapsed_ms_u128).unwrap_or(u64::MAX);
        debug!(
            rows = items.len(),
            has_more = last_evaluated_key.is_some(),
            elapsed_ms,
            "scan_table.complete"
        );
        let bytes_read = wire_items_payload_bytes(&items) as usize;
        record_read(items.len(), bytes_read);
        record_read_cost("scan_table", "scan", 1, bytes_read as u64);
        Ok((items, last_evaluated_key))
    }

    pub(crate) async fn do_scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        if request.index_name.is_some() {
            return Err(StorageError::validation(
                "versioned internal scans are supported only on base tables",
            ));
        }

        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let (items, last_evaluated_key) = self.do_scan_table(request).await?;
        let mut keyed_items = Vec::with_capacity(items.len());
        for item in items {
            let item_map = item.to_attribute_map()?;
            let split = split_item_into_key_and_attributes_sync(item_map, &table_info)?;
            keyed_items.push((item, split.key_attributes));
        }

        let table_name = request.table_name.clone();
        let keys = keyed_items
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let versions = call_sqlite(&self.connection, move |conn| {
            let sqlite = crate::utils::SqliteConn::Connection(conn);
            let mut versions = Vec::with_capacity(keys.len());
            for key in &keys {
                versions.push(storage_types::ItemStreamVersion::try_from(
                    SQLiteStorageProvider::do_get_item_revision(&table_name, key, &sqlite)?,
                )?);
            }
            Ok(versions)
        })
        .await?;

        let versioned_items = keyed_items
            .into_iter()
            .zip(versions)
            .map(
                |((item, _key), item_stream_version)| ItemVersionedWireItem {
                    item,
                    item_stream_version,
                },
            )
            .collect();

        Ok((versioned_items, last_evaluated_key))
    }

    pub(crate) async fn do_query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        if request.consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        let span = Span::current();
        span.record(
            "req_limit",
            u64::from(request.limit.unwrap_or(DEFAULT_QUERY_LIMIT)),
        );
        span.record(
            "scan_forward",
            field::display(request.scan_index_forward.unwrap_or(true)),
        );
        if let Some(index_name) = &request.index_name {
            span.record("index_name", index_name.as_ref());
        }
        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;
        let read_target = plan_read_target(&request.table_name, &table_info, &request.index_name)?;
        let where_clause = parse_key_condition_expression(
            &request.key_condition_expression,
            &read_target.key_schema,
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )?;
        let effective_limit = calc_limit(request.limit, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)?;
        let table_key_schema = read_target.table_key_schema_for_index.as_deref();
        let (sql, values) = build_sql_query(
            &read_target.physical_name,
            &read_target.key_schema,
            Some(where_clause),
            exclusive_start_key,
            effective_limit,
            request.scan_index_forward,
            table_key_schema,
        )?;
        let start = std::time::Instant::now();
        let index_name_opt = request.index_name.clone();
        let key_schema_cloned = read_target.key_schema;
        let origin = read_target.origin;
        let table_info_for_read = table_info; // move
        let (items, last_evaluated_key) = call_sqlite(&self.connection, move |conn| {
            let res = execute_unified_read(
                conn,
                &sql,
                &values,
                &table_info_for_read,
                origin,
                &key_schema_cloned,
                effective_limit,
                &index_name_opt,
            )?;
            Ok::<_, StorageError>((res.items, res.last_evaluated_key))
        })
        .await?;
        let elapsed_ms_u128 = start.elapsed().as_millis();
        let elapsed_ms = u64::try_from(elapsed_ms_u128).unwrap_or(u64::MAX);
        debug!(
            rows = items.len(),
            has_more = last_evaluated_key.is_some(),
            elapsed_ms,
            "query_table.complete"
        );
        let bytes_read = wire_items_payload_bytes(&items) as usize;
        record_read(items.len(), bytes_read);
        record_read_cost("query_table", "query", 1, bytes_read as u64);
        Ok((items, last_evaluated_key))
    }

    pub(crate) async fn do_batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let total_keys: usize = request.request_items.values().map(|v| v.keys.len()).sum();
        let span = Span::current();
        span.record("table_count", request.request_items.len() as u64);
        span.record("total_keys", total_keys as u64);
        let mut total_items_returned = 0usize;
        let mut total_bytes_read = 0usize;
        let mut responses: HashMap<TableName, Vec<WireItem>> =
            HashMap::with_capacity(request.request_items.len());
        let mut unprocessed_keys: HashMap<TableName, KeysAndAttributes> = HashMap::new();

        for (table_name, keys_and_attributes) in &request.request_items {
            if keys_and_attributes.keys.is_empty() {
                continue;
            }

            let table_info = match self.get_table_info(table_name).await {
                Ok(info) => info,
                Err(e) if matches!(e.as_ref(), StorageEnum::TableNotFound { .. }) => {
                    return Err(e);
                }
                Err(_err) => {
                    unprocessed_keys.insert(table_name.clone(), keys_and_attributes.clone());
                    continue;
                }
            };

            let key_schema = &table_info.key_schema;
            if key_schema.is_empty() {
                unprocessed_keys.insert(table_name.clone(), keys_and_attributes.clone());
                continue;
            }

            let physical_name = crate::naming::physical_table_name(table_name);
            let Some(plan) =
                plan_batch_get_select(&physical_name, key_schema, &keys_and_attributes.keys)?
            else {
                continue;
            };
            let table_info_clone = table_info.clone();
            let expected_items = keys_and_attributes.keys.len();

            let query_result: StorageResult<Vec<WireItem>> =
                call_sqlite(&self.connection, move |conn| {
                    let mut stmt = conn.prepare(&plan.sql).map_err(map_sqlite_error)?;

                    let rows = stmt
                        .query_map(rusqlite::params_from_iter(plan.params.iter()), |row| {
                            let primary_key =
                                crate::key_attribute_handler::wire_item_key_attributes_from_row(
                                    row,
                                    &table_info_clone.key_schema,
                                    &table_info_clone.attribute_definitions,
                                    None,
                                )
                                .map_err(|err| storage_error_to_rusqlite(&err))?;
                            let non_key_attributes_blob = row
                                .get::<_, Option<String>>("attributes_blob")?
                                .map(String::into_bytes);
                            Ok(WireItem::local_split(
                                primary_key,
                                None,
                                non_key_attributes_blob,
                            ))
                        })
                        .map_err(map_sqlite_error)?;

                    let mut items = Vec::with_capacity(expected_items);
                    for row in rows {
                        items.push(row.map_err(map_sqlite_error)?);
                    }

                    Ok(items)
                })
                .await;

            match query_result {
                Ok(items) => {
                    if items.is_empty() {
                        continue;
                    }

                    total_items_returned += items.len();
                    total_bytes_read += wire_items_payload_bytes(&items) as usize;

                    responses.insert(table_name.clone(), items);
                }
                Err(e) => {
                    if matches!(e.as_ref(), StorageEnum::TableNotFound { .. }) {
                        return Err(e);
                    }
                    unprocessed_keys.insert(table_name.clone(), keys_and_attributes.clone());
                }
            }
        }

        let response = BatchGetWireItemResponse {
            responses: if responses.is_empty() {
                None
            } else {
                Some(responses)
            },
            unprocessed_keys: if unprocessed_keys.is_empty() {
                None
            } else {
                Some(unprocessed_keys)
            },
            consumed_capacity: None,
        };

        record_read(total_items_returned, total_bytes_read);
        record_read_cost("batch_get_item", "get", total_keys, total_bytes_read as u64);

        Ok(response)
    }
}

#[derive(Debug)]
pub(crate) struct BatchGetSelectPlan {
    pub(crate) sql: String,
    pub(crate) params: Vec<String>,
}

pub(crate) fn plan_batch_get_select(
    table_name: &str,
    key_schema: &[storage_types::KeySchemaElement],
    keys: &[storage_types::KeyAttributes],
) -> StorageResult<Option<BatchGetSelectPlan>> {
    if key_schema.is_empty() || keys.is_empty() {
        return Ok(None);
    }

    let sql = build_batch_get_select_sql(table_name, key_schema, keys.len());
    let mut params = Vec::with_capacity(key_schema.len() * keys.len());
    for key_map in keys {
        for key_element in key_schema {
            let Some(value) = key_map.get(&key_element.attribute_name) else {
                return Err(StorageError::invalid_or_missing_key());
            };
            params.push(value.inner_string().map_err(|err| {
                StorageError::validation(format!("key attribute must be scalar: {err}"))
            })?);
        }
    }

    Ok(Some(BatchGetSelectPlan { sql, params }))
}

fn build_batch_get_select_sql(
    table_name: &str,
    key_schema: &[storage_types::KeySchemaElement],
    key_count: usize,
) -> String {
    let placeholder_chars = key_count.saturating_mul(3).saturating_sub(2);
    let mut sql = String::with_capacity(
        "SELECT * FROM \"\" WHERE  IN ()".len()
            + table_name.len()
            + key_schema.len() * 12
            + placeholder_chars * key_schema.len(),
    );
    sql.push_str("SELECT * FROM \"");
    sql.push_str(table_name);
    sql.push_str("\" WHERE ");

    if key_schema.len() == 1 {
        push_sanitized_attribute_name(&mut sql, &key_schema[0].attribute_name);
        sql.push_str(" IN (");
        push_placeholders(&mut sql, key_count);
        sql.push(')');
        return sql;
    }

    sql.push('(');
    for (index, key) in key_schema.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        push_sanitized_attribute_name(&mut sql, &key.attribute_name);
    }
    sql.push_str(") IN (");
    for key_index in 0..key_count {
        if key_index > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        push_placeholders(&mut sql, key_schema.len());
        sql.push(')');
    }
    sql.push(')');
    sql
}

fn push_placeholders(sql: &mut String, count: usize) {
    for index in 0..count {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push('?');
    }
}

fn push_sanitized_attribute_name(sql: &mut String, raw: &str) {
    if !raw.contains(['-', ' ']) {
        sql.push_str(raw);
        return;
    }

    for ch in raw.chars() {
        match ch {
            '-' | ' ' => sql.push('_'),
            ch => sql.push(ch),
        }
    }
}
