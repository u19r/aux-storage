use std::collections::HashMap;

use futures::future::try_join_all;
use storage_types::{
    AttributeMap, ItemResponse, KeyAttributes, ReadSequenceConsistency, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableName, TableNamespace, TransactGetItemsRequest,
    TransactGetItemsResponse, TransactGetRequest, WireItem, context::WrappedError,
    preflight_transact_get_item_key_with_table_info, transaction_canceled_for_preflights,
    validate_no_duplicate_transact_item_keys,
};

use crate::{
    database_manager::{
        DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, read_ops::storage_api_project_item,
        record_storage_operation,
    },
    namespace_routing::{NamespaceRequestRewriter, NamespaceStorageMode},
};

impl DatabaseManager {
    pub async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        let return_consumed_capacity = request.return_consumed_capacity;
        let mut transact_items = request.transact_items;
        let mut preflights = Vec::with_capacity(transact_items.len());
        let mut tables = Vec::<PreparedTransactGetTable>::new();
        let mut table_indexes = Vec::with_capacity(transact_items.len());
        let mut consumed_capacity_counts =
            should_track_transact_get_consumed_capacity(return_consumed_capacity.as_deref())
                .then(Vec::new);

        for item in &mut transact_items {
            let requested_table_name = item.get.table_name.clone();
            item.get.table_name = TableName::new(item.get.table_name.dynamodb_resource_name());
            let table_index = if let Some(index) = tables
                .iter()
                .position(|table| table.logical_table == item.get.table_name)
            {
                index
            } else {
                tables.push(
                    self.prepare_transact_get_table(&item.get.table_name)
                        .await
                        .map_err(transact_get_table_not_found_as_resource_not_found)?,
                );
                tables.len() - 1
            };
            let table_info = &tables[table_index].logical_table_info;
            preflights.push(preflight_transact_get_item_key_with_table_info(
                item, table_info,
            )?);
            table_indexes.push(table_index);
            if let Some(counts) = consumed_capacity_counts.as_mut() {
                increment_transact_get_consumed_capacity_count(
                    counts,
                    &tables[table_index].logical_table,
                    &requested_table_name,
                );
            }
        }
        if let Some(error) = transaction_canceled_for_preflights(&preflights) {
            return Err(error);
        }
        validate_no_duplicate_transact_item_keys(&preflights)?;

        let connection_id = transact_get_connection_id(&tables)?;
        let provider = self.provider_for_request_connection(connection_id)?;
        let read_context = provider
            .begin_read_sequence_read_context(ReadSequenceConsistency::Transactional)
            .await
            .map_err(transact_get_snapshot_capability_error)?;
        let prepared_reads = transact_items
            .into_iter()
            .zip(table_indexes)
            .map(|(item, table_index)| {
                prepare_transact_get_read(item.get, &tables[table_index], &self.request_rewriter)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let items = try_join_all(prepared_reads.iter().map(|read| {
            record_storage_operation(
                "get_item",
                read_context.get_item(read.table_name.clone(), read.key.clone(), true),
            )
        }))
        .await?;

        let mut responses = Vec::with_capacity(prepared_reads.len());
        for (read, mut item) in prepared_reads.into_iter().zip(items) {
            if let (Some(namespace), Some(item)) = (read.shared_namespace.as_ref(), item.as_mut()) {
                self.request_rewriter
                    .normalize_wire_item_from_shared_table(namespace, item)?;
            }
            let item = item.map(WireItem::into_attribute_map).transpose()?;
            let item = match (item, read.projection_expression.as_deref()) {
                (Some(item), Some(projection_expression)) => {
                    let projected = storage_api_project_item(
                        &item,
                        projection_expression,
                        read.expression_attribute_names.as_ref(),
                    );
                    (!projected.is_empty()).then_some(AttributeMap::from(projected))
                }
                (Some(item), None) => Some(AttributeMap::from(item)),
                (None, _) => None,
            };
            responses.push(ItemResponse { item });
        }
        Ok(TransactGetItemsResponse {
            responses,
            consumed_capacity: consumed_capacity_counts.as_deref().and_then(|counts| {
                transact_get_consumed_capacity(return_consumed_capacity.as_deref(), counts)
            }),
        })
    }

    async fn prepare_transact_get_table(
        &self,
        logical_table: &TableName,
    ) -> StorageResult<PreparedTransactGetTable> {
        let route = self
            .resolve_namespace_route_for_table(logical_table)
            .await?;
        let (connection_id, physical_table, shared_namespace) = route.map_or_else(
            || {
                (
                    ROUTED_DEFAULT_CONNECTION_ID.to_string(),
                    logical_table.clone(),
                    None,
                )
            },
            |route| {
                (
                    route.read_target.connection_id,
                    route.read_target.table_name,
                    (route.storage_mode == NamespaceStorageMode::SharedTable)
                        .then_some(route.namespace),
                )
            },
        );
        let provider = self.provider_for_request_connection(&connection_id)?;
        let mut logical_table_info =
            record_storage_operation("get_table_info", provider.get_table_info(&physical_table))
                .await?;
        logical_table_info.table_name = logical_table.clone();
        Ok(PreparedTransactGetTable {
            logical_table: logical_table.clone(),
            logical_table_info,
            connection_id,
            physical_table,
            shared_namespace,
        })
    }
}

struct PreparedTransactGetTable {
    logical_table: TableName,
    logical_table_info: StoredTableInfo,
    connection_id: String,
    physical_table: TableName,
    shared_namespace: Option<TableNamespace>,
}

struct PreparedTransactGetRead {
    table_name: TableName,
    key: KeyAttributes,
    projection_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    shared_namespace: Option<TableNamespace>,
}

fn transact_get_connection_id(tables: &[PreparedTransactGetTable]) -> StorageResult<&str> {
    common_transact_get_connection_id(tables.iter().map(|table| table.connection_id.as_str()))
}

pub(crate) fn common_transact_get_connection_id<'a>(
    connection_ids: impl IntoIterator<Item = &'a str>,
) -> StorageResult<&'a str> {
    let mut connection_ids = connection_ids.into_iter();
    let connection_id = connection_ids
        .next()
        .unwrap_or(ROUTED_DEFAULT_CONNECTION_ID);
    if connection_ids.any(|candidate| candidate != connection_id) {
        return Err(StorageError::unsupported(
            "TransactGetItems cannot guarantee one atomic snapshot across multiple storage \
             connections",
        ));
    }
    Ok(connection_id)
}

fn prepare_transact_get_read(
    get: TransactGetRequest,
    table: &PreparedTransactGetTable,
    request_rewriter: &NamespaceRequestRewriter,
) -> StorageResult<PreparedTransactGetRead> {
    let mut key = get.key;
    if let Some(namespace) = table.shared_namespace.as_ref() {
        request_rewriter.rewrite_key_for_shared_table(namespace, &mut key)?;
    }
    Ok(PreparedTransactGetRead {
        table_name: table.physical_table.clone(),
        key,
        projection_expression: get.projection_expression,
        expression_attribute_names: get.expression_attribute_names,
        shared_namespace: table.shared_namespace.clone(),
    })
}

fn transact_get_snapshot_capability_error(error: StorageError) -> StorageError {
    let StorageEnum::Unsupported { message } = error.to_enum() else {
        return error;
    };
    StorageError::unsupported(&format!(
        "TransactGetItems requires a provider transactional snapshot: {message}"
    ))
}

struct TransactGetConsumedCapacityCount {
    canonical_table_name: TableName,
    response_table_name: TableName,
    count: usize,
}

fn should_track_transact_get_consumed_capacity(return_consumed_capacity: Option<&str>) -> bool {
    matches!(return_consumed_capacity, Some("TOTAL" | "INDEXES"))
}

fn increment_transact_get_consumed_capacity_count(
    counts: &mut Vec<TransactGetConsumedCapacityCount>,
    canonical_table_name: &TableName,
    response_table_name: &TableName,
) {
    if let Some(entry) = counts
        .iter_mut()
        .find(|entry| &entry.canonical_table_name == canonical_table_name)
    {
        entry.count += 1;
        return;
    }
    counts.push(TransactGetConsumedCapacityCount {
        canonical_table_name: canonical_table_name.clone(),
        response_table_name: response_table_name.clone(),
        count: 1,
    });
}

fn transact_get_consumed_capacity(
    return_consumed_capacity: Option<&str>,
    counts: &[TransactGetConsumedCapacityCount],
) -> Option<serde_json::Value> {
    let return_consumed_capacity = return_consumed_capacity?;
    let mut counts = counts.iter().collect::<Vec<_>>();
    counts.sort_by(|left, right| {
        right
            .canonical_table_name
            .as_ref()
            .cmp(left.canonical_table_name.as_ref())
    });
    match return_consumed_capacity {
        "TOTAL" => Some(serde_json::Value::Array(
            counts
                .iter()
                .map(|entry| {
                    let capacity_units = (entry.count as f64) * 2.0;
                    serde_json::json!({
                        "TableName": entry.response_table_name,
                        "CapacityUnits": capacity_units,
                        "ReadCapacityUnits": capacity_units
                    })
                })
                .collect(),
        )),
        "INDEXES" => Some(serde_json::Value::Array(
            counts
                .iter()
                .map(|entry| {
                    let capacity_units = (entry.count as f64) * 2.0;
                    serde_json::json!({
                        "TableName": entry.response_table_name,
                        "CapacityUnits": capacity_units,
                        "ReadCapacityUnits": capacity_units,
                        "Table": {
                            "ReadCapacityUnits": capacity_units,
                            "CapacityUnits": capacity_units
                        }
                    })
                })
                .collect(),
        )),
        "NONE" => None,
        _ => None,
    }
}

fn transact_get_table_not_found_as_resource_not_found(error: StorageError) -> StorageError {
    if let StorageEnum::TableNotFound { name, .. } = error.to_enum() {
        return StorageError::Base(StorageEnum::ResourceNotFound {
            resource_type: "table",
            resource_id: name.clone(),
        });
    }
    error
}
