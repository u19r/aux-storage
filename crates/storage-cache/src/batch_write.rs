use std::collections::{BTreeMap, HashMap};

use storage_types::{
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, EncodeWriteRequest, TableName, WriteRequest,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RoutedBatchWriteTarget {
    pub connection_id: String,
    pub physical_table: TableName,
    pub logical_table: TableName,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct PhysicalToLogicalWriteTableMap {
    by_connection: HashMap<String, HashMap<TableName, TableName>>,
}

impl PhysicalToLogicalWriteTableMap {
    pub fn insert(&mut self, target: RoutedBatchWriteTarget) {
        self.by_connection
            .entry(target.connection_id)
            .or_default()
            .insert(target.physical_table, target.logical_table);
    }

    pub fn resolve_or_physical(&self, connection_id: &str, physical_table: TableName) -> TableName {
        self.by_connection
            .get(connection_id)
            .and_then(|tables| tables.get(&physical_table))
            .cloned()
            .unwrap_or(physical_table)
    }
}

pub fn insert_routed_batch_write_request<K>(
    per_connection: &mut BTreeMap<K, BatchWriteItemRequest>,
    physical_to_logical: &mut PhysicalToLogicalWriteTableMap,
    return_consumed_capacity: &Option<String>,
    return_item_collection_metrics: &Option<String>,
    target: RoutedBatchWriteTarget,
    dispatch_key: K,
    write_requests: Vec<WriteRequest>,
) where
    K: Ord,
{
    let connection_id = target.connection_id.clone();
    let physical_table = target.physical_table.clone();
    per_connection
        .entry(dispatch_key)
        .or_insert_with(|| BatchWriteItemRequest {
            request_items: HashMap::new(),
            return_consumed_capacity: return_consumed_capacity.clone(),
            return_item_collection_metrics: return_item_collection_metrics.clone(),
        })
        .request_items
        .entry(physical_table.clone())
        .or_default()
        .extend(write_requests);
    physical_to_logical.insert(RoutedBatchWriteTarget {
        connection_id,
        physical_table,
        logical_table: target.logical_table,
    });
}

pub fn insert_routed_batch_write_encode_request<K>(
    per_connection: &mut BTreeMap<K, BatchWriteItemEncodeRequest>,
    physical_to_logical: &mut PhysicalToLogicalWriteTableMap,
    return_consumed_capacity: &Option<String>,
    return_item_collection_metrics: &Option<String>,
    target: RoutedBatchWriteTarget,
    dispatch_key: K,
    write_requests: Vec<EncodeWriteRequest>,
) where
    K: Ord,
{
    let connection_id = target.connection_id.clone();
    let physical_table = target.physical_table.clone();
    per_connection
        .entry(dispatch_key)
        .or_insert_with(|| BatchWriteItemEncodeRequest {
            request_items: HashMap::new(),
            return_consumed_capacity: return_consumed_capacity.clone(),
            return_item_collection_metrics: return_item_collection_metrics.clone(),
        })
        .request_items
        .entry(physical_table.clone())
        .or_default()
        .extend(write_requests);
    physical_to_logical.insert(RoutedBatchWriteTarget {
        connection_id,
        physical_table,
        logical_table: target.logical_table,
    });
}

pub fn merge_unprocessed_batch_write_items(
    merged_unprocessed: &mut HashMap<TableName, Vec<WriteRequest>>,
    physical_to_logical: &PhysicalToLogicalWriteTableMap,
    connection_id: &str,
    unprocessed: HashMap<TableName, Vec<WriteRequest>>,
) {
    for (physical_table, pending) in unprocessed {
        let logical_table = physical_to_logical.resolve_or_physical(connection_id, physical_table);
        merged_unprocessed
            .entry(logical_table)
            .or_default()
            .extend(pending);
    }
}
