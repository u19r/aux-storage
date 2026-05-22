#![allow(dead_code)]

use std::{collections::HashMap, sync::atomic::Ordering};

use storage_provider::StorageProvider;
use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, KeyAttributes,
    QueryTableRequest, ScanTableRequest, StorageResult, TableName, WireItem,
};

use super::RemoteStorageProvider;
use crate::provider::NO_ENDPOINT;

impl RemoteStorageProvider {
    /// test helper: inspect current primary endpoint index for failover
    /// assertions.
    pub(crate) fn debug_primary_endpoint_test_helper(&self) -> usize {
        self.primary_endpoint.load(Ordering::Relaxed) % self.endpoints.len()
    }

    /// test helper: inspect probation endpoint index for failover assertions.
    pub(crate) fn debug_probation_endpoint_test_helper(&self) -> Option<usize> {
        let value = self.probation_endpoint.load(Ordering::Relaxed);
        if value == NO_ENDPOINT || value >= self.endpoints.len() {
            None
        } else {
            Some(value)
        }
    }

    /// test helper: legacy map-returning wrapper around wire `get_item`.
    pub(crate) async fn get_item_map_test_helper(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let item =
            <Self as StorageProvider>::get_item(self, table_name, key, consistent_read).await?;
        item.map(WireItem::into_attribute_map).transpose()
    }

    /// test helper: legacy map-returning wrapper around wire `scan_table`.
    pub(crate) async fn scan_table_maps_test_helper(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = <Self as StorageProvider>::scan_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, lek))
    }

    /// test helper: legacy map-returning wrapper around wire `query_table`.
    pub(crate) async fn query_table_maps_test_helper(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = <Self as StorageProvider>::query_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, lek))
    }

    /// test helper: legacy map-returning wrapper around wire `batch_get_item`.
    pub(crate) async fn batch_get_item_maps_test_helper(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<storage_types::BatchGetItemResponse> {
        let response = <Self as StorageProvider>::batch_get_item(self, request).await?;
        decode_batch_get_response_to_maps_test_helper(response)
    }
}

fn decode_batch_get_response_to_maps_test_helper(
    response: BatchGetWireItemResponse,
) -> StorageResult<storage_types::BatchGetItemResponse> {
    let responses = if let Some(table_items) = response.responses {
        let mut decoded = HashMap::with_capacity(table_items.len());
        for (table, items) in table_items {
            let mut table_rows = Vec::with_capacity(items.len());
            for item in items {
                table_rows.push(item.into_attribute_map()?.into());
            }
            decoded.insert(table, table_rows);
        }
        Some(decoded)
    } else {
        None
    };

    Ok(storage_types::BatchGetItemResponse {
        responses,
        unprocessed_keys: response.unprocessed_keys,
        consumed_capacity: response.consumed_capacity,
    })
}
