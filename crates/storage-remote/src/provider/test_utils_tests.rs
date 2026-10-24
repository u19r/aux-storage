#![allow(dead_code)]

use std::{collections::HashMap, sync::atomic::Ordering};

use storage_provider::StorageProvider;
use storage_types::{AttributeValue, BatchGetItemRequest, KeyAttributes, StorageResult, TableName};

use super::RemoteStorageProvider;
use crate::constants::NO_ENDPOINT;

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
        item.map(|item| item.into_attribute_map().map(Into::into))
            .transpose()
    }

    /// test helper: legacy map-returning wrapper around wire `batch_get_item`.
    pub(crate) async fn batch_get_item_test_helper(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<storage_types::BatchGetItemResponse> {
        let response = <Self as StorageProvider>::batch_get_item(self, request).await?;
        Ok(storage_types::BatchGetItemResponse {
            responses: response.responses,
            unprocessed_keys: response.unprocessed_keys,
            consumed_capacity: response.consumed_capacity,
        })
    }
}
