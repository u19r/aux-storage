use std::{
    collections::HashMap,
    sync::atomic::{AtomicU32, Ordering},
};

use async_trait::async_trait;
use storage_types::{
    BatchGetItemRequest, BatchGetWireItemResponse, KeyAttributes, QueryTableRequest,
    ReadSequenceRequest, StorageError, StorageResult, TableName, TryFromWireItem, WireItem,
};

use crate::provider::StorageProviderReadContext;

/// The item budget owned by one ordinary ReadSequence attempt.
///
/// The request planner validates the graph and its public limits before a
/// context is created. Keeping the resulting total bound here gives every
/// provider read (including reads issued concurrently in one wave) one shared
/// accounting boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSequenceReadLimits {
    max_total_items: u32,
}

impl ReadSequenceReadLimits {
    #[must_use]
    pub const fn new(max_total_items: u32) -> Self {
        Self { max_total_items }
    }

    /// Build limits from a request after request/graph validation has run.
    #[must_use]
    pub fn from_request(request: &ReadSequenceRequest) -> Self {
        Self::new(
            request
                .max_total_read_items
                .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS),
        )
    }

    #[must_use]
    pub const fn max_total_items(self) -> u32 {
        self.max_total_items
    }
}

/// A typed, bounded facade over one provider-owned ReadSequence snapshot.
///
/// Providers continue to own wire reads and snapshot lifetime. This facade
/// owns sequence-wide item accounting and the one canonical WireItem decoding
/// path used by typed callers.
pub struct ReadSequenceReadContext {
    provider_context: Box<dyn StorageProviderReadContext>,
    limits: ReadSequenceReadLimits,
    items_read: AtomicU32,
}

impl ReadSequenceReadContext {
    #[must_use]
    pub fn new(
        provider_context: Box<dyn StorageProviderReadContext>,
        limits: ReadSequenceReadLimits,
    ) -> Self {
        Self {
            provider_context,
            limits,
            items_read: AtomicU32::new(0),
        }
    }

    #[must_use]
    pub fn limits(&self) -> ReadSequenceReadLimits {
        self.limits
    }

    #[must_use]
    pub fn items_read(&self) -> u32 {
        self.items_read.load(Ordering::Acquire)
    }

    /// Decode a point read directly into a storage entity or view model.
    pub async fn get_item_as<T>(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<T>>
    where
        T: TryFromWireItem,
    {
        let database_call = metrics_facade::begin_database_call("read_sequence.get_item");
        let item = self
            .provider_context
            .get_item(table_name, key, consistent_read)
            .await?;
        drop(database_call);
        self.account_items(usize::from(item.is_some()))?;
        item.as_ref().map(T::try_from_wire_item).transpose()
    }

    /// Decode every returned item in a batch while preserving table and
    /// unprocessed-key metadata.
    pub async fn batch_get_item_as<T>(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<ReadSequenceBatchGetResponse<T>>
    where
        T: TryFromWireItem,
    {
        let database_call = metrics_facade::begin_database_call("read_sequence.batch_get_item");
        let response = self.provider_context.batch_get_item(request).await?;
        drop(database_call);
        let item_count = response
            .responses
            .as_ref()
            .map(|tables| tables.values().map(Vec::len).sum())
            .unwrap_or_default();
        self.account_items(item_count)?;
        let responses = response
            .responses
            .map(|tables| decode_tables::<T>(tables))
            .transpose()?;
        Ok(ReadSequenceBatchGetResponse {
            responses,
            unprocessed_keys: response.unprocessed_keys,
            consumed_capacity: response.consumed_capacity,
        })
    }

    /// Decode query results directly into a storage entity or view model.
    pub async fn query_table_as<T>(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<T>, Option<String>)>
    where
        T: TryFromWireItem,
    {
        let database_call = metrics_facade::begin_database_call("read_sequence.query_table");
        let (items, cursor) = self.provider_context.query_table(request).await?;
        drop(database_call);
        self.account_items(items.len())?;
        let decoded = items
            .iter()
            .map(T::try_from_wire_item)
            .collect::<StorageResult<Vec<_>>>()?;
        Ok((decoded, cursor))
    }

    fn account_items(&self, count: usize) -> StorageResult<()> {
        let count = u32::try_from(count)
            .map_err(|_| StorageError::validation("ReadSequence item count exceeds u32"))?;
        loop {
            let current = self.items_read.load(Ordering::Acquire);
            let next = current
                .checked_add(count)
                .ok_or_else(|| read_sequence_limit_error(u32::MAX, self.limits.max_total_items))?;
            if next > self.limits.max_total_items {
                return Err(read_sequence_limit_error(next, self.limits.max_total_items));
            }
            if self
                .items_read
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}

/// A decoded batch response for typed ReadSequence callers.
#[derive(Debug, Clone)]
pub struct ReadSequenceBatchGetResponse<T> {
    pub responses: Option<HashMap<TableName, Vec<T>>>,
    pub unprocessed_keys: Option<HashMap<TableName, storage_types::KeysAndAttributes>>,
    pub consumed_capacity: Option<serde_json::Value>,
}

fn decode_tables<T>(
    tables: HashMap<TableName, Vec<WireItem>>,
) -> StorageResult<HashMap<TableName, Vec<T>>>
where T: TryFromWireItem {
    tables
        .into_iter()
        .map(|(table_name, items)| {
            let decoded = items
                .iter()
                .map(T::try_from_wire_item)
                .collect::<StorageResult<Vec<_>>>()?;
            Ok((table_name, decoded))
        })
        .collect()
}

fn read_sequence_limit_error(actual: u32, limit: u32) -> StorageError {
    StorageError::validation(format!(
        "ReadSequence total read item limit exceeded: {actual} > {limit}"
    ))
}

#[async_trait]
impl StorageProviderReadContext for ReadSequenceReadContext {
    fn take_retryable_read_failure(&self) -> bool {
        self.provider_context.take_retryable_read_failure()
    }

    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let database_call = metrics_facade::begin_database_call("read_sequence.get_item");
        let item = self
            .provider_context
            .get_item(table_name, key, consistent_read)
            .await?;
        drop(database_call);
        self.account_items(usize::from(item.is_some()))?;
        Ok(item)
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let database_call = metrics_facade::begin_database_call("read_sequence.batch_get_item");
        let response = self.provider_context.batch_get_item(request).await?;
        drop(database_call);
        let item_count = response
            .responses
            .as_ref()
            .map(|tables| tables.values().map(Vec::len).sum())
            .unwrap_or_default();
        self.account_items(item_count)?;
        Ok(response)
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let database_call = metrics_facade::begin_database_call("read_sequence.query_table");
        let (items, cursor) = self.provider_context.query_table(request).await?;
        drop(database_call);
        self.account_items(items.len())?;
        Ok((items, cursor))
    }
}
