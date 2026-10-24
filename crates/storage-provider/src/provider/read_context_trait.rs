use async_trait::async_trait;
use storage_types::{
    BatchGetItemRequest, BatchGetWireItemResponse, KeyAttributes, QueryTableRequest, StorageError,
    StorageResult, StoredTableInfo, TableName, WireItem,
};

use super::trait_defaults as defaults;

/// Provider-owned read context for operations that must share one backend
/// snapshot/read version.
#[async_trait]
pub trait StorageProviderReadContext: Send + Sync {
    /// Return and clear a backend signal that the current read-only attempt
    /// must be discarded and rebuilt from a fresh snapshot. Providers which
    /// cannot classify retryable read failures keep the default `false`.
    fn take_retryable_read_failure(&self) -> bool {
        defaults::take_retryable_read_failure()
    }

    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>>;

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse>;

    /// Load authoritative table metadata on this context's native session.
    /// Native multi-table operations use this hook to bind their plan after
    /// admission, without opening a second connection or preflighting outside
    /// the attempt. Contexts which do not own catalog reads keep the default.
    async fn get_table_info(&self, _table_name: &TableName) -> StorageResult<StoredTableInfo> {
        Err(StorageError::unsupported(
            "read context does not support native metadata reads",
        ))
    }

    async fn query_table(&self, request: &QueryTableRequest)
    -> StorageResult<super::RangeReadPage>;
}
