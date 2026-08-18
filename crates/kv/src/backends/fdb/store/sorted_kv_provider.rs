use storage_condition::Condition;
use storage_types::{SerializesToKey, StorageResult};

use crate::{
    backends::fdb::store::FoundationDbKvStore,
    sorted_kv_store::{
        AtomicTableWriteTransform, BatchItem, DirectWriteOperation, ItemValueCodec, OldNewItems,
        RangeResult, SortedKvReadContext, SortedKvStore, TransactWriteOperation,
        TransactWriteOutput, TransactWriteTableOperation, TransactionPriority,
    },
};

#[async_trait::async_trait]
impl SortedKvStore for FoundationDbKvStore {
    fn item_value_codec(&self) -> ItemValueCodec {
        ItemValueCodec::FoundationDbTuple
    }

    fn with_transaction_priority(&self, priority: TransactionPriority) -> Self {
        let mut clone = self.clone();
        clone.transaction_priority = priority;
        clone
    }

    async fn atomic_read_modify_write_table(
        &self,
        read_key: Vec<u8>,
        transform: AtomicTableWriteTransform,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<u8>> {
        self.atomic_read_modify_write_table_operation(
            read_key,
            transform,
            immediate_gsi_consistency,
        )
        .await
    }

    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        self.transact_write_operation(operations).await
    }

    async fn transact_write_unchecked(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        self.transact_write_unchecked_operation(operations).await
    }

    async fn transact_write_table(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        self.transact_write_table_operation(operations, immediate_gsi_consistency)
            .await
    }

    async fn transact_write_table_with_direct_writes(
        &self,
        table_operations: Vec<TransactWriteTableOperation>,
        direct_operations: Vec<DirectWriteOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        self.transact_write_table_with_direct_writes_operation(
            table_operations,
            direct_operations,
            immediate_gsi_consistency,
        )
        .await
    }

    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        self.batch_write_operation(items).await
    }

    async fn begin_read_context(&self) -> StorageResult<Box<dyn SortedKvReadContext>> {
        self.begin_read_context_operation().await
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        self.get_operation(key, consistent_read).await
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.multi_get_operation(keys, consistent_read).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()> {
        self.put_operation(key, value, condition).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.delete_operation(key).await
    }

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()> {
        self.delete_prefix_operation(prefix).await
    }

    async fn get_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        self.get_range_operation(start, exclusive_end, limit, page_token, consistent_read)
            .await
    }
}
