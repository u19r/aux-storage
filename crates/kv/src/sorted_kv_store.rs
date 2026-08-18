use std::{collections::HashMap, sync::Arc};

use storage_common::TtlConfigRecord;
use storage_condition::Condition;
use storage_provider::UpdateOperation;
use storage_types::{
    AttributeValue, ItemKey, KeyAttributes, ReplicationEventMetadata, SerializesToKey,
    StorageError, StorageResult, StoredTableInfo,
};

use crate::{
    helpers::increment_bytes,
    key_template::{KeyTemplate, PlaceholderId},
    keyspace::table_identity::TableIdentity,
};

type KeyValuePair = (Box<[u8]>, Box<[u8]>);

pub enum AtomicTableWriteDecision {
    NoWrite {
        output: Vec<u8>,
    },
    Write {
        operations: Vec<TransactWriteTableOperation>,
        output: Vec<u8>,
    },
}

pub type AtomicTableWriteTransform =
    Arc<dyn Fn(Option<&[u8]>) -> StorageResult<AtomicTableWriteDecision> + Send + Sync>;

#[derive(Debug, Clone)]
pub struct RangeResult {
    pub items: Vec<KeyValuePair>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct RangeValuesResult {
    pub values: Vec<Vec<u8>>,
    pub has_more: bool,
}

impl RangeValuesResult {}

impl RangeResult {
    #[must_use]
    pub fn into_values_result(self) -> RangeValuesResult {
        let mut values = Vec::with_capacity(self.items.len());
        for (_key, value) in self.items {
            values.push(value.into_vec());
        }
        RangeValuesResult {
            values,
            has_more: self.has_more,
        }
    }
}

#[derive(Clone)]
pub enum TransactWriteOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        condition: Option<Condition>,
    },
    PutTemplate {
        template: KeyTemplate,
        value: Vec<u8>,
        condition: Option<Condition>,
    },
    Delete {
        key: Vec<u8>,
        condition: Option<Condition>,
    },
    Check {
        key: Vec<u8>,
        condition: Condition,
    },
    CheckValue {
        key: Vec<u8>,
        expected_value: Option<Vec<u8>>,
    },
    Update {
        key: Vec<u8>,
        operations: Arc<[UpdateOperation]>,
        condition: Option<Condition>,
    },
}

#[derive(Clone)]
pub enum DirectWriteOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    PutTemplate {
        template: KeyTemplate,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    DeleteRange {
        start: Vec<u8>,
        exclusive_end: Vec<u8>,
    },
    CheckValue {
        key: Vec<u8>,
        expected_value: Option<Vec<u8>>,
    },
}

impl From<DirectWriteOperation> for TransactWriteOperation {
    fn from(value: DirectWriteOperation) -> Self {
        match value {
            DirectWriteOperation::Put { key, value } => Self::Put {
                key,
                value,
                condition: None,
            },
            DirectWriteOperation::PutTemplate { template, value } => Self::PutTemplate {
                template,
                value,
                condition: None,
            },
            DirectWriteOperation::Delete { key } => Self::Delete {
                key,
                condition: None,
            },
            DirectWriteOperation::DeleteRange { start, .. } => Self::Delete {
                key: start,
                condition: None,
            },
            DirectWriteOperation::CheckValue {
                key,
                expected_value,
            } => Self::CheckValue {
                key,
                expected_value,
            },
        }
    }
}

#[derive(Clone)]
pub enum TransactWriteTableOperation {
    Put {
        table_identity: TableIdentity,
        table_info: StoredTableInfo,
        item: HashMap<String, AttributeValue>,
        indexers: Option<Vec<String>>,
        item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
        condition: Option<Condition>,
        return_values_on_condition_check_failure: Option<String>,
        replication: Option<ReplicationEventMetadata>,
        ttl_config: Option<TtlConfigRecord>,
    },
    Delete {
        table_identity: TableIdentity,
        table_info: StoredTableInfo,
        key: KeyAttributes,
        item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
        use_key_attributes_for_missing_item_condition: bool,
        condition: Option<Condition>,
        return_values_on_condition_check_failure: Option<String>,
        replication: Option<ReplicationEventMetadata>,
        ttl_config: Option<TtlConfigRecord>,
    },
    Check {
        table_identity: TableIdentity,
        table_info: StoredTableInfo,
        key: KeyAttributes,
        condition: Condition,
        return_values_on_condition_check_failure: Option<String>,
    },
    Update {
        table_identity: TableIdentity,
        table_info: StoredTableInfo,
        key: KeyAttributes,
        operations: Arc<[UpdateOperation]>,
        indexers: Option<Vec<String>>,
        item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
        condition: Option<Condition>,
        return_values_on_condition_check_failure: Option<String>,
        replication: Option<ReplicationEventMetadata>,
        preserve_old_item: bool,
        transaction_validation: bool,
        ttl_config: Option<TtlConfigRecord>,
    },
}

#[derive(Clone, Debug)]
pub struct BatchItem {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

pub type OldNewItems = (
    Option<HashMap<String, AttributeValue>>,
    Option<HashMap<String, AttributeValue>>,
);

#[derive(Clone, Debug)]
pub struct RawKey(pub Vec<u8>);

impl SerializesToKey for RawKey {
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, storage_types::ItemKeyError> {
        Ok(self.0.clone())
    }
}

pub struct TransactWriteOutput {
    pub items: Vec<OldNewItems>,
    pub placeholder_versions: HashMap<PlaceholderId, [u8; 12]>,
}

impl TransactWriteOutput {
    #[must_use]
    pub fn new(items: Vec<OldNewItems>) -> Self {
        Self {
            items,
            placeholder_versions: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
pub trait SortedKvReadContext: Send + Sync {
    fn take_retryable_read_failure(&self) -> bool {
        false
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>>;

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>>;

    async fn get_range_values(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<RawKey>,
        consistent_read: bool,
    ) -> StorageResult<RangeValuesResult>;
}

/// Scheduling class requested for FoundationDB transactions created by a store
/// clone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionPriority {
    /// Normal foreground transaction scheduling.
    Default,
    /// Low-priority batch scheduling for maintenance work.
    Batch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemValueCodec {
    RocksDbEnvelope,
    #[cfg(feature = "foundationdb-backend")]
    FoundationDbTuple,
}

#[async_trait::async_trait]
pub trait SortedKvStore: Send + Sync + Clone {
    fn item_value_codec(&self) -> ItemValueCodec {
        ItemValueCodec::RocksDbEnvelope
    }

    fn with_transaction_priority(&self, _priority: TransactionPriority) -> Self {
        self.clone()
    }

    async fn atomic_read_modify_write_table(
        &self,
        _read_key: Vec<u8>,
        _transform: AtomicTableWriteTransform,
        _immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<u8>> {
        Err(StorageError::unsupported(
            "atomic table read-modify-write is not supported by this backend",
        ))
    }

    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput>;

    async fn transact_write_unchecked(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        let mapped = operations.into_iter().map(Into::into).collect();
        let _ = self.transact_write(mapped).await?;
        Ok(())
    }
    async fn transact_write_table(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>>;

    async fn transact_write_table_with_direct_writes(
        &self,
        _table_operations: Vec<TransactWriteTableOperation>,
        _direct_operations: Vec<DirectWriteOperation>,
        _immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        Err(StorageError::unsupported(
            "atomic table and direct writes are not supported by this backend",
        ))
    }
    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()>;

    async fn begin_read_context(&self) -> StorageResult<Box<dyn SortedKvReadContext>> {
        Err(StorageError::unsupported(
            "sorted kv read contexts are not supported by this backend",
        ))
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>>;

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>>;

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()>;

    async fn delete(&self, key: &[u8]) -> StorageResult<()>;

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()>;

    async fn get_prefix(
        &self,
        prefix: &[u8],
        scan_index_forwards: bool,
        limit: Option<u32>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        let exclusive_end = increment_bytes(prefix.to_vec());
        if scan_index_forwards {
            self.get_range(
                prefix,
                &exclusive_end,
                limit,
                None::<ItemKey>,
                consistent_read,
            )
            .await
        } else {
            self.get_range(
                &exclusive_end,
                prefix,
                limit,
                None::<ItemKey>,
                consistent_read,
            )
            .await
        }
    }

    async fn get_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult>;

    async fn get_range_values(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeValuesResult> {
        let range = self
            .get_range(start, exclusive_end, limit, page_token, consistent_read)
            .await?;
        Ok(range.into_values_result())
    }
}
