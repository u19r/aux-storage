use std::{collections::HashMap, sync::Arc};

use storage_common::TtlConfigRecord;
use storage_condition::Condition;
use storage_provider::UpdateOperation;
use storage_types::{
    AttributeValue, IndexName, ItemKey, KeyAttributes, ReplicationEventMetadata, SerializesToKey,
    StorageError, StorageResult, StoredTableInfo, WireItem,
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

impl RangeValuesResult {
    /// # Panics
    ///
    /// Will panic if item deserialization fails
    pub fn into_query_result(
        self,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let mut items = Vec::with_capacity(self.values.len());
        for data in self.values {
            let json = storage_types::storage_serde::decompress_bytes(&data)?;
            items.push(WireItem::dynamo_json(json));
        }

        let last_evaluated_key = if self.has_more {
            if let Some(last) = items.last() {
                last.last_evaluated_key(table_info, index_name)?
            } else {
                None
            }
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }
}

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

    /// # Panics
    ///
    /// Will panic if item deserialization fails
    pub fn into_query_result(
        self,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.into_values_result()
            .into_query_result(table_info, index_name)
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
        item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
        condition: Option<Condition>,
        return_values_on_condition_check_failure: Option<String>,
        replication: Option<ReplicationEventMetadata>,
        preserve_old_item: bool,
        transaction_validation: bool,
        ttl_config: Option<TtlConfigRecord>,
    },
}

impl TryFrom<&TransactWriteTableOperation> for Option<TransactWriteOperation> {
    type Error = StorageError;
    fn try_from(
        table_op: &TransactWriteTableOperation,
    ) -> Result<Option<TransactWriteOperation>, StorageError> {
        match table_op {
            TransactWriteTableOperation::Put {
                table_identity,
                table_info,
                item,
                ..
            } => {
                let key = storage_types::ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    item,
                )?;
                let key = crate::keyspace::table_keys::item_key(table_identity, &key)?;
                let value = storage_types::storage_serde::to_bytes(&item)?;
                Ok(Some(TransactWriteOperation::Put {
                    key,
                    value,
                    condition: None,
                }))
            }
            TransactWriteTableOperation::Delete {
                table_identity,
                table_info,
                key,
                ..
            } => {
                let key = storage_types::ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    key,
                )?;
                let key = crate::keyspace::table_keys::item_key(table_identity, &key)?;
                Ok(Some(TransactWriteOperation::Delete {
                    key,
                    condition: None,
                }))
            }
            _ => Ok(None),
        }
    }
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

#[async_trait::async_trait]
pub trait SortedKvStore: Send + Sync + Clone {
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
