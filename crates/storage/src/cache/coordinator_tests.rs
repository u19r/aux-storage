use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use storage_cache::{RuntimePointReadMutation, RuntimeQueryProofMutation, RuntimeWriteEffects};
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType, StorageError,
    StorageResult, StoredTableInfo, TableName, TableStatus, WireItem,
};

use crate::{
    cache_coordinator::{StorageAuthoritativeCacheOptions, StorageCacheServices},
    point_read_cache::{
        PointReadBatchGetResult, PointReadCache, PointReadGetRequest, PointReadGetResult,
    },
    query_proof_cache::{PreparedQueryProofRead, QueryProofCache},
};

#[derive(Debug, Default, Clone)]
struct RecordingPointReadState {
    prepared: Vec<PointReadGetRequest>,
    completed: Vec<PointReadGetRequest>,
    invalidated: Vec<PointReadGetRequest>,
    write_puts: Vec<PointReadGetRequest>,
}

#[derive(Debug, Default)]
struct RecordingPointReadCache {
    state: Mutex<RecordingPointReadState>,
}

impl RecordingPointReadCache {
    fn snapshot(&self) -> RecordingPointReadState {
        self.state.lock().expect("lock point-read state").clone()
    }
}

#[async_trait]
impl PointReadCache for RecordingPointReadCache {
    fn is_enabled(&self) -> bool {
        true
    }

    async fn prepare_write(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        self.state
            .lock()
            .expect("lock point-read state")
            .prepared
            .push(request.clone());
        Ok(())
    }

    async fn complete_write(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        self.state
            .lock()
            .expect("lock point-read state")
            .completed
            .push(request.clone());
        Ok(())
    }

    async fn get_eventual(
        &self,
        _request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult> {
        Ok(PointReadGetResult::Miss)
    }

    async fn batch_get_eventual(
        &self,
        request: &storage_types::BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult> {
        Ok(PointReadBatchGetResult {
            responses: HashMap::new(),
            unresolved_request_items: request.request_items.clone(),
        })
    }

    async fn write_put(
        &self,
        request: &PointReadGetRequest,
        _item: &WireItem,
        _write_version: u64,
    ) -> StorageResult<()> {
        self.state
            .lock()
            .expect("lock point-read state")
            .write_puts
            .push(request.clone());
        Ok(())
    }

    async fn write_delete(
        &self,
        _request: &PointReadGetRequest,
        _write_version: u64,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        self.state
            .lock()
            .expect("lock point-read state")
            .invalidated
            .push(request.clone());
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct FailingQueryProofState {
    invalidated_tables: Vec<TableName>,
}

#[derive(Debug, Default)]
struct FailingQueryProofCache {
    state: Mutex<FailingQueryProofState>,
}

impl FailingQueryProofCache {
    fn snapshot(&self) -> FailingQueryProofState {
        self.state.lock().expect("lock query-proof state").clone()
    }
}

#[async_trait]
impl QueryProofCache for FailingQueryProofCache {
    fn is_enabled(&self) -> bool {
        true
    }

    async fn record_base_put(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _item: &WireItem,
    ) -> StorageResult<()> {
        Err(StorageError::internal("synthetic query-proof failure"))
    }

    async fn record_base_delete(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _key: &WireItem,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate_base_coverage(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _key: &WireItem,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn record_index_transition(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _old_item: Option<&WireItem>,
        _new_item: Option<&WireItem>,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate_index_query_spaces(&self, _table_name: &TableName) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.state
            .lock()
            .expect("lock query-proof state")
            .invalidated_tables
            .push(table_name.clone());
        Ok(())
    }

    async fn record_query_page(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _request: &storage_types::QueryTableRequest,
        _items: &[WireItem],
        _has_more: bool,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn prepare_query_read(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        request: &storage_types::QueryTableRequest,
    ) -> StorageResult<PreparedQueryProofRead> {
        Ok(storage_cache::blocked_runtime_query_proof_read(
            if request.consistent_read {
                storage_cache::runtime_query_proof::RuntimeQueryReadBlockReason::StrongReadBypass
            } else {
                storage_cache::runtime_query_proof::RuntimeQueryReadBlockReason::CacheDisabled
            },
        ))
    }
}

fn table_info(table_name: &TableName) -> StoredTableInfo {
    StoredTableInfo {
        table_name: table_name.clone(),
        table_status: TableStatus::Active,
        created_at: 0.into(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn item(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
        ("payload".to_string(), AttributeValue::S("v".to_string())),
    ])
}

#[tokio::test]
async fn cache_apply_failure_invalidates_query_proof_and_releases_point_read() {
    let point_read = Arc::new(RecordingPointReadCache::default());
    let query_proof = Arc::new(FailingQueryProofCache::default());
    let services = StorageCacheServices::new(
        point_read.clone(),
        query_proof.clone(),
        StorageAuthoritativeCacheOptions::default(),
    );
    let table_name = TableName::new("users");
    let item = item("user#1", "meta");
    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user#1".to_string())),
        ("sk".to_string(), AttributeValue::S("meta".to_string())),
    ]);
    let effects = RuntimeWriteEffects {
        point_read: vec![RuntimePointReadMutation::Put {
            table_name: table_name.clone(),
            key: key.clone().into(),
            item: Box::new(WireItem::from_attribute_map(&item).expect("wire item")),
        }],
        query_proof: vec![RuntimeQueryProofMutation::RecordBasePut {
            table_name: table_name.clone(),
            table_info: table_info(&table_name),
            item,
        }],
    };

    services
        .prepare_write_intents(&effects)
        .await
        .expect("prepare point-read intent");
    services
        .apply_write_effects(&effects)
        .await
        .expect_err("query-proof failure should abort cache apply");

    let point_read_state = point_read.snapshot();
    assert_eq!(point_read_state.prepared.len(), 1);
    assert!(point_read_state.write_puts.is_empty());
    assert_eq!(point_read_state.invalidated.len(), 1);
    assert_eq!(point_read_state.completed.len(), 1);
    assert_eq!(point_read_state.invalidated[0].table_name, table_name);

    let query_proof_state = query_proof.snapshot();
    assert_eq!(query_proof_state.invalidated_tables, vec![table_name]);
}
