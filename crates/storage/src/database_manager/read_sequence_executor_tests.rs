use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use storage_provider::{SqliteSettings, StorageBackend, StorageConfig, StorageProviderReadContext};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse,
    BillingMode, CreateTableRequest, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    QueryTableRequest, ReadSequenceConsistency, ReadSequenceConsistency::Eventual, StorageError,
    StorageResult, TableName, WireItem,
};

use super::{InProcessReadSequenceLimits, ROUTED_DEFAULT_CONNECTION_ID};
use crate::{DatabaseManager, PutItemInput, QueryTableInput};

#[test]
fn given_limits_above_hard_caps_when_constructed_then_they_are_rejected() {
    let error = InProcessReadSequenceLimits::try_new(17, 1, 1, 1)
        .expect_err("operation hard cap must be enforced");
    assert!(error.to_string().contains("max_operations"));
    let error = InProcessReadSequenceLimits::try_new(1, 2_049, 1, 1)
        .expect_err("total-read hard cap must be enforced");
    assert!(error.to_string().contains("max_total_read_items"));
    let error = InProcessReadSequenceLimits::try_new(1, 1, 101, 1)
        .expect_err("per-operation hard cap must be enforced");
    assert!(error.to_string().contains("max_items_per_operation"));
    let error = InProcessReadSequenceLimits::try_new(1, 1, 1, 16 * 1024 * 1024 + 1)
        .expect_err("response-byte hard cap must be enforced");
    assert!(error.to_string().contains("max_response_bytes"));
}

#[tokio::test]
async fn given_operation_limit_when_next_read_starts_then_it_fails_before_provider_work() {
    let db = DatabaseManager::new_for_test().await.expect("test db");
    let table = TableName::new("in_process_sequence_limits");
    create_hash_table(&db, &table).await;
    put_version(&db, &table, "item#1", "before").await;
    let limits = InProcessReadSequenceLimits::try_new(1, 2, 2, 1024).expect("limits");
    let mut executor = db
        .read_sequence_executor(Eventual, limits)
        .expect("executor");

    assert!(
        executor
            .get_item(table.clone(), key("item#1"))
            .await
            .expect("first read")
            .is_some()
    );
    let error = executor
        .get_item(table, key("item#1"))
        .await
        .expect_err("second operation must exceed the limit");

    assert!(error.to_string().contains("operation limit exceeded"));
    assert_eq!(executor.stats().operations_started(), 1);
    assert_eq!(executor.stats().operations_completed(), 1);
    assert_eq!(executor.stats().requested_items(), 1);
    assert_eq!(executor.stats().returned_items(), 1);
}

#[tokio::test]
async fn given_response_byte_limit_when_item_is_larger_then_result_fails_with_accounting() {
    let db = DatabaseManager::new_for_test().await.expect("test db");
    let table = TableName::new("in_process_sequence_response_bytes");
    create_hash_table(&db, &table).await;
    put_version(&db, &table, "item#1", "before").await;
    let limits = InProcessReadSequenceLimits::try_new(1, 1, 1, 1).expect("limits");
    let mut executor = db
        .read_sequence_executor(Eventual, limits)
        .expect("executor");

    let error = executor
        .get_item(table, key("item#1"))
        .await
        .expect_err("item must exceed the response-byte limit");

    assert!(error.to_string().contains("response byte limit exceeded"));
    assert_eq!(executor.stats().operations_started(), 1);
    assert_eq!(executor.stats().operations_completed(), 1);
    assert_eq!(executor.stats().returned_items(), 1);
    assert!(executor.stats().returned_bytes() > 1);
}

#[tokio::test]
async fn given_cancelled_read_when_future_is_dropped_then_no_child_task_survives() {
    let db = DatabaseManager::new_for_test().await.expect("test db");
    let table = TableName::new("in_process_sequence_cancellation");
    create_hash_table(&db, &table).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let mut executor = db
        .read_sequence_executor(Eventual, InProcessReadSequenceLimits::default())
        .expect("executor");
    executor.set_read_context_for_test(
        ROUTED_DEFAULT_CONNECTION_ID,
        Box::new(PendingReadContext {
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
        }),
    );

    {
        let read = executor.get_item(table, key("item#1"));
        tokio::pin!(read);
        tokio::select! {
            () = entered.notified() => {}
            result = &mut read => panic!("pending provider read unexpectedly completed: {result:?}"),
        }
    }

    assert_eq!(executor.stats().operations_started(), 1);
    assert_eq!(executor.stats().operations_completed(), 0);
    drop(executor);
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn given_query_above_remaining_budget_when_executed_then_provider_limit_is_clamped() {
    let db = DatabaseManager::new_for_test().await.expect("test db");
    let table = TableName::new("in_process_sequence_query_budget");
    create_composite_table(&db, &table).await;
    for sk in ["a", "b", "c"] {
        db.put_item(
            PutItemInput::builder()
                .table_name(table.clone())
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("group".to_string())),
                    ("sk".to_string(), AttributeValue::S(sk.to_string())),
                ]))
                .build(),
        )
        .await
        .expect("put query item");
    }
    let limits = InProcessReadSequenceLimits::try_new(2, 1, 100, 1024).expect("limits");
    let mut executor = db
        .read_sequence_executor(Eventual, limits)
        .expect("executor");

    let (items, token) = executor
        .query_table(
            QueryTableInput::builder()
                .table_name(table)
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("group".to_string()),
                )]))
                .limit(100_u32)
                .build(),
        )
        .await
        .expect("bounded query");

    assert_eq!(items.len(), 1);
    assert!(token.is_some());
    assert_eq!(executor.stats().operations_started(), 1);
    assert_eq!(executor.stats().requested_items(), 1);
    assert_eq!(executor.stats().returned_items(), 1);
}

#[tokio::test]
async fn given_transactional_sequence_when_writer_commits_then_dependent_read_keeps_root_snapshot()
{
    let path = std::env::temp_dir().join(format!(
        "aux-storage-in-process-sequence-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let config = file_backed_sqlite_config(path.to_string_lossy().into_owned());
    let reader = DatabaseManager::new_for_test_with_config(config.clone())
        .await
        .expect("reader db");
    let writer = DatabaseManager::new_for_test_with_config(config)
        .await
        .expect("writer db");
    let table = TableName::new("in_process_sequence_snapshot");
    create_hash_table(&reader, &table).await;
    put_version(&reader, &table, "root", "before").await;
    put_version(&reader, &table, "child", "before").await;
    let mut executor = reader
        .read_sequence_executor(
            ReadSequenceConsistency::Transactional,
            InProcessReadSequenceLimits::default(),
        )
        .expect("transactional executor");

    let root = executor
        .get_item(table.clone(), key("root"))
        .await
        .expect("root read")
        .expect("root item");
    put_version(&writer, &table, "root", "after").await;
    put_version(&writer, &table, "child", "after").await;
    let child = executor
        .get_item(table, key("child"))
        .await
        .expect("dependent read")
        .expect("child item");

    assert_eq!(version(&root), "before");
    assert_eq!(version(&child), "before");
    assert_eq!(executor.stats().operations_started(), 2);
    assert_eq!(executor.stats().operations_completed(), 2);
    assert_eq!(executor.stats().requested_items(), 2);
    assert_eq!(executor.stats().returned_items(), 2);
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn foundationdb_executor_uses_one_transaction_and_accounts_for_each_point_read() {
    let Ok(cluster_file) = std::env::var("FDB_CLUSTER_FILE") else {
        eprintln!("Skipping live FoundationDB executor proof: FDB_CLUSTER_FILE is unset");
        return;
    };
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let config = StorageConfig {
        backend_type: StorageBackend::FoundationDb,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: Some(storage_provider::FoundationDbSettings {
            cluster_file: Some(cluster_file),
            subspace_prefix: Some(format!("tests/in-process-read-sequence/{suffix}/")),
            immediate_gsi_consistency: false,
            ..storage_provider::FoundationDbSettings::default()
        }),
        remote: None,
    };
    let db = DatabaseManager::new_for_test_with_config(config)
        .await
        .expect("live FoundationDB manager");
    let table = TableName::new("in_process_sequence_fdb_accounting");
    create_hash_table(&db, &table).await;
    put_version(&db, &table, "root", "before").await;
    put_version(&db, &table, "child", "before").await;
    crate::foundationdb_operation_metrics_reset();
    let mut executor = db
        .read_sequence_executor(
            ReadSequenceConsistency::Transactional,
            InProcessReadSequenceLimits::default(),
        )
        .expect("executor");

    let _ = executor
        .get_item(table.clone(), key("root"))
        .await
        .expect("root read");
    let _ = executor
        .get_item(table, key("child"))
        .await
        .expect("child read");

    let metrics = crate::foundationdb_operation_metrics_snapshot();
    assert_eq!(
        fdb_operation_metric(&metrics, "read_context", "transaction_start"),
        1,
        "executor must open one FoundationDB transaction\n{metrics}"
    );
    assert_eq!(
        fdb_operation_metric(&metrics, "read_context", "snapshot_point_read"),
        2,
        "executor must account for both point reads\n{metrics}"
    );
    assert_eq!(executor.stats().operations_completed(), 2);
    assert_eq!(executor.stats().returned_items(), 2);
}

async fn create_hash_table(db: &DatabaseManager, table: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create table");
}

async fn create_composite_table(db: &DatabaseManager, table: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create composite table");
}

async fn put_version(db: &DatabaseManager, table: &TableName, pk: &str, version: &str) {
    db.put_item(
        PutItemInput::builder()
            .table_name(table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                (
                    "version".to_string(),
                    AttributeValue::S(version.to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put item");
}

fn key(pk: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".to_string(), AttributeValue::S(pk.to_string()))])
}

fn version(item: &WireItem) -> String {
    match item.attribute_value("version").expect("decode version") {
        Some(AttributeValue::S(value)) => value,
        other => panic!("unexpected version: {other:?}"),
    }
}

fn file_backed_sqlite_config(path: String) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path),
        file_path: None,
        sqlite: Some(SqliteSettings {
            immediate_gsi_consistency: false,
            force_file_backed_database: true,
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(feature = "foundationdb")]
fn fdb_operation_metric(metrics: &str, path: &str, operation: &str) -> u64 {
    let needle = format!("path=\"{path}\",operation=\"{operation}\"");
    metrics
        .lines()
        .find(|line| line.contains(&needle))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0)
}

struct PendingReadContext {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}

impl Drop for PendingReadContext {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[async_trait]
impl StorageProviderReadContext for PendingReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.entered.notify_one();
        std::future::pending().await
    }

    async fn batch_get_item(
        &self,
        _request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        Err(StorageError::unsupported("unused test operation"))
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        Err(StorageError::unsupported("unused test operation"))
    }
}
