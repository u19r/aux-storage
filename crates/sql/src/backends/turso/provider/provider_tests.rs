use std::collections::HashMap;

use storage_common::TTL_SWEEP_JOB;
use storage_provider::{
    StorageProvider, StreamTrimDueMarker, StreamTrimScope, StreamTrimState, StreamTrimStateWrite,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchWriteItemRequest, BillingMode,
    CreateGlobalSecondaryIndex, CreateTableRequest, DeleteRequest, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, KeysAndAttributes, Projection, ProjectionType, PutRequest,
    QueryTableRequest, ReadSequenceConsistency, StorageEnum, StreamItemId, StreamName,
    StreamRetentionDuration, StreamSpecification, StreamViewType, TableName, TimestampMillis,
    UpdateTableRequest, UserStreamName, WriteRequest,
};
use stream_provider::{
    CursorName, CursorPosition, StoredStreamPointer, StreamDataType, StreamEnum, StreamProvider,
};
use turso::{Error as TursoError, Value as TursoValue};

use super::{
    TursoStorageProvider, attribute_scalar_to_turso_value, map_turso_error, option_string_to_value,
};
use crate::gsi_profile_support_tests::{
    REALISTIC_GSI_PROFILE_INDEXES, REALISTIC_GSI_PROFILE_ITEMS, print_gsi_profile_counters,
    realistic_gsi_profile_batches, realistic_gsi_profile_request,
};

async fn create_test_provider() -> TursoStorageProvider {
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("create turso provider");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream tables");
    provider
}

fn basic_create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
}

#[tokio::test]
async fn given_base_and_gsi_tables_when_turso_capacity_increases_then_actual_schemas_add_columns() {
    let provider = create_test_provider().await;
    provider.initialize_storage().await.unwrap();
    let table_name = TableName::new("TursoIndexerCapacitySchema");
    let index_name = IndexName::new("by_group");
    let mut request = gsi_create_table_request(&table_name);
    request.global_secondary_indexes = request.global_secondary_indexes.map(|mut indexes| {
        indexes[0].index_name = index_name.clone();
        indexes
    });
    request.max_indexers = storage_types::MaxIndexers::try_new(4).unwrap();
    provider.create_table(&request).await.unwrap();
    assert_turso_indexer_column_count(&provider, &table_name, &index_name, 4).await;

    provider
        .update_table(UpdateTableRequest {
            table_name: table_name.clone(),
            max_indexers: Some(storage_types::MaxIndexers::try_new(32).unwrap()),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            aux_stream_duration_hours: None,
            aux_default_item_stream_duration_hours: None,
            global_secondary_index_updates: None,
            replica_updates: None,
            sse_specification: None,
            stream_specification: None,
            table_class: None,
        })
        .await
        .unwrap();
    assert_turso_indexer_column_count(&provider, &table_name, &index_name, 32).await;
}

async fn assert_turso_indexer_column_count(
    provider: &TursoStorageProvider,
    table_name: &TableName,
    index_name: &IndexName,
    expected: usize,
) {
    let connection = provider.primary_connection().await.unwrap();
    for table in [
        crate::naming::physical_table_name(table_name),
        crate::naming::physical_gsi_table_name(table_name, index_name),
    ] {
        let mut rows = connection
            .query(&format!("PRAGMA table_info(\"{table}\")"), ())
            .await
            .unwrap();
        let mut columns = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            columns.push(row.get::<String>(1).unwrap());
        }
        assert_eq!(
            columns
                .iter()
                .filter(|column| column.starts_with("__aux_indexer_"))
                .count(),
            expected,
            "physical table {table}",
        );
        assert!(columns.contains(&format!("__aux_indexer_{}", expected - 1)));
        assert!(!columns.contains(&format!("__aux_indexer_{expected}")));
    }
}

fn gsi_create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gpk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("gsi1"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gpk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]))
}

#[tokio::test]
async fn turso_capabilities_advertise_durable_item_guards_without_transactions() {
    let provider = create_test_provider().await;
    assert!(provider.supports_guarded_writes());
    assert!(!provider.supports_guarded_transaction_writes());
}

#[tokio::test]
async fn turso_read_sequence_context_reuses_one_snapshot_for_get_batch_get_and_query() {
    let dir = crate::sql_test_support::temp_dir("sqlite");
    let db_path = dir.path().join("read-sequence-snapshot.db");
    let provider = TursoStorageProvider::new(db_path.to_string_lossy().as_ref())
        .await
        .expect("create turso provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!("turso_read_sequence_{}", uuid::Uuid::now_v7()));
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");
    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-1".to_string())),
                ("name".to_string(), AttributeValue::S("before".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("seed item");

    let key = storage_types::KeyAttributes::from([(
        "pk".to_string(),
        AttributeValue::S("u-1".to_string()),
    )]);
    let read_context = provider
        .begin_read_sequence_read_context(ReadSequenceConsistency::Transactional)
        .await
        .expect("begin read sequence context");
    let initial = read_context
        .get_item(table_name.clone(), key.clone(), true)
        .await
        .expect("snapshot get")
        .expect("snapshot item")
        .to_attribute_map()
        .expect("decode snapshot item");
    assert_eq!(
        initial.get("name"),
        Some(&AttributeValue::S("before".to_string()))
    );

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-1".to_string())),
                ("name".to_string(), AttributeValue::S("after".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("update item outside snapshot");

    let ordinary_read = provider
        .get_item(table_name.clone(), key.clone(), true)
        .await
        .expect("ordinary get")
        .expect("ordinary item")
        .to_attribute_map()
        .expect("decode ordinary item");
    assert_eq!(
        ordinary_read.get("name"),
        Some(&AttributeValue::S("after".to_string()))
    );

    let snapshot_get = read_context
        .get_item(table_name.clone(), key.clone(), true)
        .await
        .expect("snapshot get after update")
        .expect("snapshot item after update")
        .to_attribute_map()
        .expect("decode snapshot get item");
    assert_eq!(
        snapshot_get.get("name"),
        Some(&AttributeValue::S("before".to_string()))
    );

    let snapshot_batch = read_context
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![key.clone()].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .expect("snapshot batch get");
    let snapshot_batch_item = snapshot_batch
        .responses
        .as_ref()
        .and_then(|responses| responses.get(&table_name))
        .and_then(|items| items.first())
        .expect("snapshot batch item")
        .to_attribute_map()
        .expect("decode snapshot batch item");
    assert_eq!(
        snapshot_batch_item.get("name"),
        Some(&AttributeValue::S("before".to_string()))
    );

    let (snapshot_query, _) = read_context
        .query_table(&QueryTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("u-1".to_string()),
            )])),
            projection_expression: None,
            limit: Some(10),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: true,
        })
        .await
        .expect("snapshot query");
    let snapshot_query_item = snapshot_query
        .first()
        .expect("snapshot query item")
        .to_attribute_map()
        .expect("decode snapshot query item");
    assert_eq!(
        snapshot_query_item.get("name"),
        Some(&AttributeValue::S("before".to_string()))
    );
}

#[test]
fn map_turso_busy_errors_to_transaction_conflict() {
    let error = map_turso_error(TursoError::Busy("database is locked".to_string()));
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
}

#[test]
fn map_turso_error_locked_message_to_transaction_conflict() {
    let error = map_turso_error(TursoError::Error("database is locked".to_string()));
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
}

#[test]
fn map_turso_error_no_transaction_active_to_transaction_conflict() {
    let error = map_turso_error(TursoError::Error(
        "Transaction error: cannot commit - no transaction is active".to_string(),
    ));
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
}

#[test]
fn map_turso_error_ongoing_transaction_to_transaction_conflict() {
    let error = map_turso_error(TursoError::Error(
        "Operation was rejected because there is an ongoing transaction for the item.".to_string(),
    ));
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
}

#[test]
fn map_turso_constraint_ongoing_transaction_to_transaction_conflict() {
    let error = map_turso_error(TursoError::Constraint(
        "Operation was rejected because there is an ongoing transaction for the item.".to_string(),
    ));
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
}

#[test]
fn map_turso_constraint_errors_to_validation() {
    let error = map_turso_error(TursoError::Constraint("unique violation".to_string()));
    assert!(matches!(error.as_ref(), StorageEnum::Validation { .. }));
}

#[test]
fn attribute_scalar_to_turso_value_prefers_numeric_types() {
    assert_eq!(
        attribute_scalar_to_turso_value(&AttributeValue::N("42".to_string())).unwrap(),
        TursoValue::Integer(42)
    );
    assert_eq!(
        attribute_scalar_to_turso_value(&AttributeValue::N("3.18".to_string())).unwrap(),
        TursoValue::Real(3.18)
    );
    assert_eq!(
        attribute_scalar_to_turso_value(&AttributeValue::N("not-a-number".to_string())).unwrap(),
        TursoValue::Text("not-a-number".to_string())
    );
}

#[test]
fn option_string_to_value_maps_none_to_null() {
    assert_eq!(option_string_to_value(None), TursoValue::Null);
    assert_eq!(
        option_string_to_value(Some("value".to_string())),
        TursoValue::Text("value".to_string())
    );
}

#[tokio::test]
async fn turso_stream_create_get_and_duplicate() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let stream_id = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await
        .expect("create stream");

    let stream = provider
        .get_stream(user_stream_name.clone())
        .await
        .expect("fetch stream")
        .expect("stream should exist");
    assert_eq!(stream.name.as_str(), user_stream_name.as_str());
    assert_eq!(stream.internal_id, stream_id);
    assert_eq!(stream.ttl_seconds, Some(3600.into()));

    let duplicate = provider
        .create_stream(UserStreamName::new("stream-name"), None, Default::default())
        .await;
    assert!(duplicate.is_err());
}

fn assert_turso_stream_unsupported_contains(err: &stream_provider::StreamError, expected: &str) {
    let StreamEnum::StorageError(storage_error) = err.as_ref() else {
        panic!("expected storage error, got {err:?}");
    };
    let StorageEnum::Unsupported { message } = storage_error.as_ref() else {
        panic!("expected unsupported storage error, got {storage_error:?}");
    };
    assert!(
        message.contains(expected),
        "expected '{message}' to contain '{expected}'"
    );
}

#[tokio::test]
async fn turso_stream_initialization_accepts_empty_storage() {
    let _provider = create_test_provider().await;
}

#[tokio::test]
async fn turso_stream_initialization_rejects_nonempty_stream_items_without_format_metadata() {
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("create turso provider");
    let conn = provider.connect().await.expect("connect");
    provider
        .execute(
            &conn,
            crate::backends::turso::sql_statements::create_user_streams_table(),
            Vec::new(),
        )
        .await
        .unwrap();
    provider
        .execute(
            &conn,
            crate::backends::turso::sql_statements::create_stream_items_table(),
            Vec::new(),
        )
        .await
        .unwrap();
    provider
        .execute(
            &conn,
            "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            vec![
                TursoValue::Text("old-stream".to_string()),
                TursoValue::Text("old-id".to_string()),
                TursoValue::Blob(vec![1]),
                TursoValue::Integer(1),
                TursoValue::Integer(StreamDataType::Text as i64),
            ],
        )
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("old stream rows without metadata should be rejected");

    assert_turso_stream_unsupported_contains(&err, "in-place upgrade");
}

#[tokio::test]
async fn turso_stream_initialization_rejects_old_format_pointer_payload_with_metadata() {
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("create turso provider");
    let conn = provider.connect().await.expect("connect");
    provider
        .execute(
            &conn,
            crate::backends::turso::sql_statements::create_user_streams_table(),
            Vec::new(),
        )
        .await
        .unwrap();
    provider
        .execute(
            &conn,
            crate::backends::turso::sql_statements::create_stream_items_table(),
            Vec::new(),
        )
        .await
        .unwrap();
    provider
        .execute(
            &conn,
            crate::backends::turso::sql_statements::create_stream_format_metadata_table(),
            Vec::new(),
        )
        .await
        .unwrap();
    provider
        .execute(
            &conn,
            crate::backends::turso::sql_statements::upsert_stream_format_version(),
            vec![
                TursoValue::Text("item_versioned_stream".to_string()),
                TursoValue::Integer(1),
            ],
        )
        .await
        .unwrap();
    let old_pointer = serde_json::json!({
        "type": "pointer",
        "stream_name": "item-stream",
        "table_name": "OldPointerTable"
    });
    let old_pointer_bytes = storage_types::storage_serde::to_bytes(&old_pointer).unwrap();
    provider
        .execute(
            &conn,
            "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            vec![
                TursoValue::Text("system".to_string()),
                TursoValue::Text("pointer-id".to_string()),
                TursoValue::Blob(old_pointer_bytes),
                TursoValue::Integer(1),
                TursoValue::Integer(StreamDataType::StreamPointer as i64),
            ],
        )
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("old pointer payload should be rejected");

    assert_turso_stream_unsupported_contains(&err, "old-format stream pointer payload");
}

#[tokio::test]
async fn turso_stream_append_read_and_cursor_lifecycle() {
    let provider = create_test_provider().await;
    let stream_name = provider
        .create_stream(UserStreamName::new("events"), None, Default::default())
        .await
        .expect("create stream");

    let item1 = provider
        .append_item(stream_name.clone(), b"item-1", None)
        .await
        .expect("append item1");
    let item2 = provider
        .append_item(stream_name.clone(), b"item-2", None)
        .await
        .expect("append item2");

    let forward = provider
        .read_forward(stream_name.clone(), None, 10)
        .await
        .expect("read forward");
    assert_eq!(forward.items.len(), 2);
    assert_eq!(forward.items[0].id, item1);
    assert_eq!(forward.items[1].id, item2);

    let backward = provider
        .read_backward(stream_name.clone(), None, 10)
        .await
        .expect("read backward");
    assert_eq!(backward.items.len(), 2);
    assert_eq!(backward.items[0].id, item2);
    assert_eq!(backward.items[1].id, item1);

    let cursor_name = CursorName::new("consumer-1");
    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .expect("create cursor");

    let from_cursor = provider
        .read_from_cursor(stream_name.clone(), cursor_name.clone(), 10)
        .await
        .expect("read from cursor");
    assert_eq!(from_cursor.items.len(), 2);

    provider
        .advance_cursor(stream_name.clone(), cursor_name.clone(), item2)
        .await
        .expect("advance cursor");

    let cursor = provider
        .get_cursor(stream_name.clone(), cursor_name.clone())
        .await
        .expect("get cursor")
        .expect("cursor should exist");
    assert_eq!(cursor.position, item2);

    provider
        .delete_cursor(stream_name.clone(), cursor_name.clone())
        .await
        .expect("delete cursor");
    assert!(
        provider
            .get_cursor(stream_name.clone(), cursor_name)
            .await
            .expect("get cursor after delete")
            .is_none()
    );

    provider
        .delete_stream(UserStreamName::new("events"))
        .await
        .expect("delete stream");
    assert!(
        provider
            .get_stream(UserStreamName::new("events"))
            .await
            .expect("get stream after delete")
            .is_none()
    );
}

#[tokio::test]
async fn turso_stream_cursor_tail_reads_only_new_items() {
    let provider = create_test_provider().await;
    let stream_name = provider
        .create_stream(UserStreamName::new("cursor-tail"), None, Default::default())
        .await
        .expect("create stream");

    let _ = provider
        .append_item(stream_name.clone(), b"before-1", None)
        .await
        .expect("append before-1");
    let _ = provider
        .append_item(stream_name.clone(), b"before-2", None)
        .await
        .expect("append before-2");

    let cursor_name = CursorName::new("consumer-tail");
    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Tail,
        )
        .await
        .expect("create tail cursor");

    let after1 = provider
        .append_item(stream_name.clone(), b"after-1", None)
        .await
        .expect("append after-1");
    let after2 = provider
        .append_item(stream_name.clone(), b"after-2", None)
        .await
        .expect("append after-2");

    let page = provider
        .read_from_cursor(stream_name, cursor_name, 10)
        .await
        .expect("read from cursor");
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, after1);
    assert_eq!(page.items[1].id, after2);
}

#[tokio::test]
async fn turso_delete_missing_stream_returns_not_found() {
    let provider = create_test_provider().await;
    let error = provider
        .delete_stream(UserStreamName::new("missing-stream"))
        .await
        .expect_err("delete missing stream should fail");

    assert!(matches!(
        error.as_ref(),
        StreamEnum::ResourceNotFound { resource_type, .. } if *resource_type == "stream"
    ));
}

#[tokio::test]
async fn turso_put_item_uses_upsert_without_duplicate_rows() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_upsert_table");
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("name".to_string(), AttributeValue::S("Ada".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert row");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("name".to_string(), AttributeValue::S("Grace".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("update row with upsert");

    let item = provider
        .get_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("user-1".to_string()))]).into(),
            true,
        )
        .await
        .expect("fetch row")
        .expect("row should exist")
        .to_attribute_map()
        .expect("decode row");
    assert_eq!(
        item.get("name"),
        Some(&AttributeValue::S("Grace".to_string()))
    );

    let (rows, _lek) = provider
        .scan_table(&storage_types::ScanTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            limit: Some(10),
            exclusive_start_key: None,
            consistent_read: true,
        })
        .await
        .expect("scan table");
    assert_eq!(rows.len(), 1, "upsert should keep a single logical row");
}

#[tokio::test]
async fn turso_put_update_delete_write_dynamodb_storage_stream_records() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_stream_table");
    let request = basic_create_table_request(&table_name).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    provider
        .create_table(&request)
        .await
        .expect("create stream-enabled table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("name".to_string(), AttributeValue::S("Ada".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert row");
    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("name".to_string(), AttributeValue::S("Grace".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("update row");
    provider
        .delete_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("user-1".to_string()))]).into(),
            None,
            None,
            None,
        )
        .await
        .expect("delete row");

    let system_page = provider
        .read_forward(StreamName::system_table_stream(), None, 10)
        .await
        .expect("read system stream");
    assert_eq!(system_page.items.len(), 3);

    let table_page = provider
        .read_forward(StreamName::table_stream(&table_name), None, 10)
        .await
        .expect("read table stream");
    assert_eq!(table_page.items.len(), 3);

    for (index, item) in system_page.items.iter().enumerate() {
        let pointer: StoredStreamPointer =
            storage_types::storage_serde::from_bytes(&item.data).expect("decode stream pointer");
        let expected_version = storage_types::ItemStreamVersion::new((index + 1) as u64);
        assert_eq!(pointer.target_item_stream_version(), expected_version);
        assert_ne!(
            item.id,
            StreamItemId::from(expected_version),
            "pointer stream row ids must not double as item stream versions"
        );
    }

    let (records, _last_evaluated_key) = provider
        .get_stream_records_from_pointer_stream(
            StreamName::table_stream(&table_name),
            &request.key_schema,
            None,
            Some(10),
        )
        .await
        .expect("read dynamodb stream records");
    let sequence_numbers = records
        .iter()
        .map(|record| record.sequence_number.clone())
        .collect::<Vec<_>>();
    let cursors = records
        .iter()
        .map(|record| record.cursor.clone().expect("stream cursor"))
        .collect::<Vec<_>>();
    assert_eq!(sequence_numbers, cursors);
    assert!(
        sequence_numbers
            .windows(2)
            .all(|window| window[0] < window[1])
    );
    assert!(
        records[2]
            .old_image
            .as_ref()
            .is_some_and(|old| old.contains_key("name")),
        "delete stream record should include the old image"
    );
}

#[tokio::test]
async fn turso_custom_stream_duration_trim_backend_deletes_table_stream_page() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_custom_duration_trim");
    let mut request = basic_create_table_request(&table_name).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    request.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    provider.create_table(&request).await.expect("create table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("name".to_string(), AttributeValue::S("Ada".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert row");

    let conn = provider.connect().await.expect("connect");
    let _ = provider
        .execute(
            &conn,
            "UPDATE sys_stream_items SET created_at = ?1",
            vec![TursoValue::Integer(0)],
        )
        .await
        .expect("age stream rows");
    let _ = provider
        .execute(
            &conn,
            "UPDATE sys_stream_pointer_index SET created_at = ?1",
            vec![TursoValue::Integer(0)],
        )
        .await
        .expect("age pointer index rows");
    let scope = StreamTrimScope::table("test-table-scope", table_name.clone());
    let marker = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(0), scope.clone(), 2);
    provider
        .write_stream_trim_state(
            &conn,
            StreamTrimStateWrite {
                state: StreamTrimState {
                    scope,
                    policy_version: 2,
                    retention: StreamRetentionDuration::FiniteHours(1),
                    effective_retention: StreamRetentionDuration::FiniteHours(1),
                    next_due_at: Some(TimestampMillis::from_timestamp(0)),
                    oldest_retained_version: None,
                    oldest_retained_timestamp: None,
                    latest_version: None,
                    latest_timestamp: None,
                    updated_at: TimestampMillis::from_timestamp(0),
                },
                next_marker: Some(marker),
            },
        )
        .await
        .expect("write due trim state");
    drop(conn);

    provider
        .run_job(TTL_SWEEP_JOB)
        .await
        .expect("run custom stream trim job");

    let table_page = provider
        .read_forward(StreamName::table_stream(&table_name), None, 10)
        .await
        .expect("read table stream");
    assert!(table_page.items.is_empty());

    let conn = provider.connect().await.expect("connect");
    let pointer_rows = provider
        .query_rows(
            &conn,
            "SELECT table_stream_item_id FROM sys_stream_pointer_index",
            Vec::new(),
        )
        .await
        .expect("read pointer index");
    assert!(pointer_rows.is_empty());
}

#[tokio::test]
async fn turso_list_tables_respects_exclusive_start_key() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_a = TableName::new("turso_list_tables_a");
    let table_b = TableName::new("turso_list_tables_b");
    let table_c = TableName::new("turso_list_tables_c");

    provider
        .create_table(&basic_create_table_request(&table_a))
        .await
        .expect("create table a");
    provider
        .create_table(&basic_create_table_request(&table_b))
        .await
        .expect("create table b");
    provider
        .create_table(&basic_create_table_request(&table_c))
        .await
        .expect("create table c");

    let after_a = provider
        .list_tables(10, Some(table_a.clone()))
        .await
        .expect("list tables after a");
    let names_after_a: Vec<_> = after_a
        .into_iter()
        .map(|table| table.table_name.to_string())
        .filter(|name| name.starts_with("turso_list_tables_"))
        .collect();

    assert_eq!(
        names_after_a,
        vec![table_b.to_string(), table_c.to_string()],
        "exclusive_start_key should skip the starting table and preserve ordered pagination"
    );
}

#[tokio::test]
async fn turso_batch_write_item_rolls_back_on_validation_error() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_batch_atomic_table");
    provider
        .create_table(&basic_create_table_request(&table_name))
        .await
        .expect("create table");

    let result = provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table_name.clone(),
                    vec![
                        WriteRequest {
                            put_request: Some(storage_types::PutRequest {
                                item: HashMap::from([
                                    ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                                    ("name".to_string(), AttributeValue::S("Ada".to_string())),
                                ]),
                                indexers: None,
                                aux_item_stream_ttl_hours: None,
                            }),
                            delete_request: None,
                        },
                        WriteRequest {
                            put_request: Some(storage_types::PutRequest {
                                item: HashMap::from([(
                                    "pk".to_string(),
                                    AttributeValue::S("invalid".to_string()),
                                )]),
                                indexers: None,
                                aux_item_stream_ttl_hours: None,
                            }),
                            delete_request: Some(DeleteRequest {
                                key: HashMap::from([(
                                    "pk".to_string(),
                                    AttributeValue::S("invalid".to_string()),
                                )])
                                .into(),
                                aux_item_stream_ttl_hours: None,
                            }),
                        },
                    ],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            false,
        )
        .await;
    assert!(
        matches!(result, Err(err) if matches!(err.as_ref(), StorageEnum::Validation { .. })),
        "invalid batch request should fail validation"
    );

    let fetched = provider
        .get_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("user-1".to_string()))]).into(),
            true,
        )
        .await
        .expect("get item after failed batch");
    assert!(fetched.is_none());
}

#[tokio::test]
async fn turso_gsi_updates_are_delayed_by_default() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_delayed_gsi_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table_name.clone(),
                    vec![WriteRequest {
                        put_request: Some(PutRequest {
                            item: HashMap::from([
                                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                                ("gpk".to_string(), AttributeValue::S("group-1".to_string())),
                            ]),
                            indexers: None,
                            aux_item_stream_ttl_hours: None,
                        }),
                        delete_request: None,
                    }],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            false,
        )
        .await
        .expect("batch write item");

    let request = QueryTableRequest {
        table_name: table_name.clone(),
        index_name: Some(IndexName::new("gsi1")),
        key_condition_expression: "gpk = :gpk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":gpk".to_string(),
            AttributeValue::S("group-1".to_string()),
        )])),
        projection_expression: None,
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };

    let (before, _) = provider
        .query_table(&request)
        .await
        .expect("query delayed gsi before maintenance");
    assert!(before.is_empty());

    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("run gsi update job");

    let (after, _) = provider
        .query_table(&request)
        .await
        .expect("query delayed gsi after maintenance");
    assert_eq!(after.len(), 1);
}

#[tokio::test]
async fn turso_gsi_update_job_drains_write_burst() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_gsi_burst_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    for batch_start in (0..96).step_by(24) {
        let writes = (batch_start..batch_start + 24)
            .map(|item_id| WriteRequest {
                put_request: Some(PutRequest {
                    item: HashMap::from([
                        (
                            "pk".to_string(),
                            AttributeValue::S(format!("user-{item_id}")),
                        ),
                        (
                            "gpk".to_string(),
                            AttributeValue::S("group-burst".to_string()),
                        ),
                    ]),
                    indexers: None,
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            })
            .collect();

        provider
            .batch_write_item(
                BatchWriteItemRequest {
                    request_items: HashMap::from([(table_name.clone(), writes)]),
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                },
                false,
            )
            .await
            .expect("write burst batch");
    }

    let before = query_gsi_group(&provider, table_name.clone(), "group-burst").await;
    assert!(
        before.is_empty(),
        "default mode should delay burst GSI rows"
    );

    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("run gsi update job");

    let after = query_gsi_group(&provider, table_name, "group-burst").await;
    assert_eq!(after.len(), 96);
    assert!(
        !provider.gsi_propagation_governor.lag_above_target(),
        "gsi lag should reset after draining the stream"
    );
}

#[tokio::test]
async fn turso_gsi_update_job_removes_stale_keys_after_repeated_overwrites() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_gsi_repeated_overwrite_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    put_turso_gsi_item(&provider, table_name.clone(), "user-1", Some("group-1")).await;
    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("publish initial gsi row");
    assert_eq!(
        query_gsi_group(&provider, table_name.clone(), "group-1")
            .await
            .len(),
        1
    );

    put_turso_gsi_item(&provider, table_name.clone(), "user-1", Some("group-2")).await;
    put_turso_gsi_item(&provider, table_name.clone(), "user-1", Some("group-3")).await;

    assert_eq!(
        query_gsi_group(&provider, table_name.clone(), "group-1")
            .await
            .len(),
        1,
        "default mode should leave the old GSI key visible before catch-up"
    );
    assert!(
        query_gsi_group(&provider, table_name.clone(), "group-3")
            .await
            .is_empty(),
        "default mode should delay the newest GSI key before catch-up"
    );

    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("catch up repeated gsi moves");

    assert!(
        query_gsi_group(&provider, table_name.clone(), "group-1")
            .await
            .is_empty(),
        "catch-up must remove the original stale GSI key"
    );
    assert!(
        query_gsi_group(&provider, table_name.clone(), "group-2")
            .await
            .is_empty(),
        "catch-up must remove the intermediate stale GSI key"
    );
    assert_eq!(
        query_gsi_group(&provider, table_name, "group-3")
            .await
            .len(),
        1,
        "catch-up must publish only the latest GSI key"
    );
}

#[tokio::test]
async fn turso_gsi_update_job_removes_index_entry_when_gsi_key_is_removed() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_gsi_key_removed_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    put_turso_gsi_item(&provider, table_name.clone(), "user-1", Some("group-1")).await;
    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("publish initial gsi row");
    assert_eq!(
        query_gsi_group(&provider, table_name.clone(), "group-1")
            .await
            .len(),
        1
    );

    put_turso_gsi_item(&provider, table_name.clone(), "user-1", None).await;
    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("catch up gsi key removal");

    assert!(
        query_gsi_group(&provider, table_name, "group-1")
            .await
            .is_empty(),
        "catch-up must remove the GSI row when the item no longer has the GSI key"
    );
}

#[tokio::test]
async fn turso_gsi_update_job_removes_index_entry_on_delete() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_gsi_delete_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    put_turso_gsi_item(&provider, table_name.clone(), "user-1", Some("group-1")).await;
    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("publish initial gsi row");
    assert_eq!(
        query_gsi_group(&provider, table_name.clone(), "group-1")
            .await
            .len(),
        1
    );

    provider
        .delete_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("user-1".to_string()))]).into(),
            None,
            None,
            None,
        )
        .await
        .expect("delete item");
    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("catch up delete");

    assert!(
        query_gsi_group(&provider, table_name, "group-1")
            .await
            .is_empty(),
        "catch-up must remove a stale GSI row for a deleted item"
    );
}

#[tokio::test]
#[ignore = "manual full GSI update job profile"]
async fn turso_gsi_update_job_realistic_batch_profile() {
    let provider = create_test_provider().await;
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_gsi_update_profile");
    provider
        .create_table(&realistic_gsi_profile_request(&table_name))
        .await
        .expect("create profile table");

    for batch in realistic_gsi_profile_batches(&table_name) {
        provider
            .batch_write_item(batch, false)
            .await
            .expect("write profile batch");
    }

    storage_common::provider_perf::reset_provider("turso");
    let started = std::time::Instant::now();
    provider
        .run_job(storage_common::GSI_UPDATE_JOB)
        .await
        .expect("run gsi update profile");
    let elapsed = started.elapsed();
    println!(
        "gsi_update_job_profile provider=turso items={} gsis={} elapsed_ms={:.3}",
        REALISTIC_GSI_PROFILE_ITEMS,
        REALISTIC_GSI_PROFILE_INDEXES,
        elapsed.as_secs_f64() * 1000.0
    );
    print_gsi_profile_counters("turso", "job");
    storage_common::provider_perf::reset_provider("turso");
}

#[tokio::test]
async fn turso_immediate_gsi_consistency_updates_indexes_inline() {
    let provider = create_test_provider().await;
    let provider = provider.with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_immediate_gsi_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("gpk".to_string(), AttributeValue::S("group-1".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put item");

    let (items, _) = provider
        .query_table(&QueryTableRequest {
            table_name,
            index_name: Some(IndexName::new("gsi1")),
            key_condition_expression: "gpk = :gpk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":gpk".to_string(),
                AttributeValue::S("group-1".to_string()),
            )])),
            projection_expression: None,
            limit: None,
            exclusive_start_key: None,
            scan_index_forward: None,
            consistent_read: false,
        })
        .await
        .expect("query immediate gsi");
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn turso_immediate_gsi_overwrite_removes_stale_index_key() {
    let provider = create_test_provider().await;
    let provider = provider.with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage tables");

    let table_name = TableName::new("turso_immediate_gsi_update_table");
    provider
        .create_table(&gsi_create_table_request(&table_name))
        .await
        .expect("create gsi table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("gpk".to_string(), AttributeValue::S("group-1".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put item");
    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user-1".to_string())),
                ("gpk".to_string(), AttributeValue::S("group-2".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("overwrite item");

    let old_group = query_gsi_group(&provider, table_name.clone(), "group-1").await;
    let new_group = query_gsi_group(&provider, table_name, "group-2").await;

    assert!(old_group.is_empty());
    assert_eq!(new_group.len(), 1);
}

async fn query_gsi_group(
    provider: &TursoStorageProvider,
    table_name: TableName,
    group: &str,
) -> Vec<storage_types::WireItem> {
    provider
        .query_table(&QueryTableRequest {
            table_name,
            index_name: Some(IndexName::new("gsi1")),
            key_condition_expression: "gpk = :gpk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":gpk".to_string(),
                AttributeValue::S(group.to_string()),
            )])),
            projection_expression: None,
            limit: None,
            exclusive_start_key: None,
            scan_index_forward: None,
            consistent_read: false,
        })
        .await
        .expect("query immediate gsi")
        .0
}

async fn put_turso_gsi_item(
    provider: &TursoStorageProvider,
    table_name: TableName,
    pk: &str,
    gpk: Option<&str>,
) {
    let mut item = HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))]);
    if let Some(gpk) = gpk {
        item.insert("gpk".to_string(), AttributeValue::S(gpk.to_string()));
    }

    provider
        .put_item(table_name, item, None, None, None, None)
        .await
        .expect("put gsi test item");
}
