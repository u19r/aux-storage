use std::collections::HashMap;

use queue_provider::{Queue, QueueMessage, QueueProvider, ReceiptHandle};
use storage_common::GSI_UPDATE_JOB;
use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchWriteItemRequest, BillingMode,
    CreateTableRequest, DeleteRequest, DurationSeconds, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, KeysAndAttributes, PutRequest, QueryTableRequest,
    ReplicationEventMetadata, ReplicationHybridLogicalClock, ReplicationMutation,
    ReplicationWriteSource, ScanTableRequest, StorageEnum, StreamName, StreamSpecification,
    StreamViewType, TableName, TableStatus, TimestampMillis, UpdateItemRequest, UserStreamName,
    WriteRequest,
};
use stream_provider::{CursorName, CursorPosition, StoredStreamPointer, StreamProvider};

use crate::{
    PostgresStorageProvider,
    gsi_profile_support_tests::{
        REALISTIC_GSI_PROFILE_INDEXES, REALISTIC_GSI_PROFILE_ITEMS, print_gsi_profile_counters,
        realistic_gsi_profile_batches, realistic_gsi_profile_request,
    },
};

fn postgres_test_dsn() -> Option<String> {
    std::env::var("TEST_POSTGRES_DSN")
        .ok()
        .or_else(|| std::env::var("CUCUMBER_POSTGRES_DSN").ok())
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

fn gsi_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![storage_types::CreateGlobalSecondaryIndex {
        index_name: storage_types::IndexName::new("TestGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: storage_types::Projection {
            projection_type: Some(storage_types::ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]))
}

fn gsi_query_request(table_name: &TableName) -> QueryTableRequest {
    gsi_query_request_for_partition(table_name, "grp")
}

fn gsi_query_request_for_partition(table_name: &TableName, partition: &str) -> QueryTableRequest {
    QueryTableRequest {
        table_name: table_name.clone(),
        index_name: Some(storage_types::IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S(partition.to_string()),
        )])),
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    }
}

fn sample_replication_metadata(
    region_name: &str,
    sequence_suffix: u64,
) -> ReplicationEventMetadata {
    let mut bytes = [0_u8; 12];
    bytes[4..].copy_from_slice(&sequence_suffix.to_be_bytes());
    let physical_ms = 1_700_000_000_000_i64 + sequence_suffix as i64;

    ReplicationEventMetadata {
        origin_region: region_name.to_string(),
        origin_sequence: storage_types::StreamItemId::from(bytes),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: TimestampMillis::from_timestamp(physical_ms),
            logical: sequence_suffix as u32,
        },
        origin_commit_ts: TimestampMillis::from_timestamp(physical_ms),
        table_replica_epoch: 4,
        write_source: ReplicationWriteSource::Replicated,
    }
}

#[tokio::test]
async fn postgres_table_lifecycle_works() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!("pg_smoke_{}", uuid::Uuid::now_v7()));
    let request = basic_create_table_request(&table_name);

    provider.create_table(&request).await.expect("create table");
    assert!(
        provider
            .table_exists(&table_name)
            .await
            .expect("table exists check")
    );

    let put_item = std::collections::HashMap::from([
        (
            "pk".to_string(),
            storage_types::AttributeValue::S("u-1".to_string()),
        ),
        (
            "name".to_string(),
            storage_types::AttributeValue::S("Ada".to_string()),
        ),
    ]);
    provider
        .put_item(table_name.clone(), put_item, None, None, None, None)
        .await
        .expect("put item");

    let got = provider
        .get_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("u-1".to_string()))]).into(),
            true,
        )
        .await
        .expect("get item")
        .expect("item present")
        .to_attribute_map()
        .expect("decode wire item");
    assert_eq!(got.get("name"), Some(&AttributeValue::S("Ada".into())));

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-2".to_string())),
                ("name".to_string(), AttributeValue::S("Bob".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put u-2");
    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-3".to_string())),
                ("name".to_string(), AttributeValue::S("Cid".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put u-3");

    let _ = provider
        .update_item(
            UpdateItemRequest::builder()
                .table_name(table_name.clone())
                .key(HashMap::from([(
                    "pk".to_string(),
                    AttributeValue::S("u-2".to_string()),
                )]))
                .update_expression("SET name = :name")
                .expression_attribute_values(Some(HashMap::from([(
                    ":name".to_string(),
                    AttributeValue::S("Bobby".to_string()),
                )])))
                .build(),
        )
        .await
        .expect("update u-2");

    let (queried, _) = provider
        .query_table(&QueryTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("u-2".to_string()),
            )])),
            limit: Some(10),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: true,
        })
        .await
        .expect("query by key");
    assert_eq!(queried.len(), 1);
    let queried_item = queried[0].to_attribute_map().expect("decode query item");
    assert_eq!(
        queried_item.get("name"),
        Some(&AttributeValue::S("Bobby".to_string()))
    );

    let batch_get_response = provider
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![
                        storage_types::KeyAttributes::from([(
                            "pk".to_string(),
                            AttributeValue::S("u-1".to_string()),
                        )]),
                        storage_types::KeyAttributes::from([(
                            "pk".to_string(),
                            AttributeValue::S("u-2".to_string()),
                        )]),
                    ]
                    .into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .expect("batch get");
    assert_eq!(
        batch_get_response
            .responses
            .as_ref()
            .and_then(|responses| responses.get(&table_name))
            .map_or(0, std::vec::Vec::len),
        2
    );

    let batch_write_response = provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table_name.clone(),
                    vec![
                        WriteRequest {
                            put_request: Some(PutRequest {
                                item: HashMap::from([
                                    ("pk".to_string(), AttributeValue::S("u-4".to_string())),
                                    ("name".to_string(), AttributeValue::S("Dee".to_string())),
                                ]),
                                aux_item_stream_ttl_hours: None,
                            }),
                            delete_request: None,
                        },
                        WriteRequest {
                            put_request: None,
                            delete_request: Some(DeleteRequest {
                                key: storage_types::KeyAttributes::from([(
                                    "pk".to_string(),
                                    AttributeValue::S("u-3".to_string()),
                                )]),
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
        .await
        .expect("batch write");
    assert!(batch_write_response.unprocessed_items.is_none());

    let got_u4 = provider
        .get_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("u-4".to_string()))]).into(),
            true,
        )
        .await
        .expect("get u-4");
    assert!(got_u4.is_some());
    let got_u3 = provider
        .get_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("u-3".to_string()))]).into(),
            true,
        )
        .await
        .expect("get u-3");
    assert!(got_u3.is_none());

    let (scan_page_1, lek_1) = provider
        .scan_table(&ScanTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            limit: Some(2),
            exclusive_start_key: None,
            consistent_read: true,
        })
        .await
        .expect("scan page 1");
    assert_eq!(scan_page_1.len(), 2);
    assert!(lek_1.is_some());

    let (scan_page_2, lek_2) = provider
        .scan_table(&ScanTableRequest {
            table_name: table_name.clone(),
            index_name: None,
            limit: Some(2),
            exclusive_start_key: lek_1,
            consistent_read: true,
        })
        .await
        .expect("scan page 2");
    assert_eq!(scan_page_2.len(), 1);
    assert!(lek_2.is_none());

    let deleted = provider
        .delete_item(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("u-1".to_string()))]).into(),
            None,
            None,
            None,
        )
        .await
        .expect("delete item")
        .expect("deleted item");
    assert_eq!(deleted.get("name"), Some(&AttributeValue::S("Ada".into())));

    let info = provider
        .get_table_info(&table_name)
        .await
        .expect("get table info");
    assert_eq!(info.table_name, table_name);
    assert_eq!(info.table_status, TableStatus::Active);

    let tables = provider.list_tables(100, None).await.expect("list tables");
    assert!(tables.into_iter().any(|t| t.table_name == table_name));

    provider
        .delete_table(&table_name)
        .await
        .expect("delete table");
    assert!(
        !provider
            .table_exists(&table_name)
            .await
            .expect("table not exists")
    );
}

#[tokio::test]
async fn postgres_gsi_visibility_is_delayed_by_default() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!("pg_gsi_delayed_{}", uuid::Uuid::now_v7()));
    provider
        .create_table(&gsi_request(&table_name))
        .await
        .expect("create gsi table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-1".to_string())),
                ("sk".to_string(), AttributeValue::S("item-1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put item");

    let (before, _) = provider
        .query_table(&gsi_query_request(&table_name))
        .await
        .expect("query before gsi job");
    assert!(
        before.is_empty(),
        "default mode should delay GSI visibility"
    );

    provider
        .run_job(GSI_UPDATE_JOB)
        .await
        .expect("run gsi-update");

    let (after, _) = provider
        .query_table(&gsi_query_request(&table_name))
        .await
        .expect("query after gsi job");
    assert_eq!(after.len(), 1, "gsi-update should publish the pending row");

    provider
        .delete_table(&table_name)
        .await
        .expect("delete gsi table");
}

#[tokio::test]
async fn postgres_immediate_gsi_consistency_updates_indexes_inline() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider")
        .with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!("pg_gsi_immediate_{}", uuid::Uuid::now_v7()));
    provider
        .create_table(&gsi_request(&table_name))
        .await
        .expect("create gsi table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-1".to_string())),
                ("sk".to_string(), AttributeValue::S("item-1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put item");

    let (before_job, _) = provider
        .query_table(&gsi_query_request(&table_name))
        .await
        .expect("query before gsi job");
    assert_eq!(
        before_job.len(),
        1,
        "immediate mode should publish the GSI row in the main write transaction"
    );

    provider
        .run_job(GSI_UPDATE_JOB)
        .await
        .expect("run gsi-update");

    let (after_job, _) = provider
        .query_table(&gsi_query_request(&table_name))
        .await
        .expect("query after gsi job");
    assert_eq!(
        after_job.len(),
        1,
        "no-op job should not duplicate index rows"
    );

    provider
        .delete_table(&table_name)
        .await
        .expect("delete gsi table");
}

#[tokio::test]
async fn postgres_immediate_gsi_consistency_moves_index_entries_inline() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider")
        .with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!("pg_gsi_move_{}", uuid::Uuid::now_v7()));
    provider
        .create_table(&gsi_request(&table_name))
        .await
        .expect("create gsi table");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-1".to_string())),
                ("sk".to_string(), AttributeValue::S("item-1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("001".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put initial item");

    provider
        .put_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-1".to_string())),
                ("sk".to_string(), AttributeValue::S("item-1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp-2".to_string())),
                ("gsi_sk".to_string(), AttributeValue::S("002".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("move gsi row");

    let (old_items, _) = provider
        .query_table(&gsi_query_request_for_partition(&table_name, "grp"))
        .await
        .expect("query old partition");
    assert!(
        old_items.is_empty(),
        "immediate mode should remove the old GSI row in the same write transaction"
    );

    let (new_items, _) = provider
        .query_table(&gsi_query_request_for_partition(&table_name, "grp-2"))
        .await
        .expect("query new partition");
    assert_eq!(
        new_items.len(),
        1,
        "immediate mode should insert the new GSI row in the same write transaction"
    );

    provider
        .delete_table(&table_name)
        .await
        .expect("delete gsi table");
}

#[tokio::test]
#[ignore = "manual full GSI update job profile; requires live Postgres"]
async fn postgres_gsi_update_job_realistic_batch_profile() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let table_name = TableName::new(&format!("pg_gsi_profile_{}", uuid::Uuid::now_v7()));
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

    storage_common::provider_perf::reset_provider("postgres");
    let started = std::time::Instant::now();
    provider
        .run_job(GSI_UPDATE_JOB)
        .await
        .expect("run gsi update profile");
    let elapsed = started.elapsed();
    println!(
        "gsi_update_job_profile provider=postgres items={} gsis={} elapsed_ms={:.3}",
        REALISTIC_GSI_PROFILE_ITEMS,
        REALISTIC_GSI_PROFILE_INDEXES,
        elapsed.as_secs_f64() * 1000.0
    );
    print_gsi_profile_counters("postgres", "job");
    storage_common::provider_perf::reset_provider("postgres");

    provider
        .delete_table(&table_name)
        .await
        .expect("delete profile table");
}

#[tokio::test]
async fn postgres_stream_enabled_put_many_items_records_events() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table_name = TableName::new(&format!("pg_stream_many_{}", uuid::Uuid::now_v7()));
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

    for i in 1..=120u32 {
        provider
            .put_item(
                table_name.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S(format!("large-{i}"))),
                    (
                        "data".to_string(),
                        AttributeValue::S(format!("Item data {i}")),
                    ),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap_or_else(|err| {
                let internal = match err.as_ref() {
                    StorageEnum::InternalServerError { message } => message.clone(),
                    other => other.to_string(),
                };
                panic!("put item {i} failed: {err}; internal={internal}; debug={err:?}");
            });
    }
}

#[tokio::test]
async fn postgres_list_tables_respects_exclusive_start_key() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");

    let prefix = format!("pg_list_tables_{}", uuid::Uuid::now_v7());
    let table_a = TableName::new(&format!("{prefix}_a"));
    let table_b = TableName::new(&format!("{prefix}_b"));
    let table_c = TableName::new(&format!("{prefix}_c"));

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
        .filter(|name| name.starts_with(&prefix))
        .collect();

    assert_eq!(
        names_after_a,
        vec![table_b.to_string(), table_c.to_string()],
        "exclusive_start_key should skip the starting table and preserve ordered pagination"
    );

    provider
        .delete_table(&table_a)
        .await
        .expect("delete table a");
    provider
        .delete_table(&table_b)
        .await
        .expect("delete table b");
    provider
        .delete_table(&table_c)
        .await
        .expect("delete table c");
}

#[tokio::test]
async fn postgres_apply_replication_mutation_preserves_replication_metadata() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new(&dsn, 8)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table_name = TableName::new(&format!("pg_replication_{}", uuid::Uuid::now_v7()));
    let request = basic_create_table_request(&table_name).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    provider.create_table(&request).await.expect("create table");

    let metadata = sample_replication_metadata("eu-west-2", 13);
    provider
        .apply_replication_mutation(ReplicationMutation {
            table_name: table_name.clone(),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("u-99".to_string()))]).into(),
            new_image: Some(HashMap::from([
                ("pk".to_string(), AttributeValue::S("u-99".to_string())),
                ("name".to_string(), AttributeValue::S("Remote".to_string())),
            ])),
            old_image: None,
            metadata: metadata.clone(),
        })
        .await
        .expect("apply replication mutation");

    let page = provider
        .read_forward(StreamName::table_stream(&table_name), None, 10)
        .await
        .expect("read table stream");
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&page.items[0].data).expect("decode pointer");
    assert_eq!(stored_pointer.replication_metadata(), Some(&metadata));
    assert_eq!(
        stored_pointer.target_item_stream_version(),
        storage_types::ItemStreamVersion::new(1)
    );
    assert_ne!(
        page.items[0].id,
        storage_types::StreamItemId::from(stored_pointer.target_item_stream_version())
    );
}

#[tokio::test]
async fn postgres_concurrent_same_key_writes_allocate_contiguous_item_versions() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = std::sync::Arc::new(
        PostgresStorageProvider::new(&dsn, 16)
            .await
            .expect("postgres provider"),
    );
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table_name = TableName::new(&format!("pg_same_key_versions_{}", uuid::Uuid::now_v7()));
    let request = basic_create_table_request(&table_name).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    provider.create_table(&request).await.expect("create table");

    let write_count = 16_u64;
    let mut tasks = Vec::new();
    for value in 0..write_count {
        let provider = std::sync::Arc::clone(&provider);
        let table_name = table_name.clone();
        tasks.push(tokio::spawn(async move {
            provider
                .put_item(
                    table_name,
                    HashMap::from([
                        ("pk".to_string(), AttributeValue::S("same-key".to_string())),
                        ("value".to_string(), AttributeValue::N(value.to_string())),
                    ]),
                    None,
                    None,
                    None,
                    None,
                )
                .await
        }));
    }

    for task in tasks {
        task.await
            .expect("join concurrent put")
            .expect("concurrent put succeeds");
    }

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("same-key".to_string()),
        None,
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");
    let page = provider
        .read_forward(item_stream, None, 100)
        .await
        .expect("read item stream");

    let versions = page
        .items
        .iter()
        .map(|item| storage_types::ItemStreamVersion::from(item.id).get())
        .collect::<Vec<_>>();
    assert_eq!(versions, (1..=write_count).collect::<Vec<_>>());
}

#[tokio::test]
async fn postgres_concurrent_different_key_writes_use_per_item_versions() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = std::sync::Arc::new(
        PostgresStorageProvider::new(&dsn, 16)
            .await
            .expect("postgres provider"),
    );
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let table_name = TableName::new(&format!("pg_diff_key_versions_{}", uuid::Uuid::now_v7()));
    let request = basic_create_table_request(&table_name).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    provider.create_table(&request).await.expect("create table");

    let write_count = 16_u64;
    let mut tasks = Vec::new();
    for value in 0..write_count {
        let provider = std::sync::Arc::clone(&provider);
        let table_name = table_name.clone();
        tasks.push(tokio::spawn(async move {
            provider
                .put_item(
                    table_name,
                    HashMap::from([
                        ("pk".to_string(), AttributeValue::S(format!("key-{value}"))),
                        ("value".to_string(), AttributeValue::N(value.to_string())),
                    ]),
                    None,
                    None,
                    None,
                    None,
                )
                .await
        }));
    }

    for task in tasks {
        task.await
            .expect("join concurrent put")
            .expect("concurrent put succeeds");
    }

    for value in 0..write_count {
        let item_key = ItemKey::table_key(
            table_name.clone(),
            AttributeValue::S(format!("key-{value}")),
            None,
        );
        let item_stream =
            StreamName::table_item_stream(&table_name, &item_key).expect("item stream");
        let page = provider
            .read_forward(item_stream, None, 10)
            .await
            .expect("read item stream");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            storage_types::ItemStreamVersion::from(page.items[0].id).get(),
            1
        );
    }
}

#[tokio::test]
async fn postgres_stream_lifecycle_works() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new_with_tls(&dsn, 8, 2, true)
        .await
        .expect("postgres provider");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");

    let user_stream = UserStreamName::new(&format!("pg_stream_{}", uuid::Uuid::now_v7()));
    let internal_stream = provider
        .create_stream(user_stream.clone(), None, Default::default())
        .await
        .expect("create stream");
    let first_item = provider
        .append_item(internal_stream.clone(), b"one", None)
        .await
        .expect("append one");
    provider
        .append_item(internal_stream.clone(), b"two", None)
        .await
        .expect("append two");

    let read = provider
        .read_forward(internal_stream.clone(), None, 10)
        .await
        .expect("read forward");
    assert_eq!(read.items.len(), 2);

    let cursor_name = CursorName::new("pg-stream-lifecycle");
    provider
        .create_cursor(
            internal_stream.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .expect("create cursor");
    let from_cursor = provider
        .read_from_cursor(internal_stream.clone(), cursor_name.clone(), 10)
        .await
        .expect("read from cursor");
    assert!(!from_cursor.items.is_empty());

    provider
        .advance_cursor(internal_stream.clone(), cursor_name.clone(), first_item)
        .await
        .expect("advance cursor");
    provider
        .delete_cursor(internal_stream.clone(), cursor_name)
        .await
        .expect("delete cursor");
    provider
        .delete_stream(user_stream)
        .await
        .expect("delete stream");
}

#[tokio::test]
async fn postgres_queue_lifecycle_works() {
    let Some(dsn) = postgres_test_dsn() else {
        return;
    };
    let provider = PostgresStorageProvider::new_with_tls(&dsn, 8, 2, true)
        .await
        .expect("postgres provider");
    provider.initialize().await.expect("initialize queue");

    let queue_name = format!("pg_queue_{}", uuid::Uuid::now_v7().simple());
    provider
        .create_queue(Queue {
            queue_name: queue_name.clone(),
            queue_url: queue_name.clone(),
            attributes: HashMap::new(),
            created_at: TimestampMillis::now(),
        })
        .await
        .expect("create queue");
    provider
        .send_message(QueueMessage {
            queue_url: queue_name.clone(),
            body: "hello".to_string(),
            created_at: TimestampMillis::now(),
            ..QueueMessage::default()
        })
        .await
        .expect("send message");

    let messages = provider
        .receive_messages(
            &queue_name,
            1,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive messages");
    assert_eq!(messages.len(), 1);
    let receipt_handle = ReceiptHandle::from(messages[0].receipt_handle.as_str());
    provider
        .change_message_visibility(
            &queue_name,
            receipt_handle.clone(),
            DurationSeconds::from(5),
        )
        .await
        .expect("change visibility");
    provider
        .update_message_snapshot_checkpoint(
            &queue_name,
            receipt_handle.clone(),
            "{\"ok\":true}".to_string(),
        )
        .await
        .expect("update checkpoint");
    provider
        .delete_message(&queue_name, receipt_handle)
        .await
        .expect("delete message");
}
