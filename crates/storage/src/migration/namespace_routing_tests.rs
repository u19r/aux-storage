#![cfg(feature = "sqlite")]

use std::{collections::HashMap, time::Duration};

use storage_common::GSI_UPDATE_JOB;
use storage_provider::{StorageBackend, StorageConnectionConfig, StorageConnectionRegistry};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, KeysAndAttributes, StorageEnum, StorageError, TableName,
    TableNamespace, TransactConditionCheckRequest, TransactEncodeItem, TransactUpdateRequest,
    TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest, context::WrappedError as _,
};

use crate::{
    CutoverWatcher, DatabaseManager, DeleteItemInput, NamespaceRequestRewriter, PutItemInput,
    QueryTableInput, ScanTableInput, Tables, UpdateItemInput, is_retryable_pause_error,
    namespace_routing::reject_direct_shared_table_access, namespace_source_table,
};

fn sqlite_connection(path: &str) -> StorageConnectionConfig {
    StorageConnectionConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

async fn new_routed_db() -> DatabaseManager {
    DatabaseManager::new_with_connection_registry(StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([("default".to_string(), sqlite_connection(":memory:"))]),
    })
    .await
    .expect("database manager")
}

async fn new_multi_connection_routed_db() -> DatabaseManager {
    DatabaseManager::new_with_connection_registry(StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([
            ("default".to_string(), sqlite_connection(":memory:")),
            ("tenant-store".to_string(), sqlite_connection(":memory:")),
        ]),
    })
    .await
    .expect("multi-connection database manager")
}

async fn create_simple_shared_table(db: &DatabaseManager, loc: u16) {
    let table_name = Tables::shared_namespace(loc);
    create_simple_table_on_connection(db, "default", table_name).await;
}

async fn create_simple_table_on_connection(
    db: &DatabaseManager,
    connection_id: &str,
    table_name: TableName,
) {
    let request = CreateTableRequest::new(
        table_name,
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
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table_on_connection(connection_id, &request)
        .await
        .expect("create routed table");
}

async fn put_location_dictionary(db: &DatabaseManager, mappings: &[(u16, &str)]) {
    for (code, connection_id) in mappings {
        let item = HashMap::from([
            (
                "pk".to_string(),
                AttributeValue::S("SYS#ROUTING".to_string()),
            ),
            (
                "sk".to_string(),
                AttributeValue::S(format!("LOC#{code:05}")),
            ),
            ("loc".to_string(), AttributeValue::N(code.to_string())),
            (
                "connection_id".to_string(),
                AttributeValue::S((*connection_id).to_string()),
            ),
            (
                "backend_kind".to_string(),
                AttributeValue::S("sqlite".to_string()),
            ),
            ("metadata".to_string(), AttributeValue::M(HashMap::new())),
            ("updated_at".to_string(), AttributeValue::N("0".to_string())),
        ]);
        db.put_item(
            PutItemInput::builder()
                .table_name(Tables::sys_namespaces())
                .item(item)
                .build(),
        )
        .await
        .expect("put location dictionary entry");
    }
}

async fn put_namespace_route_metadata(
    db: &DatabaseManager,
    namespace: &TableNamespace,
    st: u8,
    loc: u16,
) {
    let mut item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("NS#{}", namespace.as_str())),
        ),
        ("sk".to_string(), AttributeValue::S("META".to_string())),
        (
            "id".to_string(),
            AttributeValue::S(namespace.as_str().to_string()),
        ),
        ("st".to_string(), AttributeValue::N(st.to_string())),
        ("loc".to_string(), AttributeValue::N(loc.to_string())),
        (
            "migration_mode".to_string(),
            AttributeValue::M(HashMap::from([(
                "mode".to_string(),
                AttributeValue::S("single".to_string()),
            )])),
        ),
    ]);
    if st == 1 {
        item.insert("gsi2pk".to_string(), AttributeValue::S("ST#1".to_string()));
        item.insert(
            "gsi2sk".to_string(),
            AttributeValue::S(namespace.as_str().to_string()),
        );
    }
    db.put_item(
        PutItemInput::builder()
            .table_name(Tables::sys_namespaces())
            .item(item)
            .build(),
    )
    .await
    .expect("put tenant metadata");
}

async fn put_tenant_dual_write_metadata(
    db: &DatabaseManager,
    namespace: &TableNamespace,
    st: u8,
    loc: u16,
    old_loc: u16,
    new_loc: u16,
    cutover_at_ms: i64,
) {
    let mut item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("NS#{}", namespace.as_str())),
        ),
        ("sk".to_string(), AttributeValue::S("META".to_string())),
        (
            "id".to_string(),
            AttributeValue::S(namespace.as_str().to_string()),
        ),
        ("st".to_string(), AttributeValue::N(st.to_string())),
        ("loc".to_string(), AttributeValue::N(loc.to_string())),
        (
            "migration_mode".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "mode".to_string(),
                    AttributeValue::S("dual_write".to_string()),
                ),
                (
                    "old_loc".to_string(),
                    AttributeValue::N(old_loc.to_string()),
                ),
                (
                    "new_loc".to_string(),
                    AttributeValue::N(new_loc.to_string()),
                ),
                (
                    "cutover_at_ms".to_string(),
                    AttributeValue::N(cutover_at_ms.to_string()),
                ),
            ])),
        ),
    ]);
    if st == 1 {
        item.insert("gsi2pk".to_string(), AttributeValue::S("ST#1".to_string()));
        item.insert(
            "gsi2sk".to_string(),
            AttributeValue::S(namespace.as_str().to_string()),
        );
    }
    db.put_item(
        PutItemInput::builder()
            .table_name(Tables::sys_namespaces())
            .item(item)
            .build(),
    )
    .await
    .expect("put tenant dual write metadata");
}

async fn put_shared_namespace_route_metadata(
    db: &DatabaseManager,
    namespace: &TableNamespace,
    loc: u16,
) {
    put_namespace_route_metadata(db, namespace, 1, loc).await;
}

async fn put_dedicated_namespace_route_metadata(
    db: &DatabaseManager,
    namespace: &TableNamespace,
    loc: u16,
) {
    put_namespace_route_metadata(db, namespace, 0, loc).await;
}

async fn put_cutover_event(
    db: &DatabaseManager,
    namespace: &TableNamespace,
    migration_id: &str,
    old_loc: u16,
    new_loc: u16,
    effective_at_ms: i64,
) {
    let encoded_ms = format!("{:020}", effective_at_ms);
    let gsi3sk = format!("{encoded_ms}#{}#{migration_id}", namespace.storage_key());
    let table_sk = format!(
        "CUTOVER#{encoded_ms}#{}#{migration_id}",
        namespace.storage_key()
    );
    let item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("SYS#CUTOVER".to_string()),
        ),
        ("sk".to_string(), AttributeValue::S(table_sk)),
        (
            "namespace".to_string(),
            AttributeValue::S(namespace.as_str().to_string()),
        ),
        (
            "migration_id".to_string(),
            AttributeValue::S(migration_id.to_string()),
        ),
        (
            "old_loc".to_string(),
            AttributeValue::N(old_loc.to_string()),
        ),
        (
            "new_loc".to_string(),
            AttributeValue::N(new_loc.to_string()),
        ),
        (
            "effective_at_ms".to_string(),
            AttributeValue::N(effective_at_ms.to_string()),
        ),
        (
            "status".to_string(),
            AttributeValue::S("scheduled".to_string()),
        ),
        (
            "gsi3pk".to_string(),
            AttributeValue::S("CUTOVER".to_string()),
        ),
        ("gsi3sk".to_string(), AttributeValue::S(gsi3sk)),
    ]);
    db.put_item(
        PutItemInput::builder()
            .table_name(Tables::sys_namespaces())
            .item(item)
            .build(),
    )
    .await
    .expect("put cutover event");
}

#[tokio::test]
async fn shared_table_routing_rewrites_keys_and_blocks_unsafe_forms() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let logical_table = Tables::namespace(&namespace);
    let shared_table = Tables::shared_namespace(1);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                (
                    "name".to_string(),
                    AttributeValue::S("primary-user".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put routed item");

    let provider = db.maintenance_provider();
    let prefixed_key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("{}#USER#1", namespace.as_str())),
        ),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
    ]);
    let prefixed_item = provider
        .get_item(shared_table.clone(), prefixed_key.into(), true)
        .await
        .expect("get shared item")
        .expect("item exists");
    let prefixed_map = prefixed_item.into_attribute_map().expect("decode item");
    assert_eq!(
        prefixed_map.get("name"),
        Some(&AttributeValue::S("primary-user".to_string()))
    );

    let logical_item = db
        .get_item_map(
            logical_table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
            ]),
        )
        .await
        .expect("get routed item through logical table")
        .expect("logical item exists");
    assert_eq!(
        logical_item.get("pk"),
        Some(&AttributeValue::S("USER#1".to_string()))
    );

    let (query_items, _) = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table.clone())
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("USER#1".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("query shared via logical table");
    assert_eq!(query_items.len(), 1);
    assert_eq!(
        query_items[0].get("pk"),
        Some(&AttributeValue::S("USER#1".to_string()))
    );

    let foreign_tenant = TableNamespace::new();
    provider
        .put_item(
            shared_table,
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("{}#USER#1", foreign_tenant.as_str())),
                ),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                (
                    "name".to_string(),
                    AttributeValue::S("foreign-user".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert foreign tenant row");

    let (filtered_items, _) = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table.clone())
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("USER#1".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("query with foreign row present");
    assert_eq!(filtered_items.len(), 1);

    let unsafe_query_err = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table.clone())
                .key_condition_expression("begins_with(pk, :pk)".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("USER#".to_string()),
                )]))
                .build(),
        )
        .await
        .expect_err("unsupported key condition must fail closed");
    assert!(unsafe_query_err.to_string().contains("failed closed"));

    let scan_err = db
        .scan_table(ScanTableInput::builder().table_name(logical_table).build())
        .await
        .expect_err("scan on shared table must fail closed");
    assert!(scan_err.to_string().contains("failed closed"));
}

#[tokio::test]
async fn routed_transaction_failure_returns_logical_all_old_item() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;
    let logical_table = Tables::namespace(&namespace);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#TXN".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ("state".to_string(), AttributeValue::S("winner".to_string())),
            ]))
            .build(),
    )
    .await
    .expect("seed routed item");

    let error = db
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                update: Some(TransactUpdateRequest {
                    table_name: logical_table,
                    key: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("USER#TXN".to_string())),
                        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                    ])
                    .into(),
                    update_expression: "SET #state = :next".to_string(),
                    indexers: None,
                    condition_expression: Some("#state = :expected".to_string()),
                    expression_attribute_names: Some(HashMap::from([(
                        "#state".to_string(),
                        "state".to_string(),
                    )])),
                    expression_attribute_values: Some(HashMap::from([
                        (":next".to_string(), AttributeValue::S("next".to_string())),
                        (
                            ":expected".to_string(),
                            AttributeValue::S("loser".to_string()),
                        ),
                    ])),
                    return_values_on_condition_check_failure: Some("ALL_OLD".to_string()),
                    aux_item_stream_ttl_hours: None,
                }),
                ..TransactWriteItem::default()
            }],
            ..TransactWriteItemsRequest::default()
        })
        .await
        .expect_err("condition must fail");

    let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
        panic!("expected transaction cancellation, got {error}");
    };
    let item: HashMap<String, AttributeValue> = serde_json::from_str(
        reasons[0]
            .splitn(3, '\t')
            .nth(2)
            .expect("conditional failure item"),
    )
    .expect("decode conditional failure item");
    assert_eq!(
        item.get("pk"),
        Some(&AttributeValue::S("USER#TXN".to_string()))
    );
    assert_eq!(
        item.get("state"),
        Some(&AttributeValue::S("winner".to_string()))
    );
}

#[tokio::test]
async fn resolved_get_uses_one_route_snapshot_across_cutover() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    create_simple_shared_table(&db, 2).await;
    put_location_dictionary(&db, &[(1, "default"), (2, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;
    let logical_table = Tables::namespace(&namespace);
    let logical_key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("USER#SNAPSHOT".to_string()),
        ),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
    ]);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("USER#SNAPSHOT".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ("location".to_string(), AttributeValue::N("1".to_string())),
            ]))
            .build(),
    )
    .await
    .expect("seed old route");

    db.maintenance_provider()
        .put_item(
            Tables::shared_namespace(2),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("{}#USER#SNAPSHOT", namespace.as_str())),
                ),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ("location".to_string(), AttributeValue::N("2".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("seed new route");

    let resolved = db
        .resolve_storage_operation(logical_table.clone())
        .await
        .expect("resolve before cutover")
        .validate_key(logical_key.clone().into())
        .expect("validate logical key");

    let now_ms = storage_types::TimestampMillis::now().timestamp_millis();
    put_cutover_event(
        &db,
        &namespace,
        "resolved-operation-cutover",
        1,
        2,
        now_ms - 1,
    )
    .await;
    db.run_job(GSI_UPDATE_JOB).await;
    let resolver = db.route_resolver_for_tests().expect("route resolver");
    CutoverWatcher::new(
        resolver,
        db.default_storage_for_tests(),
        db.default_admission_controller(),
    )
    .poll_once()
    .await
    .expect("apply cutover");

    let snapshot_item = db
        .get_item_with_resolved_operation(resolved, true)
        .await
        .expect("read from resolved route")
        .expect("snapshot item")
        .into_attribute_map()
        .expect("decode snapshot item");
    assert_eq!(
        snapshot_item.get("location"),
        Some(&AttributeValue::N("1".to_string()))
    );

    let fresh_item = db
        .get_item_map(logical_table, logical_key)
        .await
        .expect("read fresh route")
        .expect("fresh item");
    assert_eq!(
        fresh_item.get("location"),
        Some(&AttributeValue::N("2".to_string()))
    );
}

#[tokio::test]
async fn batch_get_keeps_logical_namespaces_that_share_one_physical_table_separate() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create namespace routing table");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let first_namespace = TableNamespace::from_seed("first");
    let second_namespace = TableNamespace::from_seed("second");
    put_shared_namespace_route_metadata(&db, &first_namespace, 1).await;
    put_shared_namespace_route_metadata(&db, &second_namespace, 1).await;
    let first_table = Tables::namespace(&first_namespace);
    let second_table = Tables::namespace(&second_namespace);

    for (table, value) in [(&first_table, "first"), (&second_table, "second")] {
        db.put_item(
            PutItemInput::builder()
                .table_name(table.clone())
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("ITEM#1".to_string())),
                    ("sk".to_string(), AttributeValue::S("VALUE".to_string())),
                    ("value".to_string(), AttributeValue::S(value.to_string())),
                ]))
                .build(),
        )
        .await
        .expect("put routed item");
    }

    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("ITEM#1".to_string())),
        ("sk".to_string(), AttributeValue::S("VALUE".to_string())),
    ]);
    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([
                (
                    first_table.clone(),
                    KeysAndAttributes {
                        keys: vec![key.clone().into()].into(),
                        attributes_to_get: None,
                        consistent_read: None,
                        projection_expression: None,
                        expression_attribute_names: None,
                    },
                ),
                (
                    second_table.clone(),
                    KeysAndAttributes {
                        keys: vec![key.into()].into(),
                        attributes_to_get: None,
                        consistent_read: None,
                        projection_expression: None,
                        expression_attribute_names: None,
                    },
                ),
            ]),
            return_consumed_capacity: None,
        })
        .await
        .expect("batch get routed items")
        .responses
        .expect("batch get responses");

    assert_eq!(
        response[&first_table][0]
            .attribute_value("value")
            .expect("first value"),
        Some(AttributeValue::S("first".to_string()))
    );
    assert_eq!(
        response[&second_table][0]
            .attribute_value("value")
            .expect("second value"),
        Some(AttributeValue::S("second".to_string()))
    );
}

#[tokio::test]
async fn batch_get_groups_dedicated_tables_on_one_routed_connection() {
    let db = new_multi_connection_routed_db().await;
    put_location_dictionary(&db, &[(2, "tenant-store")]).await;
    let first_namespace = TableNamespace::from_seed("dedicated-first");
    let second_namespace = TableNamespace::from_seed("dedicated-second");

    for namespace in [&first_namespace, &second_namespace] {
        put_dedicated_namespace_route_metadata(&db, namespace, 2).await;
        create_simple_table_on_connection(&db, "tenant-store", Tables::namespace(namespace)).await;
        db.put_item(
            PutItemInput::builder()
                .table_name(Tables::namespace(namespace))
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("ITEM#1".to_string())),
                    ("sk".to_string(), AttributeValue::S("VALUE".to_string())),
                    (
                        "namespace".to_string(),
                        AttributeValue::S(namespace.as_str().to_string()),
                    ),
                ]))
                .build(),
        )
        .await
        .expect("put routed dedicated item");
    }

    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("ITEM#1".to_string())),
        ("sk".to_string(), AttributeValue::S("VALUE".to_string())),
    ]);
    let first_table = Tables::namespace(&first_namespace);
    let second_table = Tables::namespace(&second_namespace);
    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([
                (
                    first_table.clone(),
                    KeysAndAttributes {
                        keys: vec![key.clone().into()].into(),
                        attributes_to_get: None,
                        consistent_read: Some(true),
                        projection_expression: None,
                        expression_attribute_names: None,
                    },
                ),
                (
                    second_table.clone(),
                    KeysAndAttributes {
                        keys: vec![key.into()].into(),
                        attributes_to_get: None,
                        consistent_read: Some(false),
                        projection_expression: None,
                        expression_attribute_names: None,
                    },
                ),
            ]),
            return_consumed_capacity: None,
        })
        .await
        .expect("batch get grouped routed items")
        .responses
        .expect("grouped responses");

    for (table, namespace) in [
        (first_table, first_namespace),
        (second_table, second_namespace),
    ] {
        assert_eq!(
            response[&table][0]
                .attribute_value("namespace")
                .expect("namespace value"),
            Some(AttributeValue::S(namespace.as_str().to_string()))
        );
    }
}

#[tokio::test]
async fn shared_table_query_pagination_round_trips_logical_start_keys() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let logical_table = Tables::namespace(&namespace);
    for (sk, name) in [
        ("PROFILE#001", "first-profile"),
        ("PROFILE#002", "second-profile"),
    ] {
        db.put_item(
            PutItemInput::builder()
                .table_name(logical_table.clone())
                .item(HashMap::from([
                    (
                        "pk".to_string(),
                        AttributeValue::S("USER#PAGED".to_string()),
                    ),
                    ("sk".to_string(), AttributeValue::S(sk.to_string())),
                    ("name".to_string(), AttributeValue::S(name.to_string())),
                ]))
                .build(),
        )
        .await
        .expect("put paged routed item");
    }

    let first_page = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table.clone())
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("USER#PAGED".to_string()),
                )]))
                .limit(1_u32)
                .build(),
        )
        .await
        .expect("query first routed page");
    assert_eq!(first_page.0.len(), 1);
    assert_eq!(
        first_page.0[0].get("pk"),
        Some(&AttributeValue::S("USER#PAGED".to_string()))
    );
    assert_eq!(
        first_page.0[0].get("sk"),
        Some(&AttributeValue::S("PROFILE#001".to_string()))
    );
    let first_lek = first_page.1.expect("first page should have lek");

    let second_page = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table)
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("USER#PAGED".to_string()),
                )]))
                .limit(1_u32)
                .exclusive_start_key(first_lek)
                .build(),
        )
        .await
        .expect("query second routed page");
    assert_eq!(second_page.0.len(), 1);
    assert_eq!(
        second_page.0[0].get("pk"),
        Some(&AttributeValue::S("USER#PAGED".to_string()))
    );
    assert_eq!(
        second_page.0[0].get("sk"),
        Some(&AttributeValue::S("PROFILE#002".to_string()))
    );
    assert!(second_page.1.is_none());
}

#[tokio::test]
async fn shared_table_batch_get_returns_canonical_items() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let logical_table = Tables::namespace(&namespace);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#2".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                (
                    "name".to_string(),
                    AttributeValue::S("batch-user".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put routed item");

    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                logical_table.clone(),
                KeysAndAttributes {
                    keys: vec![
                        HashMap::from([
                            ("pk".to_string(), AttributeValue::S("USER#2".to_string())),
                            ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                        ])
                        .into(),
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
        .expect("batch get item");

    let items = response
        .responses
        .and_then(|mut tables| tables.remove(&logical_table))
        .expect("logical table response");
    assert_eq!(items.len(), 1);
    let map = items
        .into_iter()
        .next()
        .expect("item")
        .into_attribute_map()
        .expect("decode item");
    assert_eq!(
        map.get("pk"),
        Some(&AttributeValue::S("USER#2".to_string()))
    );
}

#[tokio::test]
async fn shared_table_update_routes_to_physical_table_and_returns_logical_attributes() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let logical_table = Tables::namespace(&namespace);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#3".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ("name".to_string(), AttributeValue::S("before".to_string())),
            ]))
            .build(),
    )
    .await
    .expect("put routed item");

    let response = db
        .update_item(
            UpdateItemInput::builder()
                .table_name(logical_table.clone())
                .key(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("USER#3".to_string())),
                    ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ]))
                .update_expression("SET #name = :name".to_string())
                .condition_expression("#pk = :expected_pk".to_string())
                .expression_attribute_names(HashMap::from([
                    ("#name".to_string(), "name".to_string()),
                    ("#pk".to_string(), "pk".to_string()),
                ]))
                .expression_attribute_values(HashMap::from([
                    (":name".to_string(), AttributeValue::S("after".to_string())),
                    (
                        ":expected_pk".to_string(),
                        AttributeValue::S("USER#3".to_string()),
                    ),
                ]))
                .return_values(storage_types::ReturnValuesOldNewUpdated::AllNew)
                .build(),
        )
        .await
        .expect("update routed item");

    let attributes = response.attributes.expect("all new attributes");
    assert_eq!(
        attributes.get("pk"),
        Some(&AttributeValue::S("USER#3".to_string()))
    );
    assert_eq!(
        attributes.get("name"),
        Some(&AttributeValue::S("after".to_string()))
    );

    let provider = db.maintenance_provider();
    let physical = provider
        .get_item(
            Tables::shared_namespace(1),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("{}#USER#3", namespace.as_str())),
                ),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
            ])
            .into(),
            true,
        )
        .await
        .expect("read physical item")
        .expect("physical item exists")
        .into_attribute_map()
        .expect("decode physical item");
    assert_eq!(
        physical.get("name"),
        Some(&AttributeValue::S("after".to_string()))
    );
}

#[tokio::test]
async fn shared_table_delete_routes_to_physical_table_and_returns_removed_logical_item() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let logical_table = Tables::namespace(&namespace);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#4".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                (
                    "name".to_string(),
                    AttributeValue::S("delete-me".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put routed item");

    let deleted = db
        .delete_item(
            DeleteItemInput::builder()
                .table_name(logical_table.clone())
                .key(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("USER#4".to_string())),
                    ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ]))
                .condition_expression("#pk = :expected_pk".to_string())
                .expression_attribute_names(HashMap::from([("#pk".to_string(), "pk".to_string())]))
                .expression_attribute_values(HashMap::from([(
                    ":expected_pk".to_string(),
                    AttributeValue::S("USER#4".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("delete routed item")
        .expect("deleted item is returned");

    assert_eq!(
        deleted.get("pk"),
        Some(&AttributeValue::S("USER#4".to_string()))
    );
    assert_eq!(
        deleted.get("name"),
        Some(&AttributeValue::S("delete-me".to_string()))
    );
    let remaining = db
        .get_item_map(
            logical_table,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#4".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
            ]),
        )
        .await
        .expect("read deleted logical item");
    assert!(remaining.is_none());
}

#[tokio::test]
async fn shared_table_route_fails_closed_for_unknown_location() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 9).await;

    let err = db
        .put_item(
            PutItemInput::builder()
                .table_name(Tables::namespace(&namespace))
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("A#1".to_string())),
                    ("sk".to_string(), AttributeValue::S("B#1".to_string())),
                ]))
                .build(),
        )
        .await
        .expect_err("unknown loc must fail");
    assert!(err.to_string().contains("unknown location code 9"));
}

#[tokio::test]
async fn cutover_watcher_applies_late_and_scheduled_events() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    create_simple_shared_table(&db, 2).await;
    put_location_dictionary(&db, &[(1, "default"), (2, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let resolver = db.route_resolver_for_tests().expect("route resolver");
    let route_before = resolver
        .resolve_route(&namespace)
        .await
        .expect("initial route");
    assert_eq!(
        route_before.read_target.table_name,
        Tables::shared_namespace(1)
    );

    let now_ms = storage_types::TimestampMillis::now().timestamp_millis();
    put_cutover_event(&db, &namespace, "late-cutover", 1, 2, now_ms - 25).await;
    db.run_job(GSI_UPDATE_JOB).await;

    let watcher = CutoverWatcher::new(
        resolver.clone(),
        db.default_storage_for_tests(),
        db.default_admission_controller(),
    );
    watcher.poll_once().await.expect("poll late cutover");

    let route_after_late = resolver
        .resolve_route(&namespace)
        .await
        .expect("route after late cutover");
    assert_eq!(
        route_after_late.read_target.table_name,
        Tables::shared_namespace(2)
    );

    put_cutover_event(&db, &namespace, "future-cutover", 2, 1, now_ms + 125).await;
    db.run_job(GSI_UPDATE_JOB).await;
    watcher.poll_once().await.expect("poll scheduled cutover");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let route_after_scheduled = resolver
        .resolve_route(&namespace)
        .await
        .expect("route after scheduled cutover");
    assert_eq!(
        route_after_scheduled.read_target.table_name,
        Tables::shared_namespace(1)
    );
}

#[tokio::test]
async fn dual_write_route_dedupes_same_physical_write_target() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    put_location_dictionary(&db, &[(2, "default")]).await;

    let namespace = TableNamespace::new();
    let now_ms = storage_types::TimestampMillis::now().timestamp_millis();
    put_tenant_dual_write_metadata(&db, &namespace, 0, 0, 0, 2, now_ms + 60_000).await;

    let resolver = db.route_resolver_for_tests().expect("route resolver");
    let route = resolver
        .resolve_route(&namespace)
        .await
        .expect("resolve dual write route");

    assert_eq!(route.read_target.connection_id, "default");
    assert_eq!(route.read_target.table_name, Tables::namespace(&namespace));
    assert_eq!(route.write_targets.len(), 1);
    assert_eq!(route.write_targets[0].connection_id, "default");
    assert_eq!(
        route.write_targets[0].table_name,
        Tables::namespace(&namespace)
    );
    assert_eq!(route.write_targets[0].loc, 0);
}

#[tokio::test]
async fn cutover_watcher_ignores_missing_m_table() {
    let db = new_routed_db().await;
    let resolver = db.route_resolver_for_tests().expect("route resolver");
    let watcher = CutoverWatcher::new(
        resolver,
        db.default_storage_for_tests(),
        db.default_admission_controller(),
    );

    watcher
        .poll_once()
        .await
        .expect("missing metadata table should be treated as no cutover work");
}

#[test]
fn namespace_source_table_uses_dedicated_or_shared_table_based_on_storage_mode_code() {
    let namespace = TableNamespace::new();

    let dedicated = namespace_source_table(&namespace, 0, 7);
    let shared = namespace_source_table(&namespace, 1, 7);

    assert_eq!(dedicated.table_name, Tables::namespace(&namespace));
    assert!(!dedicated.is_shared_table);
    assert_eq!(shared.table_name, Tables::shared_namespace(7));
    assert!(shared.is_shared_table);
}

#[test]
fn direct_shared_table_access_is_rejected_but_logical_tenant_routes_are_allowed() {
    let shared_table = Tables::shared_namespace(9);
    let namespace_table = TableName::new(&"n01JV4YQ4T9YQ3X36M4A42B5Q8Z");

    let error =
        reject_direct_shared_table_access(&shared_table).expect_err("shared table should fail");

    assert!(
        error
            .to_string()
            .contains("direct shared table access is not allowed")
    );
    reject_direct_shared_table_access(&namespace_table).expect("namespace route should be allowed");
}

#[test]
fn throttled_errors_are_retryable_pause_errors() {
    let throttled = StorageError::Base(StorageEnum::Throttled {
        message: "rate exceeded".to_string(),
    });
    let validation = StorageError::validation("not retryable");

    assert!(is_retryable_pause_error(&throttled));
    assert!(!is_retryable_pause_error(&validation));
}

#[tokio::test]
async fn shared_table_route_refreshes_dictionary_after_new_location_added() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 3).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 3).await;

    let first_error = db
        .put_item(
            PutItemInput::builder()
                .table_name(Tables::namespace(&namespace))
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("U#1".to_string())),
                    ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ]))
                .build(),
        )
        .await
        .expect_err("unknown location should fail before dictionary update");
    assert!(first_error.to_string().contains("unknown location code 3"));

    // Update dictionary with loc=3. Resolver should refresh after previous
    // miss and succeed on next request.
    put_location_dictionary(&db, &[(1, "default"), (3, "default")]).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(Tables::namespace(&namespace))
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("U#1".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                ("name".to_string(), AttributeValue::S("routed".to_string())),
            ]))
            .build(),
    )
    .await
    .expect("dictionary refresh should resolve newly added location");
}

#[tokio::test]
async fn route_switch_between_dedicated_and_shared_tables_keeps_data_isolated() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    create_simple_shared_table(&db, 1).await;
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    Tables::create_namespace_table(&db, &namespace)
        .await
        .expect("create dedicated namespace table");
    put_dedicated_namespace_route_metadata(&db, &namespace, 0).await;

    let logical_table = Tables::namespace(&namespace);
    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("U#DEDICATED".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                (
                    "origin".to_string(),
                    AttributeValue::S("dedicated".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("write dedicated row");

    let resolver = db.route_resolver_for_tests().expect("route resolver");
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;
    resolver.invalidate_namespace(&namespace);

    db.put_item(
        PutItemInput::builder()
            .table_name(logical_table.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("U#SHARED".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
                (
                    "origin".to_string(),
                    AttributeValue::S("shared".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("write shared row through routed table");

    let (shared_view, _) = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table.clone())
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("U#DEDICATED".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("query while shared");
    assert!(
        shared_view.is_empty(),
        "dedicated rows must not leak when namespace routes to shared table"
    );

    put_dedicated_namespace_route_metadata(&db, &namespace, 0).await;
    resolver.invalidate_namespace(&namespace);

    let (dedicated_view, _) = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table.clone())
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("U#DEDICATED".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("query after switching back to dedicated");
    assert_eq!(dedicated_view.len(), 1);

    let (shared_row_hidden, _) = db
        .query_table_map(
            QueryTableInput::builder()
                .table_name(logical_table)
                .key_condition_expression("pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("U#SHARED".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("query shared row from dedicated mode");
    assert!(
        shared_row_hidden.is_empty(),
        "shared rows must not leak after switching back to dedicated route"
    );
}

#[tokio::test]
async fn shared_table_routing_rewrites_partition_assignments_inside_if_not_exists_functions() {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let rewriter = NamespaceRequestRewriter::new();
    let mut request = UpdateItemRequest::builder()
        .table_name(Tables::namespace(&namespace))
        .key(HashMap::from([
            ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
            ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
        ]))
        .update_expression(
            "SET pk = if_not_exists(pk, :pk), gsi2pk = if_not_exists(gsi2pk, :gsi2pk), updated_at \
             = :updated_at"
                .to_string(),
        )
        .expression_attribute_values(Some(HashMap::from([
            (":pk".to_string(), AttributeValue::S("USER#1".to_string())),
            (
                ":gsi2pk".to_string(),
                AttributeValue::S("USER_LOOKUP#alice@example.test".to_string()),
            ),
            (
                ":updated_at".to_string(),
                AttributeValue::N("1".to_string()),
            ),
        ])))
        .build();

    rewriter
        .rewrite_update_for_shared_table(&namespace, &mut request)
        .expect("valid if_not_exists partition assignment should rewrite cleanly");

    let values = request
        .expression_attribute_values
        .expect("rewritten expression values");
    assert_eq!(
        values.get(":pk"),
        Some(&AttributeValue::S(format!("{}#USER#1", namespace.as_str())))
    );
    assert_eq!(
        values.get(":gsi2pk"),
        Some(&AttributeValue::S(format!(
            "{}#USER_LOOKUP#alice@example.test",
            namespace.as_str()
        )))
    );
    assert_eq!(
        values.get(":updated_at"),
        Some(&AttributeValue::N("1".to_string()))
    );

    let mut transact_item = TransactEncodeItem {
        update: Some(TransactUpdateRequest {
            table_name: Tables::namespace(&namespace),
            key: HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#2".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
            ])
            .into(),
            update_expression: "SET pk = if_not_exists(pk, :pk), gsi2pk = if_not_exists(gsi2pk, \
                                :gsi2pk)"
                .to_string(),
            indexers: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([
                (":pk".to_string(), AttributeValue::S("USER#2".to_string())),
                (
                    ":gsi2pk".to_string(),
                    AttributeValue::S("USER_LOOKUP#bob@example.test".to_string()),
                ),
            ])),
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        ..Default::default()
    };

    rewriter
        .rewrite_transact_encode_item_for_shared_table(&namespace, &mut transact_item)
        .expect("transact update with if_not_exists partition assignment should rewrite cleanly");

    let update = transact_item.update.expect("rewritten transact update");
    let values = update
        .expression_attribute_values
        .expect("rewritten transact expression values");
    assert_eq!(
        values.get(":pk"),
        Some(&AttributeValue::S(format!("{}#USER#2", namespace.as_str())))
    );
    assert_eq!(
        values.get(":gsi2pk"),
        Some(&AttributeValue::S(format!(
            "{}#USER_LOOKUP#bob@example.test",
            namespace.as_str()
        )))
    );
}

#[test]
fn shared_table_routing_allows_nested_document_update_paths() {
    let namespace = TableNamespace::new();
    let rewriter = NamespaceRequestRewriter::new();
    let mut request = UpdateItemRequest::builder()
        .table_name(Tables::namespace(&namespace))
        .key(HashMap::from([
            ("pk".to_string(), AttributeValue::S("ROLE#1".to_string())),
            ("sk".to_string(), AttributeValue::S("LOOKUP".to_string())),
        ]))
        .update_expression(
            "SET #entries.#entry = :entry_payload, #updated_at = :updated_at".to_string(),
        )
        .expression_attribute_names(Some(HashMap::from([
            ("#entries".to_string(), "assignment_entries".to_string()),
            ("#entry".to_string(), "role-1".to_string()),
            ("#updated_at".to_string(), "updated_at".to_string()),
        ])))
        .expression_attribute_values(Some(HashMap::from([
            (
                ":entry_payload".to_string(),
                AttributeValue::M(HashMap::from([(
                    "role_id".to_string(),
                    AttributeValue::S("role-1".to_string()),
                )])),
            ),
            (
                ":updated_at".to_string(),
                AttributeValue::N("1".to_string()),
            ),
        ])))
        .build();

    rewriter
        .rewrite_update_for_shared_table(&namespace, &mut request)
        .expect("nested document paths must not be parsed as one expression-name alias");

    assert_eq!(
        request.key.get("pk"),
        Some(&AttributeValue::S(format!("{}#ROLE#1", namespace.as_str())))
    );
    assert_eq!(
        request
            .expression_attribute_values
            .as_ref()
            .and_then(|values| values.get(":entry_payload")),
        Some(&AttributeValue::M(HashMap::from([(
            "role_id".to_string(),
            AttributeValue::S("role-1".to_string()),
        )])))
    );
}

#[tokio::test]
async fn shared_table_routing_rewrites_partition_key_placeholders_inside_update_condition_expressions()
 {
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let rewriter = NamespaceRequestRewriter::new();
    let mut request = UpdateItemRequest::builder()
        .table_name(Tables::namespace(&namespace))
        .key(HashMap::from([
            ("pk".to_string(), AttributeValue::S("USER#3".to_string())),
            ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
        ]))
        .update_expression("SET updated_at = :updated_at".to_string())
        .condition_expression(Some(
            "#pk = :expected_pk AND #version = :expected_version".to_string(),
        ))
        .expression_attribute_names(Some(HashMap::from([
            ("#pk".to_string(), "pk".to_string()),
            ("#version".to_string(), "version".to_string()),
        ])))
        .expression_attribute_values(Some(HashMap::from([
            (
                ":expected_pk".to_string(),
                AttributeValue::S("USER#3".to_string()),
            ),
            (
                ":expected_version".to_string(),
                AttributeValue::N("7".to_string()),
            ),
            (
                ":updated_at".to_string(),
                AttributeValue::N("9".to_string()),
            ),
        ])))
        .build();

    rewriter
        .rewrite_update_for_shared_table(&namespace, &mut request)
        .expect("partition-key conditions should be rewritten for shared-table routing");

    let values = request
        .expression_attribute_values
        .expect("rewritten expression values");
    assert_eq!(
        values.get(":expected_pk"),
        Some(&AttributeValue::S(format!("{}#USER#3", namespace.as_str())))
    );
    assert_eq!(
        values.get(":expected_version"),
        Some(&AttributeValue::N("7".to_string()))
    );
}

#[tokio::test]
async fn shared_table_routing_rewrites_partition_key_placeholders_inside_transact_condition_checks()
{
    let db = new_routed_db().await;
    Tables::create_sys_namespaces_table(&db)
        .await
        .expect("create sys tenants");
    put_location_dictionary(&db, &[(1, "default")]).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(&db, &namespace, 1).await;

    let rewriter = NamespaceRequestRewriter::new();
    let mut transact_item = TransactWriteItem {
        condition_check: Some(TransactConditionCheckRequest {
            table_name: Tables::namespace(&namespace),
            key: HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#4".to_string())),
                ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
            ])
            .into(),
            condition_expression: "#pk = :expected_pk".to_string(),
            expression_attribute_names: Some(HashMap::from([(
                "#pk".to_string(),
                "pk".to_string(),
            )])),
            expression_attribute_values: Some(HashMap::from([(
                ":expected_pk".to_string(),
                AttributeValue::S("USER#4".to_string()),
            )])),
            return_values_on_condition_check_failure: None,
        }),
        ..Default::default()
    };

    rewriter
        .rewrite_transact_item_for_shared_table(&namespace, &mut transact_item)
        .expect("condition checks should compare against namespace-prefixed partition keys");

    let condition_check = transact_item
        .condition_check
        .expect("rewritten condition check");
    let values = condition_check
        .expression_attribute_values
        .expect("rewritten condition values");
    assert_eq!(
        values.get(":expected_pk"),
        Some(&AttributeValue::S(format!("{}#USER#4", namespace.as_str())))
    );
}
