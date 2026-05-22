#![cfg(feature = "sqlite")]

use std::collections::HashMap;

use storage_common::GSI_UPDATE_JOB;
use storage_provider::{StorageBackend, StorageConnectionConfig, StorageConnectionRegistry};
use storage_types::{
    AttributeDefinition, AttributeValue, CreateGlobalSecondaryIndex, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, TableNamespace,
    TimestampMillis,
};

use crate::{
    BeginDualWriteInput, CompleteCutoverInput, DualWriteCoordinator, MigrationBackfillInput,
    Tables,
    migration_index_keys::{migration_index_pk, migration_index_sk},
};

const MIGRATION_INDEX_NAME: &str = "migration";

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

fn temp_sqlite_path(prefix: &str) -> String {
    let namespace = TableNamespace::new();
    let suffix = namespace.storage_key().to_string();
    std::env::temp_dir()
        .join(format!("auxfn-{prefix}-{suffix}.db"))
        .to_string_lossy()
        .to_string()
}

async fn new_multiloc_db() -> (std::sync::Arc<crate::DatabaseManager>, String, String) {
    let default_path = temp_sqlite_path("default");
    let secondary_path = temp_sqlite_path("secondary");

    let db = crate::DatabaseManager::new_with_connection_registry(StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([
            ("default".to_string(), sqlite_connection(&default_path)),
            ("secondary".to_string(), sqlite_connection(&secondary_path)),
        ]),
    })
    .await
    .expect("database manager");

    (std::sync::Arc::new(db), default_path, secondary_path)
}

async fn create_shared_table(db: &crate::DatabaseManager, connection_id: &str, location_code: u16) {
    let request = CreateTableRequest::new(
        Tables::shared_namespace(location_code),
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
                attribute_name: "migrationpk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "migrationsk".to_string(),
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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: storage_types::IndexName::new(MIGRATION_INDEX_NAME),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "migrationpk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "migrationsk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    db.create_table_on_connection(connection_id, &request)
        .await
        .expect("create shared table");
}

async fn put_location_dictionary(db: &crate::DatabaseManager) {
    for (loc, connection_id) in [(1u16, "default"), (2u16, "secondary")] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(Tables::sys_namespaces())
                .item(HashMap::from([
                    (
                        "pk".to_string(),
                        AttributeValue::S("SYS#ROUTING".to_string()),
                    ),
                    ("sk".to_string(), AttributeValue::S(format!("LOC#{loc:05}"))),
                    ("loc".to_string(), AttributeValue::N(loc.to_string())),
                    (
                        "connection_id".to_string(),
                        AttributeValue::S(connection_id.to_string()),
                    ),
                    (
                        "backend_kind".to_string(),
                        AttributeValue::S("sqlite".to_string()),
                    ),
                    ("metadata".to_string(), AttributeValue::M(HashMap::new())),
                    ("updated_at".to_string(), AttributeValue::N("0".to_string())),
                ]))
                .build(),
        )
        .await
        .expect("put location dictionary entry");
    }
}

async fn put_shared_namespace_route_metadata(
    db: &crate::DatabaseManager,
    namespace: &TableNamespace,
    loc: u16,
) {
    db.put_item(
        crate::PutItemInput::builder()
            .table_name(Tables::sys_namespaces())
            .item(HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("NS#{}", namespace.as_str())),
                ),
                ("sk".to_string(), AttributeValue::S("META".to_string())),
                (
                    "id".to_string(),
                    AttributeValue::S(namespace.as_str().to_string()),
                ),
                ("st".to_string(), AttributeValue::N("1".to_string())),
                ("loc".to_string(), AttributeValue::N(loc.to_string())),
                (
                    "migration_mode".to_string(),
                    AttributeValue::M(HashMap::from([(
                        "mode".to_string(),
                        AttributeValue::S("single".to_string()),
                    )])),
                ),
                ("gsi2pk".to_string(), AttributeValue::S("ST#1".to_string())),
                (
                    "gsi2sk".to_string(),
                    AttributeValue::S(namespace.as_str().to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put tenant metadata");
}

fn item_loc(map: &HashMap<String, AttributeValue>) -> u16 {
    match map.get("loc") {
        Some(AttributeValue::N(value)) => value.parse::<u16>().expect("loc u16"),
        _ => panic!("loc missing"),
    }
}

fn migration_mode_name(map: &HashMap<String, AttributeValue>) -> String {
    match map.get("migration_mode") {
        Some(AttributeValue::M(value)) => match value.get("mode") {
            Some(AttributeValue::S(mode)) => mode.clone(),
            _ => panic!("migration mode string missing"),
        },
        _ => panic!("migration_mode map missing"),
    }
}

fn tenant_meta_key(namespace: &TableNamespace) -> HashMap<String, AttributeValue> {
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("NS#{}", namespace.as_str())),
        ),
        ("sk".to_string(), AttributeValue::S("META".to_string())),
    ])
}

#[tokio::test]
async fn dual_write_coordinator_backfills_supplied_index_across_connections_without_leakage() {
    let (db, default_path, secondary_path) = new_multiloc_db().await;
    Tables::create_sys_namespaces_table(db.as_ref())
        .await
        .expect("create metadata table");
    create_shared_table(db.as_ref(), "default", 1).await;
    create_shared_table(db.as_ref(), "secondary", 2).await;
    put_location_dictionary(db.as_ref()).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(db.as_ref(), &namespace, 1).await;

    let source_provider = db
        .provider_for_connection_for_migration("default")
        .expect("default provider");
    let destination_provider = db
        .provider_for_connection_for_migration("secondary")
        .expect("secondary provider");

    let shared_source = Tables::shared_namespace(1);
    let shared_destination = Tables::shared_namespace(2);

    let tenant_pk = format!("{}#USER#1", namespace.as_str());
    let tenant_sk = "PROFILE#1".to_string();
    source_provider
        .put_item(
            shared_source.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(tenant_pk.clone())),
                ("sk".to_string(), AttributeValue::S(tenant_sk.clone())),
                (
                    "migrationpk".to_string(),
                    AttributeValue::S(migration_index_pk(&namespace, "USER")),
                ),
                (
                    "migrationsk".to_string(),
                    AttributeValue::S(migration_index_sk(&tenant_pk, &tenant_sk)),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("tenant-owned".to_string()),
                ),
                (
                    "updated_at".to_string(),
                    AttributeValue::N("100".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write source tenant row");

    let foreign_tenant = TableNamespace::new();
    let foreign_pk = format!("{}#USER#1", foreign_tenant.as_str());
    let foreign_sk = "PROFILE#1".to_string();
    source_provider
        .put_item(
            shared_source.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(foreign_pk.clone())),
                ("sk".to_string(), AttributeValue::S(foreign_sk.clone())),
                (
                    "migrationpk".to_string(),
                    AttributeValue::S(migration_index_pk(&foreign_tenant, "USER")),
                ),
                (
                    "migrationsk".to_string(),
                    AttributeValue::S(migration_index_sk(&foreign_pk, &foreign_sk)),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("foreign".to_string()),
                ),
                (
                    "updated_at".to_string(),
                    AttributeValue::N("100".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write foreign row");

    db.run_job(GSI_UPDATE_JOB).await;

    let coordinator = DualWriteCoordinator::new(db.clone());
    let summary = coordinator
        .backfill_via_migration_index(MigrationBackfillInput::new(
            namespace.clone(),
            storage_types::IndexName::new(MIGRATION_INDEX_NAME),
            1,
            2,
            vec!["USER".to_string()],
        ))
        .await
        .expect("migration index backfill");

    assert_eq!(summary.copied_items, 1);
    assert_eq!(summary.entities.len(), 1);
    assert_eq!(summary.entities[0].copied_items, 1);
    assert_ne!(summary.entities[0].checksum, 0);

    let copied_item = destination_provider
        .get_item(
            shared_destination.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(tenant_pk.clone())),
                ("sk".to_string(), AttributeValue::S(tenant_sk.clone())),
            ])
            .into(),
            true,
        )
        .await
        .expect("read copied item");
    assert!(copied_item.is_some(), "expected migrated tenant row");

    let foreign_item = destination_provider
        .get_item(
            shared_destination,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(foreign_pk)),
                ("sk".to_string(), AttributeValue::S(foreign_sk)),
            ])
            .into(),
            true,
        )
        .await
        .expect("read foreign item");
    assert!(
        foreign_item.is_none(),
        "foreign tenant row must not be copied by migration index"
    );

    let _ = std::fs::remove_file(default_path);
    let _ = std::fs::remove_file(secondary_path);
}

#[tokio::test]
async fn dual_write_backfill_skips_stale_source_items_when_destination_is_newer() {
    let (db, default_path, secondary_path) = new_multiloc_db().await;
    Tables::create_sys_namespaces_table(db.as_ref())
        .await
        .expect("create metadata table");
    create_shared_table(db.as_ref(), "default", 1).await;
    create_shared_table(db.as_ref(), "secondary", 2).await;
    put_location_dictionary(db.as_ref()).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(db.as_ref(), &namespace, 1).await;

    let source_provider = db
        .provider_for_connection_for_migration("default")
        .expect("default provider");
    let destination_provider = db
        .provider_for_connection_for_migration("secondary")
        .expect("secondary provider");

    let shared_source = Tables::shared_namespace(1);
    let shared_destination = Tables::shared_namespace(2);
    let tenant_pk = format!("{}#USER#1", namespace.as_str());
    let tenant_sk = "PROFILE#1".to_string();

    source_provider
        .put_item(
            shared_source,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(tenant_pk.clone())),
                ("sk".to_string(), AttributeValue::S(tenant_sk.clone())),
                (
                    "migrationpk".to_string(),
                    AttributeValue::S(migration_index_pk(&namespace, "USER")),
                ),
                (
                    "migrationsk".to_string(),
                    AttributeValue::S(migration_index_sk(&tenant_pk, &tenant_sk)),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("source-older".to_string()),
                ),
                (
                    "updated_at".to_string(),
                    AttributeValue::N("100".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write source row");

    destination_provider
        .put_item(
            shared_destination.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(tenant_pk.clone())),
                ("sk".to_string(), AttributeValue::S(tenant_sk.clone())),
                (
                    "payload".to_string(),
                    AttributeValue::S("destination-newer".to_string()),
                ),
                (
                    "updated_at".to_string(),
                    AttributeValue::N("200".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write destination row");

    db.run_job(GSI_UPDATE_JOB).await;

    let coordinator = DualWriteCoordinator::new(db.clone());
    let summary = coordinator
        .backfill_via_migration_index(MigrationBackfillInput::new(
            namespace.clone(),
            storage_types::IndexName::new(MIGRATION_INDEX_NAME),
            1,
            2,
            vec!["USER".to_string()],
        ))
        .await
        .expect("migration index backfill");
    assert_eq!(
        summary.copied_items, 0,
        "stale source version should not overwrite newer destination row"
    );

    let destination_item = destination_provider
        .get_item(
            shared_destination,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(tenant_pk)),
                ("sk".to_string(), AttributeValue::S(tenant_sk)),
            ])
            .into(),
            true,
        )
        .await
        .expect("read destination row")
        .expect("destination row exists")
        .into_attribute_map()
        .expect("decode destination row");
    assert_eq!(
        destination_item.get("payload"),
        Some(&AttributeValue::S("destination-newer".to_string()))
    );
    assert_eq!(
        destination_item.get("updated_at"),
        Some(&AttributeValue::N("200".to_string()))
    );

    let _ = std::fs::remove_file(default_path);
    let _ = std::fs::remove_file(secondary_path);
}

#[tokio::test]
async fn dual_write_backfill_fails_closed_when_updated_at_is_missing() {
    let (db, default_path, secondary_path) = new_multiloc_db().await;
    Tables::create_sys_namespaces_table(db.as_ref())
        .await
        .expect("create metadata table");
    create_shared_table(db.as_ref(), "default", 1).await;
    create_shared_table(db.as_ref(), "secondary", 2).await;
    put_location_dictionary(db.as_ref()).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(db.as_ref(), &namespace, 1).await;

    let source_provider = db
        .provider_for_connection_for_migration("default")
        .expect("default provider");

    let shared_source = Tables::shared_namespace(1);
    let tenant_pk = format!("{}#USER#1", namespace.as_str());
    let tenant_sk = "PROFILE#1".to_string();
    source_provider
        .put_item(
            shared_source,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(tenant_pk.clone())),
                ("sk".to_string(), AttributeValue::S(tenant_sk.clone())),
                (
                    "migrationpk".to_string(),
                    AttributeValue::S(migration_index_pk(&namespace, "USER")),
                ),
                (
                    "migrationsk".to_string(),
                    AttributeValue::S(migration_index_sk(&tenant_pk, &tenant_sk)),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("missing-updated-at".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write source row");

    db.run_job(GSI_UPDATE_JOB).await;

    let coordinator = DualWriteCoordinator::new(db.clone());
    let error = coordinator
        .backfill_via_migration_index(MigrationBackfillInput::new(
            namespace,
            storage_types::IndexName::new(MIGRATION_INDEX_NAME),
            1,
            2,
            vec!["USER".to_string()],
        ))
        .await
        .expect_err("missing updated_at must fail closed");
    assert!(
        error.to_string().contains("missing updated_at"),
        "expected fail-closed updated_at error, got: {error}"
    );

    let _ = std::fs::remove_file(default_path);
    let _ = std::fs::remove_file(secondary_path);
}

#[tokio::test]
async fn dual_write_coordinator_updates_migration_mode_and_cutover_status() {
    let (db, default_path, secondary_path) = new_multiloc_db().await;
    Tables::create_sys_namespaces_table(db.as_ref())
        .await
        .expect("create metadata table");
    create_shared_table(db.as_ref(), "default", 1).await;
    create_shared_table(db.as_ref(), "secondary", 2).await;
    put_location_dictionary(db.as_ref()).await;

    let namespace = TableNamespace::new();
    put_shared_namespace_route_metadata(db.as_ref(), &namespace, 1).await;

    let coordinator = DualWriteCoordinator::new(db.clone());
    let effective_at =
        TimestampMillis::from_timestamp(TimestampMillis::now().timestamp_millis() + 60_000);
    let migration_id = "mig_0001";

    coordinator
        .begin_dual_write(BeginDualWriteInput {
            namespace: namespace.clone(),
            migration_id: migration_id.to_string(),
            old_loc: 1,
            new_loc: 2,
            effective_at_ms: effective_at,
        })
        .await
        .expect("begin dual-write");

    let tenant_map = db
        .get_item_map(Tables::sys_namespaces(), tenant_meta_key(&namespace))
        .await
        .expect("get tenant metadata")
        .expect("tenant metadata exists");
    assert_eq!(migration_mode_name(&tenant_map), "dual_write".to_string());

    coordinator
        .complete_cutover(CompleteCutoverInput {
            namespace: namespace.clone(),
            migration_id: migration_id.to_string(),
            effective_at_ms: effective_at,
            new_loc: 2,
        })
        .await
        .expect("complete cutover");

    let updated_tenant_map = db
        .get_item_map(Tables::sys_namespaces(), tenant_meta_key(&namespace))
        .await
        .expect("get updated metadata")
        .expect("updated metadata exists");
    assert_eq!(item_loc(&updated_tenant_map), 2);
    assert_eq!(
        migration_mode_name(&updated_tenant_map),
        "single".to_string()
    );

    let encoded_ms = format!("{:020}", effective_at.timestamp_millis());
    let cutover_key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("SYS#CUTOVER".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S(format!(
                "CUTOVER#{encoded_ms}#{}#{migration_id}",
                namespace.storage_key()
            )),
        ),
    ]);
    let cutover_map = db
        .get_item_map(Tables::sys_namespaces(), cutover_key)
        .await
        .expect("get cutover item")
        .expect("cutover item exists");
    assert_eq!(
        cutover_map.get("status"),
        Some(&AttributeValue::S("applied".to_string()))
    );

    let _ = std::fs::remove_file(default_path);
    let _ = std::fs::remove_file(secondary_path);
}
