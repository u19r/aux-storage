#![cfg(feature = "sqlite")]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use storage_common::GSI_UPDATE_JOB;
use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection,
    ProjectionType, StorageResult, TableName, WireItem,
};
use tokio::time::timeout;

use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions, DatabaseManagerTestPauseHandle,
    InMemoryPointReadCache, InMemoryPointReadCacheConfig, PointReadBatchGetResult, PointReadCache,
    PointReadGetRequest, PointReadGetResult, PutItemInput, QueryIndexInput, UpdateItemInput,
};

#[derive(Debug, Default, Clone)]
struct ObservingPointReadCacheState {
    get_results: Vec<(PointReadGetRequest, PointReadGetResult)>,
}

#[derive(Clone)]
struct ObservingPointReadCache {
    inner: Arc<InMemoryPointReadCache>,
    state: Arc<Mutex<ObservingPointReadCacheState>>,
}

impl ObservingPointReadCache {
    fn new(inner: Arc<InMemoryPointReadCache>) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(ObservingPointReadCacheState::default())),
        }
    }

    fn snapshot(&self) -> ObservingPointReadCacheState {
        self.state
            .lock()
            .expect("lock observing cache state")
            .clone()
    }
}

#[async_trait]
impl PointReadCache for ObservingPointReadCache {
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }

    fn claim_write_version(&self) -> u64 {
        self.inner.claim_write_version()
    }

    async fn get_eventual(
        &self,
        request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult> {
        let result = self.inner.get_eventual(request).await?;
        let mut state = self.state.lock().expect("lock observing cache state");
        state.get_results.push((request.clone(), result.clone()));
        Ok(result)
    }

    async fn batch_get_eventual(
        &self,
        request: &storage_types::BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult> {
        self.inner.batch_get_eventual(request).await
    }

    async fn write_put(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        write_version: u64,
    ) -> StorageResult<()> {
        self.inner.write_put(request, item, write_version).await
    }

    async fn write_delete(
        &self,
        request: &PointReadGetRequest,
        write_version: u64,
    ) -> StorageResult<()> {
        self.inner.write_delete(request, write_version).await
    }

    async fn invalidate(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        self.inner.invalidate(request).await
    }
}

async fn create_db_with_point_read_cache_and_options(
    point_read_cache: Arc<dyn PointReadCache>,
    mut runtime_options: DatabaseManagerRuntimeOptions,
) -> DatabaseManager {
    runtime_options.enable_database_jobs = false;
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            immediate_gsi_consistency: false,
            force_file_backed_database: false,
        }),
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    DatabaseManager::new_with_config_and_runtime_options_and_point_read_cache(
        config,
        runtime_options,
        point_read_cache,
    )
    .await
    .expect("create test db with delayed-gsi sqlite")
}

async fn create_gsi_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi1pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi1sk".to_string(),
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
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi1pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi1sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }])),
    )
    .await
    .expect("create gsi table");
}

async fn put_gsi_item(
    db: &DatabaseManager,
    table_name: &TableName,
    pk: &str,
    gsi_pk: &str,
    gsi_sk: &str,
    payload: &str,
) {
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                ("gsi1pk".to_string(), AttributeValue::S(gsi_pk.to_string())),
                ("gsi1sk".to_string(), AttributeValue::S(gsi_sk.to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(payload.to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put gsi item");
}

async fn query_gsi_items(
    db: &DatabaseManager,
    table_name: &TableName,
    gsi_pk: &str,
) -> Vec<HashMap<String, AttributeValue>> {
    let (items, _lek) = db
        .query_index_map(
            QueryIndexInput::builder()
                .table_name(table_name.clone())
                .index_name(IndexName::new("gsi1"))
                .key_condition_expression("gsi1pk = :pk".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S(gsi_pk.to_string()),
                )]))
                .scan_index_forward(true)
                .build(),
        )
        .await
        .expect("query gsi");
    items
}

async fn eventual_get_item(
    db: &DatabaseManager,
    table_name: &TableName,
    pk: &str,
) -> Option<HashMap<String, AttributeValue>> {
    db.get_item_with_consistent_read(table_name.clone(), item_key(pk), false)
        .await
        .expect("eventual get succeeds")
        .map(|item| item.into_attribute_map().expect("wire item to map"))
}

fn item_key(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))])
}

fn string_attr(item: &HashMap<String, AttributeValue>, name: &str) -> String {
    match item.get(name) {
        Some(AttributeValue::S(value)) => value.clone(),
        other => panic!("expected string attribute {name}, got {other:?}"),
    }
}

fn query_item_pks(items: &[HashMap<String, AttributeValue>]) -> Vec<String> {
    items.iter().map(|item| string_attr(item, "pk")).collect()
}

#[tokio::test]
async fn queued_gsi_query_misses_recent_put_while_point_read_cache_serves_eventual_get() {
    let observing = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let db = create_db_with_point_read_cache_and_options(
        observing.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .run_gsi_maintenance_after_write(Some(false))
            .build(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_gsi_put_window");
    create_gsi_table(&db, &table_name).await;

    put_gsi_item(&db, &table_name, "item#1", "team#a", "001", "alpha").await;

    let before_drain = query_gsi_items(&db, &table_name, "team#a").await;
    assert!(
        before_drain.is_empty(),
        "queued GSI maintenance should leave recent put invisible to the index"
    );

    let eventual = eventual_get_item(&db, &table_name, "item#1")
        .await
        .expect("point read should see newly written item");
    assert_eq!(string_attr(&eventual, "payload"), "alpha");

    let observed = observing.snapshot();
    assert!(
        matches!(
            observed.get_results.last(),
            Some((_, PointReadGetResult::Hit(_)))
        ),
        "eventual get should be served by the point-read cache"
    );

    db.run_job(GSI_UPDATE_JOB).await;

    let after_drain = query_gsi_items(&db, &table_name, "team#a").await;
    assert_eq!(query_item_pks(&after_drain), vec!["item#1".to_string()]);
}

#[tokio::test]
async fn paused_post_write_window_allows_db_read_before_cache_warm_and_before_gsi_update() {
    let pause = DatabaseManagerTestPauseHandle::armed();
    let observing = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let db = Arc::new(
        create_db_with_point_read_cache_and_options(
            observing.clone(),
            DatabaseManagerRuntimeOptions::builder()
                .run_gsi_maintenance_after_write(Some(true))
                .pause_after_storage_write(Some(pause.clone()))
                .build(),
        )
        .await,
    );
    let table_name = TableName::new("point_read_cache_gsi_pause_window");
    create_gsi_table(&db, &table_name).await;

    let db_for_write = Arc::clone(&db);
    let table_for_write = table_name.clone();
    let write_task = tokio::spawn(async move {
        put_gsi_item(
            db_for_write.as_ref(),
            &table_for_write,
            "item#1",
            "team#a",
            "001",
            "alpha",
        )
        .await;
    });

    timeout(Duration::from_secs(5), pause.wait_until_reached())
        .await
        .expect("write should reach post-storage pause");

    let while_paused = eventual_get_item(&db, &table_name, "item#1")
        .await
        .expect("db should expose committed item even before cache warm");
    assert_eq!(string_attr(&while_paused, "payload"), "alpha");

    let before_resume_query = query_gsi_items(&db, &table_name, "team#a").await;
    assert!(
        before_resume_query.is_empty(),
        "GSI query should still miss while maintenance is paused"
    );

    let paused_observed = observing.snapshot();
    assert!(
        matches!(
            paused_observed.get_results.last(),
            Some((_, PointReadGetResult::Miss))
        ),
        "eventual read during pause should miss cache and fall through to storage"
    );

    pause.resume();
    write_task.await.expect("write task should finish");

    let after_resume = eventual_get_item(&db, &table_name, "item#1")
        .await
        .expect("item should still be readable after resume");
    assert_eq!(string_attr(&after_resume, "payload"), "alpha");

    let resumed_observed = observing.snapshot();
    assert!(
        matches!(
            resumed_observed.get_results.last(),
            Some((_, PointReadGetResult::Hit(_)))
        ),
        "eventual read after resume should hit the warmed cache"
    );

    let after_resume_query = query_gsi_items(&db, &table_name, "team#a").await;
    assert_eq!(
        query_item_pks(&after_resume_query),
        vec!["item#1".to_string()]
    );
}

#[tokio::test]
async fn queued_gsi_query_space_move_leaves_old_index_stale_until_drain_but_get_returns_post_image()
{
    let observing = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let db = create_db_with_point_read_cache_and_options(
        observing.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .run_gsi_maintenance_after_write(Some(false))
            .build(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_gsi_partition_move");
    create_gsi_table(&db, &table_name).await;
    put_gsi_item(&db, &table_name, "item#1", "team#a", "001", "alpha").await;
    db.run_job(GSI_UPDATE_JOB).await;

    db.update_item(
        UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(item_key("item#1"))
            .update_expression(
                "SET gsi1pk = :new_pk, gsi1sk = :new_sk, payload = :payload".to_string(),
            )
            .expression_attribute_values(HashMap::from([
                (
                    ":new_pk".to_string(),
                    AttributeValue::S("team#b".to_string()),
                ),
                (":new_sk".to_string(), AttributeValue::S("900".to_string())),
                (
                    ":payload".to_string(),
                    AttributeValue::S("beta".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("update gsi membership");

    let old_partition_before_drain = query_gsi_items(&db, &table_name, "team#a").await;
    assert_eq!(
        query_item_pks(&old_partition_before_drain),
        vec!["item#1".to_string()],
        "old GSI query space should remain stale until maintenance drains"
    );

    let new_partition_before_drain = query_gsi_items(&db, &table_name, "team#b").await;
    assert!(
        new_partition_before_drain.is_empty(),
        "new GSI query space should miss until maintenance drains"
    );

    let eventual = eventual_get_item(&db, &table_name, "item#1")
        .await
        .expect("point read should return updated post-image");
    assert_eq!(string_attr(&eventual, "gsi1pk"), "team#b");
    assert_eq!(string_attr(&eventual, "payload"), "beta");

    let observed = observing.snapshot();
    assert!(
        matches!(
            observed.get_results.last(),
            Some((_, PointReadGetResult::Hit(_)))
        ),
        "eventual post-update read should come from the warmed cache"
    );

    db.run_job(GSI_UPDATE_JOB).await;

    let old_partition_after_drain = query_gsi_items(&db, &table_name, "team#a").await;
    assert!(old_partition_after_drain.is_empty());

    let new_partition_after_drain = query_gsi_items(&db, &table_name, "team#b").await;
    assert_eq!(
        query_item_pks(&new_partition_after_drain),
        vec!["item#1".to_string()]
    );
}

#[tokio::test]
async fn queued_gsi_sort_key_rewrite_reorders_results_only_after_drain() {
    let db = create_db_with_point_read_cache_and_options(
        Arc::new(InMemoryPointReadCache::new(
            InMemoryPointReadCacheConfig::default(),
        )),
        DatabaseManagerRuntimeOptions::builder()
            .run_gsi_maintenance_after_write(Some(false))
            .build(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_gsi_sort_rewrite");
    create_gsi_table(&db, &table_name).await;
    put_gsi_item(&db, &table_name, "item#1", "team#a", "001", "alpha").await;
    put_gsi_item(&db, &table_name, "item#2", "team#a", "010", "beta").await;
    db.run_job(GSI_UPDATE_JOB).await;

    let before_update = query_gsi_items(&db, &table_name, "team#a").await;
    assert_eq!(
        query_item_pks(&before_update),
        vec!["item#1".to_string(), "item#2".to_string()]
    );

    db.update_item(
        UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(item_key("item#1"))
            .update_expression("SET gsi1sk = :new_sk".to_string())
            .expression_attribute_values(HashMap::from([(
                ":new_sk".to_string(),
                AttributeValue::S("999".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update gsi sort key");

    let before_drain = query_gsi_items(&db, &table_name, "team#a").await;
    assert_eq!(
        query_item_pks(&before_drain),
        vec!["item#1".to_string(), "item#2".to_string()],
        "queued rewrite should leave old GSI order visible until maintenance drains"
    );

    db.run_job(GSI_UPDATE_JOB).await;

    let after_drain = query_gsi_items(&db, &table_name, "team#a").await;
    assert_eq!(
        query_item_pks(&after_drain),
        vec!["item#2".to_string(), "item#1".to_string()]
    );
}
