use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use storage_cache::{
    CacheReadOutcome, CacheState, ObservedRead, ReadRequest, compare_observed_read,
};
use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BillingMode, CreateTableRequest,
    DurableAbsenceProof, DurableItemRevision, KeyAttributeType, KeyAttributes, KeySchemaElement,
    KeyType, KeysAndAttributes, ReturnValuesOldNewUpdated, StorageResult, TableName,
    TransactConditionCheckRequest, TransactEncodeItem, TransactEncodePutRequest,
    TransactPutRequest, TransactWriteItem, TransactWriteItemsEncodeRequest,
    TransactWriteItemsRequest, TryIntoWireItem, WireItem,
};

use crate::{
    AuthoritativePointReadHit, AuthoritativePointReadPurpose, AuthoritativePointReadResult,
    DatabaseManager, DatabaseManagerRuntimeOptions, InMemoryPointReadCache,
    InMemoryPointReadCacheConfig, PointReadBatchGetResult, PointReadCache,
    PointReadCacheEvictionPolicy, PointReadGetRequest, PointReadGetResult, PutItemInput,
    StorageAuthoritativeCacheOptions, UpdateItemInput,
};

#[derive(Debug, Default, Clone)]
struct RecordingPointReadCacheState {
    get_requests: Vec<PointReadGetRequest>,
    authoritative_get_requests: Vec<(PointReadGetRequest, AuthoritativePointReadPurpose)>,
    batch_requests: Vec<BatchGetItemRequest>,
    authoritative_batch_requests: Vec<(BatchGetItemRequest, AuthoritativePointReadPurpose)>,
    write_puts: Vec<(PointReadGetRequest, WireItem)>,
    write_puts_with_revision: Vec<(PointReadGetRequest, WireItem, DurableItemRevision)>,
    write_deletes: Vec<PointReadGetRequest>,
    write_deletes_with_absence_proof: Vec<(PointReadGetRequest, DurableAbsenceProof)>,
    invalidations: Vec<PointReadGetRequest>,
    next_get_result: Option<PointReadGetResult>,
    next_authoritative_get_result: Option<AuthoritativePointReadResult>,
    next_batch_result: Option<PointReadBatchGetResult>,
    next_authoritative_batch_result: Option<PointReadBatchGetResult>,
}

#[derive(Debug, Default)]
struct RecordingPointReadCache {
    state: Mutex<RecordingPointReadCacheState>,
}

impl RecordingPointReadCache {
    fn with_get_result(result: PointReadGetResult) -> Self {
        Self {
            state: Mutex::new(RecordingPointReadCacheState {
                next_get_result: Some(result),
                ..RecordingPointReadCacheState::default()
            }),
        }
    }

    fn with_batch_result(result: PointReadBatchGetResult) -> Self {
        Self {
            state: Mutex::new(RecordingPointReadCacheState {
                next_batch_result: Some(result),
                ..RecordingPointReadCacheState::default()
            }),
        }
    }

    fn with_authoritative_get_result(result: AuthoritativePointReadResult) -> Self {
        Self {
            state: Mutex::new(RecordingPointReadCacheState {
                next_authoritative_get_result: Some(result),
                ..RecordingPointReadCacheState::default()
            }),
        }
    }

    fn with_authoritative_batch_result(result: PointReadBatchGetResult) -> Self {
        Self {
            state: Mutex::new(RecordingPointReadCacheState {
                next_authoritative_batch_result: Some(result),
                ..RecordingPointReadCacheState::default()
            }),
        }
    }

    fn snapshot(&self) -> RecordingPointReadCacheState {
        self.state.lock().expect("lock cache state").clone()
    }
}

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
        request: &BatchGetItemRequest,
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

#[async_trait]
impl PointReadCache for RecordingPointReadCache {
    fn is_enabled(&self) -> bool {
        true
    }

    async fn get_eventual(
        &self,
        request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult> {
        let mut state = self.state.lock().expect("lock cache state");
        state.get_requests.push(request.clone());
        Ok(state
            .next_get_result
            .clone()
            .unwrap_or(PointReadGetResult::Miss))
    }

    async fn get_authoritative(
        &self,
        request: &PointReadGetRequest,
        purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<AuthoritativePointReadResult> {
        let mut state = self.state.lock().expect("lock cache state");
        state
            .authoritative_get_requests
            .push((request.clone(), purpose));
        Ok(state
            .next_authoritative_get_result
            .clone()
            .unwrap_or(AuthoritativePointReadResult::Miss))
    }

    async fn batch_get_eventual(
        &self,
        request: &BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult> {
        let mut state = self.state.lock().expect("lock cache state");
        state.batch_requests.push(request.clone());
        Ok(state
            .next_batch_result
            .clone()
            .unwrap_or(PointReadBatchGetResult {
                responses: HashMap::new(),
                unresolved_request_items: request.request_items.clone(),
            }))
    }

    async fn batch_get_authoritative(
        &self,
        request: &BatchGetItemRequest,
        purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<PointReadBatchGetResult> {
        let mut state = self.state.lock().expect("lock cache state");
        state
            .authoritative_batch_requests
            .push((request.clone(), purpose));
        Ok(state
            .next_authoritative_batch_result
            .clone()
            .unwrap_or(PointReadBatchGetResult {
                responses: HashMap::new(),
                unresolved_request_items: request.request_items.clone(),
            }))
    }

    async fn write_put(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        _write_version: u64,
    ) -> StorageResult<()> {
        let mut state = self.state.lock().expect("lock cache state");
        state.write_puts.push((request.clone(), item.clone()));
        Ok(())
    }

    async fn write_delete(
        &self,
        request: &PointReadGetRequest,
        _write_version: u64,
    ) -> StorageResult<()> {
        let mut state = self.state.lock().expect("lock cache state");
        state.write_deletes.push(request.clone());
        Ok(())
    }

    async fn write_put_with_revision(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        revision: DurableItemRevision,
        _write_version: u64,
    ) -> StorageResult<()> {
        let mut state = self.state.lock().expect("lock cache state");
        state
            .write_puts_with_revision
            .push((request.clone(), item.clone(), revision));
        Ok(())
    }

    async fn write_delete_with_absence_proof(
        &self,
        request: &PointReadGetRequest,
        proof: DurableAbsenceProof,
        _write_version: u64,
    ) -> StorageResult<()> {
        let mut state = self.state.lock().expect("lock cache state");
        state
            .write_deletes_with_absence_proof
            .push((request.clone(), proof));
        Ok(())
    }

    async fn invalidate(&self, request: &PointReadGetRequest) -> StorageResult<()> {
        let mut state = self.state.lock().expect("lock cache state");
        state.invalidations.push(request.clone());
        Ok(())
    }
}

async fn create_db_with_point_read_cache(
    point_read_cache: Arc<dyn PointReadCache>,
) -> DatabaseManager {
    create_db_with_point_read_cache_and_options(
        point_read_cache,
        DatabaseManagerRuntimeOptions::default(),
    )
    .await
}

async fn create_db_with_point_read_cache_and_options(
    point_read_cache: Arc<dyn PointReadCache>,
    runtime_options: DatabaseManagerRuntimeOptions,
) -> DatabaseManager {
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        sqlite: None,
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
    .expect("create test db with point-read cache")
}

async fn create_single_table_db_with_point_read_cache(
    point_read_cache: Arc<dyn PointReadCache>,
) -> DatabaseManager {
    create_db_with_point_read_cache_and_options(
        point_read_cache,
        DatabaseManagerRuntimeOptions::builder()
            .enable_single_table_mode(true)
            .build(),
    )
    .await
}

fn authoritative_write_preimage_options() -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions::builder()
        .authoritative_cache_options(StorageAuthoritativeCacheOptions {
            authoritative_write_preimages: true,
            ..StorageAuthoritativeCacheOptions::default()
        })
        .build()
}

async fn create_hash_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(&CreateTableRequest::new(
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
    ))
    .await
    .expect("create table");
}

async fn insert_item(db: &DatabaseManager, table_name: &TableName, pk: &str, payload: &str) {
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(payload.to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("insert item");
}

fn item_key(pk: &str) -> KeyAttributes {
    HashMap::from([("pk".to_string(), AttributeValue::S(pk.to_string()))]).into()
}

fn point_read_request(table_name: &TableName, pk: &str) -> PointReadGetRequest {
    PointReadGetRequest {
        table_name: table_name.clone(),
        key: item_key(pk),
    }
}

fn wire_item(pk: &str, payload: &str) -> WireItem {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
    .try_into_wire_item()
    .expect("build wire item")
}

fn cache_entry_weight_bytes(table_name: &TableName, pk: &str, payload: &str) -> usize {
    let key = item_key(pk);
    let key_json = serde_json::to_string(&key).expect("encode canonical test key");
    let item = wire_item(pk, payload);
    table_name.to_string().len() + key_json.len() + item.payload_len()
}

#[tokio::test]
async fn eventual_get_uses_point_read_cache_and_records_db_result() {
    let cache = Arc::new(RecordingPointReadCache::default());
    let db = create_single_table_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_get_eventual");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let baseline = cache.snapshot();

    let item = db
        .get_item_with_consistent_read(table_name.clone(), item_key("user#1"), false)
        .await
        .expect("eventual get succeeds");

    let state = cache.snapshot();
    assert_eq!(state.get_requests.len(), 1);
    assert_eq!(state.get_requests[0].table_name, table_name);
    assert_eq!(state.get_requests[0].key, item_key("user#1"));
    assert!(item.is_some(), "db result should be returned on cache miss");
    assert_eq!(state.write_puts.len(), baseline.write_puts.len());
    assert_eq!(state.write_deletes.len(), baseline.write_deletes.len());
    assert_eq!(state.invalidations.len(), baseline.invalidations.len());
}

#[tokio::test]
async fn in_memory_point_read_cache_evicts_by_byte_budget() {
    let table_name = TableName::new("point_read_cache_bytes");
    let item_weight = cache_entry_weight_bytes(&table_name, "k1", "aaaa");
    let cache = InMemoryPointReadCache::new(InMemoryPointReadCacheConfig {
        capacity: 10,
        max_bytes: (item_weight * 2) + (item_weight / 2),
        ttl: Duration::from_secs(300),
        eviction_policy: PointReadCacheEvictionPolicy::Lru,
    });

    cache
        .write_put(
            &point_read_request(&table_name, "k1"),
            &wire_item("k1", "aaaa"),
            1,
        )
        .await
        .expect("write first item");
    cache
        .write_put(
            &point_read_request(&table_name, "k2"),
            &wire_item("k2", "bbbb"),
            2,
        )
        .await
        .expect("write second item");
    cache
        .write_put(
            &point_read_request(&table_name, "k3"),
            &wire_item("k3", "cccc"),
            3,
        )
        .await
        .expect("write third item");

    let first = cache
        .get_eventual(&point_read_request(&table_name, "k1"))
        .await
        .expect("get first item");
    let second = cache
        .get_eventual(&point_read_request(&table_name, "k2"))
        .await
        .expect("get second item");
    let third = cache
        .get_eventual(&point_read_request(&table_name, "k3"))
        .await
        .expect("get third item");

    assert!(matches!(first, PointReadGetResult::Miss));
    assert!(matches!(second, PointReadGetResult::Hit(_)));
    assert!(matches!(third, PointReadGetResult::Hit(_)));
}

#[tokio::test]
async fn two_queue_resists_one_hit_scan_pollution() {
    let table_name = TableName::new("point_read_cache_two_queue");
    let hot_request = point_read_request(&table_name, "hot");

    let lru_cache = InMemoryPointReadCache::new(InMemoryPointReadCacheConfig {
        capacity: 3,
        max_bytes: 1024,
        ttl: Duration::from_secs(300),
        eviction_policy: PointReadCacheEvictionPolicy::Lru,
    });
    let two_queue_cache = InMemoryPointReadCache::new(InMemoryPointReadCacheConfig {
        capacity: 3,
        max_bytes: 1024,
        ttl: Duration::from_secs(300),
        eviction_policy: PointReadCacheEvictionPolicy::TwoQueue,
    });

    for cache in [&lru_cache, &two_queue_cache] {
        cache
            .write_put(&hot_request, &wire_item("hot", "alpha"), 1)
            .await
            .expect("seed hot item");
        let hot_hit = cache
            .get_eventual(&hot_request)
            .await
            .expect("read hot item");
        assert!(matches!(hot_hit, PointReadGetResult::Hit(_)));
    }

    for pk in ["scan-1", "scan-2", "scan-3"] {
        let request = point_read_request(&table_name, pk);
        let item = wire_item(pk, "scan");
        lru_cache
            .write_put(&request, &item, lru_cache.claim_write_version())
            .await
            .expect("populate lru scan item");
        two_queue_cache
            .write_put(&request, &item, two_queue_cache.claim_write_version())
            .await
            .expect("populate 2q scan item");
    }

    let lru_hot = lru_cache
        .get_eventual(&hot_request)
        .await
        .expect("read hot key from lru");
    let two_queue_hot = two_queue_cache
        .get_eventual(&hot_request)
        .await
        .expect("read hot key from two-queue");

    assert!(
        matches!(lru_hot, PointReadGetResult::Miss),
        "plain LRU should let one-hit scan traffic evict the hot item"
    );
    assert!(
        matches!(two_queue_hot, PointReadGetResult::Hit(_)),
        "two-queue should preserve the repeatedly used item"
    );
}

#[tokio::test]
async fn strong_get_bypasses_point_read_cache() {
    let cache = Arc::new(RecordingPointReadCache::default());
    let db = create_single_table_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_get_strong");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let baseline = cache.snapshot();

    let item = db
        .get_item_with_consistent_read(table_name, item_key("user#1"), true)
        .await
        .expect("strong get succeeds");

    let state = cache.snapshot();
    assert!(item.is_some(), "strong read should still hit storage");
    assert!(state.get_requests.is_empty());
    assert!(state.authoritative_get_requests.is_empty());
    assert_eq!(state.write_puts.len(), baseline.write_puts.len());
    assert_eq!(state.write_deletes.len(), baseline.write_deletes.len());
    assert_eq!(state.invalidations.len(), baseline.invalidations.len());
}

#[tokio::test]
async fn strong_get_uses_authoritative_point_read_cache_when_flag_enabled() {
    let cached_item_map = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("user#cached".to_string()),
        ),
        (
            "payload".to_string(),
            AttributeValue::S("from-authoritative-cache".to_string()),
        ),
    ]);
    let cached_item = cached_item_map
        .clone()
        .try_into_wire_item()
        .expect("build cached wire item");
    let cache = Arc::new(RecordingPointReadCache::with_authoritative_get_result(
        AuthoritativePointReadResult::Hit(Box::new(AuthoritativePointReadHit::Present {
            item: Box::new(cached_item),
            revision: None,
        })),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;

    let item = db
        .get_item_with_consistent_read(
            TableName::new("missing_table_for_authoritative_hit"),
            item_key("user#cached"),
            true,
        )
        .await
        .expect("authoritative cache hit should short-circuit storage");

    let state = cache.snapshot();
    assert_eq!(state.authoritative_get_requests.len(), 1);
    assert!(state.get_requests.is_empty());
    assert_eq!(
        item.expect("strong cache hit should return item")
            .into_attribute_map()
            .expect("decode item"),
        cached_item_map
    );
}

#[tokio::test]
async fn strong_get_uses_authoritative_absent_hit_when_flag_enabled() {
    let cache = Arc::new(RecordingPointReadCache::with_authoritative_get_result(
        AuthoritativePointReadResult::Hit(Box::new(AuthoritativePointReadHit::Absent {
            proof: None,
        })),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;

    let item = db
        .get_item_with_consistent_read(
            TableName::new("missing_table_for_authoritative_absence"),
            item_key("user#cached"),
            true,
        )
        .await
        .expect("authoritative absent cache hit should short-circuit storage");

    let state = cache.snapshot();
    assert_eq!(state.authoritative_get_requests.len(), 1);
    assert!(item.is_none());
}

#[tokio::test]
async fn strong_get_read_through_warms_present_hit_with_provider_revision() {
    let cache = Arc::new(RecordingPointReadCache::default());
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                strong_read_through_warming: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;
    let table_name = TableName::new("strong_read_through_present");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let baseline = cache.snapshot();

    let item = db
        .get_item_with_consistent_read(table_name.clone(), item_key("user#1"), true)
        .await
        .expect("strong get succeeds");

    let state = cache.snapshot();
    assert!(item.is_some());
    assert_eq!(state.authoritative_get_requests.len(), 1);
    assert_eq!(
        state.write_puts_with_revision.len(),
        baseline.write_puts_with_revision.len() + 1
    );
    let (request, warmed_item, revision) = state
        .write_puts_with_revision
        .last()
        .expect("read-through should warm present item with revision");
    assert_eq!(request.table_name, table_name);
    assert!(!revision.as_bytes().is_empty());
    assert_eq!(
        warmed_item
            .clone()
            .into_attribute_map()
            .expect("decode warmed item")
            .get("payload"),
        Some(&AttributeValue::S("alpha".to_string()))
    );
}

#[tokio::test]
async fn strong_get_read_through_warms_absent_hit_with_provider_proof() {
    let cache = Arc::new(RecordingPointReadCache::default());
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                strong_read_through_warming: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;
    let table_name = TableName::new("strong_read_through_absent");
    create_hash_table(&db, &table_name).await;

    let item = db
        .get_item_with_consistent_read(table_name.clone(), item_key("missing"), true)
        .await
        .expect("strong get succeeds");

    let state = cache.snapshot();
    assert!(item.is_none());
    assert_eq!(state.authoritative_get_requests.len(), 1);
    let (request, proof) = state
        .write_deletes_with_absence_proof
        .last()
        .expect("read-through should warm absence proof");
    assert_eq!(request.table_name, table_name);
    assert!(!proof.as_bytes().is_empty());
}

#[tokio::test]
async fn eventual_get_returns_cache_hit_without_storage_round_trip() {
    let cached_item_map = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("user#cached".to_string()),
        ),
        (
            "payload".to_string(),
            AttributeValue::S("from-cache".to_string()),
        ),
    ]);
    let cached_item = cached_item_map
        .clone()
        .try_into_wire_item()
        .expect("build cached wire item");
    let cache = Arc::new(RecordingPointReadCache::with_get_result(
        PointReadGetResult::Hit(Box::new(Some(cached_item.clone()))),
    ));
    let db = create_single_table_db_with_point_read_cache(cache.clone()).await;

    let item = db
        .get_item_with_consistent_read(
            TableName::new("point_read_cache_hit_without_table"),
            item_key("user#cached"),
            false,
        )
        .await
        .expect("cache hit should short-circuit storage");

    let state = cache.snapshot();
    let item = item.expect("cache hit should return an item");
    assert_eq!(
        item.into_attribute_map().expect("decode cache-hit item"),
        cached_item_map
    );
    assert_eq!(state.get_requests.len(), 1);
    assert!(state.write_puts.is_empty());
    assert!(state.write_deletes.is_empty());
    assert!(state.invalidations.is_empty());
}

#[tokio::test]
async fn eventual_batch_get_merges_cache_hits_and_records_only_db_misses() {
    let table_name = TableName::new("point_read_cache_batch_eventual");
    let cached_item_map = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user#1".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("from-cache".to_string()),
        ),
    ]);
    let cached_item = cached_item_map
        .try_into_wire_item()
        .expect("build cached batch item");
    let cache = Arc::new(RecordingPointReadCache::with_batch_result(
        PointReadBatchGetResult {
            responses: HashMap::from([(table_name.clone(), vec![cached_item.clone()])]),
            unresolved_request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#2")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(false),
                },
            )]),
        },
    ));
    let db = create_db_with_point_read_cache(cache.clone()).await;
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#2", "from-db").await;
    let baseline = cache.snapshot();

    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#1"), item_key("user#2")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(false),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .expect("eventual batch get succeeds");

    let state = cache.snapshot();
    let items = response
        .responses
        .and_then(|responses| responses.get(&table_name).cloned())
        .expect("merged response should include table items");
    assert_eq!(state.batch_requests.len(), 1);
    assert_eq!(state.write_puts.len(), baseline.write_puts.len());
    assert_eq!(state.write_deletes.len(), baseline.write_deletes.len());
    assert_eq!(state.invalidations.len(), baseline.invalidations.len());
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn strong_batch_get_bypasses_point_read_cache() {
    let cache = Arc::new(RecordingPointReadCache::default());
    let db = create_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_batch_strong");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let baseline = cache.snapshot();

    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#1")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .expect("strong batch get succeeds");

    let state = cache.snapshot();
    let items = response
        .responses
        .and_then(|responses| responses.get(&table_name).cloned())
        .expect("strong batch response should contain the table item");
    assert_eq!(items.len(), 1);
    assert!(state.batch_requests.is_empty());
    assert!(state.authoritative_batch_requests.is_empty());
    assert_eq!(state.write_puts.len(), baseline.write_puts.len());
    assert_eq!(state.write_deletes.len(), baseline.write_deletes.len());
    assert_eq!(state.invalidations.len(), baseline.invalidations.len());
}

#[tokio::test]
async fn strong_batch_get_uses_authoritative_cache_for_safe_subset_when_flag_enabled() {
    let table_name = TableName::new("strong_batch_authoritative");
    let cached_item_map = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user#1".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("from-authoritative-cache".to_string()),
        ),
    ]);
    let cached_item = cached_item_map
        .try_into_wire_item()
        .expect("build cached batch item");
    let cache = Arc::new(RecordingPointReadCache::with_authoritative_batch_result(
        PointReadBatchGetResult {
            responses: HashMap::from([(table_name.clone(), vec![cached_item])]),
            unresolved_request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#2")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
        },
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#2", "from-db").await;

    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#1"), item_key("user#2")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .expect("strong batch get succeeds");

    let state = cache.snapshot();
    let items = response
        .responses
        .and_then(|responses| responses.get(&table_name).cloned())
        .expect("merged response should include table items");
    assert_eq!(state.authoritative_batch_requests.len(), 1);
    assert!(state.batch_requests.is_empty());
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn strong_batch_get_read_through_warms_fallback_keys_with_provider_proofs() {
    let table_name = TableName::new("strong_batch_read_through");
    let cache = Arc::new(RecordingPointReadCache::with_authoritative_batch_result(
        PointReadBatchGetResult {
            responses: HashMap::new(),
            unresolved_request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#1"), item_key("missing")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
        },
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                strong_read_through_warming: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "from-db").await;

    let response = db
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![item_key("user#1"), item_key("missing")].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .expect("strong batch get succeeds");

    let state = cache.snapshot();
    let items = response
        .responses
        .and_then(|responses| responses.get(&table_name).cloned())
        .expect("response should include present durable item");
    assert_eq!(items.len(), 1);
    assert_eq!(state.write_puts_with_revision.len(), 1);
    assert_eq!(state.write_deletes_with_absence_proof.len(), 1);
    assert_eq!(state.write_puts_with_revision[0].0.key, item_key("user#1"));
    assert_eq!(
        state.write_deletes_with_absence_proof[0].0.key,
        item_key("missing")
    );
}

#[tokio::test]
async fn put_item_warms_in_memory_point_read_cache() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_single_table_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_put_warms");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;

    let cached = cache
        .get_eventual(&PointReadGetRequest {
            table_name: table_name.clone(),
            key: item_key("user#1"),
        })
        .await
        .expect("read from point cache");

    let PointReadGetResult::Hit(item) = cached else {
        panic!("put should warm the point-read cache");
    };
    let item = item.expect("put should cache a present item");
    let cached_item = item.into_attribute_map().expect("decode cached put item");
    assert_eq!(
        cached_item.get("pk"),
        Some(&AttributeValue::S("user#1".to_string()))
    );
    assert_eq!(
        cached_item.get("payload"),
        Some(&AttributeValue::S("alpha".to_string()))
    );
    assert!(
        matches!(cached_item.get("u_at"), Some(AttributeValue::N(_))),
        "put should cache the stamped updated_at field"
    );
}

#[tokio::test]
async fn transact_write_encode_put_replaces_authoritative_absence_with_present_item() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        DatabaseManagerRuntimeOptions::builder()
            .authoritative_cache_options(StorageAuthoritativeCacheOptions {
                authoritative_strong_point_reads: true,
                strong_read_through_warming: true,
                ..StorageAuthoritativeCacheOptions::default()
            })
            .build(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_transact_encode_absence_to_present");
    create_hash_table(&db, &table_name).await;

    let missing = db
        .get_item_with_consistent_read(table_name.clone(), item_key("user#1"), true)
        .await
        .expect("strong missing get succeeds");
    assert!(missing.is_none());

    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![TransactEncodeItem {
            put: Some(TransactEncodePutRequest {
                table_name: table_name.clone(),
                item: storage_types::WireEntity::unindexed(wire_item("user#1", "created")),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            update: None,
            delete: None,
            condition_check: None,
        }],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transact encode put succeeds");

    let item = db
        .get_item_with_consistent_read(table_name, item_key("user#1"), true)
        .await
        .expect("strong present get succeeds")
        .expect("transactional put should replace cached absence");
    assert_eq!(
        item.into_attribute_map()
            .expect("decode item")
            .get("payload"),
        Some(&AttributeValue::S("created".to_string()))
    );
}

#[tokio::test]
async fn delete_item_warms_negative_cache_entry() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_delete_negative");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;

    db.delete_item(
        crate::DeleteItemInput::builder()
            .table_name(table_name.clone())
            .key(item_key("user#1"))
            .build(),
    )
    .await
    .expect("delete item");

    let cached = cache
        .get_eventual(&PointReadGetRequest {
            table_name,
            key: item_key("user#1"),
        })
        .await
        .expect("read negative point cache");

    let PointReadGetResult::Hit(item) = cached else {
        panic!("delete should leave a negative cache entry");
    };
    assert!(item.is_none(), "delete should cache absence");
}

#[tokio::test]
async fn authoritative_point_read_misses_when_continuity_is_broken() {
    let cache = InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default());
    let table_name = TableName::new("point_read_cache_authoritative_continuity");
    let request = PointReadGetRequest {
        table_name,
        key: item_key("user#1"),
    };
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user#1".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("alpha".to_string()),
        ),
    ])
    .try_into_wire_item()
    .expect("build wire item");
    cache
        .write_put(&request, &item, cache.claim_write_version())
        .await
        .expect("write point-read cache");

    cache.mark_continuity_broken();

    let result = cache
        .get_authoritative(&request, AuthoritativePointReadPurpose::StrongGet)
        .await
        .expect("read authoritative cache");
    assert!(matches!(result, AuthoritativePointReadResult::Miss));
}

#[tokio::test]
async fn authoritative_point_read_misses_during_in_flight_write() {
    let cache = InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default());
    let table_name = TableName::new("point_read_cache_authoritative_in_flight");
    let request = PointReadGetRequest {
        table_name,
        key: item_key("user#1"),
    };
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user#1".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("alpha".to_string()),
        ),
    ])
    .try_into_wire_item()
    .expect("build wire item");
    cache
        .write_put(&request, &item, cache.claim_write_version())
        .await
        .expect("write point-read cache");

    cache
        .prepare_write(&request)
        .await
        .expect("prepare write intent");

    let result = cache
        .get_authoritative(&request, AuthoritativePointReadPurpose::StrongGet)
        .await
        .expect("read authoritative cache");
    assert!(matches!(result, AuthoritativePointReadResult::Miss));
}

#[tokio::test]
async fn update_item_warms_cached_point_read_with_post_image() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_single_table_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_update_warms");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;

    db.update_item(
        UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(item_key("user#1"))
            .update_expression("SET payload = :payload".to_string())
            .expression_attribute_values(HashMap::from([(
                ":payload".to_string(),
                AttributeValue::S("beta".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update item");

    let cached = cache
        .get_eventual(&PointReadGetRequest {
            table_name: table_name.clone(),
            key: item_key("user#1"),
        })
        .await
        .expect("read warmed point cache");

    let PointReadGetResult::Hit(item) = cached else {
        panic!("update should warm the point-read cache");
    };
    let item = item.expect("update should cache a present item");
    let cached_item = item
        .into_attribute_map()
        .expect("decode cached updated item");
    assert_eq!(
        cached_item.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
    assert!(
        matches!(cached_item.get("u_at"), Some(AttributeValue::N(_))),
        "update should retain the stamped updated_at field in cache"
    );
}

#[tokio::test]
async fn cached_conditional_put_failure_skips_durable_write() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        authoritative_write_preimage_options(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_cached_put_condition_false");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let request = point_read_request(&table_name, "user#1");
    cache
        .write_put_with_revision(
            &request,
            &wire_item("user#1", "alpha"),
            DurableItemRevision::new(1_i64.to_be_bytes().to_vec()),
            cache.claim_write_version(),
        )
        .await
        .expect("seed authoritative cache");

    let result = db
        .put_item(
            PutItemInput::builder()
                .table_name(table_name.clone())
                .item(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("user#1".to_string())),
                    ("payload".to_string(), AttributeValue::S("beta".to_string())),
                ]))
                .condition_expression("payload = :expected".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":expected".to_string(),
                    AttributeValue::S("missing".to_string()),
                )]))
                .build(),
        )
        .await;

    assert!(
        result.is_err(),
        "condition should fail from cached preimage"
    );
    let item = db
        .get_item_map(table_name, item_key("user#1"))
        .await
        .expect("read item")
        .expect("item remains");
    assert_eq!(
        item.get("payload"),
        Some(&AttributeValue::S("alpha".to_string()))
    );
}

#[tokio::test]
async fn cached_update_guard_conflict_falls_back_to_durable_path() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        authoritative_write_preimage_options(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_cached_update_guard_conflict");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let request = point_read_request(&table_name, "user#1");
    cache
        .write_put_with_revision(
            &request,
            &wire_item("user#1", "alpha"),
            DurableItemRevision::new(0_i64.to_be_bytes().to_vec()),
            cache.claim_write_version(),
        )
        .await
        .expect("seed stale authoritative cache");

    let response = db
        .update_item(
            UpdateItemInput::builder()
                .table_name(table_name.clone())
                .key(item_key("user#1"))
                .update_expression("SET payload = :payload".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":payload".to_string(),
                    AttributeValue::S("beta".to_string()),
                )]))
                .return_values(ReturnValuesOldNewUpdated::AllNew)
                .build(),
        )
        .await
        .expect("guard conflict falls back to durable update");

    let attributes = response.attributes.expect("all new attributes");
    assert_eq!(
        attributes.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
}

#[tokio::test]
async fn cached_update_uses_cached_preimage_and_return_values() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        authoritative_write_preimage_options(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_cached_update_success");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let request = point_read_request(&table_name, "user#1");
    cache
        .write_put_with_revision(
            &request,
            &wire_item("user#1", "alpha"),
            DurableItemRevision::new(1_i64.to_be_bytes().to_vec()),
            cache.claim_write_version(),
        )
        .await
        .expect("seed authoritative cache");

    let response = db
        .update_item(
            UpdateItemInput::builder()
                .table_name(table_name.clone())
                .key(item_key("user#1"))
                .update_expression("SET payload = :payload".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":payload".to_string(),
                    AttributeValue::S("beta".to_string()),
                )]))
                .return_values(ReturnValuesOldNewUpdated::AllOld)
                .build(),
        )
        .await
        .expect("cached guarded update succeeds");

    let attributes = response.attributes.expect("all old attributes");
    assert_eq!(
        attributes.get("payload"),
        Some(&AttributeValue::S("alpha".to_string()))
    );
    let cached = cache
        .get_authoritative(&request, AuthoritativePointReadPurpose::UpdatePreImage)
        .await
        .expect("read authoritative cached update");
    let AuthoritativePointReadResult::Hit(hit) = cached else {
        panic!("updated post-image should remain cached");
    };
    let AuthoritativePointReadHit::Present { item, .. } = *hit else {
        panic!("updated item should be present");
    };
    let item = item.into_attribute_map().expect("decode cached item");
    assert_eq!(
        item.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
}

#[tokio::test]
async fn cached_conditional_delete_uses_cached_preimage() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        authoritative_write_preimage_options(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_cached_delete_success");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let request = point_read_request(&table_name, "user#1");
    cache
        .write_put_with_revision(
            &request,
            &wire_item("user#1", "alpha"),
            DurableItemRevision::new(1_i64.to_be_bytes().to_vec()),
            cache.claim_write_version(),
        )
        .await
        .expect("seed authoritative cache");

    let deleted = db
        .delete_item(
            crate::DeleteItemInput::builder()
                .table_name(table_name.clone())
                .key(item_key("user#1"))
                .condition_expression("payload = :expected".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":expected".to_string(),
                    AttributeValue::S("alpha".to_string()),
                )]))
                .build(),
        )
        .await
        .expect("cached guarded delete succeeds")
        .expect("deleted item returned");

    assert_eq!(
        deleted.get("payload"),
        Some(&AttributeValue::S("alpha".to_string()))
    );
    assert!(
        db.get_item_map(table_name, item_key("user#1"))
            .await
            .expect("read deleted item")
            .is_none()
    );
}

#[tokio::test]
async fn cached_transaction_guard_conflict_falls_back_and_commits_atomically() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache_and_options(
        cache.clone(),
        authoritative_write_preimage_options(),
    )
    .await;
    let table_name = TableName::new("point_read_cache_cached_transaction_guard_conflict");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;
    let request = point_read_request(&table_name, "user#1");
    cache
        .write_put_with_revision(
            &request,
            &wire_item("user#1", "alpha"),
            DurableItemRevision::new(0_i64.to_be_bytes().to_vec()),
            cache.claim_write_version(),
        )
        .await
        .expect("seed stale authoritative cache");

    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![
            TransactWriteItem {
                condition_check: Some(TransactConditionCheckRequest {
                    table_name: table_name.clone(),
                    key: item_key("user#1"),
                    condition_expression: "payload = :expected".to_string(),
                    expression_attribute_names: None,
                    expression_attribute_values: Some(HashMap::from([(
                        ":expected".to_string(),
                        AttributeValue::S("alpha".to_string()),
                    )])),
                    return_values_on_condition_check_failure: None,
                }),
                ..TransactWriteItem::default()
            },
            TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: table_name.clone(),
                    item: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("user#2".to_string())),
                        ("payload".to_string(), AttributeValue::S("beta".to_string())),
                    ]),
                    indexers: None,
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..TransactWriteItem::default()
            },
        ],
        ..TransactWriteItemsRequest::default()
    })
    .await
    .expect("stale cached transaction guard falls back to durable transaction");

    let committed = db
        .get_item_map(table_name, item_key("user#2"))
        .await
        .expect("read committed transaction put")
        .expect("transaction put committed");
    assert_eq!(
        committed.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
}

#[tokio::test]
async fn update_item_rewrites_updated_new_response_while_warming_full_cache_entry() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_single_table_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_update_updated_new");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;

    let response = db
        .update_item(
            UpdateItemInput::builder()
                .table_name(table_name.clone())
                .key(item_key("user#1"))
                .update_expression("SET payload = :payload".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":payload".to_string(),
                    AttributeValue::S("beta".to_string()),
                )]))
                .return_values(ReturnValuesOldNewUpdated::UpdatedNew)
                .build(),
        )
        .await
        .expect("update item");

    let attributes = response
        .attributes
        .expect("updated-new response attributes");
    assert_eq!(
        attributes.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
    assert!(
        matches!(attributes.get("u_at"), Some(AttributeValue::N(_))),
        "updated-new response should include the injected updated_at field"
    );
    assert_eq!(
        attributes.len(),
        2,
        "response should only include updated fields"
    );

    let cached = cache
        .get_eventual(&PointReadGetRequest {
            table_name,
            key: item_key("user#1"),
        })
        .await
        .expect("read warmed point cache");

    let PointReadGetResult::Hit(item) = cached else {
        panic!("update should warm the point-read cache");
    };
    let item = item.expect("update should cache a present item");
    let cached_item = item
        .into_attribute_map()
        .expect("decode cached updated item");
    assert_eq!(
        cached_item.get("pk"),
        Some(&AttributeValue::S("user#1".to_string()))
    );
    assert_eq!(
        cached_item.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
}

#[tokio::test]
async fn update_item_preserves_all_old_response_and_still_warms_cache() {
    let cache = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let db = create_db_with_point_read_cache(cache.clone()).await;
    let table_name = TableName::new("point_read_cache_update_all_old");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "user#1", "alpha").await;

    let response = db
        .update_item(
            UpdateItemInput::builder()
                .table_name(table_name.clone())
                .key(item_key("user#1"))
                .update_expression("SET payload = :payload".to_string())
                .expression_attribute_values(HashMap::from([(
                    ":payload".to_string(),
                    AttributeValue::S("beta".to_string()),
                )]))
                .return_values(ReturnValuesOldNewUpdated::AllOld)
                .build(),
        )
        .await
        .expect("update item");

    let attributes = response.attributes.expect("all-old response attributes");
    assert_eq!(
        attributes.get("payload"),
        Some(&AttributeValue::S("alpha".to_string()))
    );

    let cached = cache
        .get_eventual(&PointReadGetRequest {
            table_name,
            key: item_key("user#1"),
        })
        .await
        .expect("read warmed point cache");

    let PointReadGetResult::Hit(item) = cached else {
        panic!("update should warm the point-read cache");
    };
    let item = item.expect("update should cache a present item");
    let cached_item = item
        .into_attribute_map()
        .expect("decode cached updated item");
    assert_eq!(
        cached_item.get("payload"),
        Some(&AttributeValue::S("beta".to_string()))
    );
}

#[tokio::test]
async fn oracle_eventual_get_matches_write_warmed_runtime_cache() {
    let inner = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let observing = Arc::new(ObservingPointReadCache::new(inner));
    let db = create_db_with_point_read_cache(observing.clone()).await;
    let table_name = TableName::new("point_read_cache_oracle_put");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "1", "alpha").await;

    let item = db
        .get_item_with_consistent_read(table_name, item_key("1"), false)
        .await
        .expect("eventual get succeeds after write-through put");
    let observed = observing.snapshot();
    let last = observed
        .get_results
        .last()
        .expect("eventual get should touch point cache");

    let mut oracle = CacheState::authoritative_leader_base_state();
    oracle.db_present.insert(1);
    oracle.leader.items.payload_keys.insert(1);
    compare_observed_read(
        &oracle,
        &ReadRequest::Get {
            slot: 1,
            strong: false,
            request_epoch: oracle.fresh_request_epoch(),
        },
        &ObservedRead::Get {
            outcome: match last.1 {
                PointReadGetResult::Hit(_) => CacheReadOutcome::ServeCache,
                PointReadGetResult::Miss => CacheReadOutcome::FallbackDb,
            },
            slot_present: item.is_some(),
        },
    )
    .expect("runtime point-read outcome should match oracle");
}

#[tokio::test]
async fn oracle_eventual_get_matches_negative_cache_after_delete() {
    let inner = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let observing = Arc::new(ObservingPointReadCache::new(inner));
    let db = create_db_with_point_read_cache(observing.clone()).await;
    let table_name = TableName::new("point_read_cache_oracle_delete");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "1", "alpha").await;
    db.delete_item(
        crate::DeleteItemInput::builder()
            .table_name(table_name.clone())
            .key(item_key("1"))
            .build(),
    )
    .await
    .expect("delete item");

    let item = db
        .get_item_with_consistent_read(table_name, item_key("1"), false)
        .await
        .expect("eventual get succeeds after delete");
    let observed = observing.snapshot();
    let last = observed
        .get_results
        .last()
        .expect("eventual get should touch point cache");

    let mut oracle = CacheState::authoritative_leader_base_state();
    oracle.leader.items.negative_keys.insert(1);
    compare_observed_read(
        &oracle,
        &ReadRequest::Get {
            slot: 1,
            strong: false,
            request_epoch: oracle.fresh_request_epoch(),
        },
        &ObservedRead::Get {
            outcome: match last.1 {
                PointReadGetResult::Hit(_) => CacheReadOutcome::ServeCache,
                PointReadGetResult::Miss => CacheReadOutcome::FallbackDb,
            },
            slot_present: item.is_some(),
        },
    )
    .expect("runtime negative-cache outcome should match oracle");
}

#[tokio::test]
async fn oracle_eventual_get_matches_write_warmed_runtime_cache_after_update() {
    let inner = Arc::new(InMemoryPointReadCache::new(
        InMemoryPointReadCacheConfig::default(),
    ));
    let observing = Arc::new(ObservingPointReadCache::new(inner));
    let db = create_db_with_point_read_cache(observing.clone()).await;
    let table_name = TableName::new("point_read_cache_oracle_update");
    create_hash_table(&db, &table_name).await;
    insert_item(&db, &table_name, "1", "alpha").await;
    db.update_item(
        UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(item_key("1"))
            .update_expression("SET payload = :payload".to_string())
            .expression_attribute_values(HashMap::from([(
                ":payload".to_string(),
                AttributeValue::S("beta".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update item");

    let item = db
        .get_item_with_consistent_read(table_name, item_key("1"), false)
        .await
        .expect("eventual get succeeds after write-warmed update");
    let observed = observing.snapshot();
    let last = observed
        .get_results
        .last()
        .expect("eventual get should touch point cache");

    let mut oracle = CacheState::authoritative_leader_base_state();
    oracle.db_present.insert(1);
    oracle.leader.items.payload_keys.insert(1);
    compare_observed_read(
        &oracle,
        &ReadRequest::Get {
            slot: 1,
            strong: false,
            request_epoch: oracle.fresh_request_epoch(),
        },
        &ObservedRead::Get {
            outcome: match last.1 {
                PointReadGetResult::Hit(_) => CacheReadOutcome::ServeCache,
                PointReadGetResult::Miss => CacheReadOutcome::FallbackDb,
            },
            slot_present: item.is_some(),
        },
    )
    .expect("runtime update outcome should match oracle");
}
