use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use storage_cache::{
    CacheReadOutcome, CacheState, GsiQuerySpace, ObservedRead, PartitionId, QueryDirection,
    QueryRequest, QueryTarget, ReadRequest, RuntimeQueryProofFallbackReason, Transition,
    TransitionRange, compare_observed_read,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest,
    BillingMode, CreateGlobalSecondaryIndex, CreateTableRequest, EncodePutRequest,
    EncodeWriteRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection,
    ProjectionType, PutRequest, StorageResult, TableName, TransactEncodeItem,
    TransactEncodePutRequest, TransactUpdateRequest, TransactWriteItem,
    TransactWriteItemsEncodeRequest, TransactWriteItemsRequest, TryIntoWireItem, WireItem,
    WriteRequest,
};

use crate::{
    DatabaseManager, InMemoryPointReadCache, InMemoryPointReadCacheConfig, InMemoryQueryProofCache,
    InMemoryQueryProofCacheConfig, PointReadBatchGetResult, PointReadCache, PointReadGetRequest,
    PointReadGetResult, QueryIndexInput, QueryManifestKey, QueryProofCache, QueryTableInput,
    noop_point_read_cache,
};

#[derive(Debug, Default, Clone)]
struct ObservingPointReadCacheState {
    get_requests: Vec<PointReadGetRequest>,
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

    fn reset(&self) {
        *self.state.lock().expect("lock observing cache state") =
            ObservingPointReadCacheState::default();
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
        state.get_requests.push(request.clone());
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

fn base_table_info(table_name: &TableName) -> storage_types::StoredTableInfo {
    storage_types::StoredTableInfo {
        table_name: table_name.clone(),
        table_status: storage_types::TableStatus::Active,
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

fn manifest_key(table_name: &TableName, pk: &str) -> QueryManifestKey {
    QueryManifestKey {
        table_name: table_name.clone(),
        index_name: None,
        partition_key_json: serde_json::to_string(&AttributeValue::S(pk.to_string()))
            .expect("encode partition key"),
    }
}

fn item(pk: &str, sk: &str, payload: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
}

fn gsi_item(
    pk: &str,
    sk: &str,
    gsi_pk: &str,
    gsi_sk: &str,
    payload: &str,
) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
        ("gsi1pk".to_string(), AttributeValue::S(gsi_pk.to_string())),
        ("gsi1sk".to_string(), AttributeValue::S(gsi_sk.to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
}

fn string_sort_order_repr(value: &str) -> String {
    format!("s:{value}")
}

fn numeric_item(pk: &str, sk: &str, payload: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::N(sk.to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
}

fn base_oracle_query(limit: usize) -> QueryRequest {
    QueryRequest {
        lower_bound: 0,
        upper_bound: 1,
        start_exclusive: -1,
        limit,
        byte_budget: 1024,
        only_even: false,
        direction: QueryDirection::Forward,
        target: QueryTarget::Base,
        partition: PartitionId::Left,
    }
}

fn primary_gsi_oracle_query(limit: usize) -> QueryRequest {
    QueryRequest {
        lower_bound: 0,
        upper_bound: 1,
        start_exclusive: -1,
        limit,
        byte_budget: 1024,
        only_even: false,
        direction: QueryDirection::Forward,
        target: QueryTarget::Gsi(GsiQuerySpace::Primary),
        partition: PartitionId::Left,
    }
}

fn apply_oracle_transitions(mut state: CacheState, transitions: &[Transition]) -> CacheState {
    for transition in transitions {
        state = state
            .try_apply(*transition)
            .expect("oracle transition should be valid");
    }
    state
}

async fn create_pk_sk_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(&CreateTableRequest::new(
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
    .expect("create pk/sk table");
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
                    attribute_name: "sk".to_string(),
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

async fn create_numeric_pk_sk_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
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
    .expect("create numeric pk/sk table");
}

async fn query_partition(
    db: &DatabaseManager,
    table_name: &TableName,
    pk: &str,
    limit: Option<u32>,
    exclusive_start_key: Option<String>,
) -> (Vec<HashMap<String, AttributeValue>>, Option<String>) {
    db.query_table_map(QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S(pk.to_string()),
        )])),
        limit,
        exclusive_start_key,
        scan_index_forward: Some(true),
        consistent_read: false,
    })
    .await
    .expect("query partition")
}

async fn query_gsi_partition(
    db: &DatabaseManager,
    table_name: &TableName,
    gsi_pk: &str,
    limit: Option<u32>,
    exclusive_start_key: Option<String>,
) -> (Vec<HashMap<String, AttributeValue>>, Option<String>) {
    db.query_index_map(QueryIndexInput {
        table_name: table_name.clone(),
        index_name: IndexName::new("gsi1"),
        key_condition_expression: "gsi1pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S(gsi_pk.to_string()),
        )])),
        projection_expression: None,
        limit,
        exclusive_start_key,
        scan_index_forward: Some(true),
    })
    .await
    .expect("query gsi partition")
}

fn request_to_plan_request(input: &QueryTableInput) -> storage_types::QueryTableRequest {
    storage_types::QueryTableRequest::from(QueryTableInput {
        table_name: input.table_name.clone(),
        key_condition_expression: input.key_condition_expression.clone(),
        expression_attribute_names: input.expression_attribute_names.clone(),
        expression_attribute_values: input.expression_attribute_values.clone(),
        limit: input.limit,
        exclusive_start_key: input.exclusive_start_key.clone(),
        scan_index_forward: input.scan_index_forward,
        consistent_read: input.consistent_read,
    })
}

fn request_to_index_plan_request(input: &QueryIndexInput) -> storage_types::QueryTableRequest {
    storage_types::QueryTableRequest::from(QueryIndexInput {
        table_name: input.table_name.clone(),
        index_name: input.index_name.clone(),
        key_condition_expression: input.key_condition_expression.clone(),
        expression_attribute_names: input.expression_attribute_names.clone(),
        expression_attribute_values: input.expression_attribute_values.clone(),
        projection_expression: input.projection_expression.clone(),
        limit: input.limit,
        exclusive_start_key: input.exclusive_start_key.clone(),
        scan_index_forward: input.scan_index_forward,
    })
}

fn string_attr(item: &HashMap<String, AttributeValue>, name: &str) -> String {
    match item.get(name) {
        Some(AttributeValue::S(value)) => value.clone(),
        other => panic!("expected string attribute {name}, got {other:?}"),
    }
}

fn query_item_sks(items: &[HashMap<String, AttributeValue>]) -> Vec<String> {
    items.iter().map(|item| string_attr(item, "sk")).collect()
}

#[tokio::test]
async fn in_memory_query_proof_cache_tracks_base_manifest_membership() {
    let cache = InMemoryQueryProofCache::new(InMemoryQueryProofCacheConfig::default());
    let table_name = TableName::new("query_proof_membership");
    let table_info = base_table_info(&table_name);

    cache
        .record_base_put(
            &table_name,
            &table_info,
            &WireItem::from_attribute_map(&item("tenant#1", "a", "alpha")).expect("wire item"),
        )
        .await
        .expect("record first put");
    cache
        .record_base_put(
            &table_name,
            &table_info,
            &WireItem::from_attribute_map(&item("tenant#1", "b", "beta")).expect("wire item"),
        )
        .await
        .expect("record second put");

    let snapshot = cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("snapshot should exist");

    assert_eq!(snapshot.entries.len(), 2);
    assert!(snapshot.coverage.covered_ranges.is_empty());
    assert!(snapshot.coverage.current_schema_ranges.is_empty());
}

#[tokio::test]
async fn in_memory_query_proof_cache_evicts_whole_partition_groups() {
    let cache = InMemoryQueryProofCache::new(InMemoryQueryProofCacheConfig {
        max_query_spaces: 1,
        max_manifest_entries: 8,
        max_coverage_ranges: 8,
        ..InMemoryQueryProofCacheConfig::default()
    });
    let table_name = TableName::new("query_proof_partition_lru");
    let table_info = base_table_info(&table_name);

    cache
        .record_base_put(
            &table_name,
            &table_info,
            &WireItem::from_attribute_map(&item("tenant#1", "a", "alpha")).expect("wire item"),
        )
        .await
        .expect("record first partition");
    cache
        .record_base_put(
            &table_name,
            &table_info,
            &WireItem::from_attribute_map(&item("tenant#2", "a", "beta")).expect("wire item"),
        )
        .await
        .expect("record second partition");

    assert!(
        cache
            .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
            .is_none()
    );
    assert!(
        cache
            .snapshot_base_partition(&manifest_key(&table_name, "tenant#2"))
            .is_some()
    );
}

#[tokio::test]
async fn database_manager_writes_populate_conservative_query_proof_state() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_runtime");
    create_pk_sk_table(&db, &table_name).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("tenant#1", "sk#1", "alpha"))
            .build(),
    )
    .await
    .expect("put item");

    let after_put = query_proof_cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("partition should exist after put");
    assert_eq!(after_put.entries.len(), 1);
    assert!(after_put.coverage.covered_ranges.is_empty());
    assert!(after_put.coverage.current_schema_ranges.is_empty());

    db.update_item(
        crate::UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([
                ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
                ("sk".to_string(), AttributeValue::S("sk#1".to_string())),
            ]))
            .update_expression("SET payload = :payload".to_string())
            .expression_attribute_values(HashMap::from([(
                ":payload".to_string(),
                AttributeValue::S("beta".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update item");

    let after_update = query_proof_cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("partition should still exist after update");
    assert_eq!(after_update.entries.len(), 1);
    assert!(after_update.coverage.covered_ranges.is_empty());
    assert!(after_update.coverage.current_schema_ranges.is_empty());

    db.delete_item(
        crate::DeleteItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([
                ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
                ("sk".to_string(), AttributeValue::S("sk#1".to_string())),
            ]))
            .build(),
    )
    .await
    .expect("delete item");

    assert!(
        query_proof_cache
            .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
            .is_none()
    );
}

#[tokio::test]
async fn database_manager_base_query_populates_manifest_and_coverage() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_query_fill");
    create_pk_sk_table(&db, &table_name).await;

    for (sk, payload) in [("sk#1", "alpha"), ("sk#2", "beta"), ("sk#3", "gamma")] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, payload))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let (_page, lek) = query_partition(&db, &table_name, "tenant#1", Some(2), None).await;

    let snapshot = query_proof_cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("snapshot should exist after query");

    assert_eq!(snapshot.entries.len(), 3);
    assert_eq!(snapshot.coverage.covered_ranges.len(), 1);
    assert_eq!(snapshot.coverage.current_schema_ranges.len(), 1);
    assert!(
        snapshot.coverage.covered_ranges[0]
            .start_after_exclusive
            .is_none()
    );
    assert!(
        snapshot.coverage.covered_ranges[0]
            .start_inclusive
            .is_none()
    );
    assert_eq!(
        snapshot.coverage.covered_ranges[0].end_inclusive,
        Some(string_sort_order_repr("sk#2"))
    );
    assert!(lek.is_some());
}

#[tokio::test]
async fn database_manager_second_query_page_extends_query_proof() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_query_second_page");
    create_pk_sk_table(&db, &table_name).await;

    for (sk, payload) in [("sk#1", "alpha"), ("sk#2", "beta"), ("sk#3", "gamma")] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, payload))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let (_first_page, lek) = query_partition(&db, &table_name, "tenant#1", Some(2), None).await;
    let (_second_page, second_lek) =
        query_partition(&db, &table_name, "tenant#1", Some(2), lek).await;

    let snapshot = query_proof_cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("snapshot should exist after second query");

    assert_eq!(snapshot.entries.len(), 3);
    assert_eq!(snapshot.coverage.covered_ranges.len(), 2);
    assert!(snapshot.coverage.covered_ranges.iter().any(|range| {
        range.start_after_exclusive.is_none()
            && range.start_inclusive.is_none()
            && range.end_inclusive == Some(string_sort_order_repr("sk#2"))
    }));
    assert!(
        snapshot
            .coverage
            .covered_ranges
            .iter()
            .any(
                |range| range.start_after_exclusive == Some(string_sort_order_repr("sk#2"))
                    && range.start_inclusive == Some(string_sort_order_repr("sk#3"))
                    && range.end_inclusive.is_none()
            )
    );
    assert!(second_lek.is_none());
}

#[tokio::test]
async fn database_manager_begins_with_query_only_warms_manifest_not_coverage() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_begins_with");
    create_pk_sk_table(&db, &table_name).await;

    for (sk, payload) in [("prefix#1", "alpha"), ("prefix#2", "beta")] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, payload))
                .build(),
        )
        .await
        .expect("seed item");
    }

    db.query_table_map(
        QueryTableInput::builder()
            .table_name(table_name.clone())
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)".to_string())
            .expression_attribute_values(HashMap::from([
                (":pk".to_string(), AttributeValue::S("tenant#1".to_string())),
                (
                    ":prefix".to_string(),
                    AttributeValue::S("prefix#".to_string()),
                ),
            ]))
            .scan_index_forward(true)
            .build(),
    )
    .await
    .expect("query begins_with partition");

    let snapshot = query_proof_cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("snapshot should exist after begins_with query");

    assert_eq!(snapshot.entries.len(), 2);
    assert!(snapshot.coverage.covered_ranges.is_empty());
    assert!(snapshot.coverage.current_schema_ranges.is_empty());
}

#[tokio::test]
async fn database_manager_query_plan_would_serve_covered_first_page() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_plan_page_one");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    db.query_table_map(QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    })
    .await
    .expect("warm first query page");

    let table_info = db.get_table_info(&table_name).await.expect("table info");
    let plan = query_proof_cache
        .plan_query_read(&table_name, &table_info, &request.into())
        .await
        .expect("plan query read");

    assert!(plan.would_serve_whole_page);
    assert_eq!(plan.fallback_reason, None);
    assert_eq!(plan.cache_candidate_count, 2);
}

#[tokio::test]
async fn database_manager_query_plan_rejects_page_beyond_covered_prefix() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_plan_longer_page");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    db.query_table_map(QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    })
    .await
    .expect("warm first query page");

    let longer_request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(3),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let table_info = db.get_table_info(&table_name).await.expect("table info");
    let plan = query_proof_cache
        .plan_query_read(&table_name, &table_info, &longer_request.into())
        .await
        .expect("plan query read");

    assert!(!plan.would_serve_whole_page);
    assert_eq!(
        plan.fallback_reason,
        Some(RuntimeQueryProofFallbackReason::PageBoundaryUnknown)
    );
    assert_eq!(plan.cache_candidate_count, 2);
}

#[tokio::test]
async fn database_manager_query_plan_serves_composed_adjacent_pages() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_plan_composed_pages");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let (_first_page, lek) = query_partition(&db, &table_name, "tenant#1", Some(2), None).await;
    let (_second_page, _) = query_partition(&db, &table_name, "tenant#1", Some(2), lek).await;

    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(3),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let table_info = db.get_table_info(&table_name).await.expect("table info");
    let plan = query_proof_cache
        .plan_query_read(&table_name, &table_info, &request.into())
        .await
        .expect("plan composed query read");

    assert!(plan.would_serve_whole_page);
    assert_eq!(plan.fallback_reason, None);
    assert_eq!(plan.cache_candidate_count, 3);
}

#[tokio::test]
async fn query_plan_matches_oracle_for_two_page_prefix_extension() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_oracle_prefix_extension");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let table_info = db.get_table_info(&table_name).await.expect("table info");

    let (_first_page, lek) = query_partition(&db, &table_name, "tenant#1", Some(1), None).await;
    let first_request: storage_types::QueryTableRequest = request_to_plan_request(&request);
    let first_plan = query_proof_cache
        .plan_query_read(&table_name, &table_info, &first_request)
        .await
        .expect("plan first prefix query");

    let mut first_state = CacheState {
        db_present: [0_u8, 1_u8].into_iter().collect(),
        ..CacheState::default()
    };
    first_state.leader.items.manifest_keys = [0_u8, 1_u8].into_iter().collect();
    first_state.leader.items.covered_slots = [0_u8].into_iter().collect();
    first_state.leader.items.current_schema_covered_slots = [0_u8].into_iter().collect();
    let first_decision = first_state.query_decision(&base_oracle_query(2), false, 0);

    assert_eq!(
        first_plan.would_serve_whole_page,
        first_decision.serve_whole_page
    );
    assert_eq!(
        first_plan.cache_candidate_count,
        first_decision.cache_evaluated_keys.len()
    );
    assert_eq!(first_decision.outcome, CacheReadOutcome::Mixed);

    let (_second_page, _second_lek) =
        query_partition(&db, &table_name, "tenant#1", Some(1), lek).await;
    let second_request: storage_types::QueryTableRequest = request_to_plan_request(&request);
    let second_plan = query_proof_cache
        .plan_query_read(&table_name, &table_info, &second_request)
        .await
        .expect("plan second prefix query");

    let mut second_state = CacheState {
        db_present: [0_u8, 1_u8].into_iter().collect(),
        ..CacheState::default()
    };
    second_state.leader.items.manifest_keys = [0_u8, 1_u8].into_iter().collect();
    second_state.leader.items.covered_slots = [0_u8, 1_u8].into_iter().collect();
    second_state.leader.items.current_schema_covered_slots = [0_u8, 1_u8].into_iter().collect();
    let second_decision = second_state.query_decision(&base_oracle_query(2), false, 0);

    assert_eq!(
        second_plan.would_serve_whole_page,
        second_decision.serve_whole_page
    );
    assert_eq!(
        second_plan.cache_candidate_count,
        second_decision.cache_evaluated_keys.len()
    );
    assert_eq!(second_decision.outcome, CacheReadOutcome::ServeCache);
}

#[tokio::test]
async fn query_proof_cache_materializes_ordered_primary_keys_and_lek() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_materialize_page");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (_page, _lek) = query_partition(&db, &table_name, "tenant#1", Some(2), None).await;
    let table_info = db.get_table_info(&table_name).await.expect("table info");
    let materialized = query_proof_cache
        .materialize_query_read(&table_name, &table_info, &request_to_plan_request(&request))
        .await
        .expect("materialize query page")
        .expect("materialized page");

    assert_eq!(
        materialized
            .primary_keys
            .iter()
            .map(|key| key.get("sk").cloned().expect("sk"))
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#1".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );
    assert!(materialized.last_evaluated_key.is_some());
}

#[tokio::test]
async fn database_manager_query_uses_materialized_cache_page_when_proof_and_payloads_exist() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        observing_cache.clone(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with caches");
    let table_name = TableName::new("query_proof_cache_served_page");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let first = db
        .query_table_map(QueryTableInput {
            table_name: request.table_name.clone(),
            key_condition_expression: request.key_condition_expression.clone(),
            expression_attribute_names: request.expression_attribute_names.clone(),
            expression_attribute_values: request.expression_attribute_values.clone(),
            limit: request.limit,
            exclusive_start_key: request.exclusive_start_key.clone(),
            scan_index_forward: request.scan_index_forward,
            consistent_read: request.consistent_read,
        })
        .await
        .expect("warm proof from db");
    assert_eq!(
        query_item_sks(&first.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(first.1.is_some());
    assert!(observing_cache.snapshot().get_requests.is_empty());

    let second = db
        .query_table_map(request)
        .await
        .expect("serve proven page from cache");
    assert_eq!(
        query_item_sks(&second.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert_eq!(second.1, first.1);

    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 2);
    assert_eq!(
        observed
            .get_requests
            .iter()
            .map(|request| {
                request
                    .key
                    .get("sk")
                    .cloned()
                    .expect("query cache request should include sort key")
            })
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#1".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );
}

#[tokio::test]
async fn database_manager_query_uses_cached_prefix_and_db_suffix_for_partial_coverage() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_cache_prefix_suffix");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let warm = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    db.query_table_map(warm)
        .await
        .expect("warm first page coverage");
    assert!(observing_cache.snapshot().get_requests.is_empty());

    let second = db
        .query_table_map(QueryTableInput {
            table_name: table_name.clone(),
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )])),
            limit: Some(3),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("use cached prefix and db suffix");

    assert_eq!(
        query_item_sks(&second.0),
        vec!["sk#1".to_string(), "sk#2".to_string(), "sk#3".to_string()]
    );
    assert!(second.1.is_none());

    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 2);
    assert_eq!(
        observed
            .get_requests
            .iter()
            .map(|request| {
                request
                    .key
                    .get("sk")
                    .cloned()
                    .expect("cached prefix request should include sort key")
            })
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#1".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );
}

#[tokio::test]
async fn gsi_query_proof_materializes_ordered_primary_keys_and_lek() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_gsi_materialize_page");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_sk) in [
        ("item#0", "sk#1", "001"),
        ("item#1", "sk#2", "002"),
        ("item#2", "sk#3", "003"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, "team#1", gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    let request = QueryIndexInput {
        table_name: table_name.clone(),
        index_name: IndexName::new("gsi1"),
        key_condition_expression: "gsi1pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("team#1".to_string()),
        )])),
        projection_expression: None,
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
    };

    let (_page, lek) = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert!(lek.is_some());
    let table_info = db.get_table_info(&table_name).await.expect("table info");
    let materialized = query_proof_cache
        .materialize_query_read(
            &table_name,
            &table_info,
            &request_to_index_plan_request(&request),
        )
        .await
        .expect("materialize gsi query page")
        .expect("materialized gsi page");

    assert_eq!(
        materialized
            .primary_keys
            .iter()
            .map(|key| key.get("sk").cloned().expect("sk"))
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#1".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );
    assert!(materialized.last_evaluated_key.is_some());
}

#[tokio::test]
async fn database_manager_gsi_query_uses_materialized_cache_page_when_proof_exists() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_cache_served_page");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_sk) in [
        ("item#0", "sk#1", "001"),
        ("item#1", "sk#2", "002"),
        ("item#2", "sk#3", "003"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, "team#1", gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    let first = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&first.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(first.1.is_some());
    assert!(observing_cache.snapshot().get_requests.is_empty());

    let second = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&second.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert_eq!(second.1, first.1);

    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 2);
    assert_eq!(
        observed
            .get_requests
            .iter()
            .map(|request| {
                request
                    .key
                    .get("sk")
                    .cloned()
                    .expect("query cache request should include sort key")
            })
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#1".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );

    let oracle = apply_oracle_transitions(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::AddGsiMembership { slot: 0 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: primary_gsi_oracle_query(2),
        strong: false,
        request_epoch: oracle.fresh_request_epoch(),
    };
    compare_observed_read(
        &oracle,
        &request,
        &ObservedRead::Query {
            outcome: CacheReadOutcome::ServeCache,
            serve_whole_page: true,
            cache_evaluated_keys: vec![0, 1],
            returned_page: vec![0, 1],
        },
    )
    .expect("runtime full gsi cache query should match oracle");
}

#[tokio::test]
async fn transact_write_items_encode_put_refreshes_previously_empty_gsi_query_proof() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_transact_encode_empty_to_present");
    create_gsi_table(&db, &table_name).await;

    let empty = query_gsi_partition(&db, &table_name, "tenant#1", Some(1), None).await;
    assert!(empty.0.is_empty());
    assert!(empty.1.is_none());

    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![TransactEncodeItem {
            put: Some(TransactEncodePutRequest {
                table_name: table_name.clone(),
                item: gsi_item(
                    "TD#example.test",
                    "META",
                    "tenant#1",
                    "example.test",
                    "domain",
                )
                .try_into_wire_item()
                .expect("encode domain-like item"),
                condition_expression: Some("attribute_not_exists(pk)".to_string()),
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        }],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transactional encoded put succeeds");

    observing_cache.reset();
    let present = query_gsi_partition(&db, &table_name, "tenant#1", Some(1), None).await;
    assert_eq!(query_item_sks(&present.0), vec!["META".to_string()]);
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "touched empty GSI proof should be refreshed from DB rather than served stale"
    );

    observing_cache.reset();
    let cached = query_gsi_partition(&db, &table_name, "tenant#1", Some(1), None).await;
    assert_eq!(query_item_sks(&cached.0), vec!["META".to_string()]);
    assert_eq!(observing_cache.snapshot().get_requests.len(), 1);
}

#[tokio::test]
async fn database_manager_gsi_second_page_can_be_served_from_cached_proof() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_second_page_cache_served");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_sk) in [
        ("item#0", "sk#1", "001"),
        ("item#1", "sk#2", "002"),
        ("item#2", "sk#3", "003"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, "team#1", gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    let first_page = query_gsi_partition(&db, &table_name, "team#1", Some(1), None).await;
    let second_page_start_key = first_page.1.clone();
    let second_page = query_gsi_partition(
        &db,
        &table_name,
        "team#1",
        Some(1),
        second_page_start_key.clone(),
    )
    .await;
    assert_eq!(query_item_sks(&second_page.0), vec!["sk#2".to_string()]);
    assert!(second_page.1.is_some());
    observing_cache.reset();

    let cached_second_page =
        query_gsi_partition(&db, &table_name, "team#1", Some(1), second_page_start_key).await;
    assert_eq!(
        query_item_sks(&cached_second_page.0),
        vec!["sk#2".to_string()]
    );

    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 1);
    assert_eq!(
        observed.get_requests[0]
            .key
            .get("sk")
            .cloned()
            .expect("cached second page request should include sort key"),
        AttributeValue::S("sk#2".to_string())
    );

    let oracle = apply_oracle_transitions(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::PreparePut { slot: 2 },
            Transition::LeaderCommitPut { slot: 2 },
            Transition::FollowerAcknowledgePut { slot: 2 },
            Transition::AddGsiMembership { slot: 0 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::AddGsiMembership { slot: 2 },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 2),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: QueryRequest {
            lower_bound: 0,
            upper_bound: 2,
            start_exclusive: 0,
            limit: 1,
            byte_budget: 1024,
            only_even: false,
            direction: QueryDirection::Forward,
            target: QueryTarget::Gsi(GsiQuerySpace::Primary),
            partition: PartitionId::Left,
        },
        strong: false,
        request_epoch: oracle.fresh_request_epoch(),
    };
    compare_observed_read(
        &oracle,
        &request,
        &ObservedRead::Query {
            outcome: CacheReadOutcome::ServeCache,
            serve_whole_page: true,
            cache_evaluated_keys: vec![1],
            returned_page: vec![1],
        },
    )
    .expect("runtime cached gsi continuation query should match oracle");
}

#[tokio::test]
async fn database_manager_gsi_query_uses_cached_prefix_and_db_suffix_for_partial_coverage() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_prefix_suffix");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_sk) in [
        ("item#0", "sk#1", "001"),
        ("item#1", "sk#2", "002"),
        ("item#2", "sk#3", "003"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, "team#1", gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(1), None).await;
    observing_cache.reset();

    let result = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&result.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );

    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 1);
    assert_eq!(
        observed.get_requests[0]
            .key
            .get("sk")
            .cloned()
            .expect("cached prefix request should include sort key"),
        AttributeValue::S("sk#1".to_string())
    );

    let oracle = apply_oracle_transitions(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::AddGsiMembership { slot: 0 },
            Transition::AddGsiMembership { slot: 1 },
            Transition::QueryFillGsi {
                query_space: GsiQuerySpace::Primary,
                range: TransitionRange::new(0, 0),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: primary_gsi_oracle_query(2),
        strong: false,
        request_epoch: oracle.fresh_request_epoch(),
    };
    compare_observed_read(
        &oracle,
        &request,
        &ObservedRead::Query {
            outcome: CacheReadOutcome::Mixed,
            serve_whole_page: false,
            cache_evaluated_keys: vec![0],
            returned_page: vec![0, 1],
        },
    )
    .expect("runtime mixed gsi query should match oracle");
}

#[tokio::test]
async fn database_manager_put_invalidates_only_touched_gsi_partition() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_precise_partition_invalidation");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_pk, gsi_sk) in [
        ("item#0", "sk#1", "team#1", "001"),
        ("item#1", "sk#2", "team#1", "002"),
        ("item#2", "sk#3", "team#2", "001"),
        ("item#3", "sk#4", "team#2", "002"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, gsi_pk, gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(gsi_item("item#0", "sk#1", "team#1", "001", "updated"))
            .build(),
    )
    .await
    .expect("overwrite item and update only one gsi partition");
    observing_cache.reset();

    let untouched_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&untouched_partition.0),
        vec!["sk#3".to_string(), "sk#4".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);
    observing_cache.reset();

    let touched_partition = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&touched_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "the touched partition should fall back to DB until coverage is refreshed"
    );
}

#[tokio::test]
async fn database_manager_put_moves_gsi_membership_without_invalidating_other_partitions() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_membership_move");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_pk, gsi_sk) in [
        ("item#0", "sk#1", "team#1", "002"),
        ("item#1", "sk#2", "team#2", "003"),
        ("item#2", "sk#3", "team#3", "001"),
        ("item#3", "sk#4", "team#3", "002"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, gsi_pk, gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;

    db.put_item(
        crate::PutItemInput::builder()
            .table_name(table_name.clone())
            .item(gsi_item("item#0", "sk#1", "team#2", "001", "moved"))
            .build(),
    )
    .await
    .expect("move item across gsi query spaces");

    observing_cache.reset();
    let untouched_partition = query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;
    assert_eq!(
        query_item_sks(&untouched_partition.0),
        vec!["sk#3".to_string(), "sk#4".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);

    observing_cache.reset();
    let old_partition = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert!(old_partition.0.is_empty());
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "the old query space should be refreshed from DB after membership moves"
    );

    observing_cache.reset();
    let new_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&new_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "the new query space should also refresh from DB after membership moves"
    );

    observing_cache.reset();
    let cached_new_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&cached_new_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);
}

#[tokio::test]
async fn database_manager_update_rewrites_gsi_sort_order_after_refresh() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_sort_rewrite_update");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_sk) in [("item#0", "sk#1", "002"), ("item#1", "sk#2", "003")] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, "team#1", gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;

    db.update_item(crate::UpdateItemInput {
        table_name: table_name.clone(),
        key: HashMap::from([
            ("pk".to_string(), AttributeValue::S("item#1".to_string())),
            ("sk".to_string(), AttributeValue::S("sk#2".to_string())),
        ])
        .into(),
        update_expression: "SET gsi1sk = :gsi1sk".to_string(),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":gsi1sk".to_string(),
            AttributeValue::S("001".to_string()),
        )])),
        return_values: None,
        return_old_on_condition_failure: false,
        aux_item_stream_ttl_hours: None,
    })
    .await
    .expect("rewrite gsi sort key");

    observing_cache.reset();
    let refreshed = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&refreshed.0),
        vec!["sk#2".to_string(), "sk#1".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "sort rewrites should refresh from DB before cache serving resumes"
    );

    observing_cache.reset();
    let cached = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&cached.0),
        vec!["sk#2".to_string(), "sk#1".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);
}

#[tokio::test]
async fn database_manager_batch_write_moves_gsi_membership_without_invalidating_other_partitions() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_batch_membership_move");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_pk, gsi_sk) in [
        ("item#0", "sk#1", "team#1", "002"),
        ("item#1", "sk#2", "team#2", "003"),
        ("item#2", "sk#3", "team#3", "001"),
        ("item#3", "sk#4", "team#3", "002"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, gsi_pk, gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;

    db.batch_write_item(BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: gsi_item("item#0", "sk#1", "team#2", "001", "moved"),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch move item across gsi query spaces");

    observing_cache.reset();
    let untouched_partition = query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;
    assert_eq!(
        query_item_sks(&untouched_partition.0),
        vec!["sk#3".to_string(), "sk#4".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);

    observing_cache.reset();
    let old_partition = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert!(old_partition.0.is_empty());
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "batch writes should refresh the old query space from DB once membership moves"
    );

    observing_cache.reset();
    let new_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&new_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "batch writes should refresh the new query space from DB once membership moves"
    );

    observing_cache.reset();
    let cached_new_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&cached_new_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);
}

#[tokio::test]
async fn database_manager_batch_write_encode_moves_gsi_membership_without_invalidating_other_partitions()
 {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_batch_encode_membership_move");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_pk, gsi_sk) in [
        ("item#0", "sk#1", "team#1", "002"),
        ("item#1", "sk#2", "team#2", "003"),
        ("item#2", "sk#3", "team#3", "001"),
        ("item#3", "sk#4", "team#3", "002"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, gsi_pk, gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;

    db.batch_write_item_encode(BatchWriteItemEncodeRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![EncodeWriteRequest {
                put_request: Some(EncodePutRequest {
                    item: WireItem::from_attribute_map(&gsi_item(
                        "item#0", "sk#1", "team#2", "001", "moved",
                    ))
                    .expect("encode put item"),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("batch encode move item across gsi query spaces");

    observing_cache.reset();
    let untouched_partition = query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;
    assert_eq!(
        query_item_sks(&untouched_partition.0),
        vec!["sk#3".to_string(), "sk#4".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);

    observing_cache.reset();
    let old_partition = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert!(old_partition.0.is_empty());
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "batch encode writes should refresh the old query space from DB once membership moves"
    );

    observing_cache.reset();
    let new_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&new_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "batch encode writes should refresh the new query space from DB once membership moves"
    );
}

#[tokio::test]
async fn database_manager_transact_update_rewrites_gsi_sort_order_without_invalidating_other_partitions()
 {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_transact_sort_rewrite");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_pk, gsi_sk) in [
        ("item#0", "sk#1", "team#1", "002"),
        ("item#1", "sk#2", "team#1", "003"),
        ("item#2", "sk#3", "team#2", "001"),
        ("item#3", "sk#4", "team#2", "002"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, gsi_pk, gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;

    db.transact_write_items(TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: None,
            update: Some(TransactUpdateRequest {
                table_name: table_name.clone(),
                key: HashMap::from([
                    ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                    ("sk".to_string(), AttributeValue::S("sk#2".to_string())),
                ])
                .into(),
                update_expression: "SET gsi1sk = :gsi1sk".to_string(),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: Some(HashMap::from([(
                    ":gsi1sk".to_string(),
                    AttributeValue::S("001".to_string()),
                )])),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            delete: None,
            condition_check: None,
        }],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    })
    .await
    .expect("transaction rewrite gsi sort key");

    observing_cache.reset();
    let untouched_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&untouched_partition.0),
        vec!["sk#3".to_string(), "sk#4".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);

    observing_cache.reset();
    let refreshed = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&refreshed.0),
        vec!["sk#2".to_string(), "sk#1".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "transactional sort rewrites should refresh the touched partition from DB once"
    );

    observing_cache.reset();
    let cached = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert_eq!(
        query_item_sks(&cached.0),
        vec!["sk#2".to_string(), "sk#1".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);
}

#[tokio::test]
async fn database_manager_transact_write_items_encode_put_moves_gsi_membership_without_invalidating_other_partitions()
 {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_gsi_transact_encode_membership_move");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_pk, gsi_sk) in [
        ("item#0", "sk#1", "team#1", "002"),
        ("item#1", "sk#2", "team#2", "003"),
        ("item#2", "sk#3", "team#3", "001"),
        ("item#3", "sk#4", "team#3", "002"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, gsi_pk, gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;

    db.transact_write_items_encode(TransactWriteItemsEncodeRequest {
        transact_items: vec![TransactEncodeItem {
            put: Some(TransactEncodePutRequest {
                table_name: table_name.clone(),
                item: WireItem::from_attribute_map(&gsi_item(
                    "item#0", "sk#1", "team#2", "001", "moved",
                ))
                .expect("encode transact put item"),
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
    .expect("transact encode move item across gsi query spaces");

    observing_cache.reset();
    let untouched_partition = query_gsi_partition(&db, &table_name, "team#3", Some(2), None).await;
    assert_eq!(
        query_item_sks(&untouched_partition.0),
        vec!["sk#3".to_string(), "sk#4".to_string()]
    );
    assert_eq!(observing_cache.snapshot().get_requests.len(), 2);

    observing_cache.reset();
    let old_partition = query_gsi_partition(&db, &table_name, "team#1", Some(2), None).await;
    assert!(old_partition.0.is_empty());
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "transact encode writes should refresh the old query space from DB once membership moves"
    );

    observing_cache.reset();
    let new_partition = query_gsi_partition(&db, &table_name, "team#2", Some(2), None).await;
    assert_eq!(
        query_item_sks(&new_partition.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
    assert!(
        observing_cache.snapshot().get_requests.is_empty(),
        "transact encode writes should refresh the new query space from DB once membership moves"
    );
}

#[tokio::test]
async fn reverse_base_queries_use_materialized_cache_page_when_proof_exists() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_reverse_db_backed");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2", "sk#3"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(false),
        consistent_read: false,
    };

    let first = db
        .query_table_map(QueryTableInput {
            table_name: request.table_name.clone(),
            key_condition_expression: request.key_condition_expression.clone(),
            expression_attribute_names: request.expression_attribute_names.clone(),
            expression_attribute_values: request.expression_attribute_values.clone(),
            limit: request.limit,
            exclusive_start_key: request.exclusive_start_key.clone(),
            scan_index_forward: request.scan_index_forward,
            consistent_read: request.consistent_read,
        })
        .await
        .expect("warm reverse query");
    assert_eq!(
        query_item_sks(&first.0),
        vec!["sk#3".to_string(), "sk#2".to_string()]
    );
    assert!(observing_cache.snapshot().get_requests.is_empty());

    let second = db
        .query_table_map(request)
        .await
        .expect("repeat reverse query");
    assert_eq!(
        query_item_sks(&second.0),
        vec!["sk#3".to_string(), "sk#2".to_string()]
    );
    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 2);
    assert_eq!(
        observed
            .get_requests
            .iter()
            .map(|request| {
                request
                    .key
                    .get("sk")
                    .cloned()
                    .expect("reverse query cache request should include sort key")
            })
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#3".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );
}

#[tokio::test]
async fn runtime_query_fallback_matches_storage_cache_oracle() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_runtime_oracle_fallback");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    let result = db
        .query_table_map(QueryTableInput {
            table_name: table_name.clone(),
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )])),
            limit: Some(2),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("fallback query");

    assert!(observing_cache.snapshot().get_requests.is_empty());

    let oracle = apply_oracle_transitions(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
        ],
    );
    let request = ReadRequest::Query {
        query: base_oracle_query(2),
        strong: false,
        request_epoch: oracle.fresh_request_epoch(),
    };
    compare_observed_read(
        &oracle,
        &request,
        &ObservedRead::Query {
            outcome: CacheReadOutcome::FallbackDb,
            serve_whole_page: false,
            cache_evaluated_keys: Vec::new(),
            returned_page: vec![0, 1],
        },
    )
    .expect("runtime fallback query should match oracle");
    assert_eq!(
        query_item_sks(&result.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
}

#[tokio::test]
async fn runtime_query_full_cache_serve_matches_storage_cache_oracle() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_runtime_oracle_serve");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    db.query_table_map(QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    })
    .await
    .expect("warm proof");

    let result = db
        .query_table_map(QueryTableInput {
            table_name: table_name.clone(),
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )])),
            limit: Some(2),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("cache-served query");

    let observed_requests = observing_cache.snapshot().get_requests;
    assert_eq!(observed_requests.len(), 2);

    let oracle = apply_oracle_transitions(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 1),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: base_oracle_query(2),
        strong: false,
        request_epoch: oracle.fresh_request_epoch(),
    };
    compare_observed_read(
        &oracle,
        &request,
        &ObservedRead::Query {
            outcome: CacheReadOutcome::ServeCache,
            serve_whole_page: true,
            cache_evaluated_keys: vec![0, 1],
            returned_page: vec![0, 1],
        },
    )
    .expect("runtime full cache query should match oracle");
    assert_eq!(
        query_item_sks(&result.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
}

#[tokio::test]
async fn runtime_query_mixed_prefix_matches_storage_cache_oracle() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_runtime_oracle_mixed");
    create_pk_sk_table(&db, &table_name).await;

    for sk in ["sk#1", "sk#2"] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(item("tenant#1", sk, sk))
                .build(),
        )
        .await
        .expect("seed item");
    }

    db.query_table_map(QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    })
    .await
    .expect("warm prefix proof");

    let result = db
        .query_table_map(QueryTableInput {
            table_name: table_name.clone(),
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )])),
            limit: Some(2),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: false,
        })
        .await
        .expect("mixed query");

    let observed_requests = observing_cache.snapshot().get_requests;
    assert_eq!(observed_requests.len(), 1);

    let oracle = apply_oracle_transitions(
        CacheState::authoritative_leader_base_state(),
        &[
            Transition::PreparePut { slot: 0 },
            Transition::LeaderCommitPut { slot: 0 },
            Transition::FollowerAcknowledgePut { slot: 0 },
            Transition::PreparePut { slot: 1 },
            Transition::LeaderCommitPut { slot: 1 },
            Transition::FollowerAcknowledgePut { slot: 1 },
            Transition::QueryFillBase {
                range: TransitionRange::new(0, 0),
            },
        ],
    );
    let request = ReadRequest::Query {
        query: base_oracle_query(2),
        strong: false,
        request_epoch: oracle.fresh_request_epoch(),
    };
    compare_observed_read(
        &oracle,
        &request,
        &ObservedRead::Query {
            outcome: CacheReadOutcome::Mixed,
            serve_whole_page: false,
            cache_evaluated_keys: vec![0],
            returned_page: vec![0, 1],
        },
    )
    .expect("runtime mixed query should match oracle");
    assert_eq!(
        query_item_sks(&result.0),
        vec!["sk#1".to_string(), "sk#2".to_string()]
    );
}

#[tokio::test]
async fn numeric_sort_keys_are_ordered_numerically_in_manifest() {
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(
        noop_point_read_cache(),
        query_proof_cache.clone(),
    )
    .await
    .expect("create db with query proof cache");
    let table_name = TableName::new("query_proof_numeric_order");
    create_numeric_pk_sk_table(&db, &table_name).await;

    for (sk, payload) in [("10", "ten"), ("2", "two")] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(numeric_item("tenant#1", sk, payload))
                .build(),
        )
        .await
        .expect("seed numeric item");
    }

    let (_page, _lek) = query_partition(&db, &table_name, "tenant#1", Some(10), None).await;
    let snapshot = query_proof_cache
        .snapshot_base_partition(&manifest_key(&table_name, "tenant#1"))
        .expect("snapshot should exist after numeric query");

    let sort_keys = snapshot
        .entries
        .iter()
        .map(|entry| entry.primary_key.get("sk").cloned().expect("numeric sk"))
        .collect::<Vec<_>>();
    assert_eq!(
        sort_keys,
        vec![
            AttributeValue::N("2".to_string()),
            AttributeValue::N("10".to_string()),
        ]
    );
}

#[tokio::test]
async fn reverse_gsi_queries_use_materialized_cache_page_when_proof_exists() {
    let observing_cache = Arc::new(ObservingPointReadCache::new(Arc::new(
        InMemoryPointReadCache::new(InMemoryPointReadCacheConfig::default()),
    )));
    let query_proof_cache = Arc::new(InMemoryQueryProofCache::new(
        InMemoryQueryProofCacheConfig::default(),
    ));
    let db = DatabaseManager::new_for_test_with_caches(observing_cache.clone(), query_proof_cache)
        .await
        .expect("create db with caches");
    let table_name = TableName::new("query_proof_reverse_gsi_cache_served");
    create_gsi_table(&db, &table_name).await;

    for (pk, sk, gsi_sk) in [
        ("item#0", "sk#1", "001"),
        ("item#1", "sk#2", "002"),
        ("item#2", "sk#3", "003"),
    ] {
        db.put_item(
            crate::PutItemInput::builder()
                .table_name(table_name.clone())
                .item(gsi_item(pk, sk, "team#1", gsi_sk, sk))
                .build(),
        )
        .await
        .expect("seed gsi item");
    }

    let request = QueryIndexInput {
        table_name: table_name.clone(),
        index_name: IndexName::new("gsi1"),
        key_condition_expression: "gsi1pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("team#1".to_string()),
        )])),
        projection_expression: None,
        limit: Some(2),
        exclusive_start_key: None,
        scan_index_forward: Some(false),
    };

    let first = db
        .query_index_map(QueryIndexInput {
            table_name: request.table_name.clone(),
            index_name: request.index_name.clone(),
            key_condition_expression: request.key_condition_expression.clone(),
            expression_attribute_names: request.expression_attribute_names.clone(),
            expression_attribute_values: request.expression_attribute_values.clone(),
            projection_expression: None,
            limit: request.limit,
            exclusive_start_key: request.exclusive_start_key.clone(),
            scan_index_forward: request.scan_index_forward,
        })
        .await
        .expect("warm reverse gsi query");
    assert_eq!(
        query_item_sks(&first.0),
        vec!["sk#3".to_string(), "sk#2".to_string()]
    );
    assert!(observing_cache.snapshot().get_requests.is_empty());

    let second = db
        .query_index_map(request)
        .await
        .expect("repeat reverse gsi query");
    assert_eq!(
        query_item_sks(&second.0),
        vec!["sk#3".to_string(), "sk#2".to_string()]
    );

    let observed = observing_cache.snapshot();
    assert_eq!(observed.get_requests.len(), 2);
    assert_eq!(
        observed
            .get_requests
            .iter()
            .map(|request| {
                request
                    .key
                    .get("sk")
                    .cloned()
                    .expect("reverse gsi query cache request should include sort key")
            })
            .collect::<Vec<_>>(),
        vec![
            AttributeValue::S("sk#3".to_string()),
            AttributeValue::S("sk#2".to_string()),
        ]
    );
}

#[tokio::test]
async fn query_proof_cache_replays_response_boundary_witness_for_unbounded_page() {
    let cache = InMemoryQueryProofCache::new(InMemoryQueryProofCacheConfig::default());
    let table_name = TableName::new("query_proof_response_boundary_witness");
    let table_info = base_table_info(&table_name);
    let request = QueryTableInput {
        table_name: table_name.clone(),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let wire_items = vec![
        item("tenant#1", "sk#1", "alpha")
            .try_into_wire_item()
            .expect("wire item"),
        item("tenant#1", "sk#2", "beta")
            .try_into_wire_item()
            .expect("wire item"),
    ];

    cache
        .record_query_page(
            &table_name,
            &table_info,
            &request_to_plan_request(&request),
            &wire_items,
            true,
        )
        .await
        .expect("record query page with opaque boundary");

    let plan = cache
        .plan_query_read(&table_name, &table_info, &request_to_plan_request(&request))
        .await
        .expect("plan cached replay");
    assert!(plan.would_serve_whole_page);
    assert!(plan.page_boundary_witnessed);
    assert_eq!(plan.cache_candidate_count, 2);

    let materialized = cache
        .materialize_query_read(&table_name, &table_info, &request_to_plan_request(&request))
        .await
        .expect("materialize cached replay")
        .expect("materialized page");
    assert!(materialized.page_complete);
    assert_eq!(materialized.primary_keys.len(), 2);
    assert!(materialized.last_evaluated_key.is_some());
}
