use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use alloc_counter::AllocationGuard;
use storage_provider::{StorageProvider as _, UpdateOperation};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest,
    CreateGlobalSecondaryIndex, CreateTableRequest, EncodePutRequest, EncodeWriteRequest,
    IndexName, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, Projection,
    ProjectionType, PutRequest, SerializesToKey, StoredTableInfo, StreamName, StreamSpecification,
    StreamViewType, TableName, TableStatus, TimeToLiveSpecification, TimestampMillis,
    TransactUpdateRequest, TransactWriteItem, TransactWriteItemsRequest, UpdateTimeToLiveRequest,
    WireItem, WriteRequest,
};
use stream_provider::StreamProvider as _;

use crate::{
    backends::common::plan_table_write,
    kv_support_tests::create_test_provider,
    sorted_kv_store::{SortedKvStore, TransactWriteOperation, TransactWriteTableOperation},
    storage_provider::{
        decode_wire_item_from_storage_bytes, encode_wire_item_storage_bytes,
        project_wire_item_table_key_and_ttl, ttl_index_direct_operations_for_wire_items,
        ttl_tracking_enabled, wire_item_key_token_from_item_key,
    },
};

const ITEM_COUNT: usize = 96;
const STREAM_READ_LIMIT: u32 = 256;
const TTL_ATTRIBUTE: &str = "ttl";
const TABLE_NAME_PUT_ENCODE: &str = "alloc_write_encode_ttl_stream";
const TABLE_NAME_BATCH_ENCODE: &str = "alloc_batch_encode_ttl_stream";
const TABLE_NAME_BATCH_IMMEDIATE_GSI: &str = "alloc_batch_encode_immediate_gsi";
const TRANSACT_UPDATE_ITERATIONS: usize = 64;
const TRANSACT_UPDATE_WIDTH: usize = 4;
const UPDATE_PLAN_ITERATIONS: usize = 512;

fn alloc_suite_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime")
}

fn create_table_request(table_name: &TableName) -> CreateTableRequest {
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
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }))
}

fn create_plain_table_request(table_name: &TableName) -> CreateTableRequest {
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
}

fn create_immediate_gsi_table_request(table_name: &TableName) -> CreateTableRequest {
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
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("AllocGsi"),
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
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]))
}

fn sample_item(index: usize) -> HashMap<String, AttributeValue> {
    let ttl = 2_200_000_000_u64 + u64::try_from(index).unwrap_or(0);
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        (
            "sk".to_string(),
            AttributeValue::S(format!("ITEM#{index:04}")),
        ),
        (
            "entity_type".to_string(),
            AttributeValue::S("ALLOC_PROFILE".to_string()),
        ),
        ("revision".to_string(), AttributeValue::N(index.to_string())),
        (
            TTL_ATTRIBUTE.to_string(),
            AttributeValue::N(ttl.to_string()),
        ),
        (
            "payload".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
                (
                    "flags".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("stream".to_string()),
                        AttributeValue::S("ttl".to_string()),
                    ]),
                ),
            ])),
        ),
    ])
}

fn sample_items() -> Vec<HashMap<String, AttributeValue>> {
    (0..ITEM_COUNT).map(sample_item).collect()
}

fn transact_update_item(index: usize) -> HashMap<String, AttributeValue> {
    let ttl = 2_200_000_000_u64 + u64::try_from(index).unwrap_or(0);
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("ORG#TXN#{index:04}")),
        ),
        ("sk".to_string(), AttributeValue::S("ITEM#0000".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("before".to_string()),
        ),
        ("counter".to_string(), AttributeValue::N("0".to_string())),
        (
            "tags".to_string(),
            AttributeValue::SS(vec!["old".to_string(), "stable".to_string()]),
        ),
        (
            "notes".to_string(),
            AttributeValue::L(vec![AttributeValue::S("seed".to_string())]),
        ),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            TTL_ATTRIBUTE.to_string(),
            AttributeValue::N(ttl.to_string()),
        ),
    ])
}

fn transact_update_key(index: usize) -> KeyAttributes {
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("ORG#TXN#{index:04}")),
        ),
        ("sk".to_string(), AttributeValue::S("ITEM#0000".to_string())),
    ])
    .into()
}

fn transact_update_request(table_name: &TableName, iteration: usize) -> TransactWriteItemsRequest {
    let names = HashMap::from([
        ("#payload".to_string(), "payload".to_string()),
        ("#counter".to_string(), "counter".to_string()),
        ("#tags".to_string(), "tags".to_string()),
        ("#status".to_string(), "status".to_string()),
    ]);
    let values = HashMap::from([
        (
            ":payload".to_string(),
            AttributeValue::S(format!("after-{iteration:04}")),
        ),
        (":inc".to_string(), AttributeValue::N("1".to_string())),
        (
            ":expected".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            ":tag".to_string(),
            AttributeValue::SS(vec!["old".to_string()]),
        ),
    ]);
    let transact_items = (0..TRANSACT_UPDATE_WIDTH)
        .map(|index| TransactWriteItem {
            update: Some(TransactUpdateRequest {
                table_name: table_name.clone(),
                key: transact_update_key(index),
                update_expression: "SET #payload = :payload ADD #counter :inc DELETE #tags :tag"
                    .to_string(),
                condition_expression: Some("#status = :expected".to_string()),
                expression_attribute_names: Some(names.clone()),
                expression_attribute_values: Some(values.clone()),
                return_values_on_condition_check_failure: None,
            }),
            ..Default::default()
        })
        .collect();

    TransactWriteItemsRequest {
        transact_items,
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn sample_wire_items() -> Vec<WireItem> {
    sample_items()
        .into_iter()
        .map(|item| WireItem::from_attribute_map(&item).expect("wire item"))
        .collect()
}

fn sample_batch_write_encode_request(table_name: &TableName) -> BatchWriteItemEncodeRequest {
    let writes = sample_wire_items()
        .into_iter()
        .map(|item| EncodeWriteRequest {
            put_request: Some(EncodePutRequest { item }),
            delete_request: None,
        })
        .collect::<Vec<_>>();

    BatchWriteItemEncodeRequest {
        request_items: HashMap::from([(table_name.clone(), writes)]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn sample_batch_write_immediate_gsi_request(table_name: &TableName) -> BatchWriteItemRequest {
    let writes = sample_items()
        .into_iter()
        .enumerate()
        .map(|(index, mut item)| {
            item.insert(
                "gsi_pk".to_string(),
                AttributeValue::S("ALLOC#GSI".to_string()),
            );
            item.insert("gsi_sk".to_string(), AttributeValue::N(index.to_string()));
            WriteRequest {
                put_request: Some(PutRequest { item }),
                delete_request: None,
            }
        })
        .collect::<Vec<_>>();

    BatchWriteItemRequest {
        request_items: HashMap::from([(table_name.clone(), writes)]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

async fn setup_provider(table_name: &TableName) -> crate::kv_support_tests::TestProvider {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("initialize provider");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");
    provider
        .create_table(&create_table_request(table_name))
        .await
        .expect("create table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: TTL_ATTRIBUTE.to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");
    provider
}

async fn setup_immediate_gsi_provider(
    table_name: &TableName,
) -> crate::kv_support_tests::TestProvider {
    let provider = create_test_provider().with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize provider");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");
    provider
        .create_table(&create_immediate_gsi_table_request(table_name))
        .await
        .expect("create immediate gsi table");
    provider
}

async fn setup_plain_provider(table_name: &TableName) -> crate::kv_support_tests::TestProvider {
    let provider = create_test_provider();
    provider
        .initialize_storage()
        .await
        .expect("initialize provider");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");
    provider
        .create_table(&create_plain_table_request(table_name))
        .await
        .expect("create plain table");
    provider
}

async fn setup_transact_update_provider(
    table_name: &TableName,
    enable_ttl: bool,
) -> crate::kv_support_tests::TestProvider {
    let provider = setup_plain_provider(table_name).await;
    if enable_ttl {
        provider
            .update_time_to_live(UpdateTimeToLiveRequest {
                table_name: table_name.clone(),
                time_to_live_specification: TimeToLiveSpecification {
                    attribute_name: TTL_ATTRIBUTE.to_string(),
                    enabled: true,
                },
            })
            .await
            .expect("enable ttl");
    }
    for index in 0..TRANSACT_UPDATE_WIDTH {
        provider
            .put_item(
                table_name.clone(),
                transact_update_item(index),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("preload transaction update item");
    }
    provider
}

async fn assert_stream_entries(provider: &crate::kv_support_tests::TestProvider) {
    let page = provider
        .read_forward(StreamName::system_table_stream(), None, STREAM_READ_LIMIT)
        .await
        .expect("read stream entries");
    assert!(
        page.items.len() >= ITEM_COUNT,
        "expected at least {ITEM_COUNT} stream entries, got {}",
        page.items.len()
    );
}

async fn assert_ttl_rows_for_wire_items(
    provider: &crate::kv_support_tests::TestProvider,
    table_name: &TableName,
    items: &[WireItem],
) {
    let table_info = provider
        .get_table_info(table_name)
        .await
        .expect("table info");
    for item in items {
        let ttl_key = storage_common::ttl::ttl_index_key_for_wire_item(
            table_name,
            &table_info,
            TTL_ATTRIBUTE,
            item,
        )
        .expect("compute ttl key")
        .expect("ttl key should exist");
        assert!(
            provider
                .kv_store
                .get(&ttl_key, true)
                .await
                .expect("load ttl row")
                .is_some(),
            "missing ttl row for wire item"
        );
    }
}

fn measure_put_item_encode_stream_ttl_baseline() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new(TABLE_NAME_PUT_ENCODE);
    let provider = runtime.block_on(setup_provider(&table_name));
    let verification_items = sample_wire_items();
    let write_items = sample_wire_items();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_put_item_encode_stream_ttl_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    runtime.block_on(async {
        for item in write_items {
            provider
                .put_item_encode(table_name.clone(), item, None, None, None, None)
                .await
                .expect("put item encode");
        }
    });
    let report = guard.finish();

    runtime.block_on(async {
        assert_stream_entries(&provider).await;
        assert_ttl_rows_for_wire_items(&provider, &table_name, &verification_items).await;
    });
    report
}

fn measure_batch_write_encode_stream_ttl_baseline() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new(TABLE_NAME_BATCH_ENCODE);
    let provider = runtime.block_on(setup_provider(&table_name));
    let verification_items = sample_wire_items();
    let request = sample_batch_write_encode_request(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_batch_write_encode_stream_ttl_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    runtime.block_on(async {
        provider
            .batch_write_item_encode(request, true)
            .await
            .expect("batch write item encode");
    });
    let report = guard.finish();

    runtime.block_on(async {
        assert_stream_entries(&provider).await;
        assert_ttl_rows_for_wire_items(&provider, &table_name, &verification_items).await;
    });
    report
}

fn measure_batch_write_immediate_gsi_baseline() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new(TABLE_NAME_BATCH_IMMEDIATE_GSI);
    let provider = runtime.block_on(setup_immediate_gsi_provider(&table_name));
    let request = sample_batch_write_immediate_gsi_request(&table_name);

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_batch_write_immediate_gsi_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    runtime.block_on(async {
        provider
            .batch_write_item(request, true)
            .await
            .expect("batch write item immediate gsi");
    });
    guard.finish()
}

fn measure_transact_update_plain_table() -> alloc_counter::AllocationReport<'static> {
    measure_transact_update_path(
        "alloc_transact_update_plain",
        false,
        "kv_transact_update_plain_no_old_image",
        "plain_no_old_image",
    )
}

fn measure_transact_update_ttl_table() -> alloc_counter::AllocationReport<'static> {
    measure_transact_update_path(
        "alloc_transact_update_ttl",
        true,
        "kv_transact_update_ttl_old_image_required",
        "ttl_old_image_required",
    )
}

fn measure_transact_update_path(
    table_name: &'static str,
    enable_ttl: bool,
    test_name: &'static str,
    label: &'static str,
) -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new(table_name);
    let provider = runtime.block_on(setup_transact_update_provider(&table_name, enable_ttl));
    let requests = (0..TRANSACT_UPDATE_ITERATIONS)
        .map(|iteration| transact_update_request(&table_name, iteration))
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(module_path!(), test_name, file!(), line!(), Some(label));
    runtime.block_on(async {
        for request in requests {
            provider
                .transact_write_items(request)
                .await
                .expect("transact update items");
        }
    });
    guard.finish()
}

fn measure_put_item_encode_projection_and_encode_stage() -> alloc_counter::AllocationReport<'static>
{
    let runtime = runtime();
    let table_name = TableName::new("alloc_write_encode_stage_projection");
    let provider = runtime.block_on(setup_provider(&table_name));
    let table_info = runtime
        .block_on(provider.get_table_info(&table_name))
        .expect("table info");
    let ttl_config = runtime
        .block_on(provider.load_ttl_config(&table_name))
        .expect("ttl config");
    let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
    let ttl_attribute = if should_track_ttl {
        ttl_config
            .as_ref()
            .map(|config| config.attribute_name.as_str())
    } else {
        None
    };
    let items = sample_wire_items();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_put_item_encode_stage_projection_and_encode",
        file!(),
        line!(),
        Some("component"),
    );
    for item in &items {
        let (item_key, _projected_ttl_value) =
            project_wire_item_table_key_and_ttl(item, &table_info, ttl_attribute)
                .expect("project key + ttl");
        let _item_key_bytes = item_key.serialize_to_bytes().expect("serialize item key");
        if should_track_ttl {
            let _item_key_token =
                wire_item_key_token_from_item_key(&item_key).expect("item key token");
        }
        let _value = encode_wire_item_storage_bytes(item).expect("encode wire bytes");
    }
    guard.finish()
}

fn measure_put_item_encode_old_read_stage() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new("alloc_write_encode_stage_old_read");
    let provider = runtime.block_on(setup_provider(&table_name));
    let table_info = runtime
        .block_on(provider.get_table_info(&table_name))
        .expect("table info");
    let ttl_config = runtime
        .block_on(provider.load_ttl_config(&table_name))
        .expect("ttl config");
    let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
    let ttl_attribute = if should_track_ttl {
        ttl_config
            .as_ref()
            .map(|config| config.attribute_name.as_str())
    } else {
        None
    };
    let keys = sample_wire_items()
        .into_iter()
        .map(|item| {
            let (item_key, _) =
                project_wire_item_table_key_and_ttl(&item, &table_info, ttl_attribute)
                    .expect("project key + ttl");
            item_key.serialize_to_bytes().expect("serialize key")
        })
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_put_item_encode_stage_old_read",
        file!(),
        line!(),
        Some("component"),
    );
    runtime.block_on(async {
        for key in &keys {
            let old_bytes = provider
                .kv_store
                .get(key.as_slice(), true)
                .await
                .expect("read old bytes");
            if should_track_ttl {
                let _old_item = old_bytes
                    .as_deref()
                    .map(decode_wire_item_from_storage_bytes)
                    .transpose()
                    .expect("decode old item");
            }
        }
    });
    guard.finish()
}

fn measure_update_plan_preserve_old_baseline() -> alloc_counter::AllocationReport<'static> {
    measure_update_plan_old_item_retention(true, "kv_update_plan_preserve_old_baseline", "baseline")
}

fn measure_update_plan_skip_old_optimized() -> alloc_counter::AllocationReport<'static> {
    measure_update_plan_old_item_retention(false, "kv_update_plan_skip_old_optimized", "optimized")
}

fn measure_update_plan_old_item_retention(
    preserve_old_item: bool,
    test_name: &'static str,
    label: &'static str,
) -> alloc_counter::AllocationReport<'static> {
    let table_info = update_plan_table_info();
    let current_item = update_plan_item();
    let current_bytes = storage_types::storage_serde::to_bytes(&current_item)
        .expect("serialize current update item");
    let update_key = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("pk".to_string())),
        ("sk".to_string(), AttributeValue::S("sk".to_string())),
    ]));
    let update_operations = vec![
        UpdateOperation::Set {
            field: "payload_0".to_string().into(),
            value: AttributeValue::S("updated".to_string()),
        },
        UpdateOperation::Add {
            field: "counter".to_string().into(),
            value: AttributeValue::N("1".to_string()),
        },
    ];
    let update_operations = Arc::<[UpdateOperation]>::from(update_operations);

    let guard = AllocationGuard::start(module_path!(), test_name, file!(), line!(), Some(label));
    for _ in 0..UPDATE_PLAN_ITERATIONS {
        let plan = plan_table_write(
            &[TransactWriteTableOperation::Update {
                table_info: table_info.clone(),
                key: update_key.clone(),
                operations: Arc::clone(&update_operations),
                condition: None,
                replication: None,
                preserve_old_item,
                transaction_validation: false,
                ttl_config: None,
            }],
            vec![Some(current_bytes.clone())],
            &[None],
            false,
        )
        .expect("plan update table write");
        std::hint::black_box((plan.results.len(), plan.mutations.len()));
    }
    guard.finish()
}

fn update_plan_table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("alloc_update_plan"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![attr("pk"), attr("sk")],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

fn update_plan_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk".to_string()));
    for index in 0..32 {
        item.insert(
            format!("payload_{index}"),
            AttributeValue::S(format!("payload-{index}-{}", "x".repeat(128))),
        );
    }
    item
}

fn attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: KeyAttributeType::S,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}

fn measure_put_item_encode_stream_envelope_stage() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new("alloc_write_encode_stage_stream");
    let provider = runtime.block_on(setup_provider(&table_name));
    let table_info = runtime
        .block_on(provider.get_table_info(&table_name))
        .expect("table info");
    let ttl_config = runtime
        .block_on(provider.load_ttl_config(&table_name))
        .expect("ttl config");
    let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
    let ttl_attribute = if should_track_ttl {
        ttl_config
            .as_ref()
            .map(|config| config.attribute_name.as_str())
    } else {
        None
    };
    let contexts = sample_wire_items()
        .into_iter()
        .map(|item| {
            let (item_key, _) =
                project_wire_item_table_key_and_ttl(&item, &table_info, ttl_attribute)
                    .expect("project key + ttl");
            let value = encode_wire_item_storage_bytes(&item).expect("encode wire bytes");
            (item_key, value)
        })
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_put_item_encode_stage_stream_envelope",
        file!(),
        line!(),
        Some("component"),
    );
    for (item_key, value) in &contexts {
        let _entries = crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
            &table_name,
            item_key,
            value.as_slice(),
            None,
            storage_types::StreamItemId::random(),
            false,
            None,
        )
        .expect("create stream entries");
    }
    guard.finish()
}

fn measure_put_item_encode_ttl_ops_stage() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new("alloc_write_encode_stage_ttl_ops");
    let provider = runtime.block_on(setup_provider(&table_name));
    let table_info = runtime
        .block_on(provider.get_table_info(&table_name))
        .expect("table info");
    let ttl_config = runtime
        .block_on(provider.load_ttl_config(&table_name))
        .expect("ttl config");
    let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
    let ttl_attribute = if should_track_ttl {
        ttl_config
            .as_ref()
            .map(|config| config.attribute_name.as_str())
    } else {
        None
    };
    let contexts = sample_wire_items()
        .into_iter()
        .map(|item| {
            let (item_key, projected_ttl_value) =
                project_wire_item_table_key_and_ttl(&item, &table_info, ttl_attribute)
                    .expect("project key + ttl");
            let item_key_token =
                wire_item_key_token_from_item_key(&item_key).expect("item key token");
            (item, item_key_token, projected_ttl_value)
        })
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_put_item_encode_stage_ttl_ops",
        file!(),
        line!(),
        Some("component"),
    );
    for (item, item_key_token, projected_ttl_value) in &contexts {
        let _ttl_ops = ttl_index_direct_operations_for_wire_items(
            &table_name,
            &table_info,
            ttl_config.as_ref(),
            None,
            Some(item),
            Some(item_key_token.as_str()),
            *projected_ttl_value,
        )
        .expect("ttl direct operations");
    }
    guard.finish()
}

fn measure_put_item_encode_execute_stage() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new("alloc_write_encode_stage_execute");
    let provider = runtime.block_on(setup_provider(&table_name));
    let table_info = runtime
        .block_on(provider.get_table_info(&table_name))
        .expect("table info");
    let ttl_config = runtime
        .block_on(provider.load_ttl_config(&table_name))
        .expect("ttl config");
    let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
    let ttl_attribute = if should_track_ttl {
        ttl_config
            .as_ref()
            .map(|config| config.attribute_name.as_str())
    } else {
        None
    };
    let mut planned_operations: Vec<Vec<TransactWriteOperation>> = Vec::new();
    for item in sample_wire_items() {
        let (item_key, projected_ttl_value) =
            project_wire_item_table_key_and_ttl(&item, &table_info, ttl_attribute)
                .expect("project key + ttl");
        let item_key_bytes = item_key.serialize_to_bytes().expect("serialize key");
        let item_key_token = wire_item_key_token_from_item_key(&item_key).expect("item key token");
        let value = encode_wire_item_storage_bytes(&item).expect("encode wire bytes");

        let mut operations = Vec::with_capacity(6);
        let stream_entries =
            crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
                &table_name,
                &item_key,
                value.as_slice(),
                None,
                storage_types::StreamItemId::random(),
                false,
                None,
            )
            .expect("create stream entries");
        operations.extend(stream_entries.into_iter().map(|(template, value)| {
            TransactWriteOperation::PutTemplate {
                template,
                value,
                condition: None,
            }
        }));

        let ttl_ops = ttl_index_direct_operations_for_wire_items(
            &table_name,
            &table_info,
            ttl_config.as_ref(),
            None,
            Some(&item),
            Some(item_key_token.as_str()),
            projected_ttl_value,
        )
        .expect("ttl direct operations");
        operations.extend(ttl_ops);
        operations.push(TransactWriteOperation::Put {
            key: item_key_bytes,
            value,
            condition: None,
        });
        planned_operations.push(operations);
    }

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_put_item_encode_stage_execute",
        file!(),
        line!(),
        Some("component"),
    );
    runtime.block_on(async {
        for operations in planned_operations {
            let _ = provider
                .kv_store
                .transact_write(operations)
                .await
                .expect("execute transaction");
        }
    });
    guard.finish()
}

#[test]
fn kv_put_item_encode_stream_ttl_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p kv write_path_alloc_tests --
    // --nocapture`): allocation_count=15476, allocated_bytes=3574370.
    let _suite_lock = alloc_suite_lock();
    let report = measure_put_item_encode_stream_ttl_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn kv_batch_write_item_encode_stream_ttl_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p kv write_path_alloc_tests --
    // --nocapture`): allocation_count=15574, allocated_bytes=3595513.
    let _suite_lock = alloc_suite_lock();
    let report = measure_batch_write_encode_stream_ttl_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn kv_batch_write_immediate_gsi_allocation_baseline_tests() {
    let _suite_lock = alloc_suite_lock();
    let report = measure_batch_write_immediate_gsi_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn kv_transact_update_item_allocation_profile_tests() {
    let _suite_lock = alloc_suite_lock();
    let plain = measure_transact_update_plain_table();
    let ttl = measure_transact_update_ttl_table();

    alloc_counter::emit_report(&plain);
    alloc_counter::emit_report(&ttl);

    assert!(plain.allocation_count > 0);
    assert!(plain.allocated_bytes > 0);
    assert!(ttl.allocation_count > 0);
    assert!(ttl.allocated_bytes > 0);
}

#[test]
fn kv_put_item_encode_stage_allocation_breakdown_tests() {
    // Snapshot (2026-02-18, `cargo test -p kv write_path_alloc_tests --
    // --nocapture`): projection: allocation_count=2592, allocated_bytes=886204
    // old_read: allocation_count=96, allocated_bytes=12288
    // stream_envelope: allocation_count=3552, allocated_bytes=1144185
    // ttl_ops: allocation_count=1056, allocated_bytes=115200
    // execute: allocation_count=1248, allocated_bytes=158208
    let _suite_lock = alloc_suite_lock();
    let projection = measure_put_item_encode_projection_and_encode_stage();
    let old_read = measure_put_item_encode_old_read_stage();
    let stream_envelope = measure_put_item_encode_stream_envelope_stage();
    let ttl_ops = measure_put_item_encode_ttl_ops_stage();
    let execute = measure_put_item_encode_execute_stage();

    alloc_counter::emit_report(&projection);
    alloc_counter::emit_report(&old_read);
    alloc_counter::emit_report(&stream_envelope);
    alloc_counter::emit_report(&ttl_ops);
    alloc_counter::emit_report(&execute);

    assert!(projection.allocation_count > 0);
    assert!(stream_envelope.allocation_count > 0);
    assert!(execute.allocation_count > 0);
}

#[test]
fn given_update_plan_does_not_need_old_item_when_old_result_is_skipped_then_allocations_drop_tests()
{
    let _suite_lock = alloc_suite_lock();
    let baseline = measure_update_plan_preserve_old_baseline();
    let optimized = measure_update_plan_skip_old_optimized();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected skipped old-item result to allocate less often, baseline={} optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected skipped old-item result to allocate fewer bytes, baseline={} optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}
