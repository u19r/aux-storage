use std::{
    collections::HashMap,
    env,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage_common::provider_perf::{reset_provider, snapshot_provider};
use storage_provider::{
    StorageProvider, StreamTrimMarkerOutcome, StreamTrimScope, StreamTrimState,
};
use storage_types::{
    AttributeDefinition, AttributeValue, CreateGlobalSecondaryIndex, CreateTableRequest, IndexName,
    ItemKey, KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType,
    StorageResult, StreamItemId, StreamKey, StreamName, StreamRetentionDuration, TableName,
    TimestampMillis, UpdateTableRequest,
};
use stream_provider::{StoredStreamPointer, StreamDataType, StreamItem, StreamProvider};

use crate::{
    constants,
    kv_support_tests::{TestProvider, create_test_provider},
    sorted_kv_store::SortedKvStore,
    storage_ops::stream_duration::{
        self, item_stream_policy_version, stream_pointer_index_key, stream_pointer_table_key,
        table_stream_policy_version,
    },
    stream::item_codec,
};

const WRITE_PROFILE_ITEMS: usize = 128;
const WRITE_PROFILE_ITEMS_ENV: &str = "CUSTOM_STREAM_DURATION_WRITE_PROFILE_ITEMS";
const REPEATED_TTL_KEYS: usize = 32;
const REPEATED_TTL_WRITES_PER_KEY: usize = 4;
const CLEANUP_HOT_KEYS: usize = 8;
const CLEANUP_HOT_OLD_ROWS_PER_KEY: usize = 24;
const CLEANUP_COLD_KEYS: usize = 96;
const CLEANUP_COLD_OLD_ROWS_PER_KEY: usize = 2;
const CLEANUP_RECENT_ROWS_PER_KEY: usize = 1;
const CLEANUP_FOREVER_SCOPES: usize = 16;
const PROFILE_GSI_COUNT: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual RocksDB custom stream-duration write profile; run explicitly with --ignored \
            --nocapture"]
async fn custom_stream_duration_realistic_write_profile() -> StorageResult<()> {
    let write_profile_items = write_profile_items();
    let provider = create_profile_provider().await?;
    let table = TableName::new("CustomDurationWriteProfile");
    provider
        .create_table(&profile_table_request(
            &table,
            StreamRetentionDuration::FiniteHours(72),
            StreamRetentionDuration::FiniteHours(72),
        ))
        .await?;

    reset_provider("kv");
    reset_backend_profile_metrics();
    let standard =
        measure_puts(&provider, &table, "standard", 0, write_profile_items, None).await?;
    let standard_backend = backend_profile_counters();
    let standard_counts = profile_counts(&provider).await?;
    emit_write_profile(
        "standard",
        &standard,
        &standard_counts,
        &standard_backend,
        0,
        0,
    )
    .await?;

    reset_backend_profile_metrics();
    let finite_ttl = measure_puts(
        &provider,
        &table,
        "finite_ttl",
        write_profile_items,
        write_profile_items,
        Some(StreamRetentionDuration::FiniteHours(24)),
    )
    .await?;
    let finite_backend = backend_profile_counters();
    let finite_counts = profile_counts(&provider).await?;
    let finite_stale = stale_marker_count(&provider).await?;
    emit_write_profile(
        "finite_ttl",
        &finite_ttl,
        &finite_counts,
        &finite_backend,
        finite_stale,
        write_profile_items,
    )
    .await?;

    let repeated_start_markers = finite_counts.due_markers;
    reset_backend_profile_metrics();
    let repeated_ttl = measure_repeated_ttl_puts(&provider, &table).await?;
    let repeated_backend = backend_profile_counters();
    let repeated_counts = profile_counts(&provider).await?;
    let repeated_stale = stale_marker_count(&provider).await?;
    emit_write_profile(
        "repeated_ttl",
        &repeated_ttl,
        &repeated_counts,
        &repeated_backend,
        repeated_stale,
        REPEATED_TTL_KEYS,
    )
    .await?;
    let expected_repeated_markers = repeated_start_markers + REPEATED_TTL_KEYS;
    if expected_repeated_markers <= storage_common::MAX_GENERIC_LIMIT as usize {
        assert_eq!(
            repeated_counts.due_markers, expected_repeated_markers,
            "repeated identical TTL writes should upsert one marker per hot key, not one marker \
             per write"
        );
    }
    assert_eq!(
        repeated_stale, 0,
        "stable policy versions should not create stale repeated-TTL markers"
    );

    reset_backend_profile_metrics();
    let shrink = measure_table_duration_update(
        &provider,
        &table,
        "table_shrink",
        StreamRetentionDuration::FiniteHours(12),
    )
    .await?;
    let shrink_backend = backend_profile_counters();
    let shrink_counts = profile_counts(&provider).await?;
    emit_table_update_profile("table_shrink", &shrink, &shrink_counts, &shrink_backend).await?;

    reset_backend_profile_metrics();
    let expansion = measure_table_duration_update(
        &provider,
        &table,
        "table_expansion",
        StreamRetentionDuration::FiniteHours(96),
    )
    .await?;
    let expansion_backend = backend_profile_counters();
    let expansion_counts = profile_counts(&provider).await?;
    emit_table_update_profile(
        "table_expansion",
        &expansion,
        &expansion_counts,
        &expansion_backend,
    )
    .await?;

    Ok(())
}

fn write_profile_items() -> usize {
    env::var(WRITE_PROFILE_ITEMS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|items| *items > 0)
        .unwrap_or(WRITE_PROFILE_ITEMS)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual RocksDB custom stream-duration cleanup profile; run explicitly with --ignored \
            --nocapture"]
async fn custom_stream_duration_cleanup_capacity_profile() -> StorageResult<()> {
    let provider = create_profile_provider().await?;
    let table = TableName::new("CustomDurationCleanupProfile");
    provider
        .create_table(&profile_table_request(
            &table,
            StreamRetentionDuration::FiniteHours(1),
            StreamRetentionDuration::FiniteHours(1),
        ))
        .await?;

    let now = TimestampMillis::now();
    let old_created_at = now - (2 * constants::MILLIS_PER_HOUR);
    let recent_created_at = now;
    let seeded =
        seed_cleanup_stream_rows(&provider, &table, old_created_at, recent_created_at).await?;
    write_due_table_state(&provider, &table, now - constants::MILLIS_PER_HOUR).await?;

    reset_provider("kv");
    let guard = AllocationGuard::start(
        module_path!(),
        "custom_stream_duration_cleanup_capacity_profile",
        file!(),
        line!(),
        Some("table_and_item_cleanup"),
    );
    let started = Instant::now();
    let mut passes = 0usize;
    loop {
        passes += 1;
        provider.run_job(storage_common::STREAM_TRIM_JOB).await?;
        if StorageProvider::list_due_stream_trim_markers(&provider, TimestampMillis::now(), 1024)
            .await?
            .is_empty()
        {
            break;
        }
        if passes > 32 {
            break;
        }
    }
    let elapsed = started.elapsed();
    let allocations = guard.finish();
    let counts = profile_counts(&provider).await?;
    let stale_markers = stale_marker_count(&provider).await?;
    let backlog_age_ms = trim_backlog_age_ms(&provider).await?;
    let counters = counter_totals();
    println!(
        "{{\"schema_version\":1,\"event\":\"custom_stream_duration_cleanup_profile\",\"label\":\"\
         table_and_item_cleanup\",\"workload_items\":{},\"hot_keys\":{},\"cold_keys\":{},\"\
         forever_scopes\":{},\"passes\":{},\"runtime_ms\":{},\"state_rows\":{},\"due_markers\":{},\
         \"stale_markers\":{},\"trim_backlog_age_ms\":{},\"rows_deleted\":{},\"range_deletes\":{},\
         \"batch_deletes\":{},\"allocations\":{},\"memory_bytes\":{}}}",
        seeded,
        CLEANUP_HOT_KEYS,
        CLEANUP_COLD_KEYS,
        CLEANUP_FOREVER_SCOPES,
        passes,
        elapsed.as_millis(),
        counts.state_rows,
        counts.due_markers,
        stale_markers,
        backlog_age_ms,
        counters.rows_deleted,
        counters.range_deletes,
        counters.point_deletes,
        allocations.allocation_count,
        allocations.allocated_bytes
    );
    assert_eq!(stale_markers, 0);
    assert!(
        counters.range_deletes > 0,
        "cleanup profile should exercise the bounded range-delete stream path"
    );
    assert_eq!(
        counters.point_deletes, 0,
        "cleanup profile should clear pointer/index rows with bounded ranges"
    );

    Ok(())
}

async fn create_profile_provider() -> StorageResult<TestProvider> {
    let provider = create_test_provider()
        .with_immediate_gsi_consistency(true)
        .with_database_jobs_enabled(false);
    provider.initialize_storage().await?;
    provider
        .initialize_stream()
        .await
        .map_err(stream_provider::StreamError::into_storage_enum)?;
    Ok(provider)
}

async fn measure_puts(
    provider: &TestProvider,
    table: &TableName,
    label: &'static str,
    start: usize,
    count: usize,
    ttl: Option<StreamRetentionDuration>,
) -> StorageResult<ProfileMeasurement> {
    let guard = AllocationGuard::start(
        module_path!(),
        "custom_stream_duration_realistic_write_profile",
        file!(),
        line!(),
        Some(label),
    );
    let started = Instant::now();
    for id in start..start + count {
        provider
            .put_item_with_stream_ttl(
                table.clone(),
                profile_item(label, id),
                None,
                None,
                None,
                None,
                ttl,
            )
            .await?;
    }
    Ok(ProfileMeasurement {
        elapsed: started.elapsed(),
        allocations: guard.finish(),
        operations: count,
    })
}

async fn measure_repeated_ttl_puts(
    provider: &TestProvider,
    table: &TableName,
) -> StorageResult<ProfileMeasurement> {
    let guard = AllocationGuard::start(
        module_path!(),
        "custom_stream_duration_realistic_write_profile",
        file!(),
        line!(),
        Some("repeated_ttl"),
    );
    let started = Instant::now();
    for rewrite in 0..REPEATED_TTL_WRITES_PER_KEY {
        for id in 0..REPEATED_TTL_KEYS {
            let mut item = profile_item("repeated", id);
            item.insert(
                "rewrite".to_string(),
                AttributeValue::N(rewrite.to_string()),
            );
            provider
                .put_item_with_stream_ttl(
                    table.clone(),
                    item,
                    None,
                    None,
                    None,
                    None,
                    Some(StreamRetentionDuration::FiniteHours(24)),
                )
                .await?;
        }
    }
    Ok(ProfileMeasurement {
        elapsed: started.elapsed(),
        allocations: guard.finish(),
        operations: REPEATED_TTL_KEYS * REPEATED_TTL_WRITES_PER_KEY,
    })
}

async fn emit_write_profile(
    label: &str,
    measurement: &ProfileMeasurement,
    counts: &stream_duration::StreamTrimDebugCounts,
    backend: &BackendProfileCounters,
    stale_markers: usize,
    expected_policy_scopes: usize,
) -> StorageResult<()> {
    let counters = counter_totals();
    println!(
        concat!(
            "{{\"schema_version\":1,\"event\":\"custom_stream_duration_write_profile\",",
            "\"label\":\"{}\",\"operations\":{},\"runtime_ms\":{},\"allocations\":{},",
            "\"memory_bytes\":{},\"state_rows\":{},\"due_markers\":{},",
            "\"stale_markers\":{},\"expected_policy_scopes\":{},\"rows_deleted\":{},",
            "\"range_deletes\":{},\"batch_deletes\":{},\"fdb_table_attempts\":{},",
            "\"fdb_table_mutations\":{},\"fdb_table_applied_mutations\":{},",
            "\"fdb_table_gsi_mutations\":{},\"fdb_table_commits\":{},",
            "\"fdb_table_retries\":{},\"fdb_table_sets\":{},\"fdb_table_write_bytes\":{},",
            "\"fdb_table_write_key_bytes\":{},\"fdb_table_read_key_bytes\":{}}}"
        ),
        label,
        measurement.operations,
        measurement.elapsed.as_millis(),
        measurement.allocations.allocation_count,
        measurement.allocations.allocated_bytes,
        counts.state_rows,
        counts.due_markers,
        stale_markers,
        expected_policy_scopes,
        counters.rows_deleted,
        counters.range_deletes,
        counters.point_deletes,
        backend.table_attempts,
        backend.table_mutations,
        backend.table_applied_mutations,
        backend.table_gsi_mutations,
        backend.table_commits,
        backend.table_retries,
        backend.table_sets,
        backend.table_write_bytes,
        backend.table_write_key_bytes,
        backend.table_read_key_bytes
    );
    Ok(())
}

async fn measure_table_duration_update(
    provider: &TestProvider,
    table: &TableName,
    label: &'static str,
    retention: StreamRetentionDuration,
) -> StorageResult<ProfileMeasurement> {
    let guard = AllocationGuard::start(
        module_path!(),
        "custom_stream_duration_realistic_write_profile",
        file!(),
        line!(),
        Some(label),
    );
    let started = Instant::now();
    provider
        .update_table(UpdateTableRequest {
            table_name: table.clone(),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: None,
            sse_specification: None,
            stream_specification: None,
            aux_stream_duration_hours: Some(retention),
            aux_default_item_stream_duration_hours: None,
            table_class: None,
        })
        .await?;
    Ok(ProfileMeasurement {
        elapsed: started.elapsed(),
        allocations: guard.finish(),
        operations: 1,
    })
}

async fn emit_table_update_profile(
    label: &str,
    measurement: &ProfileMeasurement,
    counts: &stream_duration::StreamTrimDebugCounts,
    backend: &BackendProfileCounters,
) -> StorageResult<()> {
    println!(
        concat!(
            "{{\"schema_version\":1,\"event\":\"custom_stream_duration_table_update_profile\",",
            "\"label\":\"{}\",\"operations\":{},\"runtime_ms\":{},\"allocations\":{},",
            "\"memory_bytes\":{},\"state_rows\":{},\"due_markers\":{},",
            "\"rows_deleted\":0,\"range_deletes\":0,\"batch_deletes\":0,",
            "\"fdb_table_attempts\":{},\"fdb_table_mutations\":{},",
            "\"fdb_table_applied_mutations\":{},\"fdb_table_gsi_mutations\":{},",
            "\"fdb_table_commits\":{},\"fdb_table_retries\":{},\"fdb_table_sets\":{},",
            "\"fdb_table_write_bytes\":{},\"fdb_table_write_key_bytes\":{},",
            "\"fdb_table_read_key_bytes\":{}}}"
        ),
        label,
        measurement.operations,
        measurement.elapsed.as_millis(),
        measurement.allocations.allocation_count,
        measurement.allocations.allocated_bytes,
        counts.state_rows,
        counts.due_markers,
        backend.table_attempts,
        backend.table_mutations,
        backend.table_applied_mutations,
        backend.table_gsi_mutations,
        backend.table_commits,
        backend.table_retries,
        backend.table_sets,
        backend.table_write_bytes,
        backend.table_write_key_bytes,
        backend.table_read_key_bytes
    );
    Ok(())
}

async fn profile_counts(
    provider: &TestProvider,
) -> StorageResult<stream_duration::StreamTrimDebugCounts> {
    provider
        .stream_trim_debug_counts_kv(
            TimestampMillis::now() + (10 * 365 * 24 * constants::MILLIS_PER_HOUR),
        )
        .await
}

async fn stale_marker_count(provider: &TestProvider) -> StorageResult<usize> {
    let markers = StorageProvider::list_due_stream_trim_markers(
        provider,
        TimestampMillis::now() + (10 * 365 * 24 * constants::MILLIS_PER_HOUR),
        u32::MAX as usize,
    )
    .await?;
    let mut stale = 0usize;
    for marker in markers {
        let Some(state) = provider.load_stream_trim_state_kv(&marker.scope).await? else {
            stale += 1;
            continue;
        };
        if state.validated_marker_outcome(&marker) == StreamTrimMarkerOutcome::Stale {
            stale += 1;
        }
    }
    Ok(stale)
}

async fn trim_backlog_age_ms(provider: &TestProvider) -> StorageResult<i64> {
    let markers =
        StorageProvider::list_due_stream_trim_markers(provider, TimestampMillis::now(), 1024)
            .await?;
    let Some(oldest_due) = markers
        .iter()
        .map(|marker| marker.due_bucket.timestamp_millis())
        .min()
    else {
        return Ok(0);
    };
    Ok(TimestampMillis::now()
        .timestamp_millis()
        .saturating_sub(oldest_due))
}

fn profile_table_request(
    table: &TableName,
    table_retention: StreamRetentionDuration,
    default_item_retention: StreamRetentionDuration,
) -> CreateTableRequest {
    let mut attributes = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ];
    let mut indexes = Vec::with_capacity(PROFILE_GSI_COUNT);
    for index in 0..PROFILE_GSI_COUNT {
        let gsi_pk = format!("gsi{index}_pk");
        let gsi_sk = format!("gsi{index}_sk");
        attributes.push(AttributeDefinition {
            attribute_name: gsi_pk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        attributes.push(AttributeDefinition {
            attribute_name: gsi_sk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        indexes.push(CreateGlobalSecondaryIndex {
            index_name: IndexName::new(&format!("gsi{index}")),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: gsi_pk,
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: gsi_sk,
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
    }
    let mut request = CreateTableRequest::new(
        table.clone(),
        attributes,
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
    .with_global_secondary_indexes(Some(indexes))
    .with_stream_specification(Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    }));
    request.aux_stream_duration_hours = Some(table_retention);
    request.aux_default_item_stream_duration_hours = Some(default_item_retention);
    request
}

fn profile_item(prefix: &str, id: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(2 + PROFILE_GSI_COUNT * 2 + 12);
    item.insert(
        "pk".to_string(),
        AttributeValue::S(format!("{prefix}-partition-{id:064}")),
    );
    item.insert("sk".to_string(), AttributeValue::S(format!("{id:032}")));
    for index in 0..PROFILE_GSI_COUNT {
        item.insert(
            format!("gsi{index}_pk"),
            AttributeValue::S(format!("group-{index}-{}", id % 64)),
        );
        item.insert(
            format!("gsi{index}_sk"),
            AttributeValue::S(format!("{prefix}-{id:032}")),
        );
    }
    for attr in 0..10 {
        item.insert(
            format!("attr{attr}"),
            AttributeValue::S(format!(
                "{prefix}-{id:032}-attribute-{attr}-{}",
                "x".repeat(90 + (id + attr) % 40)
            )),
        );
    }
    item.insert(
        "ttl".to_string(),
        AttributeValue::N((1_900_000_000_i64 + id as i64).to_string()),
    );
    item
}

async fn seed_cleanup_stream_rows(
    provider: &TestProvider,
    table: &TableName,
    old_created_at: TimestampMillis,
    recent_created_at: TimestampMillis,
) -> StorageResult<usize> {
    let mut stream_rows = 0usize;
    for key_index in 0..CLEANUP_HOT_KEYS {
        stream_rows += seed_item_stream_rows(
            provider,
            table,
            "hot",
            key_index,
            CLEANUP_HOT_OLD_ROWS_PER_KEY,
            old_created_at,
            recent_created_at,
        )
        .await?;
    }
    for key_index in 0..CLEANUP_COLD_KEYS {
        stream_rows += seed_item_stream_rows(
            provider,
            table,
            "cold",
            key_index,
            CLEANUP_COLD_OLD_ROWS_PER_KEY,
            old_created_at,
            recent_created_at,
        )
        .await?;
    }
    for key_index in 0..CLEANUP_FOREVER_SCOPES {
        write_forever_item_state(provider, table, "forever", key_index, recent_created_at).await?;
    }
    Ok(stream_rows)
}

async fn seed_item_stream_rows(
    provider: &TestProvider,
    table: &TableName,
    group: &str,
    key_index: usize,
    old_rows: usize,
    old_created_at: TimestampMillis,
    recent_created_at: TimestampMillis,
) -> StorageResult<usize> {
    let item_key = profile_item_key(table, group, key_index);
    let item_stream = StreamName::table_item_stream(table, &item_key)?;
    let item = profile_item(group, key_index);
    for row in 0..old_rows {
        let id = stream_id_from_u64(stream_seed(group, key_index, row));
        insert_stream_item(
            provider,
            &StreamName::table_stream(table),
            &build_pointer_stream_item(id, old_created_at, table, item_stream.clone()),
        )
        .await?;
        insert_stream_item(
            provider,
            &item_stream,
            &build_item_stream_item(id, old_created_at, item_stream.clone(), &item),
        )
        .await?;
        insert_stream_pointer_indexes(provider, table, &item_stream, id).await?;
    }
    for row in 0..CLEANUP_RECENT_ROWS_PER_KEY {
        let id = stream_id_from_u64(stream_seed(group, key_index, 9_000_000 + row));
        insert_stream_item(
            provider,
            &StreamName::table_stream(table),
            &build_pointer_stream_item(id, recent_created_at, table, item_stream.clone()),
        )
        .await?;
        insert_stream_item(
            provider,
            &item_stream,
            &build_item_stream_item(id, recent_created_at, item_stream.clone(), &item),
        )
        .await?;
        insert_stream_pointer_indexes(provider, table, &item_stream, id).await?;
    }
    Ok(old_rows + CLEANUP_RECENT_ROWS_PER_KEY)
}

async fn insert_stream_item(
    provider: &TestProvider,
    stream_name: &StreamName,
    stream_item: &StreamItem,
) -> StorageResult<()> {
    let key: StreamKey = stream_name + &stream_item.id;
    let bytes = item_codec::encode_stream_item(stream_item).expect("stream item bytes");
    provider.kv_store.put(key.as_ref(), &bytes, None).await
}

async fn insert_stream_pointer_indexes(
    provider: &TestProvider,
    table: &TableName,
    item_stream: &StreamName,
    stream_item_id: StreamItemId,
) -> StorageResult<()> {
    provider
        .kv_store
        .put(
            &stream_pointer_index_key(table, item_stream, stream_item_id),
            b"",
            None,
        )
        .await?;
    provider
        .kv_store
        .put(&stream_pointer_table_key(table, stream_item_id), b"", None)
        .await
}

async fn write_due_table_state(
    provider: &TestProvider,
    table: &TableName,
    due_at: TimestampMillis,
) -> StorageResult<()> {
    provider
        .write_stream_trim_state(StreamTrimState {
            scope: StreamTrimScope::table(format!("kv-table:{table}"), table.clone()),
            policy_version: table_stream_policy_version(
                StreamRetentionDuration::FiniteHours(1),
                StreamRetentionDuration::FiniteHours(1),
            ),
            retention: StreamRetentionDuration::FiniteHours(1),
            effective_retention: StreamRetentionDuration::FiniteHours(1),
            next_due_at: Some(due_at),
            oldest_retained_version: None,
            oldest_retained_timestamp: None,
            latest_version: None,
            latest_timestamp: None,
            updated_at: due_at,
        })
        .await
}

async fn write_forever_item_state(
    provider: &TestProvider,
    table: &TableName,
    group: &str,
    key_index: usize,
    updated_at: TimestampMillis,
) -> StorageResult<()> {
    let item_key = profile_item_key(table, group, key_index);
    let item_stream = StreamName::table_item_stream(table, &item_key)?;
    let scope_id = stream_duration::item_stream_scope_id(&item_stream);
    let item_key_hash = stream_duration::item_stream_key_hash(&item_stream);
    provider
        .write_stream_trim_state(StreamTrimState {
            scope: StreamTrimScope::item(scope_id, table.clone(), item_key_hash),
            policy_version: item_stream_policy_version(
                StreamRetentionDuration::Forever,
                StreamRetentionDuration::FiniteHours(1),
            ),
            retention: StreamRetentionDuration::Forever,
            effective_retention: StreamRetentionDuration::Forever,
            next_due_at: None,
            oldest_retained_version: None,
            oldest_retained_timestamp: None,
            latest_version: None,
            latest_timestamp: None,
            updated_at,
        })
        .await
}

fn profile_item_key(table: &TableName, group: &str, key_index: usize) -> ItemKey {
    ItemKey::table_key(
        table.clone(),
        AttributeValue::S(format!("{group}-partition-{key_index:064}")),
        Some(AttributeValue::S(format!("{key_index:032}"))),
    )
}

fn build_pointer_stream_item(
    stream_item_id: StreamItemId,
    created_at: TimestampMillis,
    table_name: &TableName,
    item_stream: StreamName,
) -> StreamItem {
    let stored_pointer = StoredStreamPointer::pointer(
        item_stream,
        table_name.clone(),
        storage_types::ItemStreamVersion::from(stream_item_id),
    );
    StreamItem {
        id: stream_item_id,
        stream_name: None,
        data: storage_types::storage_serde::to_bytes(&stored_pointer).expect("pointer bytes"),
        data_type: StreamDataType::StreamPointer,
        created_at,
    }
}

fn build_item_stream_item(
    stream_item_id: StreamItemId,
    created_at: TimestampMillis,
    stream_name: StreamName,
    item: &HashMap<String, AttributeValue>,
) -> StreamItem {
    StreamItem {
        id: stream_item_id,
        stream_name: Some(stream_name),
        data: storage_types::storage_serde::to_bytes(item).expect("item bytes"),
        data_type: StreamDataType::DynamoDbJson,
        created_at,
    }
}

fn stream_id_from_u64(value: u64) -> StreamItemId {
    let mut bytes = [0u8; 12];
    bytes[4..].copy_from_slice(&value.to_be_bytes());
    StreamItemId::from(bytes)
}

fn stream_seed(group: &str, key_index: usize, row: usize) -> u64 {
    let group_offset = match group {
        "hot" => 1_000_000,
        "cold" => 2_000_000,
        _ => 3_000_000,
    };
    group_offset + (key_index as u64 * 1_000) + row as u64
}

fn counter_totals() -> TrimCounterTotals {
    let counters = snapshot_provider("kv");
    TrimCounterTotals {
        rows_deleted: counter_amount(&counters, "custom_stream_duration_rows_deleted"),
        range_deletes: counter_amount(&counters, "custom_stream_duration_range_deletes"),
        point_deletes: counter_amount(&counters, "custom_stream_duration_point_deletes"),
    }
}

fn counter_amount(
    counters: &[storage_common::provider_perf::PerfCounterSnapshot],
    name: &str,
) -> u64 {
    counters
        .iter()
        .find(|counter| counter.name == name)
        .map(|counter| counter.total_amount)
        .unwrap_or(0)
}

fn reset_backend_profile_metrics() {
    reset_provider("foundationdb");
    #[cfg(feature = "foundationdb-backend")]
    crate::backends::fdb::foundationdb_operation_metrics_reset();
}

fn backend_profile_counters() -> BackendProfileCounters {
    let provider_counters = snapshot_provider("foundationdb");
    #[cfg(feature = "foundationdb-backend")]
    {
        let metrics = crate::backends::fdb::foundationdb_operation_metrics_snapshot();
        BackendProfileCounters {
            table_attempts: counter_amount(&provider_counters, "table_write_attempt"),
            table_mutations: counter_amount(&provider_counters, "table_write_mutations"),
            table_applied_mutations: counter_amount(
                &provider_counters,
                "table_write_applied_mutations",
            ),
            table_gsi_mutations: counter_amount(&provider_counters, "table_write_gsi_mutations"),
            table_commits: fdb_operation_metric(&metrics, "transact_write_table", "commit"),
            table_retries: fdb_operation_metric(&metrics, "transact_write_table", "retry"),
            table_sets: fdb_operation_metric(&metrics, "transact_write_table", "set"),
            table_write_bytes: fdb_byte_metric(&metrics, "transact_write_table", "write"),
            table_write_key_bytes: fdb_byte_metric(&metrics, "transact_write_table", "write_key"),
            table_read_key_bytes: fdb_byte_metric(&metrics, "transact_write_table", "read_key"),
        }
    }

    #[cfg(not(feature = "foundationdb-backend"))]
    {
        BackendProfileCounters {
            table_attempts: counter_amount(&provider_counters, "table_write_attempt"),
            table_mutations: counter_amount(&provider_counters, "table_write_mutations"),
            table_applied_mutations: counter_amount(
                &provider_counters,
                "table_write_applied_mutations",
            ),
            table_gsi_mutations: counter_amount(&provider_counters, "table_write_gsi_mutations"),
            ..BackendProfileCounters::default()
        }
    }
}

#[cfg(feature = "foundationdb-backend")]
fn fdb_operation_metric(metrics: &str, path: &str, operation: &str) -> u64 {
    fdb_metric(
        metrics,
        &format!("foundationdb_operations_total{{path=\"{path}\",operation=\"{operation}\"}} "),
    )
}

#[cfg(feature = "foundationdb-backend")]
fn fdb_byte_metric(metrics: &str, path: &str, direction: &str) -> u64 {
    fdb_metric(
        metrics,
        &format!(
            "foundationdb_operation_bytes_total{{path=\"{path}\",direction=\"{direction}\"}} "
        ),
    )
}

#[cfg(feature = "foundationdb-backend")]
fn fdb_metric(metrics: &str, prefix: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

struct ProfileMeasurement {
    elapsed: Duration,
    allocations: alloc_counter::AllocationReport<'static>,
    operations: usize,
}

#[derive(Default)]
struct BackendProfileCounters {
    table_attempts: u64,
    table_mutations: u64,
    table_applied_mutations: u64,
    table_gsi_mutations: u64,
    table_commits: u64,
    table_retries: u64,
    table_sets: u64,
    table_write_bytes: u64,
    table_write_key_bytes: u64,
    table_read_key_bytes: u64,
}

struct TrimCounterTotals {
    rows_deleted: u64,
    range_deletes: u64,
    point_deletes: u64,
}
