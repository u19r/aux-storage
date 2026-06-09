use std::{borrow::Cow, collections::HashMap, time::Instant};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StorageError, StorageResult,
    StoredTableInfo, TableName, TableStatus, TimestampMillis,
};

use crate::{GsiPhysicalName, backends::sqlite::process_gsi_updates::*};

const ITERATIONS: usize = 1_024;

fn realistic_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(16);
    item.insert(
        "pk".to_string(),
        AttributeValue::S("tenant#00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001".to_string()),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S("item#0001#sort-key-component-with-realistic-dynamodb-length-000000000000000000000000000000".to_string()),
    );
    item.insert(
        "ttl".to_string(),
        AttributeValue::N("2200000000".to_string()),
    );
    item.insert(
        "status".to_string(),
        AttributeValue::S("active".to_string()),
    );
    item.insert("attempts".to_string(), AttributeValue::N("7".to_string()));
    item.insert("payload".to_string(), AttributeValue::S("x".repeat(1_100)));
    item.insert(
        "category".to_string(),
        AttributeValue::S("category#1".to_string()),
    );
    item.insert(
        "owner".to_string(),
        AttributeValue::S("owner#2".to_string()),
    );
    item.insert(
        "gsi0pk".to_string(),
        AttributeValue::S(format!("gsi0#partition#{:092}", 1)),
    );
    item.insert(
        "gsi0sk".to_string(),
        AttributeValue::S(format!("gsi0#sort#{:092}", 1)),
    );
    item
}

fn table_info() -> GsiUpdateTableInfo {
    let stored = StoredTableInfo {
        table_name: TableName::new("hashmap_perf_table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![attr("pk"), attr("sk"), attr("gsi0pk"), attr("gsi0sk")],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi0"),
            key_schema: vec![key("gsi0pk", KeyType::Hash), key("gsi0sk", KeyType::Range)],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    };
    GsiUpdateTableInfo::from(stored)
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

fn legacy_build_attributes_blob(
    item: &HashMap<String, AttributeValue>,
    gsi: &GsiUpdateIndex,
    table_info: &GsiUpdateTableInfo,
) -> Result<Cow<'static, str>, StorageError> {
    if matches!(gsi.projection_plan, ProjectionPlan::KeysOnly) {
        return Ok(Cow::Borrowed("{}"));
    }

    let mut non_key_attributes: HashMap<String, AttributeValue> = HashMap::new();
    match &gsi.projection_plan {
        ProjectionPlan::KeysOnly => {}
        ProjectionPlan::Include(attrs) => {
            for attr in attrs {
                if let Some(value) = item.get(attr)
                    && !is_key_attribute(attr.as_str(), &gsi.key_names, &table_info.key_names)
                {
                    non_key_attributes.insert(attr.clone(), value.clone());
                }
            }
        }
        ProjectionPlan::All => {
            for (key, value) in item {
                if is_key_attribute(key.as_str(), &gsi.key_names, &table_info.key_names) {
                    continue;
                }
                non_key_attributes.insert(key.clone(), value.clone());
            }
        }
    }
    if non_key_attributes.is_empty() {
        return Ok(Cow::Borrowed("{}"));
    }
    serde_json::to_string(&non_key_attributes)
        .map(Cow::Owned)
        .map_err(|err| StorageError::internal(&format!("serialize attributes failed: {err}")))
}

fn measure_attributes_blob(
    label: &'static str,
    build: impl Fn(
        &HashMap<String, AttributeValue>,
        &GsiUpdateIndex,
        &GsiUpdateTableInfo,
    ) -> Result<Cow<'static, str>, StorageError>,
) -> alloc_counter::AllocationReport<'static> {
    let item = realistic_item();
    let table = table_info();
    let gsi = table.gsi_by_name(&IndexName::new("gsi0")).expect("gsi");
    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_gsi_attributes_blob_hashmap_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut bytes = 0usize;
    for _ in 0..ITERATIONS {
        let blob = build(&item, gsi, &table).expect("build attributes blob");
        bytes += blob.len();
    }
    std::hint::black_box(bytes);
    guard.finish()
}

fn measure_attributes_blob_runtime(
    label: &str,
    build: impl Fn(
        &HashMap<String, AttributeValue>,
        &GsiUpdateIndex,
        &GsiUpdateTableInfo,
    ) -> Result<Cow<'static, str>, StorageError>,
) -> f64 {
    let item = realistic_item();
    let table = table_info();
    let gsi = table.gsi_by_name(&IndexName::new("gsi0")).expect("gsi");
    let started = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..ITERATIONS {
        let blob = build(&item, gsi, &table).expect("build attributes blob");
        bytes += blob.len();
    }
    let elapsed = started.elapsed();
    let ns_per_iter = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!(
        "{label} iterations={ITERATIONS} bytes={bytes} elapsed_ms={:.3} \
         ns_per_iter={ns_per_iter:.2}",
        elapsed.as_secs_f64() * 1_000.0,
    );
    ns_per_iter
}

fn legacy_backfill_item_plan(
    item: &HashMap<String, AttributeValue>,
    table: &GsiUpdateTableInfo,
    gsi: &GsiUpdateIndex,
) -> StorageResult<usize> {
    let gsi_key = {
        let mut k = HashMap::new();
        for key_name in &gsi.key_names {
            if let Some(value) = item.get(key_name) {
                k.insert(key_name.clone(), value.clone());
            }
        }
        k
    };
    let main_key = {
        let mut k = HashMap::new();
        for key_name in &table.key_names {
            if let Some(value) = item.get(key_name) {
                k.insert(key_name.clone(), value.clone());
            }
        }
        k
    };
    let filtered = match &gsi.projection_plan {
        ProjectionPlan::All => item.clone(),
        ProjectionPlan::KeysOnly => {
            let mut f = HashMap::new();
            for (key, value) in &gsi_key {
                f.insert(key.clone(), value.clone());
            }
            for (key, value) in &main_key {
                f.insert(key.clone(), value.clone());
            }
            f
        }
        ProjectionPlan::Include(attrs) => {
            let mut f = HashMap::new();
            for (key, value) in &gsi_key {
                f.insert(key.clone(), value.clone());
            }
            for (key, value) in &main_key {
                f.insert(key.clone(), value.clone());
            }
            for attr_name in attrs {
                if let Some(value) = item.get(attr_name) {
                    f.insert(attr_name.clone(), value.clone());
                }
            }
            f
        }
    };
    let gsi_table_name =
        GsiPhysicalName::compose(&table.source.table_name.sanitized_name(), "gsi0").to_string();
    let mut all_columns: Vec<String> = gsi_key.keys().cloned().collect();
    let mut all_values: Vec<String> = gsi_key
        .values()
        .map(|value| value.inner_string())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| StorageError::internal(&format!("gsi key scalar conversion: {err}")))?;
    for (key, value) in main_key {
        all_columns.push(format!("table_{key}"));
        all_values.push(value.inner_string().map_err(|err| {
            StorageError::internal(&format!("gsi main key scalar conversion: {err}"))
        })?);
    }
    let non_key_attributes: HashMap<String, AttributeValue> = filtered
        .into_iter()
        .filter(|(key, _)| {
            !all_columns
                .iter()
                .any(|column| column == key || column == &format!("table_{key}"))
        })
        .collect();
    let attributes_blob = if non_key_attributes.is_empty() {
        "{}".to_string()
    } else {
        serde_json::to_string(&non_key_attributes)
            .map_err(|err| StorageError::internal(&err.to_string()))?
    };
    all_columns.push("attributes_blob".to_string());
    all_values.push(attributes_blob);
    let placeholders = (1..=all_values.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let columns_str = all_columns.join(", ");
    let sql = format!(
        "INSERT OR REPLACE INTO \"{gsi_table_name}\" ({columns_str}) VALUES ({placeholders})"
    );
    Ok(sql.len() + all_values.iter().map(String::len).sum::<usize>())
}

fn optimized_backfill_item_plan(
    item: &HashMap<String, AttributeValue>,
    table: &GsiUpdateTableInfo,
    gsi: &GsiUpdateIndex,
) -> StorageResult<usize> {
    let Some(gsi_key) = full_key(item, &gsi.key_names) else {
        return Ok(0);
    };
    let Some(main_key) = full_key(item, &table.key_names) else {
        return Ok(0);
    };
    let attributes_blob = build_attributes_blob(item, gsi, table)?;
    let mut all_values = Vec::with_capacity(gsi_key.len() + main_key.len() + 1);
    push_key_values(&mut all_values, &gsi_key, "gsi key scalar conversion")?;
    push_key_values(&mut all_values, &main_key, "gsi main key scalar conversion")?;
    all_values.push(attributes_blob);
    Ok(gsi.insert_sql.len() + all_values.iter().map(|value| value.len()).sum::<usize>())
}

fn measure_backfill_item_plan(
    label: &'static str,
    plan: impl Fn(
        &HashMap<String, AttributeValue>,
        &GsiUpdateTableInfo,
        &GsiUpdateIndex,
    ) -> StorageResult<usize>,
) -> alloc_counter::AllocationReport<'static> {
    let item = realistic_item();
    let table = table_info();
    let gsi = table.gsi_by_name(&IndexName::new("gsi0")).expect("gsi");
    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_gsi_backfill_item_plan_hashmap_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut bytes = 0usize;
    for _ in 0..ITERATIONS {
        bytes += plan(&item, &table, gsi).expect("plan backfill item");
    }
    std::hint::black_box(bytes);
    guard.finish()
}

#[test]
fn sqlite_process_gsi_updates_allocation_profile_tests() {
    assert_sqlite_gsi_attributes_blob_avoids_temporary_hashmap();
    assert_sqlite_gsi_backfill_item_plan_avoids_temporary_hashmaps();
}

fn assert_sqlite_gsi_attributes_blob_avoids_temporary_hashmap() {
    let legacy = measure_attributes_blob(
        "sqlite_gsi_attributes_blob_legacy_hashmap",
        legacy_build_attributes_blob,
    );
    let optimized = measure_attributes_blob(
        "sqlite_gsi_attributes_blob_vec_pairs",
        build_attributes_blob,
    );

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}

fn assert_sqlite_gsi_backfill_item_plan_avoids_temporary_hashmaps() {
    let legacy = measure_backfill_item_plan(
        "sqlite_gsi_backfill_item_plan_legacy_hashmaps",
        legacy_backfill_item_plan,
    );
    let optimized = measure_backfill_item_plan(
        "sqlite_gsi_backfill_item_plan_preplanned_vecs",
        optimized_backfill_item_plan,
    );

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture before/after gsi blob changes"]
fn sqlite_gsi_attributes_blob_vec_pairs_runtime_perf_probe() {
    let legacy = measure_attributes_blob_runtime(
        "sqlite_gsi_attributes_blob_legacy_hashmap",
        legacy_build_attributes_blob,
    );
    let optimized = measure_attributes_blob_runtime(
        "sqlite_gsi_attributes_blob_vec_pairs",
        build_attributes_blob,
    );

    assert!(
        optimized <= legacy,
        "expected allocation-focused change not to regress runtime, legacy={legacy:.2}ns \
         optimized={optimized:.2}ns"
    );
}
