use std::{borrow::Cow, collections::HashMap};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeValue, CreateGlobalSecondaryIndex, IndexName, KeySchemaElement, KeyType, Projection,
    ProjectionType, StorageResult, StoredTableInfo, TableName, TableStatus, TimestampMillis,
};

use crate::gsi_lifecycle::*;

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

fn table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("gsi_lifecycle_perf_table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: Vec::new(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

fn create_gsi() -> CreateGlobalSecondaryIndex {
    CreateGlobalSecondaryIndex {
        index_name: IndexName::new("gsi0"),
        key_schema: vec![key("gsi0pk", KeyType::Hash), key("gsi0sk", KeyType::Range)],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}

fn legacy_backfill_row_plan(
    item: &HashMap<String, AttributeValue>,
    table_info: &StoredTableInfo,
    create: &CreateGlobalSecondaryIndex,
) -> StorageResult<usize> {
    let mut gsi_key = HashMap::new();
    for key_element in &create.key_schema {
        if let Some(attr_value) = item.get(&key_element.attribute_name) {
            gsi_key.insert(key_element.attribute_name.clone(), attr_value.clone());
        }
    }
    let mut main_table_key = HashMap::new();
    for key_element in &table_info.key_schema {
        if let Some(attr_value) = item.get(&key_element.attribute_name) {
            main_table_key.insert(key_element.attribute_name.clone(), attr_value.clone());
        }
    }

    let filtered_item = apply_gsi_projection(item, &gsi_key, &main_table_key, &create.projection);
    let non_key_attributes =
        non_key_attributes_for_gsi_row(filtered_item, &gsi_key, &main_table_key);
    let attributes_blob = encode_gsi_attributes_blob(&non_key_attributes)?;

    let gsi_table_name =
        crate::naming::physical_gsi_table_name(&table_info.table_name, &create.index_name);
    let mut all_columns: Vec<String> = gsi_key.keys().cloned().collect();
    all_columns.extend(main_table_key.keys().map(|k| format!("table_{k}")));
    all_columns.push("attributes_blob".to_string());

    let mut all_values: Vec<String> = gsi_key
        .values()
        .map(scalar_gsi_value)
        .collect::<StorageResult<Vec<_>>>()?;
    let mut main_table_values: Vec<String> = main_table_key
        .values()
        .map(scalar_gsi_value)
        .collect::<StorageResult<Vec<_>>>()?;
    all_values.append(&mut main_table_values);
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

fn optimized_backfill_row_plan(
    item: &HashMap<String, AttributeValue>,
    table_info: &StoredTableInfo,
    create: &CreateGlobalSecondaryIndex,
) -> StorageResult<usize> {
    let insert_sql = gsi_backfill_insert_sql(
        &table_info.table_name,
        &create.index_name,
        &create.key_schema,
        &table_info.key_schema,
    );
    let Some(gsi_key) = key_attribute_refs(item, &create.key_schema) else {
        return Ok(0);
    };
    let Some(main_table_key) = key_attribute_refs(item, &table_info.key_schema) else {
        return Ok(0);
    };
    let attributes_blob =
        encode_gsi_projected_attributes_blob(item, &gsi_key, &main_table_key, &create.projection)?;
    let mut all_values: Vec<Cow<'static, str>> =
        Vec::with_capacity(gsi_key.len() + main_table_key.len() + 1);
    push_scalar_key_values(&mut all_values, &gsi_key)?;
    push_scalar_key_values(&mut all_values, &main_table_key)?;
    all_values.push(attributes_blob);
    Ok(insert_sql.len() + all_values.iter().map(|value| value.len()).sum::<usize>())
}

fn measure_backfill_row_plan(
    label: &'static str,
    plan: impl Fn(
        &HashMap<String, AttributeValue>,
        &StoredTableInfo,
        &CreateGlobalSecondaryIndex,
    ) -> StorageResult<usize>,
) -> alloc_counter::AllocationReport<'static> {
    let item = realistic_item();
    let table_info = table_info();
    let create = create_gsi();
    let guard = AllocationGuard::start(
        module_path!(),
        "gsi_lifecycle_backfill_row_plan_hashmap_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut bytes = 0usize;
    for _ in 0..ITERATIONS {
        bytes += plan(&item, &table_info, &create).expect("plan gsi lifecycle backfill row");
    }
    std::hint::black_box(bytes);
    guard.finish()
}

#[test]
fn gsi_lifecycle_backfill_row_plan_avoids_temporary_hashmaps_tests() {
    let legacy = measure_backfill_row_plan(
        "gsi_lifecycle_backfill_row_plan_legacy_hashmaps",
        legacy_backfill_row_plan,
    );
    let optimized = measure_backfill_row_plan(
        "gsi_lifecycle_backfill_row_plan_preplanned_vecs",
        optimized_backfill_row_plan,
    );

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}
