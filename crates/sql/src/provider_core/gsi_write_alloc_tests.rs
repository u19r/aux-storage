use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_common::{GsiKeyParts, key_parts_to_map, plan_gsi_write_actions};
use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StorageError, StorageResult,
    StoredTableInfo, TableName, TableStatus, TimestampMillis,
};

use crate::provider_core::gsi_write::*;

const ITERATIONS: usize = 1_024;

fn realistic_item(gsi_pk: &str, payload_marker: &str) -> HashMap<String, AttributeValue> {
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
    item.insert(
        "payload".to_string(),
        AttributeValue::S(format!("{payload_marker}{}", "x".repeat(1_100))),
    );
    item.insert(
        "category".to_string(),
        AttributeValue::S("category#1".to_string()),
    );
    item.insert(
        "owner".to_string(),
        AttributeValue::S("owner#2".to_string()),
    );
    item.insert("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string()));
    item.insert(
        "gsi_sk".to_string(),
        AttributeValue::S(format!("gsi0#sort#{:092}", 1)),
    );
    item
}

fn table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![attr("pk"), attr("sk"), attr("gsi_pk"), attr("gsi_sk")],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi"),
            key_schema: vec![key("gsi_pk", KeyType::Hash), key("gsi_sk", KeyType::Range)],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
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

fn string_param(value: &AttributeValue) -> StorageResult<String> {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => Ok(value.clone()),
        _ => Err(StorageError::validation("test value must be scalar")),
    }
}

#[allow(clippy::type_complexity)]
fn options() -> GsiSqlPlanOptions<
    'static,
    String,
    impl Fn(&TableName, &IndexName) -> String,
    impl Fn(&AttributeValue) -> StorageResult<String>,
    impl Fn() -> String,
    impl Fn(usize, Option<&KeyAttributeType>) -> String,
    impl Fn(&str, Option<&str>) -> String,
> {
    GsiSqlPlanOptions::new(
        |table_name, index_name| format!("{}_{}", table_name.as_ref(), index_name.as_ref()),
        string_param,
        String::new,
        |index, _| format!("${index}"),
        |attribute_name, prefix| match prefix {
            Some(prefix) => format!("{prefix}{attribute_name}"),
            None => attribute_name.to_string(),
        },
        GsiUpsertStyle::OnConflictUpdateReturning,
        TableKeyColumnStyle::PrefixedAttributeNames,
        PlaceholderNumbering::AcrossPlan,
        GsiAttributesBlobStyle::NonKeyAttributes,
    )
}

fn first_put_action<'a>(
    table_info: &'a StoredTableInfo,
    old_item: Option<&'a HashMap<String, AttributeValue>>,
    new_item: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<(
    &'a GlobalSecondaryIndex,
    GsiKeyParts<'a>,
    GsiKeyParts<'a>,
    HashMap<String, AttributeValue>,
)> {
    for action in plan_gsi_write_actions(table_info, old_item, new_item)? {
        if let storage_common::GsiWriteAction::Put {
            index,
            gsi_key,
            table_key,
            projected_item,
        } = action
        {
            return Ok((index, gsi_key, table_key, projected_item));
        }
    }
    Err(StorageError::internal("missing put action"))
}

fn legacy_key_binding_work(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<usize> {
    let options = options();
    let (index, gsi_key, table_key, _) = first_put_action(table_info, old_item, new_item)?;
    let gsi_key = key_parts_to_map(&gsi_key);
    let table_key = key_parts_to_map(&table_key);
    let mut bytes = 0usize;
    for key in &index.key_schema {
        let value = gsi_key
            .get(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        bytes += (options.scalar_param)(value)?.len();
    }
    for key in &table_info.key_schema {
        let value = table_key
            .get(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        bytes += (options.scalar_param)(value)?.len();
    }
    Ok(bytes)
}

fn optimized_key_binding_work(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<usize> {
    let options = options();
    let (index, gsi_key, table_key, _) = first_put_action(table_info, old_item, new_item)?;
    let mut bytes = 0usize;
    for key in &index.key_schema {
        let value = gsi_key
            .iter()
            .find(|part| part.name == key.attribute_name)
            .map(|part| part.value)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        bytes += (options.scalar_param)(value)?.len();
    }
    for key in &table_info.key_schema {
        let value = table_key
            .iter()
            .find(|part| part.name == key.attribute_name)
            .map(|part| part.value)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        bytes += (options.scalar_param)(value)?.len();
    }
    Ok(bytes)
}

fn legacy_projected_blob_work(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<usize> {
    let (index, _, _, projected_item) = first_put_action(table_info, old_item, new_item)?;
    let attributes: HashMap<String, AttributeValue> = projected_item
        .iter()
        .filter(|(name, _)| {
            !index
                .key_schema
                .iter()
                .chain(table_info.key_schema.iter())
                .any(|key| key.attribute_name == **name)
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let blob = if attributes.is_empty() {
        "{}".to_string()
    } else {
        serde_json::to_string(&attributes)?
    };
    Ok(blob.len())
}

fn optimized_projected_blob_work(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<usize> {
    let (index, _, _, projected_item) = first_put_action(table_info, old_item, new_item)?;
    let blob = projected_attributes_blob(
        &projected_item,
        &index.key_schema,
        &table_info.key_schema,
        GsiAttributesBlobStyle::NonKeyAttributes,
    )?;
    Ok(blob.len())
}

fn measure_work(
    label: &'static str,
    work: impl Fn(
        &StoredTableInfo,
        Option<&HashMap<String, AttributeValue>>,
        Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<usize>,
) -> alloc_counter::AllocationReport<'static> {
    let table = table_info();
    let old = realistic_item(&format!("gsi0#partition#{:092}", 1), "old");
    let new = realistic_item(&format!("gsi0#partition#{:092}", 2), "new");
    let guard = AllocationGuard::start(
        module_path!(),
        "sql_gsi_planner_hashmap_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut bytes = 0usize;
    for _ in 0..ITERATIONS {
        bytes += work(&table, Some(&old), Some(&new)).expect("plan gsi sql");
    }
    std::hint::black_box(bytes);
    guard.finish()
}

#[test]
fn sql_gsi_planner_avoids_key_part_hashmaps_tests() {
    let legacy = measure_work("sql_gsi_planner_legacy_key_maps", legacy_key_binding_work);
    let optimized = measure_work(
        "sql_gsi_planner_borrowed_key_parts",
        optimized_key_binding_work,
    );

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn sql_gsi_planner_projected_blob_avoids_hashmap_tests() {
    let legacy = measure_work(
        "sql_gsi_planner_legacy_projected_blob_hashmap",
        legacy_projected_blob_work,
    );
    let optimized = measure_work(
        "sql_gsi_planner_projected_blob_pairs",
        optimized_projected_blob_work,
    );

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}
