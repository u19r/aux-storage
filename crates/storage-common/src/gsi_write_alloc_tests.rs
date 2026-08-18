use std::{collections::HashMap, hint::black_box};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StoredTableInfo, TableName, TableStatus,
    TimestampMillis,
};

use crate::{
    GsiKeyParts, apply_gsi_projection, key_parts, plan_gsi_write_actions, ttl::is_ttl_index,
};

#[derive(Debug)]
enum LegacyGsiWriteAction<'a> {
    Delete {
        index: GlobalSecondaryIndex,
        gsi_key: GsiKeyParts<'a>,
        table_key: GsiKeyParts<'a>,
    },
    Put {
        index: GlobalSecondaryIndex,
        gsi_key: GsiKeyParts<'a>,
        table_key: GsiKeyParts<'a>,
        projected_item: HashMap<String, AttributeValue>,
    },
}

#[test]
fn given_repeated_gsi_write_planning_when_index_is_borrowed_then_allocations_drop_tests() {
    let table = allocation_table_info();
    let old = allocation_item("old-gsi");
    let new = allocation_item("new-gsi");

    let baseline = measure_legacy_owned_index_plan(&table, &old, &new);
    let optimized = measure_borrowed_index_plan(&table, &old, &new);

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected borrowed index plan to allocate less often, baseline={} optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected borrowed index plan to allocate fewer bytes, baseline={} optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

#[test]
fn given_unprojected_update_when_projection_compared_by_borrow_then_allocations_drop_tests() {
    let table = allocation_table_info();
    let old = allocation_item_with_payload("stable-gsi", "old payload");
    let new = allocation_item_with_payload("stable-gsi", "new payload");

    let baseline = measure_legacy_project_then_compare(&table, &old, &new);
    let optimized = measure_borrowed_index_plan(&table, &old, &new);

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected borrowed projection comparison to allocate less often, baseline={} optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected borrowed projection comparison to allocate fewer bytes, baseline={} optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn measure_legacy_owned_index_plan(
    table: &StoredTableInfo,
    old: &HashMap<String, AttributeValue>,
    new: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "gsi_write_plan_owned_index_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..128 {
        let actions = legacy_plan_gsi_write_actions(table, Some(old), Some(new));
        black_box(legacy_action_shape(&actions));
    }
    guard.finish()
}

fn measure_legacy_project_then_compare(
    table: &StoredTableInfo,
    old: &HashMap<String, AttributeValue>,
    new: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "gsi_write_plan_project_then_compare_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..128 {
        let actions = legacy_project_then_compare_plan(table, old, new);
        black_box(legacy_action_shape(&actions));
    }
    guard.finish()
}

fn measure_borrowed_index_plan(
    table: &StoredTableInfo,
    old: &HashMap<String, AttributeValue>,
    new: &HashMap<String, AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "gsi_write_plan_borrowed_index_optimized",
        file!(),
        line!(),
        Some("optimized"),
    );
    for _ in 0..128 {
        let actions = plan_gsi_write_actions(table, Some(old), Some(new)).expect("plan gsi writes");
        black_box(
            actions
                .iter()
                .map(|action| match action {
                    crate::GsiWriteAction::Delete {
                        index,
                        gsi_key,
                        table_key,
                    } => index.key_schema.len() + gsi_key.len() + table_key.len(),
                    crate::GsiWriteAction::Put {
                        index,
                        gsi_key,
                        table_key,
                        projected_item,
                    } => {
                        index.key_schema.len()
                            + gsi_key.len()
                            + table_key.len()
                            + projected_item.len()
                    }
                })
                .sum::<usize>(),
        );
    }
    guard.finish()
}

fn legacy_plan_gsi_write_actions<'a>(
    table_info: &'a StoredTableInfo,
    old_item: Option<&'a HashMap<String, AttributeValue>>,
    new_item: Option<&'a HashMap<String, AttributeValue>>,
) -> Vec<LegacyGsiWriteAction<'a>> {
    let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
        return Vec::new();
    };

    let mut actions = Vec::new();
    for gsi in gsis.iter().filter(|gsi| !is_ttl_index(&gsi.index_name)) {
        let old_gsi_key = key_parts(&gsi.key_schema, old_item);
        let old_table_key = key_parts(&table_info.key_schema, old_item);
        let new_gsi_key = key_parts(&gsi.key_schema, new_item);
        let new_table_key = key_parts(&table_info.key_schema, new_item);

        if let (Some(gsi_key), Some(table_key)) = (old_gsi_key.as_ref(), old_table_key.as_ref())
            && new_gsi_key.as_ref() != Some(gsi_key)
        {
            actions.push(LegacyGsiWriteAction::Delete {
                index: gsi.clone(),
                gsi_key: gsi_key.clone(),
                table_key: table_key.clone(),
            });
        }

        let (Some(item), Some(gsi_key), Some(table_key)) =
            (new_item, new_gsi_key.as_ref(), new_table_key.as_ref())
        else {
            continue;
        };
        actions.push(LegacyGsiWriteAction::Put {
            index: gsi.clone(),
            gsi_key: gsi_key.clone(),
            table_key: table_key.clone(),
            projected_item: apply_gsi_projection(
                item,
                Some(&gsi.projection),
                &table_info.key_schema,
                &gsi.key_schema,
            ),
        });
    }

    actions
}

fn legacy_project_then_compare_plan<'a>(
    table_info: &'a StoredTableInfo,
    old_item: &'a HashMap<String, AttributeValue>,
    new_item: &'a HashMap<String, AttributeValue>,
) -> Vec<LegacyGsiWriteAction<'a>> {
    let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
        return Vec::new();
    };

    let mut actions = Vec::new();
    for gsi in gsis.iter().filter(|gsi| !is_ttl_index(&gsi.index_name)) {
        let old_gsi_key = key_parts(&gsi.key_schema, Some(old_item));
        let new_gsi_key = key_parts(&gsi.key_schema, Some(new_item));
        let new_table_key = key_parts(&table_info.key_schema, Some(new_item));

        let (Some(old_gsi_key), Some(gsi_key), Some(table_key)) = (
            old_gsi_key.as_ref(),
            new_gsi_key.as_ref(),
            new_table_key.as_ref(),
        ) else {
            continue;
        };
        let projected_item = apply_gsi_projection(
            new_item,
            Some(&gsi.projection),
            &table_info.key_schema,
            &gsi.key_schema,
        );
        if old_gsi_key == gsi_key {
            let old_projected = apply_gsi_projection(
                old_item,
                Some(&gsi.projection),
                &table_info.key_schema,
                &gsi.key_schema,
            );
            if old_projected == projected_item {
                continue;
            }
        }

        actions.push(LegacyGsiWriteAction::Put {
            index: gsi.clone(),
            gsi_key: gsi_key.clone(),
            table_key: table_key.clone(),
            projected_item,
        });
    }

    actions
}

fn legacy_action_shape(actions: &[LegacyGsiWriteAction<'_>]) -> usize {
    actions
        .iter()
        .map(|action| match action {
            LegacyGsiWriteAction::Delete {
                index,
                gsi_key,
                table_key,
            } => index.key_schema.len() + gsi_key.len() + table_key.len(),
            LegacyGsiWriteAction::Put {
                index,
                gsi_key,
                table_key,
                projected_item,
            } => index.key_schema.len() + gsi_key.len() + table_key.len() + projected_item.len(),
        })
        .sum()
}

fn allocation_table_info() -> StoredTableInfo {
    StoredTableInfo {
        max_indexers: storage_types::MaxIndexers::ZERO,
        table_name: TableName::new("clone-audit-table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![
            attr("pk"),
            attr("sk"),
            attr("gsi_pk"),
            attr("gsi_sk"),
            attr("payload"),
        ],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: Some(
            (0..8)
                .map(|index| GlobalSecondaryIndex {
                    index_name: IndexName::new(&format!("clone_audit_gsi_{index}")),
                    key_schema: vec![key("gsi_pk", KeyType::Hash), key("gsi_sk", KeyType::Range)],
                    projection: Projection {
                        projection_type: Some(ProjectionType::KeysOnly),
                        non_key_attributes: None,
                    },
                })
                .collect(),
        ),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn allocation_item(gsi_prefix: &str) -> HashMap<String, AttributeValue> {
    allocation_item_with_payload(gsi_prefix, &"x".repeat(512))
}

fn allocation_item_with_payload(
    gsi_prefix: &str,
    payload: &str,
) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("pk".to_string())),
        ("sk".to_string(), AttributeValue::S("sk".to_string())),
        (
            "gsi_pk".to_string(),
            AttributeValue::S(format!("{gsi_prefix}-pk")),
        ),
        (
            "gsi_sk".to_string(),
            AttributeValue::S(format!("{gsi_prefix}-sk")),
        ),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
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
