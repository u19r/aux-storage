//! Wide contract tests for `BatchGetPlan`: request grouping, fatal vs
//! unprocessed metadata mapping, ordered finish, and exact-length guards.

use std::{collections::HashMap, sync::Arc};

use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, KeysAndAttributes, MaxIndexers, StorageError, StoredTableInfo,
    StreamRetentionDuration, TableName, TableStatus, TimestampMillis, WireItem,
    context::WrappedError as _,
};

use super::batch::{BatchGetPlan, BatchGetTableResult};

fn key_schema() -> Vec<KeySchemaElement> {
    vec![KeySchemaElement {
        attribute_name: "id".to_string(),
        key_type: KeyType::Hash,
    }]
}

fn table_metadata(table_name: &str) -> Arc<StoredTableInfo> {
    Arc::new(StoredTableInfo {
        table_name: TableName::new(table_name),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: key_schema(),
        max_indexers: MaxIndexers::ZERO,
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        stream_enabled_at: None,
        table_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    })
}

fn item_key(value: &str) -> KeyAttributes {
    HashMap::from([("id".to_string(), AttributeValue::S(value.to_string()))]).into()
}

fn table_keys(table: &str, values: &[&str]) -> (TableName, KeysAndAttributes) {
    (
        TableName::new(table),
        KeysAndAttributes {
            keys: values.iter().map(|value| item_key(value)).collect(),
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: None,
        },
    )
}

fn wire_item(value: &str) -> WireItem {
    WireItem::dynamo_json(value.as_bytes().to_vec())
}

#[test]
fn given_empty_table_entry_when_planned_then_table_is_omitted() {
    let request = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([
            table_keys("full", &["a"]),
            table_keys("empty", &[]),
        ]),
        return_consumed_capacity: None,
    };

    let plan = BatchGetPlan::from(request);

    assert_eq!(plan.tables.len(), 1);
    assert_eq!(plan.tables[0].table_name, TableName::new("full"));
    assert_eq!(&plan.tables[0].keys.keys[..], &[item_key("a")]);
}

#[test]
fn given_table_not_found_when_bind_then_whole_request_fails() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("missing", &["a"])]),
        return_consumed_capacity: None,
    });

    let error = plan.tables[0]
        .bind(Err(StorageError::table_not_found("missing")))
        .unwrap_err();

    assert!(
        error.to_string().contains("missing"),
        "unexpected error: {error}"
    );
}

#[test]
fn given_access_error_or_empty_schema_when_bind_then_table_is_unprocessed() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("table", &["a"])]),
        return_consumed_capacity: None,
    });

    let access = plan.tables[0]
        .bind(Err(StorageError::internal("bootstrap fence failed")))
        .unwrap();
    assert!(access.is_none());

    let mut schemaless = (*table_metadata("table")).clone();
    schemaless.key_schema.clear();
    let empty_schema = plan.tables[0].bind(Ok(Arc::new(schemaless))).unwrap();
    assert!(empty_schema.is_none());
}

#[test]
fn given_matching_metadata_when_bind_then_borrows_request_keys_in_order() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("table", &["a", "b"])]),
        return_consumed_capacity: None,
    });

    let bound = plan.tables[0]
        .bind(Ok(Arc::clone(&table_metadata("table"))))
        .unwrap()
        .unwrap();

    assert_eq!(bound.keys, &[item_key("a"), item_key("b")]);
}

#[test]
fn given_mixed_table_results_when_finish_then_maps_match_existing_shape() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([
            table_keys("hits", &["a", "b"]),
            table_keys("misses", &["c"]),
        ]),
        return_consumed_capacity: None,
    });
    let mut results = Vec::new();
    for table in &plan.tables {
        if table.table_name == TableName::new("hits") {
            results.push(table.finish_read(&mut vec![Some(wire_item("a")), None].into_iter()));
        } else {
            results.push(BatchGetTableResult::Unprocessed);
        }
    }

    let response = plan.finish(results).unwrap();

    let responses = response.responses.unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(
        format!("{:?}", responses.get(&TableName::new("hits")).unwrap()),
        format!("{:?}", vec![wire_item("a")])
    );
    let unprocessed = response.unprocessed_keys.unwrap();
    assert_eq!(unprocessed.len(), 1);
    assert_eq!(
        unprocessed
            .get(&TableName::new("misses"))
            .unwrap()
            .keys
            .len(),
        1
    );
    assert!(response.consumed_capacity.is_none());
}

#[test]
fn given_result_count_mismatch_when_finish_then_internal_error() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("table", &["a"])]),
        return_consumed_capacity: None,
    });

    let error = plan.finish(Vec::new()).unwrap_err();

    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("table result count")
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn given_all_missing_success_when_finish_then_group_omitted_from_both_maps() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("table", &["a", "b"])]),
        return_consumed_capacity: None,
    });
    let results = plan
        .tables
        .iter()
        .map(|table| table.finish_read(&mut vec![None, None].into_iter()))
        .collect();
    let response = plan.finish(results).unwrap();

    assert!(
        response.responses.is_none(),
        "all-missing table must not produce an empty response group"
    );
    assert!(
        response.unprocessed_keys.is_none(),
        "successful all-missing table must not report unprocessed keys"
    );
}

fn optioned_keys(
    keys: Vec<storage_types::KeyAttributes>,
    consistent_read: Option<bool>,
) -> KeysAndAttributes {
    KeysAndAttributes {
        keys: keys.into_iter().collect(),
        attributes_to_get: Some(vec!["value".to_string()]),
        projection_expression: Some("#v".to_string()),
        expression_attribute_names: Some(storage_types::VecMap::from_iter([(
            "#v".to_string(),
            "value".to_string(),
        )])),
        consistent_read,
    }
}

#[test]
fn given_nonempty_options_when_table_unprocessed_then_options_preserved() {
    let first = TableName::new("point-read-options-a");
    let second = TableName::new("point-read-options-b");
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([
            (
                first.clone(),
                optioned_keys(
                    vec![KeyAttributes::from_iter([(
                        "pk".to_owned(),
                        AttributeValue::S("k1".to_owned()),
                    )])],
                    Some(true),
                ),
            ),
            (
                second.clone(),
                optioned_keys(
                    vec![KeyAttributes::from_iter([(
                        "pk".to_owned(),
                        AttributeValue::S("k2".to_owned()),
                    )])],
                    Some(false),
                ),
            ),
        ]),
        return_consumed_capacity: None,
    });
    assert_eq!(plan.tables.len(), 2);
    let results = plan
        .tables
        .iter()
        .map(|_| Ok(BatchGetTableResult::Unprocessed))
        .collect::<storage_types::StorageResult<Vec<_>>>()
        .expect("unprocessed results");
    let response = plan.finish_api_response(results).expect("finish");
    assert!(
        response
            .responses
            .expect("requested API response groups")
            .is_empty()
    );
    let unprocessed = response.unprocessed_keys.expect("unprocessed keys");
    assert_eq!(unprocessed.len(), 2);
    for (table, expected_consistent) in [(first, Some(true)), (second, Some(false))] {
        let kept = unprocessed
            .get(&table)
            .unwrap_or_else(|| panic!("missing unprocessed entry for {table:?}"));
        assert_eq!(kept.keys.len(), 1);
        assert_eq!(
            kept.attributes_to_get,
            Some(vec!["value".to_string()]),
            "attributes_to_get lost for {table:?}"
        );
        assert_eq!(
            kept.projection_expression,
            Some("#v".to_string()),
            "projection lost for {table:?}"
        );
        assert_eq!(
            kept.expression_attribute_names,
            Some(storage_types::VecMap::from_iter([(
                "#v".to_string(),
                "value".to_string()
            )])),
            "expression names lost for {table:?}"
        );
        assert_eq!(
            kept.consistent_read, expected_consistent,
            "consistent_read lost for {table:?}"
        );
    }
}
