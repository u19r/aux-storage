//! Wide contract tests for `BatchGetPlan`: request grouping, fatal vs
//! unprocessed metadata mapping, ordered finish, and exact-length guards.

use std::{collections::HashMap, sync::Arc};

use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchProofSelection,
    DurableBatchPointReadProof, DurableBatchPointReadProofEntry, DurableItemRevision,
    DurablePointReadProof, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    KeysAndAttributes, MaxIndexers, StorageError, StoredTableInfo, StreamRetentionDuration,
    TableName, TableStatus, TimestampMillis, WireItem, context::WrappedError as _,
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
        table_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        default_item_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
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

fn consistent_table_keys(values: &[&str]) -> (TableName, KeysAndAttributes) {
    let (table, mut keys) = table_keys("table", values);
    keys.consistent_read = Some(true);
    (table, keys)
}

fn item_with_key(value: &str) -> WireItem {
    item_with_value(value, "payload")
}

fn item_with_value(key: &str, value: &str) -> WireItem {
    WireItem::try_from(&HashMap::from([
        ("id".to_string(), AttributeValue::S(key.to_string())),
        ("value".to_string(), AttributeValue::S(value.to_string())),
    ]))
    .unwrap()
}

fn present_proof(value: &str) -> DurableBatchPointReadProofEntry {
    DurableBatchPointReadProofEntry {
        key: item_key(value),
        proof: DurablePointReadProof::Present {
            item: Box::new(item_with_key(value)),
            indexers: Vec::new(),
            revision: DurableItemRevision::new(vec![1]),
        },
    }
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
fn given_short_row_vector_when_finish_read_then_internal_error() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("table", &["a", "b"])]),
        return_consumed_capacity: None,
    });

    let error = plan.tables[0]
        .finish_read(Ok(vec![Some(wire_item("a"))]))
        .unwrap_err();

    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("observation count")
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn given_physical_read_error_when_finish_read_then_table_is_unprocessed() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([table_keys("table", &["a"])]),
        return_consumed_capacity: None,
    });

    let result = plan.tables[0]
        .finish_read(Err(StorageError::internal("statement failed")))
        .unwrap();

    assert!(matches!(result, BatchGetTableResult::Unprocessed));
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
            results.push(
                table
                    .finish_read(Ok(vec![Some(wire_item("a")), None]))
                    .unwrap(),
            );
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
        .map(|table| table.finish_read(Ok(vec![None, None])).unwrap())
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

#[test]
fn given_missing_consistent_proof_slot_when_validated_then_internal_error() {
    let request = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([consistent_table_keys(&["a", "b"])]),
        return_consumed_capacity: None,
    };
    let plan = BatchGetPlan::from(request);
    let proofs = DurableBatchPointReadProof {
        responses: HashMap::from([(TableName::new("table"), vec![present_proof("a")])]),
        unprocessed_keys: HashMap::new(),
    };

    let results = vec![
        plan.tables[0]
            .finish_read(Ok(vec![Some(item_with_key("a")), None]))
            .unwrap(),
    ];
    let error = plan
        .finish_with_proofs(results, proofs, BatchProofSelection::ConsistentTables)
        .unwrap_err();
    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("proof slots")
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn given_absence_proof_with_response_item_when_validated_then_internal_error() {
    let request = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([consistent_table_keys(&["a"])]),
        return_consumed_capacity: None,
    };
    let plan = BatchGetPlan::from(request);
    let proofs = DurableBatchPointReadProof {
        responses: HashMap::from([(
            TableName::new("table"),
            vec![DurableBatchPointReadProofEntry {
                key: item_key("a"),
                proof: DurablePointReadProof::Absent {
                    proof: storage_types::DurableAbsenceProof::new(vec![1]),
                },
            }],
        )]),
        unprocessed_keys: HashMap::new(),
    };

    let results = vec![
        plan.tables[0]
            .finish_read(Ok(vec![Some(item_with_key("a"))]))
            .unwrap(),
    ];
    let error = plan
        .finish_with_proofs(results, proofs, BatchProofSelection::ConsistentTables)
        .unwrap_err();
    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("absence proof has an item slot")
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn given_matching_unprocessed_consistent_proof_when_validated_then_accepts() {
    let request = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([consistent_table_keys(&["a"])]),
        return_consumed_capacity: None,
    };
    let plan = BatchGetPlan::from(request.clone());
    let keys = request
        .request_items
        .get(&TableName::new("table"))
        .unwrap()
        .clone();
    let proofs = DurableBatchPointReadProof {
        responses: HashMap::new(),
        unprocessed_keys: HashMap::from([(TableName::new("table"), keys)]),
    };

    plan.finish_with_proofs(
        vec![BatchGetTableResult::Unprocessed],
        proofs,
        BatchProofSelection::ConsistentTables,
    )
    .unwrap();
}

#[test]
fn given_consistent_success_and_unprocessed_tables_when_finished_then_proofs_stay_aligned() {
    let (successful_table, mut successful_keys) = table_keys("successful", &["a", "missing"]);
    successful_keys.consistent_read = Some(true);
    let (failed_table, mut failed_keys) = table_keys("failed", &["b"]);
    failed_keys.consistent_read = Some(true);
    let request = BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([
            (successful_table.clone(), successful_keys.clone()),
            (failed_table.clone(), failed_keys.clone()),
        ]),
        return_consumed_capacity: None,
    };
    let plan = BatchGetPlan::from(request);
    let mut results = Vec::with_capacity(plan.tables.len());
    let mut proofs = DurableBatchPointReadProof::default();
    for table in &plan.tables {
        if table.table_name == successful_table {
            results.push(
                table
                    .finish_read(Ok(vec![Some(item_with_key("a")), None]))
                    .unwrap(),
            );
            proofs.responses.insert(
                successful_table.clone(),
                vec![
                    DurableBatchPointReadProofEntry {
                        key: item_key("a"),
                        proof: DurablePointReadProof::Present {
                            item: Box::new(item_with_key("a")),
                            indexers: Vec::new(),
                            revision: DurableItemRevision::new(vec![1]),
                        },
                    },
                    DurableBatchPointReadProofEntry {
                        key: item_key("missing"),
                        proof: DurablePointReadProof::Absent {
                            proof: storage_types::DurableAbsenceProof::new(vec![2]),
                        },
                    },
                ],
            );
        } else {
            results.push(BatchGetTableResult::Unprocessed);
            proofs
                .unprocessed_keys
                .insert(failed_table.clone(), failed_keys.clone());
        }
    }

    let result = plan
        .finish_with_proofs(results, proofs, BatchProofSelection::ConsistentTables)
        .unwrap();
    let response = result.response();
    assert_eq!(
        response
            .responses
            .as_ref()
            .and_then(|responses| responses.get(&successful_table))
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(
        response
            .unprocessed_keys
            .as_ref()
            .and_then(|tables| tables.get(&failed_table))
            .map(|keys| keys.keys.len()),
        Some(1)
    );
    assert_eq!(result.proofs().responses[&successful_table].len(), 2);
    let unprocessed_proof = &result.proofs().unprocessed_keys[&failed_table];
    assert_eq!(unprocessed_proof.keys, failed_keys.keys);
    assert_eq!(
        unprocessed_proof.consistent_read,
        failed_keys.consistent_read
    );
}

#[test]
fn given_consistent_finish_when_absence_proof_matches_then_validated_result_is_returned() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([consistent_table_keys(&["a"])]),
        return_consumed_capacity: None,
    });
    let results = vec![plan.tables[0].finish_read(Ok(vec![None])).unwrap()];
    let proofs = DurableBatchPointReadProof {
        responses: HashMap::from([(
            TableName::new("table"),
            vec![DurableBatchPointReadProofEntry {
                key: item_key("a"),
                proof: DurablePointReadProof::Absent {
                    proof: storage_types::DurableAbsenceProof::new(vec![1]),
                },
            }],
        )]),
        unprocessed_keys: HashMap::new(),
    };

    let result = plan
        .finish_with_proofs(results, proofs, BatchProofSelection::ConsistentTables)
        .unwrap();
    assert!(result.response().responses.is_none());
    assert_eq!(result.proofs().responses.len(), 1);
}

#[test]
fn given_consistent_finish_when_proof_key_is_wrong_then_result_is_rejected() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([consistent_table_keys(&["a"])]),
        return_consumed_capacity: None,
    });
    let results = vec![plan.tables[0].finish_read(Ok(vec![None])).unwrap()];
    let proofs = DurableBatchPointReadProof {
        responses: HashMap::from([(
            TableName::new("table"),
            vec![DurableBatchPointReadProofEntry {
                key: item_key("wrong"),
                proof: DurablePointReadProof::Absent {
                    proof: storage_types::DurableAbsenceProof::new(vec![1]),
                },
            }],
        )]),
        unprocessed_keys: HashMap::new(),
    };

    let error = plan
        .finish_with_proofs(results, proofs, BatchProofSelection::ConsistentTables)
        .unwrap_err();
    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("proof key does not match")
        ),
        "unexpected proof association error: {error:?}"
    );
}

#[test]
fn given_consistent_finish_when_proof_payload_is_swapped_then_result_is_rejected() {
    let plan = BatchGetPlan::from(BatchGetItemRequest {
        request_items: storage_types::VecMap::from_iter([consistent_table_keys(&["a"])]),
        return_consumed_capacity: None,
    });
    let results = vec![
        plan.tables[0]
            .finish_read(Ok(vec![Some(item_with_value("a", "response"))]))
            .unwrap(),
    ];
    let proofs = DurableBatchPointReadProof {
        responses: HashMap::from([(
            TableName::new("table"),
            vec![DurableBatchPointReadProofEntry {
                key: item_key("a"),
                proof: DurablePointReadProof::Present {
                    item: Box::new(item_with_value("a", "proof")),
                    indexers: Vec::new(),
                    revision: DurableItemRevision::new(vec![1]),
                },
            }],
        )]),
        unprocessed_keys: HashMap::new(),
    };

    let error = plan
        .finish_with_proofs(results, proofs, BatchProofSelection::ConsistentTables)
        .unwrap_err();
    assert!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::InternalServerError { message }
            if message.contains("payload")
        ),
        "unexpected proof payload error: {error:?}"
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
