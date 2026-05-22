use std::collections::HashMap;

use storage_types::{
    AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest, DeleteRequest,
    EncodePutRequest, EncodeWriteRequest, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, Projection, ProjectionType, PutRequest,
    StoredTableInfo, TableName, TableStatus, TimestampMillis, TransactDeleteRequest,
    TransactEncodeItem, TransactEncodePutRequest, TransactPutRequest, TransactUpdateRequest,
    TransactWriteItem, WireItem, WriteRequest,
};

use crate::runtime_write_plan::{
    RuntimeBaseWrite, RuntimeIndexTransition, RuntimePointReadMutation,
    RuntimePreparedIndexPrewrite, RuntimeQueryProofMutation, build_delete_item_cache_effects,
    build_index_transition, build_pending_delete_index_transition,
    build_pending_put_index_transition, build_pending_update_index_transition,
    build_put_item_cache_effects, collect_base_writes_for_batch_write,
    collect_pending_index_transition_update_lookups,
    collect_pending_query_proof_targets_for_transact_write_items,
    collect_pending_query_proof_targets_for_transact_write_items_encode,
    collect_point_read_mutations_for_batch_write,
    collect_point_read_mutations_for_batch_write_encode,
    collect_point_read_mutations_for_transact_write_items,
    collect_point_read_mutations_for_transact_write_items_encode,
    collect_query_proof_targets_for_batch_write,
    collect_query_proof_targets_for_batch_write_encode, collect_transact_write_encode_table_names,
    collect_transact_write_table_names, compose_delete_item_effects, compose_put_item_effects,
    compose_update_item_effects, compose_write_effects, extract_primary_key_from_item,
    finalize_pending_index_transitions, finalize_update_cache_effects, maybe_indexed_table_info,
    maybe_prepare_index_prewrite, point_read_delete, point_read_invalidate,
    point_read_put_from_item, point_read_put_from_wire_item, prepare_update_cache_write,
    table_requires_index_tracking,
};

fn table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("tbl"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::default(),
        attribute_definitions: vec![storage_types::AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

fn key() -> KeyAttributes {
    HashMap::from([("pk".into(), AttributeValue::S("1".into()))]).into()
}

fn item() -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S("1".into())),
        ("v".into(), AttributeValue::S("x".into())),
    ])
}

#[test]
fn compose_write_effects_keeps_point_reads_when_query_proof_disabled() {
    let effects = compose_write_effects(
        vec![RuntimePointReadMutation::Delete {
            table_name: TableName::new("tbl"),
            key: key(),
        }],
        vec![RuntimeBaseWrite::Delete {
            table_name: TableName::new("tbl"),
            table_info: table_info(),
            key: key(),
        }],
        vec![],
        false,
    );

    assert_eq!(effects.point_read.len(), 1);
    assert!(effects.query_proof.is_empty());
}

#[test]
fn compose_write_effects_combines_base_and_index_mutations() {
    let effects = compose_write_effects(
        vec![RuntimePointReadMutation::Put {
            table_name: TableName::new("tbl"),
            key: key(),
            item: Box::new(WireItem::from_attribute_map(&item()).expect("wire item")),
        }],
        vec![RuntimeBaseWrite::Put {
            table_name: TableName::new("tbl"),
            table_info: table_info(),
            item: item(),
        }],
        vec![RuntimeIndexTransition {
            table_name: TableName::new("tbl"),
            table_info: table_info(),
            old_item: Some(item()),
            new_item: None,
        }],
        true,
    );

    assert_eq!(effects.point_read.len(), 1);
    assert_eq!(effects.query_proof.len(), 2);
    assert!(matches!(
        &effects.query_proof[0],
        RuntimeQueryProofMutation::RecordBasePut { .. }
    ));
    assert!(matches!(
        &effects.query_proof[1],
        RuntimeQueryProofMutation::RecordIndexTransition { .. }
    ));
}

#[test]
fn extract_primary_key_from_item_uses_key_schema_order() {
    let extracted = extract_primary_key_from_item(&table_info().key_schema, &item()).expect("key");
    assert_eq!(extracted, key());
}

#[test]
fn point_read_mutation_builders_preserve_table_key_and_payload() {
    let table_name = TableName::new("tbl");
    let put = point_read_put_from_item(&table_name, &table_info().key_schema, &item())
        .expect("put mutation");
    let delete = point_read_delete(&table_name, &key());
    let invalidate = point_read_invalidate(&table_name, &key());
    let from_wire = point_read_put_from_wire_item(
        &table_name,
        key(),
        WireItem::from_attribute_map(&item()).expect("wire item"),
    );

    assert!(matches!(put, RuntimePointReadMutation::Put { .. }));
    assert!(matches!(delete, RuntimePointReadMutation::Delete { .. }));
    assert!(matches!(
        invalidate,
        RuntimePointReadMutation::Invalidate { .. }
    ));
    assert!(matches!(from_wire, RuntimePointReadMutation::Put { .. }));
}

#[test]
fn compose_put_delete_and_update_item_effects_build_expected_query_proof_transitions() {
    let table_name = TableName::new("tbl");

    let put_effects = compose_put_item_effects(
        RuntimePointReadMutation::Put {
            table_name: table_name.clone(),
            key: key(),
            item: Box::new(WireItem::from_attribute_map(&item()).expect("wire item")),
        },
        &table_name,
        table_info(),
        &item(),
        Some(RuntimePreparedIndexPrewrite {
            table_info: table_info(),
            old_item: Some(item()),
        }),
        true,
    );
    assert!(matches!(
        &put_effects.query_proof[0],
        RuntimeQueryProofMutation::RecordBasePut { .. }
    ));
    assert!(matches!(
        &put_effects.query_proof[1],
        RuntimeQueryProofMutation::RecordIndexTransition { .. }
    ));

    let delete_effects = compose_delete_item_effects(
        RuntimePointReadMutation::Delete {
            table_name: table_name.clone(),
            key: key(),
        },
        &table_name,
        table_info(),
        &key(),
        Some(RuntimePreparedIndexPrewrite {
            table_info: table_info(),
            old_item: Some(item()),
        }),
        true,
    );
    assert!(matches!(
        &delete_effects.query_proof[0],
        RuntimeQueryProofMutation::RecordBaseDelete { .. }
    ));

    let update_effects = compose_update_item_effects(
        RuntimePointReadMutation::Invalidate {
            table_name: table_name.clone(),
            key: key(),
        },
        &table_name,
        table_info(),
        &key(),
        Some(RuntimePreparedIndexPrewrite {
            table_info: table_info(),
            old_item: Some(item()),
        }),
        Some(item()),
        true,
    );
    assert!(matches!(
        &update_effects.query_proof[0],
        RuntimeQueryProofMutation::InvalidateBaseCoverage { .. }
    ));
    assert!(matches!(
        &update_effects.query_proof[1],
        RuntimeQueryProofMutation::RecordIndexTransition { .. }
    ));
}

#[test]
fn collect_base_writes_for_batch_write_expands_puts_and_deletes() {
    let table_name = TableName::new("tbl");
    let request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![
                WriteRequest {
                    put_request: Some(PutRequest { item: item() }),
                    delete_request: None,
                },
                WriteRequest {
                    put_request: None,
                    delete_request: Some(DeleteRequest { key: key() }),
                },
            ],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };
    let table_infos = HashMap::from([(table_name, table_info())]);

    let base_writes = collect_base_writes_for_batch_write(&request, &table_infos);

    assert_eq!(base_writes.len(), 2);
    assert!(matches!(base_writes[0], RuntimeBaseWrite::Put { .. }));
    assert!(matches!(base_writes[1], RuntimeBaseWrite::Delete { .. }));
}

#[test]
fn collect_point_read_mutations_for_batch_and_transact_requests() {
    let table_name = TableName::new("tbl");
    let table_infos = HashMap::from([(table_name.clone(), table_info())]);
    let batch_request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![
                WriteRequest {
                    put_request: Some(PutRequest { item: item() }),
                    delete_request: None,
                },
                WriteRequest {
                    put_request: None,
                    delete_request: Some(DeleteRequest { key: key() }),
                },
            ],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let batch_mutations =
        collect_point_read_mutations_for_batch_write(&batch_request, &table_infos)
            .expect("batch mutations");
    assert!(matches!(
        batch_mutations.as_slice(),
        [
            RuntimePointReadMutation::Put { .. },
            RuntimePointReadMutation::Delete { .. }
        ]
    ));

    let transact_items = vec![TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: table_name.clone(),
            item: item(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: Some(TransactUpdateRequest {
            table_name: table_name.clone(),
            key: key(),
            update_expression: "SET #v = :v".into(),
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([("#v".into(), "v".into())])),
            expression_attribute_values: Some(HashMap::from([(
                ":v".into(),
                AttributeValue::S("y".into()),
            )])),
            return_values_on_condition_check_failure: None,
        }),
        delete: Some(TransactDeleteRequest {
            table_name: table_name.clone(),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        condition_check: None,
    }];

    let transact_mutations =
        collect_point_read_mutations_for_transact_write_items(&transact_items, &table_infos)
            .expect("transact mutations");
    assert_eq!(transact_mutations.len(), 3);
    assert!(matches!(
        &transact_mutations[0],
        RuntimePointReadMutation::Put { .. }
    ));
    assert!(matches!(
        &transact_mutations[1],
        RuntimePointReadMutation::Invalidate { .. }
    ));
    assert!(matches!(
        &transact_mutations[2],
        RuntimePointReadMutation::Delete { .. }
    ));
}

#[test]
fn collect_point_read_mutations_for_encode_requests_preserves_wire_items() {
    let table_name = TableName::new("tbl");
    let table_infos = HashMap::from([(table_name.clone(), table_info())]);
    let wire_item = WireItem::from_attribute_map(&item()).expect("wire item");
    let batch_request = BatchWriteItemEncodeRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![EncodeWriteRequest {
                put_request: Some(EncodePutRequest {
                    item: wire_item.clone(),
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let batch_mutations =
        collect_point_read_mutations_for_batch_write_encode(&batch_request, &table_infos)
            .expect("batch encode mutations");
    assert!(matches!(
        &batch_mutations[0],
        RuntimePointReadMutation::Put { .. }
    ));

    let transact_items = vec![TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: table_name.clone(),
            item: wire_item,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: None,
        delete: None,
        condition_check: None,
    }];

    let transact_mutations =
        collect_point_read_mutations_for_transact_write_items_encode(&transact_items, &table_infos)
            .expect("transact encode mutations");
    assert!(matches!(
        &transact_mutations[0],
        RuntimePointReadMutation::Put { .. }
    ));
}

#[test]
fn build_put_and_delete_item_cache_effects_use_loaded_facts_directly() {
    let table_name = TableName::new("tbl");

    let put_effects = build_put_item_cache_effects(
        &table_name,
        table_info(),
        &item(),
        Some(RuntimePreparedIndexPrewrite {
            table_info: table_info(),
            old_item: Some(item()),
        }),
        true,
    )
    .expect("put effects");
    assert!(matches!(
        &put_effects.point_read[0],
        RuntimePointReadMutation::Put { .. }
    ));
    assert!(matches!(
        &put_effects.query_proof[0],
        RuntimeQueryProofMutation::RecordBasePut { .. }
    ));

    let delete_effects = build_delete_item_cache_effects(
        &table_name,
        table_info(),
        &key(),
        Some(RuntimePreparedIndexPrewrite {
            table_info: table_info(),
            old_item: Some(item()),
        }),
        true,
    );
    assert!(matches!(
        &delete_effects.point_read[0],
        RuntimePointReadMutation::Delete { .. }
    ));
    assert!(matches!(
        &delete_effects.query_proof[0],
        RuntimeQueryProofMutation::RecordBaseDelete { .. }
    ));
}

#[test]
fn index_transition_builders_capture_loaded_state_without_storage_helpers() {
    let table_name = TableName::new("tbl");
    let put = build_pending_put_index_transition(&table_name, table_info(), Some(item()), item());
    assert!(matches!(
        put.kind,
        crate::runtime_write_plan::RuntimePendingIndexTransitionKind::Put { .. }
    ));

    let update =
        build_pending_update_index_transition(&table_name, table_info(), Some(item()), key());
    assert_eq!(update.update_lookup(), Some((table_name.clone(), key())));

    let delete = build_pending_delete_index_transition(&table_name, table_info(), Some(item()));
    assert!(matches!(
        delete.kind,
        crate::runtime_write_plan::RuntimePendingIndexTransitionKind::Delete
    ));

    let direct = build_index_transition(&table_name, table_info(), Some(item()), None);
    assert!(direct.new_item.is_none());
}

#[test]
fn index_tracking_policy_only_activates_for_tables_with_gsis_when_enabled() {
    let plain = table_info();
    assert!(!table_requires_index_tracking(&plain));
    assert!(maybe_indexed_table_info(true, plain.clone()).is_none());
    assert!(maybe_prepare_index_prewrite(true, plain, Some(item())).is_none());

    let mut indexed = table_info();
    indexed.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
        index_name: IndexName::new("gsi1"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "v".into(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
    }]);

    assert!(table_requires_index_tracking(&indexed));
    assert!(maybe_indexed_table_info(false, indexed.clone()).is_none());
    let prewrite = maybe_prepare_index_prewrite(true, indexed, Some(item()));
    assert!(prewrite.is_some());
}

#[test]
fn query_proof_target_collectors_expand_batch_and_transact_request_shapes() {
    let table_name = TableName::new("tbl");
    let table_infos = HashMap::from([(table_name.clone(), table_info())]);

    let batch_request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![
                WriteRequest {
                    put_request: Some(PutRequest { item: item() }),
                    delete_request: None,
                },
                WriteRequest {
                    put_request: None,
                    delete_request: Some(DeleteRequest { key: key() }),
                },
            ],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };
    let batch_targets =
        collect_query_proof_targets_for_batch_write(&batch_request, &table_infos).expect("targets");
    assert_eq!(batch_targets.len(), 2);
    assert_eq!(batch_targets[0].old_item_lookup_key, key());
    assert!(matches!(
        batch_targets[1].kind,
        crate::runtime_write_plan::RuntimeIndexTransitionTargetKind::Delete
    ));

    let transact_items = vec![TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: table_name.clone(),
            item: item(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: Some(TransactUpdateRequest {
            table_name: table_name.clone(),
            key: key(),
            update_expression: "SET #v = :v".into(),
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([("#v".into(), "v".into())])),
            expression_attribute_values: Some(HashMap::from([(
                ":v".into(),
                AttributeValue::S("y".into()),
            )])),
            return_values_on_condition_check_failure: None,
        }),
        delete: Some(TransactDeleteRequest {
            table_name: table_name.clone(),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        condition_check: None,
    }];
    let pending_targets =
        collect_pending_query_proof_targets_for_transact_write_items(&transact_items, &table_infos)
            .expect("pending targets");
    assert_eq!(pending_targets.len(), 3);
    assert_eq!(pending_targets[1].old_item_lookup_key, key());
}

#[test]
fn encode_query_proof_target_collectors_convert_wire_items_once() {
    let table_name = TableName::new("tbl");
    let table_infos = HashMap::from([(table_name.clone(), table_info())]);
    let wire_item = WireItem::from_attribute_map(&item()).expect("wire item");

    let batch_request = BatchWriteItemEncodeRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            vec![EncodeWriteRequest {
                put_request: Some(EncodePutRequest {
                    item: wire_item.clone(),
                }),
                delete_request: Some(DeleteRequest { key: key() }),
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };
    let batch_targets =
        collect_query_proof_targets_for_batch_write_encode(&batch_request, &table_infos)
            .expect("batch encode targets");
    assert_eq!(batch_targets.len(), 2);
    assert_eq!(batch_targets[0].old_item_lookup_key, key());

    let transact_items = vec![TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: table_name.clone(),
            item: wire_item,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: None,
        delete: Some(TransactDeleteRequest {
            table_name: table_name.clone(),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        condition_check: None,
    }];
    let pending_targets = collect_pending_query_proof_targets_for_transact_write_items_encode(
        &transact_items,
        &table_infos,
    )
    .expect("transact encode targets");
    assert_eq!(pending_targets.len(), 2);
    assert_eq!(pending_targets[0].old_item_lookup_key, key());
}

#[test]
fn prepare_and_finalize_update_cache_effects_round_trip_post_image() {
    let table_name = TableName::new("tbl");
    let prepared = prepare_update_cache_write(
        &table_name,
        table_info(),
        &key(),
        Some(RuntimePreparedIndexPrewrite {
            table_info: table_info(),
            old_item: Some(item()),
        }),
    );

    let put_effects = finalize_update_cache_effects(
        prepared.clone(),
        Some(WireItem::from_attribute_map(&item()).expect("wire item")),
        true,
    )
    .expect("put effects");
    assert!(matches!(
        &put_effects.point_read[0],
        RuntimePointReadMutation::Put { .. }
    ));
    assert!(matches!(
        &put_effects.query_proof[0],
        RuntimeQueryProofMutation::InvalidateBaseCoverage { .. }
    ));

    let invalidate_effects =
        finalize_update_cache_effects(prepared, None, true).expect("invalidate effects");
    assert!(matches!(
        &invalidate_effects.point_read[0],
        RuntimePointReadMutation::Invalidate { .. }
    ));
}

#[test]
fn collect_transact_table_names_deduplicates_plain_and_encode_requests() {
    let table_a = TableName::new("tbl-a");
    let table_b = TableName::new("tbl-b");

    let transact_items = vec![TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: table_a.clone(),
            item: item(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: Some(TransactUpdateRequest {
            table_name: table_a.clone(),
            key: key(),
            update_expression: "SET #v = :v".into(),
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([("#v".into(), "v".into())])),
            expression_attribute_values: Some(HashMap::from([(
                ":v".into(),
                AttributeValue::S("y".into()),
            )])),
            return_values_on_condition_check_failure: None,
        }),
        delete: Some(TransactDeleteRequest {
            table_name: table_b.clone(),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        condition_check: None,
    }];
    assert_eq!(
        collect_transact_write_table_names(&transact_items),
        vec![table_a.clone(), table_b.clone()]
    );

    let transact_encode_items = vec![TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: table_a.clone(),
            item: WireItem::from_attribute_map(&item()).expect("wire item"),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: None,
        delete: Some(TransactDeleteRequest {
            table_name: table_b.clone(),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        condition_check: None,
    }];
    assert_eq!(
        collect_transact_write_encode_table_names(&transact_encode_items),
        vec![table_a, table_b]
    );
}

#[test]
fn pending_transition_resolution_helpers_keep_order_and_finalize_updates() {
    let table_name = TableName::new("tbl");
    let transitions = vec![
        build_pending_put_index_transition(&table_name, table_info(), Some(item()), item()),
        build_pending_update_index_transition(&table_name, table_info(), Some(item()), key()),
        build_pending_delete_index_transition(&table_name, table_info(), Some(item())),
    ];

    let update_lookups = collect_pending_index_transition_update_lookups(&transitions);
    assert_eq!(update_lookups[0], None);
    assert_eq!(update_lookups[1], Some((table_name.clone(), key())));
    assert_eq!(update_lookups[2], None);

    let finalized = finalize_pending_index_transitions(transitions, vec![None, Some(item()), None])
        .expect("finalized transitions");
    assert_eq!(finalized.len(), 3);
    assert_eq!(finalized[0].new_item, Some(item()));
    assert_eq!(finalized[1].new_item, Some(item()));
    assert_eq!(finalized[2].new_item, None);
}
