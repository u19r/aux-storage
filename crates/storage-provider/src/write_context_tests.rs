use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeValue, PutItemEncodeRequest, PutItemRequest, StorageResult, TableName, WireItem,
    context::WrappedError as _,
};

use crate::{
    BatchWriteContexts, OrderedOperationContexts, PhysicalWrite, TransactOperationContext,
    TtlWriteContext,
};

#[test]
fn consumed_physical_write_mapping_preserves_payload_and_sidecars() {
    let write = physical_put_write(valid_wire_item());
    let expected = legacy_clone_map(&write).unwrap();
    let actual = consuming_map(write).unwrap();

    assert_eq!(actual.ordinal(), expected.ordinal());
    assert_eq!(actual.physical_table(), expected.physical_table());
    assert!(actual.context() == expected.context());
    assert_eq!(actual.request().table_name, expected.request().table_name);
    assert_eq!(
        storage_types::AttributeMap::try_from(&actual.request().item).unwrap(),
        storage_types::AttributeMap::try_from(&expected.request().item).unwrap()
    );
    assert_eq!(actual.request().indexers, expected.request().indexers);
    assert_eq!(
        actual.request().condition_expression,
        expected.request().condition_expression
    );
    assert_eq!(
        actual.request().expression_attribute_names,
        expected.request().expression_attribute_names
    );
    assert_eq!(
        actual.request().expression_attribute_values,
        expected.request().expression_attribute_values
    );
    assert_eq!(
        actual.request().return_values,
        expected.request().return_values
    );
    assert_eq!(
        actual.request().return_values_on_condition_check_failure,
        expected.request().return_values_on_condition_check_failure
    );
    assert_eq!(
        actual.request().aux_item_stream_ttl_hours,
        expected.request().aux_item_stream_ttl_hours
    );
}

#[test]
fn given_invalid_wire_item_when_mapped_then_write_validation_rejects_it() {
    let invalid = WireItem::dynamo_json(b"not-json".to_vec());
    let mapped = consuming_map(physical_put_write(invalid)).unwrap();
    let error = crate::PutItemShape::try_from_request(mapped.into_parts().2)
        .err()
        .unwrap();
    assert!(matches!(
        error.to_enum(),
        storage_types::StorageEnum::Validation { .. }
    ));
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn consumed_physical_write_mapping_avoids_payload_and_sidecar_clones() {
    const ITERATIONS: usize = 256;
    let inputs = (0..ITERATIONS)
        .map(|_| physical_put_write(valid_wire_item()))
        .collect::<Vec<_>>();
    let baseline_inputs = inputs.clone();

    let baseline = measure_legacy_clone_map(&baseline_inputs);
    let current = measure_consuming_map(inputs);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);

    assert!(
        current.allocation_count < baseline.allocation_count,
        "consuming map should allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "consuming map should allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );
}

fn physical_put_write(item: WireItem) -> PhysicalWrite<PutItemEncodeRequest, TtlWriteContext> {
    let table_name = TableName::new("orders");
    let context = TtlWriteContext::direct(table_name.clone()).unwrap();
    let request = PutItemEncodeRequest {
        table_name: table_name.clone(),
        item,
        indexers: Some(vec!["gsi-a".to_string(), "gsi-b".to_string()]),
        condition_expression: Some("attribute_exists(#id)".to_string()),
        expression_attribute_names: Some(storage_types::VecMap::from_iter([(
            "#id".to_string(),
            "id".to_string(),
        )])),
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":expected".to_string(),
            AttributeValue::S("value".to_string()),
        )])),
        return_values: None,
        return_old_on_condition_failure: true,
        aux_item_stream_ttl_hours: None,
    };
    PhysicalWrite::try_new(7, table_name, request, context).unwrap()
}

fn valid_wire_item() -> WireItem {
    let attributes = (0..10)
        .map(|index| {
            (
                format!("attribute_{index}"),
                AttributeValue::S(format!("{}-{index}", "x".repeat(256))),
            )
        })
        .collect::<HashMap<_, _>>();
    WireItem::try_from(&attributes).unwrap()
}

fn legacy_clone_map(
    write: &PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>,
) -> StorageResult<PhysicalWrite<PutItemRequest, TtlWriteContext>> {
    let PutItemEncodeRequest {
        table_name,
        item,
        indexers,
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
        return_values,
        return_old_on_condition_failure,
        aux_item_stream_ttl_hours,
    } = write.request().clone();
    PhysicalWrite::try_new(
        write.ordinal(),
        write.physical_table().clone(),
        PutItemRequest {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            expected: None,
            conditional_operator: None,
            return_values,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours,
        },
        write.context().clone(),
    )
}

fn consuming_map(
    write: PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>,
) -> StorageResult<PhysicalWrite<PutItemRequest, TtlWriteContext>> {
    write.try_map_request(|request| {
        let PutItemEncodeRequest {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = request;
        Ok(PutItemRequest {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            expected: None,
            conditional_operator: None,
            return_values,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours,
        })
    })
}

fn measure_legacy_clone_map(
    inputs: &[PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "physical_write_legacy_clone_map",
        file!(),
        line!(),
        Some("baseline"),
    );
    for input in inputs {
        let mapped = legacy_clone_map(input).unwrap();
        std::hint::black_box(mapped);
    }
    guard.finish()
}

fn measure_consuming_map(
    inputs: Vec<PhysicalWrite<PutItemEncodeRequest, TtlWriteContext>>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "physical_write_consuming_map",
        file!(),
        line!(),
        Some("optimized"),
    );
    for input in inputs {
        let mapped = consuming_map(input).unwrap();
        std::hint::black_box(mapped);
    }
    guard.finish()
}

fn alignment_table() -> TableName {
    TableName::new("alignment-orders")
}

fn alignment_context(table: &TableName, ordinal: usize) -> TransactOperationContext {
    TransactOperationContext::try_new(
        ordinal,
        table.clone(),
        TtlWriteContext::direct(table.clone()).unwrap(),
    )
    .unwrap()
}

fn alignment_transact_contexts(table: &TableName, count: usize) -> OrderedOperationContexts {
    OrderedOperationContexts::try_new(
        (0..count)
            .map(|ordinal| alignment_context(table, ordinal))
            .collect(),
    )
    .unwrap()
}

fn alignment_put_item(table: TableName) -> storage_types::TransactWriteItem {
    storage_types::TransactWriteItem {
        put: Some(storage_types::TransactPutRequest {
            table_name: table,
            item: WireItem::dynamo_json(b"{}".to_vec()),
            indexers: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        update: None,
        delete: None,
        condition_check: None,
    }
}

#[test]
fn given_aligned_transact_items_when_validated_then_accepts() {
    let table = alignment_table();
    let items = vec![alignment_put_item(table.clone())];
    let contexts = alignment_transact_contexts(&table, 1);

    contexts.validate_transact_items(&items).unwrap();
}

#[test]
fn given_transact_count_mismatch_when_validated_then_rejects() {
    let table = alignment_table();
    let items = vec![
        alignment_put_item(table.clone()),
        alignment_put_item(table.clone()),
    ];
    let contexts = alignment_transact_contexts(&table, 1);

    assert!(
        contexts.validate_transact_items(&items).is_err(),
        "count mismatch must fail closed"
    );
}

#[test]
fn given_actionless_transact_item_when_validated_then_rejects() {
    let table = alignment_table();
    let items = vec![storage_types::TransactWriteItem::default()];
    let contexts = alignment_transact_contexts(&table, 1);

    assert!(
        contexts.validate_transact_items(&items).is_err(),
        "actionless item must fail closed"
    );
}

#[test]
fn given_wrong_table_context_when_validated_then_rejects() {
    let table = alignment_table();
    let other = TableName::new("alignment-other");
    let items = vec![alignment_put_item(table)];
    let contexts = alignment_transact_contexts(&other, 1);

    assert!(
        contexts.validate_transact_items(&items).is_err(),
        "wrong table must fail closed"
    );
}

#[test]
fn given_aligned_batch_request_when_validated_then_accepts() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let table = alignment_table();
    let request = BatchWriteItemRequest {
        request_items: storage_types::VecMap::from_iter([(
            table.clone(),
            vec![
                WriteRequest {
                    put_request: Some(PutRequest {
                        item: WireItem::dynamo_json(b"{}".to_vec()),
                        indexers: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    delete_request: None,
                },
                WriteRequest {
                    put_request: Some(PutRequest {
                        item: WireItem::dynamo_json(b"{}".to_vec()),
                        indexers: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    delete_request: None,
                },
            ],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };
    let contexts = BatchWriteContexts::try_new(vec![
        TtlWriteContext::direct(table.clone()).unwrap(),
        TtlWriteContext::direct(table).unwrap(),
    ])
    .unwrap();

    contexts.validate_batch_request(&request).unwrap();
}

#[test]
fn given_batch_count_mismatch_when_validated_then_rejects() {
    use storage_types::BatchWriteItemRequest;

    let table = alignment_table();
    let request = BatchWriteItemRequest {
        request_items: storage_types::VecMap::default(),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };
    let contexts =
        BatchWriteContexts::try_new(vec![TtlWriteContext::direct(table).unwrap()]).unwrap();

    assert!(
        contexts.validate_batch_request(&request).is_err(),
        "count mismatch must fail closed"
    );
}

#[test]
fn given_aligned_encode_items_when_validated_then_accepts() {
    use storage_types::{TransactEncodeItem, TransactEncodePutRequest, WireEntity};

    let table = alignment_table();
    let wire = WireItem::try_from(&HashMap::new()).unwrap();
    let items = vec![TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: table.clone(),
            item: WireEntity::unindexed(wire),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        update: None,
        delete: None,
        condition_check: None,
    }];
    let contexts = alignment_transact_contexts(&table, 1);

    contexts.validate_transact_encode_items(&items).unwrap();
}
