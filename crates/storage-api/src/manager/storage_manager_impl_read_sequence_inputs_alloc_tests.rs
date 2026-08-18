use std::collections::{BTreeMap, HashMap};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeMap, AttributeValue, QueryRequest, QueryResponse, ReadSequenceFromInput,
    ReadSequenceInputCardinality, ReadSequenceInvocationPayload, ReadSequenceInvocationResult,
    ReadSequenceNode, ReadSequenceNodeInput, ReadSequenceNodeOperation, ReadSequenceNodeResult,
    ReadSequenceOnMissing, ReadSequenceSelector, TableName,
};

use super::storage_manager_impl_read_sequence_inputs::{
    ResolvedInput, bind_string_template, resolve_inputs,
};

const ITEM_COUNT: usize = 64;
const ITERATIONS: usize = 128;

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn given_realistic_query_items_when_resolving_a_key_then_allocations_are_bounded() {
    let node = dependent_node();
    let results = vec![Some(source_result())];
    let node_names = vec!["parents".to_string()];
    let guard = AllocationGuard::start(
        module_path!(),
        "given_realistic_query_items_when_resolving_a_key_then_allocations_are_bounded",
        file!(),
        line!(),
        Some("query_key_selector"),
    );

    for _ in 0..ITERATIONS {
        let resolved = resolve_inputs(&node, &results, &node_names).expect("resolve input");
        assert_eq!(resolved["parent_pk"].len(), ITEM_COUNT);
    }

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count <= 20_000);
    assert!(report.allocated_bytes <= 1_200_000);
}

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn given_long_template_inputs_when_binding_then_only_the_output_string_is_allocated() {
    let inputs = BTreeMap::from([
        (
            "id".to_string(),
            ResolvedInput {
                value: AttributeValue::S("018f7f8e-31ad-7c23-a764-9e2f63c6d946".to_string()),
                reference: storage_types::ReadSequenceInputReference {
                    node: "source".to_string(),
                    invocation_ordinal: 0,
                    item_ordinal: Some(0),
                },
            },
        ),
        (
            "sub_id".to_string(),
            ResolvedInput {
                value: AttributeValue::S("018f7f8e-31ad-7c23-a764-9e2f63c6d947".to_string()),
                reference: storage_types::ReadSequenceInputReference {
                    node: "source".to_string(),
                    invocation_ordinal: 0,
                    item_ordinal: Some(0),
                },
            },
        ),
    ]);
    let guard = AllocationGuard::start(
        module_path!(),
        "given_long_template_inputs_when_binding_then_only_the_output_string_is_allocated",
        file!(),
        line!(),
        Some("read_sequence_string_template_binding"),
    );
    for _ in 0..ITERATIONS {
        let value = bind_string_template("entity#{id}#sub_model#{sub_id}#v1", &inputs)
            .expect("bind template");
        assert!(matches!(value, AttributeValue::S(value) if value.len() == 93));
    }
    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert_eq!(report.allocation_count, ITERATIONS as u64);
}

fn dependent_node() -> ReadSequenceNode {
    let mut request = QueryRequest::new(TableName::new("children"), "pk = :pk".to_string());
    request.expression_attribute_values = Some(HashMap::from([(
        ":pk".to_string(),
        storage_types::read_sequence_input_marker("parent_pk"),
    )]));
    ReadSequenceNode {
        name: "children".to_string(),
        operation: ReadSequenceNodeOperation::Query(request),
        inputs: Some(BTreeMap::from([(
            "parent_pk".to_string(),
            ReadSequenceNodeInput {
                mapped_key_source: None,
                from: ReadSequenceFromInput {
                    node: "parents".to_string(),
                    select: ReadSequenceSelector("$.Query.Items[*].pk".to_string()),
                },
                cardinality: ReadSequenceInputCardinality::Many,
                on_missing: ReadSequenceOnMissing::Error,
            },
        )])),
        iterate: Some("parent_pk".to_string()),
        after: None,
    }
}

fn source_result() -> ReadSequenceNodeResult {
    let items = (0..ITEM_COUNT).map(realistic_item).collect::<Vec<_>>();
    ReadSequenceNodeResult {
        name: "parents".to_string(),
        invocations: vec![ReadSequenceInvocationResult {
            ordinal: 0,
            input_refs: BTreeMap::new(),
            result: ReadSequenceInvocationPayload::Query(QueryResponse {
                count: items.len() as u32,
                scanned_count: items.len() as u32,
                items: Some(items),
                last_evaluated_key: None,
                consumed_capacity: None,
            }),
        }],
    }
}

fn realistic_item(index: usize) -> AttributeMap {
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("tenant#{index:04}")),
        ),
        (
            "sk".to_string(),
            AttributeValue::S(format!("item#{index:04}")),
        ),
        (
            "gsi1pk".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        ("gsi1sk".to_string(), AttributeValue::N(index.to_string())),
        (
            "gsi2pk".to_string(),
            AttributeValue::S("region#eu".to_string()),
        ),
        ("gsi2sk".to_string(), AttributeValue::N(index.to_string())),
        ("status".to_string(), AttributeValue::S("ready".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::N("4102444800".to_string()),
        ),
        ("version".to_string(), AttributeValue::N("7".to_string())),
        ("body".to_string(), AttributeValue::S("x".repeat(1_024))),
    ])
    .into()
}
