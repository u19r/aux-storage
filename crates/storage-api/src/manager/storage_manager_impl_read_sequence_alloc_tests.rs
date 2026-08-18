use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_provider::{ReadSequenceFlatResult, ReadSequenceFlatRow};
use storage_types::{
    AttributeMap, AttributeValue, QueryRequest, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeOperation, ReadSequenceRequest, TableName,
};

use super::storage_manager_impl_read_sequence::consume_whole_plan_rows_for_allocation_test;

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn given_owned_provider_rows_when_decoding_then_items_are_not_cloned() {
    let plan = storage_types::plan_read_sequence(&request()).expect("plan request");
    let rows = vec![ReadSequenceFlatRow {
        node: ReadSequenceNodeId::from_index(0),
        invocation_ordinal: 0,
        input_refs: Default::default(),
        result: ReadSequenceFlatResult::Query {
            items: (0..64).map(realistic_item).collect(),
            count: 64,
            scanned_count: 64,
            last_evaluated_key: None,
        },
    }];
    let guard = AllocationGuard::start(
        module_path!(),
        "given_owned_provider_rows_when_decoding_then_items_are_not_cloned",
        file!(),
        line!(),
        Some("whole_plan_consuming_decode"),
    );

    let invocation_count =
        consume_whole_plan_rows_for_allocation_test(&plan, rows).expect("decode rows");

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert_eq!(invocation_count, 1);
    assert!(report.allocation_count <= 12);
    assert!(report.allocated_bytes <= 16_384);
}

fn request() -> ReadSequenceRequest {
    let mut query = QueryRequest::new(TableName::new("items"), "pk = :pk".to_string());
    query.expression_attribute_values = Some(HashMap::from([(
        ":pk".to_string(),
        AttributeValue::S("tenant#0000".to_string()),
    )]));
    ReadSequenceRequest::new(vec![ReadSequenceNode::new(
        "items",
        ReadSequenceNodeOperation::Query(query),
    )])
}

fn realistic_item(index: usize) -> AttributeMap {
    HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("tenant#{index:04}")),
        ),
        ("body".to_string(), AttributeValue::S("x".repeat(1_024))),
    ])
    .into()
}
