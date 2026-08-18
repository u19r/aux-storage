use std::collections::BTreeSet;

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeValue, GetItemRequest, KeyAttributes, QueryRequest, ReadSequenceConsistency,
    ReadSequenceFromInput, ReadSequenceInputCardinality, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeInput, ReadSequenceNodeOperation, ReadSequenceOnMissing, ReadSequencePlan,
    ReadSequenceRequest, ReadSequenceSelector,
};

use crate::provider::{
    ReadSequenceMappedOptions, ReadSequenceMappedRejectionReason, ReadSequencePhysicalDescriptor,
    ReadSequencePhysicalOperation, select_read_sequence_mapped_edges,
};

fn plan() -> ReadSequencePlan {
    let root = ReadSequenceNode {
        name: "root".into(),
        operation: ReadSequenceNodeOperation::Query(storage_types::QueryRequest {
            table_name: storage_types::TableName::new("items"),
            index_name: None,
            key_condition_expression: "pk = :pk".into(),
            attributes_to_get: None,
            conditional_operator: None,
            filter_expression: None,
            query_filter: None,
            projection_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(
                [(":pk".into(), AttributeValue::S("x".into()))]
                    .into_iter()
                    .collect(),
            ),
            exclusive_start_key: None,
            limit: Some(2),
            return_consumed_capacity: None,
            consistent_read: None,
            scan_index_forward: None,
            select: None,
        }),
        inputs: None,
        iterate: None,
        after: None,
    };
    let input = ReadSequenceNodeInput {
        from: ReadSequenceFromInput {
            node: "root".into(),
            select: ReadSequenceSelector("$.Query.Items[*].pk".into()),
        },
        mapped_key_source: None,
        cardinality: ReadSequenceInputCardinality::Many,
        on_missing: storage_types::ReadSequenceOnMissing::Skip,
    };
    let child = ReadSequenceNode {
        name: "child".into(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest {
            table_name: storage_types::TableName::new("items"),
            key: [("pk".into(), storage_types::read_sequence_input_marker("pk"))]
                .into_iter()
                .collect(),
            attributes_to_get: None,
            consistent_read: None,
            projection_expression: None,
            expression_attribute_names: None,
            return_consumed_capacity: None,
        }),
        inputs: Some([("pk".into(), input)].into_iter().collect()),
        iterate: Some("pk".into()),
        after: None,
    };
    storage_types::plan_read_sequence(&ReadSequenceRequest::new(vec![root, child]))
        .expect("valid plan")
}

#[test]
fn selects_only_eligible_tuple_edge_and_records_reason() {
    let descriptors = eligible_descriptors();
    let selected = select_read_sequence_mapped_edges(
        &plan(),
        &descriptors,
        ReadSequenceMappedOptions {
            foundationdb: true,
            api_version: 740,
            enabled: true,
            consistency: ReadSequenceConsistency::Eventual,
        },
    );
    assert_eq!(selected.selected.len(), 1);
    assert!(selected.assessments[0].reason.is_none());
}

#[test]
fn selects_point_get_to_partition_query_edge() {
    let descriptors = [
        (
            ReadSequenceNodeId::from_index(0),
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::Point,
                tuple_schema: true,
                tuple_prefix_safe: true,
                selector_physical: true,
                ..Default::default()
            },
        ),
        (
            ReadSequenceNodeId::from_index(1),
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::PrefixRange,
                tuple_schema: true,
                tuple_prefix_safe: true,
                selector_physical: true,
                ..Default::default()
            },
        ),
    ];
    let selection = select_read_sequence_mapped_edges(
        &get_query_plan(),
        &descriptors,
        ReadSequenceMappedOptions {
            foundationdb: true,
            api_version: 740,
            enabled: true,
            consistency: ReadSequenceConsistency::Eventual,
        },
    );
    assert_eq!(selection.selected.len(), 1);
    assert_eq!(selection.selected[0].input_name, "account");
    assert!(selection.assessments[0].reason.is_none());
}

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn given_eligible_edge_when_selecting_repeatedly_then_allocations_are_bounded() {
    let plan = plan();
    let descriptors = eligible_descriptors();
    let options = ReadSequenceMappedOptions {
        foundationdb: true,
        api_version: 740,
        enabled: true,
        consistency: ReadSequenceConsistency::Eventual,
    };
    let guard = AllocationGuard::start(
        module_path!(),
        "given_eligible_edge_when_selecting_repeatedly_then_allocations_are_bounded",
        file!(),
        line!(),
        Some("mapped_edge_selection"),
    );

    for _ in 0..4_096 {
        let selection = select_read_sequence_mapped_edges(&plan, &descriptors, options);
        assert_eq!(selection.selected.len(), 1);
    }

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count <= 21_000);
    assert!(report.allocated_bytes <= 1_700_000);
}

fn eligible_descriptors() -> Vec<(ReadSequenceNodeId, ReadSequencePhysicalDescriptor)> {
    [
        (
            ReadSequenceNodeId::from_index(0),
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::PrefixRange,
                tuple_schema: true,
                tuple_prefix_safe: true,
                selector_physical: true,
                ..Default::default()
            },
        ),
        (
            ReadSequenceNodeId::from_index(1),
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::Point,
                tuple_schema: true,
                tuple_prefix_safe: true,
                selector_physical: true,
                ..Default::default()
            },
        ),
    ]
    .into_iter()
    .collect()
}

#[test]
fn every_physical_guard_is_bounded_and_deterministic() {
    let mut descriptor = ReadSequencePhysicalDescriptor {
        operation: ReadSequencePhysicalOperation::PrefixRange,
        tuple_schema: true,
        tuple_prefix_safe: true,
        selector_physical: true,
        ..Default::default()
    };
    let mut child = ReadSequencePhysicalDescriptor {
        operation: ReadSequencePhysicalOperation::Point,
        tuple_schema: true,
        tuple_prefix_safe: true,
        selector_physical: true,
        ..Default::default()
    };
    let options = ReadSequenceMappedOptions {
        foundationdb: true,
        api_version: 740,
        enabled: true,
        consistency: ReadSequenceConsistency::Eventual,
    };
    let assessment = |parent: &ReadSequencePhysicalDescriptor,
                      child: &ReadSequencePhysicalDescriptor|
     -> Option<ReadSequenceMappedRejectionReason> {
        let descriptors = [
            (ReadSequenceNodeId::from_index(0), parent.clone()),
            (ReadSequenceNodeId::from_index(1), child.clone()),
        ];
        select_read_sequence_mapped_edges(&plan(), &descriptors, options)
            .assessments
            .first()
            .and_then(|edge| edge.reason)
    };

    descriptor.tuple_types_match = false;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::TupleTypeMismatch)
    );
    descriptor.tuple_types_match = true;
    descriptor.tuple_prefix_safe = false;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::NonTupleSource)
    );
    descriptor.tuple_prefix_safe = true;
    descriptor.selector_physical = false;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::SelectorNotPhysical)
    );
    descriptor.selector_physical = true;
    descriptor.unsupported_projection = true;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::ProjectionSemantics)
    );
    descriptor.unsupported_projection = false;
    descriptor.secondary_limit_safe = false;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::SecondaryLimit)
    );
    descriptor.secondary_limit_safe = true;
    descriptor.continuation_safe = false;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::Continuation)
    );
    descriptor.continuation_safe = true;
    descriptor.read_your_writes = true;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::ReadYourWrites)
    );
    descriptor.read_your_writes = false;
    descriptor.estimated_miss_cost_high = true;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::EstimatedMissCost)
    );
    descriptor.estimated_miss_cost_high = false;
    descriptor.latency_benefit = false;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::NoLatencyBenefit)
    );
    child.operation = ReadSequencePhysicalOperation::Other;
    descriptor.latency_benefit = true;
    assert_eq!(
        assessment(&descriptor, &child),
        Some(ReadSequenceMappedRejectionReason::ChildOperation)
    );
}

#[test]
fn selected_edges_are_maximal_under_parent_and_child_ownership() {
    let descriptors = [
        (
            ReadSequenceNodeId::from_index(0),
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::PrefixRange,
                tuple_schema: true,
                tuple_prefix_safe: true,
                selector_physical: true,
                ..Default::default()
            },
        ),
        (
            ReadSequenceNodeId::from_index(1),
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::Point,
                tuple_schema: true,
                tuple_prefix_safe: true,
                selector_physical: true,
                ..Default::default()
            },
        ),
    ];
    let selection = select_read_sequence_mapped_edges(
        &plan(),
        &descriptors,
        ReadSequenceMappedOptions {
            foundationdb: true,
            api_version: 740,
            enabled: true,
            consistency: ReadSequenceConsistency::Eventual,
        },
    );
    assert_eq!(selection.selected.len(), 1);
    assert!(
        selection
            .assessments
            .iter()
            .filter(|assessment| assessment.reason.is_none())
            .all(|assessment| selection.selected.iter().any(|selected| {
                selected.parent == assessment.parent
                    && selected.child == assessment.child
                    && selected.input_name == assessment.input_name
            }))
    );
}

#[test]
fn generated_small_dags_have_deterministic_maximal_selection() {
    for shape in 0..720 {
        let (plan, descriptors) = generated_plan(shape);
        let options = ReadSequenceMappedOptions {
            foundationdb: true,
            api_version: 740,
            enabled: true,
            consistency: ReadSequenceConsistency::Eventual,
        };
        let selection = select_read_sequence_mapped_edges(&plan, &descriptors, options);
        assert_eq!(
            selection,
            select_read_sequence_mapped_edges(&plan, &descriptors, options)
        );

        let mut endpoints = BTreeSet::new();
        for edge in &selection.selected {
            assert!(endpoints.insert(edge.parent));
            assert!(endpoints.insert(edge.child));
        }
        for assessment in selection
            .assessments
            .iter()
            .filter(|edge| edge.reason.is_none())
        {
            assert!(selection.selected.iter().any(|edge| {
                edge.parent == assessment.parent
                    || edge.child == assessment.parent
                    || edge.parent == assessment.child
                    || edge.child == assessment.child
            }));
        }
    }
}

fn generated_plan(
    mut shape: usize,
) -> (
    ReadSequencePlan,
    Vec<(ReadSequenceNodeId, ReadSequencePhysicalDescriptor)>,
) {
    let mut nodes = (0..6)
        .map(|index| generated_get(&format!("node{index}")))
        .collect::<Vec<_>>();
    for (child_index, node) in nodes.iter_mut().enumerate().skip(1) {
        let choice = shape % (child_index + 1);
        shape /= child_index + 1;
        if choice == 0 {
            continue;
        }
        let parent_index = choice - 1;
        let input_name = format!("parent{parent_index}");
        node.operation = ReadSequenceNodeOperation::Get(GetItemRequest::new(
            storage_types::TableName::new("items"),
            KeyAttributes::from([(
                String::from("id"),
                storage_types::read_sequence_input_marker(&input_name),
            )]),
        ));
        node.inputs_mut().insert(
            input_name.clone(),
            ReadSequenceNodeInput {
                from: ReadSequenceFromInput {
                    node: format!("node{parent_index}"),
                    select: ReadSequenceSelector("$.Get.Item.id".into()),
                },
                mapped_key_source: None,
                cardinality: ReadSequenceInputCardinality::Many,
                on_missing: ReadSequenceOnMissing::Skip,
            },
        );
        node.iterate = Some(input_name);
    }
    let request = ReadSequenceRequest::new(nodes);
    let plan = storage_types::plan_read_sequence(&request).expect("generated DAG");
    let descriptors = (0..plan.nodes.len())
        .map(|index| {
            (
                ReadSequenceNodeId::from_index(index),
                ReadSequencePhysicalDescriptor {
                    operation: ReadSequencePhysicalOperation::PrefixRange,
                    tuple_schema: true,
                    tuple_prefix_safe: true,
                    selector_physical: true,
                    ..Default::default()
                },
            )
        })
        .collect();
    (plan, descriptors)
}

fn generated_get(name: &str) -> ReadSequenceNode {
    ReadSequenceNode {
        name: name.to_string(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
            storage_types::TableName::new("items"),
            KeyAttributes::from([(String::from("id"), AttributeValue::S(name.to_string()))]),
        )),
        inputs: None,
        iterate: None,
        after: None,
    }
}

fn get_query_plan() -> ReadSequencePlan {
    let parent = ReadSequenceNode {
        name: "account".into(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
            storage_types::TableName::new("accounts"),
            KeyAttributes::from([(String::from("pk"), AttributeValue::S("account-1".into()))]),
        )),
        inputs: None,
        iterate: None,
        after: None,
    };
    let mut query = QueryRequest::new(
        storage_types::TableName::new("events"),
        "account = :account".into(),
    );
    query.expression_attribute_values = Some(
        [(
            String::from(":account"),
            storage_types::read_sequence_input_marker("account"),
        )]
        .into_iter()
        .collect(),
    );
    let child = ReadSequenceNode {
        name: "events".into(),
        operation: ReadSequenceNodeOperation::Query(query),
        inputs: Some(
            [(
                String::from("account"),
                ReadSequenceNodeInput {
                    from: ReadSequenceFromInput {
                        node: "account".into(),
                        select: ReadSequenceSelector("$.Get.Item.pk".into()),
                    },
                    mapped_key_source: None,
                    cardinality: ReadSequenceInputCardinality::One,
                    on_missing: ReadSequenceOnMissing::Error,
                },
            )]
            .into_iter()
            .collect(),
        ),
        iterate: None,
        after: None,
    };
    storage_types::plan_read_sequence(&ReadSequenceRequest::new(vec![parent, child]))
        .expect("valid get/query plan")
}
