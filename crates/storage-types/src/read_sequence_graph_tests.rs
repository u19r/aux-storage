use std::collections::BTreeMap;

use crate::{
    AttributeValue, GetItemRequest, KeyAttributes, QueryRequest, ReadSequenceConsistency,
    ReadSequenceFromInput, ReadSequenceInputCardinality, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeInput, ReadSequenceNodeOperation, ReadSequenceOnMissing, ReadSequenceRequest,
    ReadSequenceSelector, ReadSequenceValidationError, TableName, plan_read_sequence,
    read_sequence_input_marker, read_sequence_input_marker_name, read_sequence_string_template,
    read_sequence_string_template_name,
};

fn get(name: &str) -> ReadSequenceNode {
    ReadSequenceNode::new(
        name,
        ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("items"),
            KeyAttributes::from([(String::from("id"), AttributeValue::S(name.to_string()))]),
        )),
    )
}

fn request(nodes: Vec<ReadSequenceNode>, outputs: Option<Vec<&str>>) -> ReadSequenceRequest {
    ReadSequenceRequest {
        nodes,
        outputs: outputs.map(|values| values.into_iter().map(str::to_string).collect()),
        ..Default::default()
    }
}

#[test]
fn default_request_and_root_node_omit_optional_fields_tests() {
    let mut node = get("root");
    assert!(node.inputs.is_none());
    assert!(node.after.is_none());
    node.inputs = Some(BTreeMap::new());
    node.after = Some(Vec::new());

    let request = ReadSequenceRequest::new(vec![node]);
    assert_eq!(request.read_consistency, ReadSequenceConsistency::Eventual);
    let value = serde_json::to_value(request).expect("serialize request");
    assert!(value["Nodes"][0].get("Inputs").is_none());
    assert!(value["Nodes"][0].get("After").is_none());
    assert!(value.get("MaxSequenceSteps").is_none());
}

#[test]
fn node_builder_sets_only_present_optional_fields_tests() {
    let input = ReadSequenceNodeInput {
        from: ReadSequenceFromInput {
            node: "root".to_string(),
            select: ReadSequenceSelector("$.Get.Item.id".to_string()),
        },
        mapped_key_source: None,
        cardinality: ReadSequenceInputCardinality::Many,
        on_missing: ReadSequenceOnMissing::Skip,
    };
    let node = ReadSequenceNode::builder()
        .name("child")
        .operation(ReadSequenceNodeOperation::Get(GetItemRequest::new(
            TableName::new("items"),
            KeyAttributes::from([("id".to_string(), read_sequence_input_marker("id"))]),
        )))
        .inputs(BTreeMap::from([("id".to_string(), input)]))
        .iterate("id")
        .build();

    assert_eq!(node.inputs().len(), 1);
    assert_eq!(node.iterate.as_deref(), Some("id"));
    assert!(node.after.is_none());
}

#[test]
fn given_late_declared_parent_when_planning_then_graph_is_valid() {
    let child = ReadSequenceNode {
        after: Some(vec!["parent".to_string()]),
        ..get("child")
    };
    let plan = plan_read_sequence(&request(vec![child, get("parent")], None)).expect("plan");
    assert_eq!(
        plan.graph
            .topological_order
            .iter()
            .map(|node| plan.graph.node_name(*node).unwrap())
            .collect::<Vec<_>>(),
        vec!["parent", "child"]
    );
}

#[test]
fn given_diamond_when_planning_then_waves_are_stable_and_merge_waits() {
    let nodes = vec![
        get("root"),
        ReadSequenceNode {
            after: Some(vec!["root".to_string()]),
            ..get("left")
        },
        ReadSequenceNode {
            after: Some(vec!["root".to_string()]),
            ..get("right")
        },
        ReadSequenceNode {
            after: Some(vec!["left".to_string(), "right".to_string()]),
            ..get("merge")
        },
    ];
    let plan = plan_read_sequence(&request(nodes, None)).expect("plan");
    assert_eq!(plan.graph.waves.len(), 3);
    assert_eq!(plan.graph.waves[0], vec![ReadSequenceNodeId::from_index(0)]);
    assert_eq!(
        plan.graph.waves[1],
        vec![
            ReadSequenceNodeId::from_index(1),
            ReadSequenceNodeId::from_index(2)
        ]
    );
    assert_eq!(plan.graph.waves[2], vec![ReadSequenceNodeId::from_index(3)]);
}

#[test]
fn given_cycle_when_planning_then_cycle_is_rejected() {
    let mut a = get("a");
    a.after = Some(vec!["b".to_string()]);
    let mut b = get("b");
    b.after = Some(vec!["a".to_string()]);
    assert!(matches!(
        plan_read_sequence(&request(vec![a, b], None)),
        Err(ReadSequenceValidationError::DependencyCycle { cycle })
            if cycle == vec!["a", "b", "a"]
    ));
}

#[test]
fn given_self_dependency_when_planning_then_it_is_rejected_before_reads() {
    let mut node = get("self");
    node.after = Some(vec!["self".to_string()]);
    assert!(matches!(
        plan_read_sequence(&request(vec![node], None)),
        Err(ReadSequenceValidationError::SelfDependency { node }) if node == "self"
    ));
}

#[test]
fn given_unknown_after_node_when_planning_then_it_is_rejected() {
    let mut node = get("child");
    node.after = Some(vec!["missing".to_string()]);
    assert!(matches!(
        plan_read_sequence(&request(vec![node], None)),
        Err(ReadSequenceValidationError::UnknownNode { referenced, .. })
            if referenced == "missing"
    ));
}

#[test]
fn given_unselected_node_when_planning_then_unreachable_is_rejected() {
    assert!(matches!(
        plan_read_sequence(&request(vec![get("a"), get("b")], Some(vec!["a"]))),
        Err(ReadSequenceValidationError::UnreachableNode { node }) if node == "b"
    ));
}

#[test]
fn given_node_count_above_configured_step_limit_when_planning_then_limit_is_rejected() {
    let mut request = request(vec![get("a"), get("b")], None);
    request.max_sequence_steps = Some(1);
    assert!(matches!(
        plan_read_sequence(&request),
        Err(ReadSequenceValidationError::NodeLimitExceeded {
            actual: 2,
            limit: 1
        })
    ));
}

#[test]
fn given_input_marker_when_serializing_then_wire_uses_from_input() {
    let operation = ReadSequenceNodeOperation::Query(QueryRequest {
        table_name: TableName::new("groups"),
        expression_attribute_values: Some(std::collections::HashMap::from([(
            String::from(":id"),
            read_sequence_input_marker("subject"),
        )])),
        ..QueryRequest::new(TableName::new("groups"), "id = :id".to_string())
    });
    let node = ReadSequenceNode {
        name: "groups".to_string(),
        operation,
        inputs: Some(BTreeMap::from([(
            "subject".to_string(),
            ReadSequenceNodeInput {
                from: ReadSequenceFromInput {
                    node: "subject".to_string(),
                    select: ReadSequenceSelector("$.Get.Item.id".to_string()),
                },
                mapped_key_source: None,
                cardinality: ReadSequenceInputCardinality::One,
                on_missing: ReadSequenceOnMissing::Error,
            },
        )])),
        iterate: None,
        after: None,
    };
    let value = serde_json::to_value(node).expect("serialize");
    assert_eq!(
        value["Operation"]["Query"]["ExpressionAttributeValues"][":id"]["FromInput"],
        "subject"
    );
}

#[test]
fn given_string_template_when_round_tripping_then_wire_shape_is_stable() {
    let value = serde_json::json!({
        "Name": "model",
        "Operation": {"Get": {
            "TableName": "items",
            "Key": {
                "pk": {"StringTemplate": "entity#{id}#sub_model#{sub_id}#v1"}
            }
        }},
        "Inputs": {
            "id": {
                "From": {"Node": "entity", "Select": "$.Get.Item.id.S"},
                "Cardinality": "ONE",
                "OnMissing": "ERROR"
            },
            "sub_id": {
                "From": {"Node": "sub_model", "Select": "$.Get.Item.sub_id.S"},
                "Cardinality": "ONE",
                "OnMissing": "ERROR"
            }
        }
    });

    let node: ReadSequenceNode = serde_json::from_value(value.clone()).expect("template node");
    let ReadSequenceNodeOperation::Get(request) = &node.operation else {
        panic!("expected Get");
    };
    assert_eq!(
        request
            .key
            .get("pk")
            .and_then(read_sequence_string_template_name),
        Some("entity#{id}#sub_model#{sub_id}#v1")
    );
    assert_eq!(
        serde_json::to_value(node).expect("serialize template"),
        value
    );
}

#[test]
fn given_multi_parent_string_template_when_planning_then_both_inputs_are_dependencies() {
    let request: ReadSequenceRequest = serde_json::from_value(serde_json::json!({
        "Nodes": [
            {
                "Name": "entity",
                "Operation": {"Get": {
                    "TableName": "lookups",
                    "Key": {"pk": {"S": "entity_context#42"}}
                }}
            },
            {
                "Name": "sub_model",
                "Operation": {"Get": {
                    "TableName": "lookups",
                    "Key": {"pk": {"S": "sub_model_context#7"}}
                }}
            },
            {
                "Name": "model",
                "Operation": {"Get": {
                    "TableName": "items",
                    "Key": {
                        "pk": {"StringTemplate": "entity#{id}#sub_model#{sub_id}#v1"}
                    }
                }},
                "Inputs": {
                    "id": {
                        "From": {"Node": "entity", "Select": "$.Get.Item.id.S"},
                        "Cardinality": "ONE"
                    },
                    "sub_id": {
                        "From": {"Node": "sub_model", "Select": "$.Get.Item.sub_id.S"},
                        "Cardinality": "ONE"
                    }
                }
            }
        ],
        "Outputs": ["model"]
    }))
    .expect("request");

    let plan = plan_read_sequence(&request).expect("plan");
    assert_eq!(
        plan.graph.dependencies[2],
        vec![
            ReadSequenceNodeId::from_index(0),
            ReadSequenceNodeId::from_index(1)
        ]
    );
}

#[test]
fn given_undeclared_string_template_input_when_planning_then_it_is_rejected() {
    let mut child = get("child");
    let ReadSequenceNodeOperation::Get(get_request) = &mut child.operation else {
        panic!("expected Get");
    };
    get_request.key = KeyAttributes::from([(
        "id".to_string(),
        read_sequence_string_template("entity#{missing}"),
    )]);

    assert!(matches!(
        plan_read_sequence(&request(vec![child], None)),
        Err(ReadSequenceValidationError::UnknownInput { input, .. }) if input == "missing"
    ));
}

#[test]
fn given_invalid_string_template_when_planning_then_it_is_rejected() {
    let mut child = get("child");
    let ReadSequenceNodeOperation::Get(get_request) = &mut child.operation else {
        panic!("expected Get");
    };
    get_request.key = KeyAttributes::from([(
        "id".to_string(),
        read_sequence_string_template("entity#{id"),
    )]);

    assert!(matches!(
        plan_read_sequence(&request(vec![child], None)),
        Err(ReadSequenceValidationError::InvalidStringTemplate { .. })
    ));
}

#[test]
fn given_map_attribute_named_from_input_when_deserializing_then_it_stays_a_map() {
    let value = serde_json::json!({
        "Name": "root",
        "Operation": {"Get": {
            "TableName": "items",
            "Key": {"id": {"M": {"FromInput": {"S": "literal"}}}}
        }}
    });
    let node: ReadSequenceNode = serde_json::from_value(value).expect("map attribute");
    let ReadSequenceNodeOperation::Get(request) = node.operation else {
        panic!("expected get");
    };
    assert!(matches!(
        request.key.get("id"),
        Some(AttributeValue::M(values)) if matches!(values.get("FromInput"), Some(AttributeValue::S(value)) if value == "literal")
    ));
}

#[test]
fn given_literal_input_marker_string_when_round_tripping_then_it_is_not_bound() {
    let AttributeValue::S(literal) = read_sequence_input_marker("literal") else {
        panic!("marker helper must return a string");
    };
    let value = serde_json::json!({
        "Name": "root",
        "Operation": {"Get": {
            "TableName": "items",
            "Key": {"id": {"S": literal}}
        }}
    });
    let node: ReadSequenceNode = serde_json::from_value(value.clone()).expect("literal node");
    let ReadSequenceNodeOperation::Get(request) = &node.operation else {
        panic!("expected get");
    };
    assert!(read_sequence_input_marker_name(request.key.get("id").unwrap()).is_none());
    let encoded = serde_json::to_value(node).expect("serialize literal node");
    assert_eq!(encoded, value);
}
