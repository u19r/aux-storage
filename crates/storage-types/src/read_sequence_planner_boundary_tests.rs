use crate::{
    AttributeValue, GetItemRequest, IndexName, KeyAttributes, QueryRequest,
    ReadSequenceConsistency, ReadSequenceNode, ReadSequenceNodeOperation, ReadSequenceRequest,
    ReadSequenceValidationError, TableName, plan_read_sequence, read_sequence_input_marker,
};

#[derive(Debug, Clone, Copy)]
enum Scenario {
    AcceptedLateParent,
    AcceptedTransactionalPoint,
    RejectedEmptySequence,
    RejectedCycle,
    RejectedUnknownDependency,
    RejectedUnknownInput,
    RejectedStrongGsi,
    RejectedTransactionalGsi,
    RejectedNodeLimit,
    RejectedFanoutCap,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::AcceptedLateParent => "accepted_late_parent",
            Self::AcceptedTransactionalPoint => "accepted_transactional_point",
            Self::RejectedEmptySequence => "rejected_empty_sequence",
            Self::RejectedCycle => "rejected_cycle",
            Self::RejectedUnknownDependency => "rejected_unknown_dependency",
            Self::RejectedUnknownInput => "rejected_unknown_input",
            Self::RejectedStrongGsi => "rejected_strong_gsi",
            Self::RejectedTransactionalGsi => "rejected_transactional_gsi",
            Self::RejectedNodeLimit => "rejected_node_limit",
            Self::RejectedFanoutCap => "rejected_fanout_cap",
        }
    }

    const fn expected_shape(self) -> (i64, i64) {
        match self {
            Self::AcceptedLateParent => (2, 2),
            Self::AcceptedTransactionalPoint => (1, 1),
            Self::RejectedEmptySequence => (0, 0),
            Self::RejectedCycle => (2, 0),
            Self::RejectedUnknownDependency | Self::RejectedUnknownInput => (1, 0),
            Self::RejectedStrongGsi | Self::RejectedTransactionalGsi => (1, 0),
            Self::RejectedNodeLimit => (9, 0),
            Self::RejectedFanoutCap => (1, 0),
        }
    }
}

fn check_scenario(scenario: Scenario) -> anyhow::Result<()> {
    let result = plan_read_sequence(&request_for(scenario));
    if scenario_is_accepted(scenario) {
        verify_accepted_plan(scenario, &result?)
    } else {
        ensure_expected_error(scenario, result.unwrap_err())
    }
}

fn verify_accepted_plan(scenario: Scenario, plan: &crate::ReadSequencePlan) -> anyhow::Result<()> {
    let (node_count, wave_count) = (
        i64::try_from(plan.nodes.len())?,
        i64::try_from(plan.graph.waves.len())?,
    );
    let expected = scenario.expected_shape();
    anyhow::ensure!(
        (node_count, wave_count) == expected,
        "{} shape was ({node_count}, {wave_count}), expected {expected:?}",
        scenario.name()
    );
    let order = plan
        .graph
        .topological_order
        .iter()
        .map(|node| plan.graph.node_name(*node).unwrap_or("<missing>"))
        .collect::<Vec<_>>();
    let expected_order = match scenario {
        Scenario::AcceptedLateParent => vec!["root", "child"],
        Scenario::AcceptedTransactionalPoint => vec!["root"],
        _ => Vec::new(),
    };
    anyhow::ensure!(
        order == expected_order,
        "{} topological order was {order:?}, expected {expected_order:?}",
        scenario.name()
    );
    Ok(())
}

fn scenario_is_accepted(scenario: Scenario) -> bool {
    matches!(
        scenario,
        Scenario::AcceptedLateParent | Scenario::AcceptedTransactionalPoint
    )
}

fn ensure_expected_error(
    scenario: Scenario,
    error: ReadSequenceValidationError,
) -> anyhow::Result<()> {
    let error_message = error.to_string();
    let matches = expected_error_matches(scenario, &error);
    anyhow::ensure!(
        matches,
        "{} returned unexpected error: {error_message}",
        scenario.name()
    );
    Ok(())
}

fn expected_error_matches(scenario: Scenario, error: &ReadSequenceValidationError) -> bool {
    match scenario {
        Scenario::RejectedEmptySequence => {
            matches!(error, ReadSequenceValidationError::EmptySequence)
        }
        Scenario::RejectedCycle => {
            matches!(error, ReadSequenceValidationError::DependencyCycle { .. })
        }
        Scenario::RejectedUnknownDependency => matches!(
            error,
            ReadSequenceValidationError::UnknownNode { referenced, .. }
                if referenced == "missing"
        ),
        Scenario::RejectedUnknownInput => matches!(
            error,
            ReadSequenceValidationError::UnknownInput { input, .. }
                if input == "missing"
        ),
        Scenario::RejectedStrongGsi => {
            matches!(error, ReadSequenceValidationError::StrongGsiRejected)
        }
        Scenario::RejectedTransactionalGsi => {
            matches!(error, ReadSequenceValidationError::TransactionalGsiRejected)
        }
        Scenario::RejectedNodeLimit => matches!(
            error,
            ReadSequenceValidationError::NodeLimitExceeded {
                actual: 9,
                limit: 8
            }
        ),
        Scenario::RejectedFanoutCap => matches!(
            error,
            ReadSequenceValidationError::FanoutLimitExceeded {
                actual: 1025,
                limit: 1024
            }
        ),
        Scenario::AcceptedLateParent | Scenario::AcceptedTransactionalPoint => false,
    }
}

fn request_for(scenario: Scenario) -> ReadSequenceRequest {
    match scenario {
        Scenario::AcceptedLateParent => late_parent_request(),
        Scenario::AcceptedTransactionalPoint => transactional_point_request(),
        Scenario::RejectedEmptySequence => ReadSequenceRequest::new(Vec::new()),
        Scenario::RejectedCycle => cycle_request(),
        Scenario::RejectedUnknownDependency => unknown_dependency_request(),
        Scenario::RejectedUnknownInput => unknown_input_request(),
        Scenario::RejectedStrongGsi => gsi_consistency_request(ReadSequenceConsistency::Strong),
        Scenario::RejectedTransactionalGsi => {
            gsi_consistency_request(ReadSequenceConsistency::Transactional)
        }
        Scenario::RejectedNodeLimit => node_limit_request(),
        Scenario::RejectedFanoutCap => fanout_limit_request(),
    }
}

fn late_parent_request() -> ReadSequenceRequest {
    let mut child = get("child");
    child.after = Some(vec!["root".to_string()]);
    ReadSequenceRequest::new(vec![child, get("root")])
}

fn transactional_point_request() -> ReadSequenceRequest {
    let mut request = ReadSequenceRequest::new(vec![get("root")]);
    request.read_consistency = ReadSequenceConsistency::Transactional;
    request
}

fn cycle_request() -> ReadSequenceRequest {
    let mut first = get("first");
    first.after = Some(vec!["second".to_string()]);
    let mut second = get("second");
    second.after = Some(vec!["first".to_string()]);
    ReadSequenceRequest::new(vec![first, second])
}

fn unknown_dependency_request() -> ReadSequenceRequest {
    let mut child = get("child");
    child.after = Some(vec!["missing".to_string()]);
    ReadSequenceRequest::new(vec![child])
}

fn unknown_input_request() -> ReadSequenceRequest {
    let mut child = get("child");
    if let ReadSequenceNodeOperation::Get(request) = &mut child.operation {
        request.key =
            KeyAttributes::from([("id".to_string(), read_sequence_input_marker("missing"))]);
    }
    ReadSequenceRequest::new(vec![child])
}

fn gsi_consistency_request(consistency: ReadSequenceConsistency) -> ReadSequenceRequest {
    let mut request = ReadSequenceRequest::new(vec![gsi_query()]);
    request.read_consistency = consistency;
    request
}

fn node_limit_request() -> ReadSequenceRequest {
    ReadSequenceRequest::new((0..9).map(|index| get(&format!("node{index}"))).collect())
}

fn fanout_limit_request() -> ReadSequenceRequest {
    let mut request = ReadSequenceRequest::new(vec![get("root")]);
    request.max_fanout_per_step = Some(1025);
    request
}

fn get(name: &str) -> ReadSequenceNode {
    ReadSequenceNode::new(
        name,
        ReadSequenceNodeOperation::Get(Box::new(GetItemRequest::new(
            TableName::new("items"),
            KeyAttributes::from([(String::from("id"), AttributeValue::S(name.to_string()))]),
        ))),
    )
}

fn gsi_query() -> ReadSequenceNode {
    let mut query = QueryRequest::new(TableName::new("items"), "gsi_pk = :pk".to_string());
    query.index_name = Some(IndexName::new("gsi1"));
    query.expression_attribute_values = Some(crate::AttributeMap::from_iter([(
        ":pk".to_string(),
        AttributeValue::S("lookup".to_string()),
    )]));
    ReadSequenceNode::new("query", ReadSequenceNodeOperation::Query(Box::new(query)))
}

#[test]
fn given_planner_scenarios_when_replayed_then_each_expected_boundary_holds() -> anyhow::Result<()> {
    for scenario in [
        Scenario::AcceptedLateParent,
        Scenario::AcceptedTransactionalPoint,
        Scenario::RejectedEmptySequence,
        Scenario::RejectedCycle,
        Scenario::RejectedUnknownDependency,
        Scenario::RejectedUnknownInput,
        Scenario::RejectedStrongGsi,
        Scenario::RejectedTransactionalGsi,
        Scenario::RejectedNodeLimit,
        Scenario::RejectedFanoutCap,
    ] {
        check_scenario(scenario)?;
    }
    Ok(())
}
