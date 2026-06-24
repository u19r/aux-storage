use std::collections::BTreeMap;

use alloc_counter::AllocationGuard;

use crate::{
    AttributeMap, AttributeValue, GetItemRequest, IndexName, KeyAttributes,
    ParsedReadSequenceSelector, QueryRequest, ReadSequenceConsistency, ReadSequenceForEach,
    ReadSequenceJoin, ReadSequenceJoinType, ReadSequenceOnMissing, ReadSequencePlannerInput,
    ReadSequenceRequest, ReadSequenceSelectedContext, ReadSequenceSelector, ReadSequenceStep,
    ReadSequenceValidationCapabilities, ReadSequenceValidationError, TableName,
    bind_read_sequence_attribute_value, plan_read_sequence,
};

const ALLOCATION_PROFILE_ITERATIONS: usize = 256;

fn key(id: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".to_string(), AttributeValue::S(id.to_string()))])
}

fn root_get_step() -> ReadSequenceStep {
    let mut select = BTreeMap::new();
    select.insert(
        "org_id".to_string(),
        ReadSequenceSelector("$.org_id".to_string()),
    );
    ReadSequenceStep {
        name: "user".to_string(),
        select,
        get: Some(GetItemRequest::new(TableName::new("Users"), key("user#1"))),
        batch_get: None,
        query: None,
        for_each: None,
    }
}

fn child_get_step() -> ReadSequenceStep {
    ReadSequenceStep {
        name: "org".to_string(),
        select: BTreeMap::new(),
        get: None,
        batch_get: None,
        query: None,
        for_each: Some(ReadSequenceForEach {
            from: ReadSequenceSelector("user.Item.org_id".to_string()),
            as_name: "org_id".to_string(),
            on_missing: ReadSequenceOnMissing::Error,
            get: Some(GetItemRequest::new(
                TableName::new("Organizations"),
                key("org#1"),
            )),
            batch_get: None,
            query: None,
            join: ReadSequenceJoin {
                to: "user".to_string(),
                as_name: "org".to_string(),
                join_type: ReadSequenceJoinType::RequiredOne,
            },
        }),
    }
}

fn valid_request() -> ReadSequenceRequest {
    ReadSequenceRequest {
        read_consistency: ReadSequenceConsistency::Eventual,
        max_sequence_steps: None,
        max_root_items: None,
        max_fanout_per_step: None,
        max_intermediate_items: None,
        max_total_read_items: None,
        max_child_query_items_per_parent: None,
        max_response_bytes: None,
        max_selector_bindings_per_step: None,
        max_selector_path_depth: None,
        next_sequence_token: None,
        return_consumed_capacity: Some("TOTAL".to_string()),
        sequence: vec![root_get_step(), child_get_step()],
    }
}

#[test]
fn given_get_child_get_sequence_when_validating_then_request_is_accepted() {
    valid_request().validate().expect("valid request");
}

#[test]
fn given_duplicate_step_names_when_validating_then_request_is_rejected() {
    let mut request = valid_request();
    request.sequence[1].name = "user".to_string();

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::DuplicateStepName { .. })
    ));
}

#[test]
fn given_child_references_later_step_when_validating_then_request_is_rejected() {
    let mut request = valid_request();
    request.sequence.swap(0, 1);

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::UnknownDependency { .. })
    ));
}

#[test]
fn given_too_many_steps_when_validating_then_request_is_rejected() {
    let mut request = valid_request();
    request.max_sequence_steps = Some(1);

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::StepLimitExceeded { .. })
    ));
}

#[test]
fn given_selector_path_too_deep_when_validating_then_request_is_rejected() {
    let mut request = valid_request();
    request.max_selector_path_depth = Some(2);
    request.sequence[0].select.insert(
        "deep".to_string(),
        ReadSequenceSelector("$.a.b.c".to_string()),
    );

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::SelectorPathTooDeep { .. })
    ));
}

#[test]
fn given_strong_gsi_query_when_validating_then_request_is_rejected() {
    let mut request = valid_request();
    request.read_consistency = ReadSequenceConsistency::Strong;
    request.sequence[0] = ReadSequenceStep {
        name: "orders".to_string(),
        select: BTreeMap::new(),
        get: None,
        batch_get: None,
        query: Some(
            QueryRequest::new(
                TableName::new("Orders"),
                "customer_id = :customer".to_string(),
            )
            .with_index_name(Some(IndexName::new("by_customer"))),
        ),
        for_each: None,
    };
    request.sequence.truncate(1);

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::StrongGsiRejected)
    ));
}

#[test]
fn given_transactional_gsi_without_immediate_consistency_when_validating_then_request_is_rejected()
{
    let mut request = valid_request();
    request.read_consistency = ReadSequenceConsistency::Transactional;
    request.sequence[0] = ReadSequenceStep {
        name: "orders".to_string(),
        select: BTreeMap::new(),
        get: None,
        batch_get: None,
        query: Some(
            QueryRequest::new(
                TableName::new("Orders"),
                "customer_id = :customer".to_string(),
            )
            .with_index_name(Some(IndexName::new("by_customer"))),
        ),
        for_each: None,
    };
    request.sequence.truncate(1);

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::TransactionalGsiRejected)
    ));
}

#[test]
fn given_transactional_gsi_with_immediate_consistency_when_validating_then_request_is_accepted() {
    let mut request = valid_request();
    request.read_consistency = ReadSequenceConsistency::Transactional;
    request.sequence[0] = ReadSequenceStep {
        name: "orders".to_string(),
        select: BTreeMap::new(),
        get: None,
        batch_get: None,
        query: Some(
            QueryRequest::new(
                TableName::new("Orders"),
                "customer_id = :customer".to_string(),
            )
            .with_index_name(Some(IndexName::new("by_customer"))),
        ),
        for_each: None,
    };
    request.sequence.truncate(1);

    request
        .validate_with_capabilities(ReadSequenceValidationCapabilities {
            eventual_reads: true,
            strong_reads: true,
            transactional_reads: true,
            immediate_gsi_consistency: true,
        })
        .expect("immediate GSI mode permits transactional GSI reads");
}

#[test]
fn given_provider_without_strong_reads_when_validating_strong_request_then_request_is_rejected() {
    let mut request = valid_request();
    request.read_consistency = ReadSequenceConsistency::Strong;

    assert!(matches!(
        request.validate_with_capabilities(ReadSequenceValidationCapabilities {
            eventual_reads: true,
            strong_reads: false,
            transactional_reads: true,
            immediate_gsi_consistency: false,
        }),
        Err(ReadSequenceValidationError::UnsupportedConsistency {
            consistency: ReadSequenceConsistency::Strong
        })
    ));
}

#[test]
fn given_provider_without_transactional_reads_when_validating_transactional_request_then_request_is_rejected()
 {
    let mut request = valid_request();
    request.read_consistency = ReadSequenceConsistency::Transactional;

    assert!(matches!(
        request.validate_with_capabilities(ReadSequenceValidationCapabilities {
            eventual_reads: true,
            strong_reads: true,
            transactional_reads: false,
            immediate_gsi_consistency: false,
        }),
        Err(ReadSequenceValidationError::UnsupportedConsistency {
            consistency: ReadSequenceConsistency::Transactional
        })
    ));
}

#[test]
fn given_child_query_without_limit_when_validating_then_request_is_rejected() {
    let mut request = valid_request();
    let mut query = QueryRequest::new(TableName::new("Teams"), "org_id = :org".to_string());
    query.limit = None;
    request.sequence[1].for_each.as_mut().expect("child").get = None;
    request.sequence[1].for_each.as_mut().expect("child").query = Some(query);

    assert!(matches!(
        request.validate(),
        Err(ReadSequenceValidationError::ChildQueryLimitRequired { .. })
    ));
}

#[test]
fn given_valid_selector_when_parsing_then_segments_are_bounded_and_evaluable() {
    let selector =
        ParsedReadSequenceSelector::parse(&ReadSequenceSelector("$.profile.org_id.S".to_string()))
            .expect("parse selector");
    assert_eq!(selector.depth(), 3);

    let mut profile = std::collections::HashMap::new();
    profile.insert("org_id".to_string(), AttributeValue::S("org#1".to_string()));
    let mut item = AttributeMap::new();
    item.insert("profile", AttributeValue::M(profile));

    assert_eq!(
        selector.evaluate_item(&item).expect("evaluate"),
        Some(AttributeValue::S("org#1".to_string()))
    );
}

#[test]
fn given_missing_selector_value_when_evaluating_then_none_is_returned() {
    let selector =
        ParsedReadSequenceSelector::parse(&ReadSequenceSelector("$.missing.S".to_string()))
            .expect("parse selector");
    assert_eq!(
        selector
            .evaluate_item(&AttributeMap::new())
            .expect("evaluate"),
        None
    );
}

#[test]
fn given_template_context_when_binding_then_scalar_values_are_interpolated() {
    let mut context = ReadSequenceSelectedContext::default();
    context.insert("order.invoice_id.S", AttributeValue::S("inv#1".to_string()));
    context.insert("order.total.N", AttributeValue::N("25".to_string()));
    context.insert("order.blob.B", AttributeValue::B("abc".to_string()));

    assert_eq!(
        bind_read_sequence_attribute_value(
            &AttributeValue::S("invoice#${order.invoice_id.S}".to_string()),
            &context,
        )
        .expect("bind string"),
        AttributeValue::S("invoice#inv#1".to_string())
    );
    assert_eq!(
        bind_read_sequence_attribute_value(
            &AttributeValue::N("${order.total.N}".to_string()),
            &context
        )
        .expect("bind number"),
        AttributeValue::N("25".to_string())
    );
    assert_eq!(
        bind_read_sequence_attribute_value(
            &AttributeValue::B("${order.blob.B}".to_string()),
            &context
        )
        .expect("bind binary"),
        AttributeValue::B("abc".to_string())
    );
}

#[test]
fn given_set_template_context_when_binding_then_set_value_is_preserved() {
    let mut context = ReadSequenceSelectedContext::default();
    context.insert(
        "user.team_ids.SS",
        AttributeValue::SS(vec!["team#1".to_string(), "team#2".to_string()]),
    );

    assert_eq!(
        bind_read_sequence_attribute_value(
            &AttributeValue::SS(vec!["${user.team_ids.SS}".to_string()]),
            &context,
        )
        .expect("bind set"),
        AttributeValue::SS(vec!["team#1".to_string(), "team#2".to_string()])
    );
}

#[test]
fn read_sequence_selector_extraction_allocation_baseline_tests() {
    let report = measure_selector_extraction_allocations();

    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn read_sequence_template_binding_allocation_baseline_tests() {
    let report = measure_template_binding_allocations();

    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

fn measure_selector_extraction_allocations() -> alloc_counter::AllocationReport<'static> {
    let selector = ParsedReadSequenceSelector::parse(&ReadSequenceSelector(
        "$.profile.addresses[1].org_id.S".to_string(),
    ))
    .expect("selector");
    let mut second_address = std::collections::HashMap::new();
    second_address.insert("org_id".to_string(), AttributeValue::S("org#2".to_string()));
    let mut first_address = std::collections::HashMap::new();
    first_address.insert("org_id".to_string(), AttributeValue::S("org#1".to_string()));
    let mut profile = std::collections::HashMap::new();
    profile.insert(
        "addresses".to_string(),
        AttributeValue::L(vec![
            AttributeValue::M(first_address),
            AttributeValue::M(second_address),
        ]),
    );
    let item = AttributeMap::from(std::collections::HashMap::from([(
        "profile".to_string(),
        AttributeValue::M(profile),
    )]));
    let guard = AllocationGuard::start(
        module_path!(),
        "read_sequence_selector_extraction_allocation_profile_tests",
        file!(),
        line!(),
        Some("selector_extraction"),
    );

    for _ in 0..ALLOCATION_PROFILE_ITERATIONS {
        let selected = selector.evaluate_item(&item).expect("evaluate selector");
        std::hint::black_box(selected);
    }

    guard.finish()
}

fn measure_template_binding_allocations() -> alloc_counter::AllocationReport<'static> {
    let mut context = ReadSequenceSelectedContext::default();
    context.insert("root_pk", AttributeValue::S("pk#1".to_string()));
    context.insert("root_sk", AttributeValue::S("sk#1".to_string()));
    context.insert(
        "child_ids",
        AttributeValue::SS(vec!["child#1".to_string(), "child#2".to_string()]),
    );
    let value = AttributeValue::M(std::collections::HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("parent#${root_pk}".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S("${root_sk}".to_string()),
        ),
        (
            "child_ids".to_string(),
            AttributeValue::SS(vec!["${child_ids}".to_string()]),
        ),
    ]));
    let guard = AllocationGuard::start(
        module_path!(),
        "read_sequence_template_binding_allocation_profile_tests",
        file!(),
        line!(),
        Some("template_binding"),
    );

    for _ in 0..ALLOCATION_PROFILE_ITERATIONS {
        let bound = bind_read_sequence_attribute_value(&value, &context).expect("bind template");
        std::hint::black_box(bound);
    }

    guard.finish()
}

#[test]
fn given_join_modes_when_planning_then_child_plans_preserve_join_semantics() {
    for join_type in [
        ReadSequenceJoinType::LeftOne,
        ReadSequenceJoinType::RequiredOne,
        ReadSequenceJoinType::Array,
        ReadSequenceJoinType::InnerOne,
    ] {
        let mut request = valid_request();
        request.sequence[1]
            .for_each
            .as_mut()
            .expect("child")
            .join
            .join_type = join_type;
        let mut input = ReadSequencePlannerInput::default();
        input.parent_counts.insert("user".to_string(), 2);

        let plan = plan_read_sequence(&request, &input).expect("plan");
        assert_eq!(plan.child_steps[0].join_type, join_type);
        assert_eq!(plan.child_steps[0].parent_count, 2);
    }
}

#[test]
fn given_parent_count_above_fanout_when_planning_then_plan_is_rejected() {
    let mut request = valid_request();
    request.max_fanout_per_step = Some(1);
    let mut input = ReadSequencePlannerInput::default();
    input.parent_counts.insert("user".to_string(), 2);

    assert!(matches!(
        plan_read_sequence(&request, &input),
        Err(ReadSequenceValidationError::FanoutLimitExceeded { .. })
    ));
}

#[test]
fn given_query_child_get_when_planning_then_sanitized_gsi_warning_is_emitted() {
    let mut request = valid_request();
    request.sequence[0] = ReadSequenceStep {
        name: "orders".to_string(),
        select: BTreeMap::new(),
        get: None,
        batch_get: None,
        query: Some(QueryRequest::new(
            TableName::new("Orders"),
            "customer_id = :customer".to_string(),
        )),
        for_each: None,
    };
    request.sequence[1].for_each.as_mut().expect("child").from =
        ReadSequenceSelector("orders.Items.invoice_id".to_string());
    request.sequence[1]
        .for_each
        .as_mut()
        .expect("child")
        .join
        .to = "orders".to_string();

    let mut input = ReadSequencePlannerInput::default();
    input.parent_counts.insert("orders".to_string(), 1);
    let plan = plan_read_sequence(&request, &input).expect("plan");
    let warning = plan.warning.expect("warning");

    assert_eq!(warning.code, "BetterModeledAsGsi");
    let suggested = warning.suggested_gsi.expect("suggestion");
    assert_eq!(suggested.partition_key.attribute_name, "customer_id");
    assert!(!warning.message.contains("customer#"));
    assert!(!suggested.partition_key.source.contains(":customer"));
}

#[test]
fn given_ambiguous_query_child_get_when_planning_then_gsi_warning_is_not_emitted() {
    let mut request = valid_request();
    request.sequence[0] = ReadSequenceStep {
        name: "orders".to_string(),
        select: BTreeMap::new(),
        get: None,
        batch_get: None,
        query: Some(QueryRequest::new(
            TableName::new("Orders"),
            "customer_id BETWEEN :start AND :end".to_string(),
        )),
        for_each: None,
    };
    request.sequence[1].for_each.as_mut().expect("child").from =
        ReadSequenceSelector("orders.Items.invoice_id".to_string());
    request.sequence[1]
        .for_each
        .as_mut()
        .expect("child")
        .join
        .to = "orders".to_string();

    let plan = plan_read_sequence(&request, &ReadSequencePlannerInput::default()).expect("plan");
    assert!(plan.warning.is_none());
}
