use std::{borrow::Cow, collections::HashMap, sync::Arc};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeMap, AttributeValue, ReturnValuesOldNewUpdated,
    condition::{AttributePath, PathElement},
};

use super::logic::remove_set_members;
use crate::{
    constants_tests::{UPDATE_CLONE_AUDIT_ITERATIONS, UPDATE_EXPRESSION_AUDIT_ITERATIONS},
    update_logic::{
        BoundUpdateOperation, UpdateOperation, apply_bound_update_operations,
        apply_update_operations, before_update_item, parse_update_expression,
        return_values_need_old_item, return_values_need_updated_fields, value::BoundUpdateOperand,
    },
};

/// An unaliased path from its text; `.` separates map member names.
fn attr_path(text: &str) -> AttributePath {
    let mut names = text.split('.');
    let mut path = AttributePath::top(names.next().unwrap_or_default().to_string());
    for name in names {
        path.push(PathElement::Name(name.to_string()));
    }
    path
}

/// `{a.b: top, a: {b: nested}, l[0]: literal, l: [element]}`.
fn punctuated_name_item() -> AttributeMap {
    AttributeMap::from(HashMap::from([
        ("a.b".to_string(), AttributeValue::S("top".to_string())),
        (
            "a".to_string(),
            AttributeValue::M(AttributeMap::from(HashMap::from([(
                "b".to_string(),
                AttributeValue::S("nested".to_string()),
            )]))),
        ),
        ("l[0]".to_string(), AttributeValue::S("literal".to_string())),
        (
            "l".to_string(),
            AttributeValue::L(vec![AttributeValue::S("element".to_string())]),
        ),
    ]))
}

fn punctuated_aliases() -> storage_types::VecMap<String, String> {
    storage_types::VecMap::from_iter([
        ("#a".to_string(), "a.b".to_string()),
        ("#l".to_string(), "l[0]".to_string()),
    ])
}

#[test]
fn given_aliases_naming_punctuated_attributes_when_parsing_update_then_paths_are_one_name() {
    let names = punctuated_aliases();
    let mut values = AttributeMap::new();
    values.insert(":v".to_string(), AttributeValue::S("set".to_string()));

    let operations =
        parse_update_expression("SET #a = :v, #l = :v", Some(&names), Some(&values)).unwrap();

    let paths = operations
        .iter()
        .map(|operation| operation.field_path().elements().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        [
            vec![PathElement::Name("a.b".to_string())],
            vec![PathElement::Name("l[0]".to_string())],
        ]
    );
}

#[test]
fn given_unaliased_path_with_alias_segment_when_parsing_update_then_segments_are_elements() {
    let names = storage_types::VecMap::from_iter([("#c".to_string(), "x.y".to_string())]);
    let operations = parse_update_expression("REMOVE a.b[2].#c", Some(&names), None).unwrap();

    assert_eq!(
        operations[0].field_path().elements(),
        [
            PathElement::Name("a".to_string()),
            PathElement::Name("b".to_string()),
            PathElement::Index(2),
            PathElement::Name("x.y".to_string()),
        ]
    );
}

#[test]
fn given_punctuated_aliases_when_set_applies_then_top_level_attributes_are_written() {
    let names = punctuated_aliases();
    let mut values = AttributeMap::new();
    values.insert(":v".to_string(), AttributeValue::S("set".to_string()));
    let operations =
        parse_update_expression("SET #a = :v, #l = :v", Some(&names), Some(&values)).unwrap();

    let updated = apply_update_operations(punctuated_name_item(), &operations).unwrap();

    let mut expected = punctuated_name_item();
    expected.insert("a.b".to_string(), AttributeValue::S("set".to_string()));
    expected.insert("l[0]".to_string(), AttributeValue::S("set".to_string()));
    assert_eq!(updated, expected);
}

#[test]
fn given_punctuated_aliases_when_remove_applies_then_only_top_level_attributes_are_removed() {
    let names = punctuated_aliases();
    let (operations, _) = before_update_item("REMOVE #a, #l", None, Some(&names), None).unwrap();

    let updated = apply_bound_update_operations(punctuated_name_item(), &operations).unwrap();

    let mut expected = punctuated_name_item();
    expected.remove("a.b");
    expected.remove("l[0]");
    assert_eq!(updated, expected);
}

#[test]
fn given_alias_naming_a_bracketed_attribute_when_reading_updated_old_then_only_it_is_returned() {
    let names = punctuated_aliases();
    let (operations, _) = before_update_item("REMOVE #l", None, Some(&names), None).unwrap();

    let attributes =
        super::response::updated_attribute_map_borrowed(&operations, &punctuated_name_item())
            .expect("the removed attribute existed");

    assert_eq!(
        attributes,
        AttributeMap::from(HashMap::from([(
            "l[0]".to_string(),
            AttributeValue::S("literal".to_string()),
        )]))
    );
}

#[test]
fn given_set_on_a_nested_member_when_reading_updated_new_then_response_nests_the_member() {
    let mut values = AttributeMap::new();
    values.insert(":v".to_string(), AttributeValue::S("deep".to_string()));
    let (operations, _) = before_update_item("SET a.b = :v", None, None, Some(&values)).unwrap();
    let updated = apply_bound_update_operations(punctuated_name_item(), &operations).unwrap();

    let attributes = super::response::updated_attribute_map(&operations, updated)
        .expect("the nested member was written");

    assert_eq!(
        attributes,
        AttributeMap::from(HashMap::from([(
            "a".to_string(),
            AttributeValue::M(AttributeMap::from(HashMap::from([(
                "b".to_string(),
                AttributeValue::S("deep".to_string()),
            )]))),
        )]))
    );
}

#[test]
fn given_set_members_at_a_nested_path_when_delete_applies_then_members_leave_the_nested_set() {
    let item = AttributeMap::from(HashMap::from([(
        "m".to_string(),
        AttributeValue::M(AttributeMap::from(HashMap::from([(
            "tags".to_string(),
            AttributeValue::SS(vec!["x".to_string(), "y".to_string()]),
        )]))),
    )]));
    let mut values = AttributeMap::new();
    values.insert(
        ":gone".to_string(),
        AttributeValue::SS(vec!["x".to_string()]),
    );
    let operations = parse_update_expression("DELETE m.tags :gone", None, Some(&values)).unwrap();

    let updated = apply_update_operations(item, &operations).unwrap();

    assert_eq!(
        updated.get("m"),
        Some(&AttributeValue::M(AttributeMap::from(HashMap::from([(
            "tags".to_string(),
            AttributeValue::SS(vec!["y".to_string()]),
        )]))))
    );
}

#[test]
fn given_set_delete_members_when_measuring_removal_then_one_pass_replaces_repeated_retains() {
    let existing = (0..64)
        .map(|index| format!("value-{index}"))
        .collect::<Vec<_>>();
    let deleted = (0..16)
        .map(|index| format!("value-{}", index * 2))
        .collect::<Vec<_>>();
    let baseline_passes = measure_repeated_set_removal(&existing, &deleted);
    let current_passes = measure_single_pass_set_removal(&existing, &deleted);

    assert_eq!(baseline_passes, deleted.len());
    assert_eq!(current_passes, 1);
}

fn measure_repeated_set_removal(existing: &[String], deleted: &[String]) -> usize {
    let mut values = existing.to_vec();
    for item_to_delete in deleted {
        values.retain(|item| item != item_to_delete);
    }
    std::hint::black_box(values);
    deleted.len()
}

fn measure_single_pass_set_removal(existing: &[String], deleted: &[String]) -> usize {
    let mut values = existing.to_vec();
    remove_set_members(&mut values, deleted);
    std::hint::black_box(values);
    1
}

#[test]
fn update_logic_allocation_profile_tests() {
    assert_return_values_do_not_need_old_when_applying_update_then_full_item_clone_is_skipped();
    assert_update_expression_binding_and_condition_cache_allocation_profile();
    assert_response_field_collection_allocation_profile();
    assert_arithmetic_bound_operand_construction_allocation_profile();
}

fn assert_return_values_do_not_need_old_when_applying_update_then_full_item_clone_is_skipped() {
    let item = clone_audit_item();
    let operations = clone_audit_operations();

    let baseline = measure_preserve_old_for_update_response(&item, &operations);
    let optimized = measure_preserve_old_only_if_needed(&item, &operations);

    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected conditional old-item preservation to allocate less often, baseline={} \
         optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected conditional old-item preservation to allocate fewer bytes, baseline={} \
         optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn measure_preserve_old_for_update_response(
    item: &HashMap<String, AttributeValue>,
    operations: &[UpdateOperation],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "update_apply_preserve_old_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..UPDATE_CLONE_AUDIT_ITERATIONS {
        let item_to_update = item.clone();
        let updated = apply_update_operations(item_to_update.clone().into(), operations).unwrap();
        std::hint::black_box((item_to_update.len(), updated.len()));
    }
    guard.finish()
}

fn measure_preserve_old_only_if_needed(
    item: &HashMap<String, AttributeValue>,
    operations: &[UpdateOperation],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "update_apply_conditional_old_optimized",
        file!(),
        line!(),
        Some("optimized"),
    );
    for _ in 0..UPDATE_CLONE_AUDIT_ITERATIONS {
        let item_to_update = item.clone();
        let old_for_response = return_values_need_old_item(Some(&ReturnValuesOldNewUpdated::None))
            .then(|| item_to_update.clone());
        let updated = apply_update_operations(item_to_update.into(), operations).unwrap();
        std::hint::black_box((old_for_response, updated.len()));
    }
    guard.finish()
}

fn assert_update_expression_binding_and_condition_cache_allocation_profile() {
    let names = storage_types::VecMap::from_iter([
        ("#payload".to_string(), "payload_0".to_string()),
        ("#counter".to_string(), "counter".to_string()),
        ("#status".to_string(), "status".to_string()),
        ("#tags".to_string(), "tags".to_string()),
        ("#notes".to_string(), "notes".to_string()),
    ]);
    let values = storage_types::AttributeMap::from_iter([
        (
            ":payload".to_string(),
            AttributeValue::S("updated".to_string()),
        ),
        (":inc".to_string(), AttributeValue::N("1".to_string())),
        (
            ":tag".to_string(),
            AttributeValue::SS(vec!["tag-new".to_string()]),
        ),
        (
            ":expected".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            ":note".to_string(),
            AttributeValue::L(vec![AttributeValue::S("note".to_string())]),
        ),
        (
            ":existing_notes".to_string(),
            AttributeValue::L(vec![AttributeValue::S("existing".to_string())]),
        ),
    ]);
    let update_expression = "SET #payload = :payload, #notes = list_append(:existing_notes, \
                             :note) ADD #counter :inc DELETE #tags :tag";
    let condition_expression = Some("#status = :expected AND attribute_exists(#payload)");

    let report = measure_update_expression_binding_and_condition_cache(
        update_expression,
        condition_expression,
        &names,
        &values,
    );
    let _ = alloc_counter::emit_report(&report);

    assert!(report.allocation_count > 0);
}

fn assert_response_field_collection_allocation_profile() {
    let operations = bound_response_field_operations();
    let legacy_default = measure_legacy_response_field_string_collection(&operations, false);
    let optimized_default = measure_optimized_response_field_collection(&operations, false);
    let legacy_updated = measure_legacy_response_field_string_collection(&operations, true);
    let optimized_updated = measure_optimized_response_field_collection(&operations, true);

    let _ = alloc_counter::emit_report(&legacy_default);
    let _ = alloc_counter::emit_report(&optimized_default);
    let _ = alloc_counter::emit_report(&legacy_updated);
    let _ = alloc_counter::emit_report(&optimized_updated);

    assert!(optimized_default.allocation_count < legacy_default.allocation_count);
    assert!(optimized_default.allocated_bytes < legacy_default.allocated_bytes);
    assert!(optimized_updated.allocation_count < legacy_updated.allocation_count);
    assert!(optimized_updated.allocated_bytes < legacy_updated.allocated_bytes);
}

fn assert_arithmetic_bound_operand_construction_allocation_profile() {
    let legacy = measure_legacy_arithmetic_bound_operand_construction();
    let optimized = measure_optimized_arithmetic_bound_operand_construction();

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes <= legacy.allocated_bytes);
}

fn measure_legacy_arithmetic_bound_operand_construction() -> alloc_counter::AllocationReport<'static>
{
    let guard = AllocationGuard::start(
        module_path!(),
        "arithmetic_bound_operand_construction_legacy",
        file!(),
        line!(),
        Some("boxed_nested_operands"),
    );
    for _ in 0..UPDATE_EXPRESSION_AUDIT_ITERATIONS {
        let operation = BoundUpdateOperation::SetExpression {
            field: Arc::new(attr_path("counter")),
            value: BoundUpdateOperand::Arithmetic {
                lhs: Box::new(BoundUpdateOperand::Path(Arc::new(attr_path("counter")))),
                operator: crate::update_logic::ArithmeticOperator::Add,
                rhs: Box::new(BoundUpdateOperand::Path(Arc::new(attr_path("increment")))),
            },
        };
        std::hint::black_box(operation);
    }
    guard.finish()
}

fn measure_optimized_arithmetic_bound_operand_construction()
-> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "arithmetic_bound_operand_construction_optimized",
        file!(),
        line!(),
        Some("single_operand_pair"),
    );
    for _ in 0..UPDATE_EXPRESSION_AUDIT_ITERATIONS {
        let operation = BoundUpdateOperation::SetArithmetic {
            field: Arc::new(attr_path("counter")),
            lhs: BoundUpdateOperand::Path(Arc::new(attr_path("counter"))),
            rhs: BoundUpdateOperand::Path(Arc::new(attr_path("increment"))),
            operator: crate::update_logic::ArithmeticOperator::Add,
        };
        std::hint::black_box(operation);
    }
    guard.finish()
}

fn bound_response_field_operations() -> Vec<BoundUpdateOperation<'static>> {
    vec![
        BoundUpdateOperation::Set {
            field: Arc::new(attr_path("payload_0")),
            value: Cow::Owned(AttributeValue::S("updated".to_string())),
        },
        BoundUpdateOperation::Add {
            field: Arc::new(attr_path("counter")),
            value: Cow::Owned(AttributeValue::N("1".to_string())),
        },
        BoundUpdateOperation::Remove {
            field: Arc::new(attr_path("old_payload")),
        },
    ]
}

fn measure_legacy_response_field_string_collection(
    operations: &[BoundUpdateOperation<'_>],
    updated_fields_needed: bool,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "update_response_field_string_collection_legacy",
        file!(),
        line!(),
        Some(if updated_fields_needed {
            "updated_fields"
        } else {
            "default"
        }),
    );
    for _ in 0..UPDATE_EXPRESSION_AUDIT_ITERATIONS {
        let response_fields = operations
            .iter()
            .map(|operation| operation.field_path().to_string())
            .collect::<Vec<_>>();
        std::hint::black_box((updated_fields_needed, response_fields));
    }
    guard.finish()
}

fn measure_optimized_response_field_collection(
    operations: &[BoundUpdateOperation<'_>],
    updated_fields_needed: bool,
) -> alloc_counter::AllocationReport<'static> {
    let return_values = updated_fields_needed.then_some(ReturnValuesOldNewUpdated::UpdatedNew);
    let guard = AllocationGuard::start(
        module_path!(),
        "update_response_field_collection_optimized",
        file!(),
        line!(),
        Some(if updated_fields_needed {
            "updated_fields"
        } else {
            "default"
        }),
    );
    for _ in 0..UPDATE_EXPRESSION_AUDIT_ITERATIONS {
        let response_fields = if return_values_need_updated_fields(return_values.as_ref()) {
            {
                operations
                    .iter()
                    .map(|operation| operation.field_path_arc())
                    .collect::<Vec<_>>()
            }
        } else {
            Default::default()
        };
        std::hint::black_box(response_fields);
    }
    guard.finish()
}

fn measure_update_expression_binding_and_condition_cache(
    update_expression: &str,
    condition_expression: Option<&str>,
    names: &storage_types::VecMap<String, String>,
    values: &storage_types::AttributeMap,
) -> alloc_counter::AllocationReport<'static> {
    let _ = before_update_item(
        update_expression,
        condition_expression,
        Some(names),
        Some(values),
    )
    .unwrap();

    let guard = AllocationGuard::start(
        module_path!(),
        "update_expression_binding_condition_cache_hit",
        file!(),
        line!(),
        Some("cache_hit"),
    );
    for _ in 0..UPDATE_EXPRESSION_AUDIT_ITERATIONS {
        let (operations, condition) = before_update_item(
            update_expression,
            condition_expression,
            Some(names),
            Some(values),
        )
        .unwrap();
        std::hint::black_box((operations.len(), condition.is_some()));
    }
    guard.finish()
}

fn clone_audit_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk".to_string()));
    for index in 0..32 {
        item.insert(
            format!("payload_{index}"),
            AttributeValue::S(format!("payload-{index}-{}", "x".repeat(128))),
        );
    }
    item
}

fn clone_audit_operations() -> Vec<UpdateOperation> {
    vec![
        UpdateOperation::Set {
            field: Arc::new(attr_path("payload_0")),
            value: AttributeValue::S("updated".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::new(attr_path("counter")),
            value: AttributeValue::N("1".to_string()),
        },
    ]
}

#[test]
fn parse_set_arithmetic_in_first_clause() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":zero".to_string(), AttributeValue::N("0".to_string()));
    values.insert(":inc".to_string(), AttributeValue::N("1".to_string()));

    let operations = parse_update_expression(
        "SET version = if_not_exists(version, :zero) + :inc, name = \"alice\"",
        None,
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 2);
    match &operations[0] {
        UpdateOperation::SetExpression { field, .. } => {
            assert_eq!(field.as_ref(), "version");
        }
        _ => panic!("Expected SET expression operation for version increment"),
    }
    match &operations[1] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "name");
            assert_eq!(value, &AttributeValue::S("alice".to_string()));
        }
        _ => panic!("Expected SET operation for name"),
    }
}

// ===== PARSING TESTS =====

#[test]
fn parse_invalid_set_operation_missing_equals() {
    let result = parse_update_expression("SET field :val", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid SET operation")
    );
}

#[test]
fn parse_invalid_add_operation_missing_value() {
    let result = parse_update_expression("ADD field", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid ADD operation")
    );
}

#[test]
fn parse_unknown_operation() {
    let result = parse_update_expression("UNKNOWN field = :val", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Unknown update operation")
    );
}

#[test]
fn parse_attribute_name_not_found() {
    let mut names = storage_types::VecMap::default();
    names.insert("#name".to_string(), "actual_name".to_string());

    let result = parse_update_expression("SET #missing = :val", Some(&names), None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("not found in ExpressionAttributeNames")
    );
}

#[test]
fn parse_attribute_value_not_found() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val".to_string(), AttributeValue::S("test".to_string()));

    let result = parse_update_expression("SET field = :missing", None, Some(&values));
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("not found in ExpressionAttributeValues")
    );
}

#[test]
fn parse_attribute_name_requires_expression_attribute_names() {
    let result = parse_update_expression("SET #field = :val", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("requires ExpressionAttributeNames")
    );
}

#[test]
fn parse_attribute_value_requires_expression_attribute_values() {
    let result = parse_update_expression("SET field = :val", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("requires ExpressionAttributeValues")
    );
}

#[test]
fn parse_invalid_literal_value() {
    let result = parse_update_expression("SET field = {\"invalid\": \"json\"", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid value expression")
    );
}

// ===== SET OPERATION TESTS =====

// ===== ADD OPERATION TESTS =====

#[test]
fn parse_multiple_add_operations() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":num".to_string(), AttributeValue::N("10".to_string()));
    values.insert(":str".to_string(), AttributeValue::S("item".to_string()));

    let operations =
        parse_update_expression("ADD counter :num, list :str", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 2);
    match &operations[0] {
        UpdateOperation::Add { field, value } => {
            assert_eq!(field.as_ref(), "counter");
            assert_eq!(value, &AttributeValue::N("10".to_string()));
        }
        _ => panic!("Expected ADD operation"),
    }
    match &operations[1] {
        UpdateOperation::Add { field, value } => {
            assert_eq!(field.as_ref(), "list");
            assert_eq!(value, &AttributeValue::S("item".to_string()));
        }
        _ => panic!("Expected ADD operation"),
    }
}

#[test]
fn apply_add_rejects_invalid_number() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::new(attr_path("score")),
        value: AttributeValue::N("NaN".to_string()),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("ADD operation requires a valid number")
    );
}

#[test]
fn apply_add_rejects_list_attribute() {
    let mut item = HashMap::new();
    item.insert(
        "tags".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("rust".to_string()),
            AttributeValue::S("programming".to_string()),
        ]),
    );

    let operations = vec![UpdateOperation::Add {
        field: Arc::new(attr_path("tags")),
        value: AttributeValue::S("async".to_string()),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("ADD operation requires a number or set operand")
    );
}

#[test]
fn apply_add_rejects_empty_set_operand() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::new(attr_path("categories")),
        value: AttributeValue::SS(vec![]),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("ADD operation requires a non-empty set operand")
    );
}

// ===== REMOVE OPERATION TESTS =====

// ===== DELETE OPERATION TESTS =====

// ===== MIXED OPERATIONS TESTS =====

// ===== COMPLEX EXPRESSION TESTS =====

// ===== EDGE CASE TESTS =====

#[test]
fn parse_invalid_delete_operation_missing_value() {
    let result = parse_update_expression("DELETE field", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid DELETE operation")
    );
}

// ===== SET FUNCTION TESTS =====

#[test]
fn parse_list_append_function() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(
        ":list1".to_string(),
        AttributeValue::L(vec![AttributeValue::S("a".to_string())]),
    );
    values.insert(
        ":list2".to_string(),
        AttributeValue::L(vec![AttributeValue::S("b".to_string())]),
    );

    let operations = parse_update_expression(
        "SET combined = list_append(:list1, :list2)",
        None,
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::SetListAppend {
            field,
            operand1,
            operand2,
        } => {
            assert_eq!(field.as_ref(), "combined");
            assert_eq!(
                operand1,
                &AttributeValue::L(vec![AttributeValue::S("a".to_string())])
            );
            assert_eq!(
                operand2,
                &AttributeValue::L(vec![AttributeValue::S("b".to_string())])
            );
        }
        _ => panic!("Expected SetListAppend operation"),
    }
}

#[test]
fn apply_list_append_with_lists() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::SetListAppend {
        field: Arc::new(attr_path("result")),
        operand1: AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
        ]),
        operand2: AttributeValue::L(vec![
            AttributeValue::S("c".to_string()),
            AttributeValue::S("d".to_string()),
        ]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("result").unwrap() {
        AttributeValue::L(list) => {
            assert_eq!(list.len(), 4);
            assert_eq!(list[0], AttributeValue::S("a".to_string()));
            assert_eq!(list[1], AttributeValue::S("b".to_string()));
            assert_eq!(list[2], AttributeValue::S("c".to_string()));
            assert_eq!(list[3], AttributeValue::S("d".to_string()));
        }
        _ => panic!("Expected list"),
    }
}

#[test]
fn apply_list_append_with_single_values() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::SetListAppend {
        field: Arc::new(attr_path("result")),
        operand1: AttributeValue::S("single".to_string()),
        operand2: AttributeValue::S("value".to_string()),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("An operand in the update expression has an incorrect data type")
    );
}

#[test]
fn apply_set_arithmetic_matches_dynamodb_decimal_number_semantics() {
    let cases = [
        ("0.1", "SET n = n + :v", "0.2", "0.3"),
        (".1", "SET n = n + :v", ".2", "0.3"),
        ("0.3", "SET n = n - :v", "0.1", "0.2"),
        (
            "0.0000000000000000000000000000000000001",
            "SET n = n + :v",
            "0.0000000000000000000000000000000000001",
            "0.0000000000000000000000000000000000002",
        ),
        (
            "99999999999999999999999999999999999998",
            "SET n = n + :v",
            "1",
            "99999999999999999999999999999999999999",
        ),
        (
            "99999999999999999999999999999999999999",
            "SET n = n + :v",
            "1",
            "100000000000000000000000000000000000000",
        ),
        (
            "1E+37",
            "SET n = n + :v",
            "1E+37",
            "20000000000000000000000000000000000000",
        ),
        (
            "1E-37",
            "SET n = n - :v",
            "1E-38",
            "0.00000000000000000000000000000000000009",
        ),
    ];

    for (initial, expression, operand, expected) in cases {
        let mut item = HashMap::new();
        item.insert("n".to_string(), AttributeValue::N(initial.to_string()));

        let mut values = storage_types::AttributeMap::new();
        values.insert(":v".to_string(), AttributeValue::N(operand.to_string()));

        let operations = parse_update_expression(expression, None, Some(&values)).unwrap();
        let updated = apply_update_operations(item.into(), &operations).unwrap();

        assert_eq!(
            updated.get("n").unwrap(),
            &AttributeValue::N(expected.to_string()),
            "{initial} via {expression} {operand}"
        );
    }
}

#[test]
fn apply_set_arithmetic_rejects_dynamodb_number_overflow_and_underflow() {
    let cases = [
        (
            "9.9999999999999999999999999999999999999E+125",
            "1E+125",
            "Number overflow. Attempting to store a number with magnitude larger than supported \
             range",
        ),
        (
            "1E-130",
            "-0.5E-130",
            "Number underflow. Attempting to store a number with magnitude smaller than supported \
             range",
        ),
    ];

    for (initial, operand, expected) in cases {
        let mut item = HashMap::new();
        item.insert("n".to_string(), AttributeValue::N(initial.to_string()));

        let mut values = storage_types::AttributeMap::new();
        values.insert(":v".to_string(), AttributeValue::N(operand.to_string()));

        let operations = parse_update_expression("SET n = n + :v", None, Some(&values)).unwrap();
        let error = apply_update_operations(item.into(), &operations).unwrap_err();

        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn parse_invalid_if_not_exists_wrong_arg_count() {
    let result = parse_update_expression("SET field = if_not_exists(path)", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("if_not_exists requires exactly 2 arguments")
    );
}

#[test]
fn parse_invalid_list_append_wrong_arg_count() {
    let result = parse_update_expression("SET field = list_append(list1)", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("list_append requires exactly 2 arguments")
    );
}

use super::value::{
    DynamoDecimal, list_append_owned_values, list_append_values,
    value_tests::add_with_aligned_scratch,
};

#[test]
fn owned_list_append_moves_nested_values_instead_of_cloning_result_items() {
    let left = nested_list(32, "left");
    let right = nested_list(32, "right");

    let baseline = measure_list_append_baseline(&left, &right);
    let current = measure_list_append_current(&left, &right);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);
    assert!(
        current.allocation_count < baseline.allocation_count,
        "expected owned list append to allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "expected owned list append to allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );

    let expected = list_append_values(&left, &right).unwrap();
    let actual = list_append_owned_values(left.clone(), right.clone()).unwrap();
    assert_eq!(actual, expected);
}

fn nested_list(count: usize, prefix: &str) -> AttributeValue {
    AttributeValue::L(
        (0..count)
            .map(|index| {
                AttributeValue::M(AttributeMap::from(std::collections::HashMap::from([(
                    "value".to_string(),
                    AttributeValue::S(format!("{prefix}-{index}-{}", "x".repeat(64))),
                )])))
            })
            .collect(),
    )
}

fn measure_list_append_baseline(
    left: &AttributeValue,
    right: &AttributeValue,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "list_append_result_clone_baseline",
        file!(),
        line!(),
        Some("borrowed_operands"),
    );
    for _ in 0..64 {
        let left = left.clone();
        let right = right.clone();
        std::hint::black_box(list_append_values(&left, &right).unwrap());
    }
    guard.finish()
}

fn measure_list_append_current(
    left: &AttributeValue,
    right: &AttributeValue,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "list_append_owned_result_move_optimized",
        file!(),
        line!(),
        Some("owned_operands"),
    );
    for _ in 0..64 {
        let left = left.clone();
        let right = right.clone();
        std::hint::black_box(list_append_owned_values(left, right).unwrap());
    }
    guard.finish()
}

#[test]
fn decimal_alignment_fixture_preserves_output_with_fewer_scratch_allocations() {
    let lhs = DynamoDecimal::parse("12345678901234567890.123456").unwrap();
    let rhs = DynamoDecimal::parse("9876543210.987654").unwrap();
    let baseline = measure_decimal_add_baseline(&lhs, &rhs);
    let current = measure_decimal_add_current(&lhs, &rhs);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);

    assert!(
        current.allocation_count < baseline.allocation_count,
        "expected virtual decimal alignment to allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "expected virtual decimal alignment to allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );

    let expected = add_with_aligned_scratch(&lhs, &rhs)
        .unwrap()
        .format()
        .unwrap();
    let actual = lhs.add(&rhs).unwrap().format().unwrap();
    assert_eq!(actual, expected);
}

fn measure_decimal_add_baseline(
    lhs: &DynamoDecimal,
    rhs: &DynamoDecimal,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "decimal_add_aligned_scratch_baseline",
        file!(),
        line!(),
        Some("aligned_digit_vectors"),
    );
    for _ in 0..256 {
        std::hint::black_box(add_with_aligned_scratch(lhs, rhs).unwrap());
    }
    guard.finish()
}

fn measure_decimal_add_current(
    lhs: &DynamoDecimal,
    rhs: &DynamoDecimal,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "decimal_add_virtual_alignment_optimized",
        file!(),
        line!(),
        Some("virtual_padding"),
    );
    for _ in 0..256 {
        std::hint::black_box(lhs.add(rhs).unwrap());
    }
    guard.finish()
}
