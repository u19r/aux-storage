use std::{borrow::Cow, collections::HashMap, sync::Arc};

use alloc_counter::AllocationGuard;
use storage_types::{AttributeMap, AttributeValue, ReturnValuesOldNewUpdated};

use super::logic::remove_set_members;
use crate::{
    constants_tests::{UPDATE_CLONE_AUDIT_ITERATIONS, UPDATE_EXPRESSION_AUDIT_ITERATIONS},
    update_logic::{
        BoundUpdateOperation, UpdateOperation, apply_bound_update_operations,
        apply_update_operations, before_update_item, parse_update_expression,
        return_values_need_old_item, return_values_need_updated_fields, update_item_response,
        updated_attributes_for_response, value::BoundUpdateOperand,
    },
};

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

#[test]
fn updated_response_preserves_top_level_values_when_extracting_from_owned_item() {
    let old_item = HashMap::from([
        (
            "payload".to_string(),
            AttributeValue::M(HashMap::from([
                ("nested".to_string(), AttributeValue::S("old".to_string())),
                (
                    "large".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("a".to_string()),
                        AttributeValue::S("b".to_string()),
                    ]),
                ),
            ])),
        ),
        (
            "unchanged".to_string(),
            AttributeValue::S("keep".to_string()),
        ),
    ]);
    let operations = vec![UpdateOperation::Set {
        field: Arc::from("payload"),
        value: AttributeValue::S("new".to_string()),
    }];

    let response = update_item_response(
        &operations,
        Some(old_item),
        None,
        Some(&ReturnValuesOldNewUpdated::UpdatedOld),
    )
    .unwrap();

    assert_eq!(
        response.attributes.map(HashMap::from),
        Some(HashMap::from([(
            "payload".to_string(),
            AttributeValue::M(HashMap::from([
                ("nested".to_string(), AttributeValue::S("old".to_string())),
                (
                    "large".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("a".to_string()),
                        AttributeValue::S("b".to_string()),
                    ])
                ),
            ])),
        )]))
    );
}

#[test]
fn updated_response_keeps_nested_projection_shape_for_owned_item() {
    let old_item = HashMap::from([(
        "payload".to_string(),
        AttributeValue::M(HashMap::from([
            ("first".to_string(), AttributeValue::S("a".to_string())),
            ("second".to_string(), AttributeValue::S("b".to_string())),
        ])),
    )]);
    let operations = vec![UpdateOperation::Set {
        field: Arc::from("payload.first"),
        value: AttributeValue::S("new".to_string()),
    }];

    let response = update_item_response(
        &operations,
        Some(old_item),
        None,
        Some(&ReturnValuesOldNewUpdated::UpdatedOld),
    )
    .unwrap();

    assert_eq!(
        response.attributes.map(HashMap::from),
        Some(HashMap::from([(
            "payload".to_string(),
            AttributeValue::M(HashMap::from([(
                "first".to_string(),
                AttributeValue::S("a".to_string()),
            )])),
        )]))
    );
}

#[test]
fn updated_response_omits_missing_selected_owned_field() {
    let old_item = HashMap::from([(
        "other".to_string(),
        AttributeValue::S("unchanged".to_string()),
    )]);
    let operations = vec![UpdateOperation::Set {
        field: Arc::from("missing"),
        value: AttributeValue::S("new".to_string()),
    }];

    let response = update_item_response(
        &operations,
        Some(old_item),
        None,
        Some(&ReturnValuesOldNewUpdated::UpdatedOld),
    )
    .unwrap();

    assert!(response.attributes.is_none());
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
    assert_updated_old_response_when_applying_update_then_only_projected_old_fields_are_cloned();
    assert_owned_updated_response_extraction_allocation_profile();
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

fn assert_updated_old_response_when_applying_update_then_only_projected_old_fields_are_cloned() {
    let item = clone_audit_item();
    let operations = clone_audit_operations();

    let baseline = measure_updated_old_full_item_clone(&item, &operations);
    let optimized = measure_updated_old_projected_fields(&item, &operations);

    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected updated-old projection to allocate less often, baseline={} optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected updated-old projection to allocate fewer bytes, baseline={} optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn assert_owned_updated_response_extraction_allocation_profile() {
    let item = clone_audit_item();
    let operations = clone_audit_operations();
    let baseline_items = (0..UPDATE_CLONE_AUDIT_ITERATIONS)
        .map(|_| item.clone())
        .collect::<Vec<_>>();
    let optimized_items = (0..UPDATE_CLONE_AUDIT_ITERATIONS)
        .map(|_| item.clone())
        .collect::<Vec<_>>();

    let baseline = measure_borrowed_updated_response_extraction(&baseline_items, &operations);
    let optimized = measure_owned_updated_response_extraction(optimized_items, &operations);

    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < baseline.allocation_count,
        "expected consuming updated response extraction to allocate less often, baseline={} \
         optimized={}",
        baseline.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < baseline.allocated_bytes,
        "expected consuming updated response extraction to allocate fewer bytes, baseline={} \
         optimized={}",
        baseline.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn measure_borrowed_updated_response_extraction(
    items: &[HashMap<String, AttributeValue>],
    operations: &[UpdateOperation],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "updated_response_borrowed_extraction_baseline",
        file!(),
        line!(),
        Some("borrowed_values"),
    );
    for item in items {
        let attributes = updated_attributes_for_response(operations, item);
        let attributes: AttributeMap = attributes.into();
        std::hint::black_box(attributes);
    }
    guard.finish()
}

fn measure_owned_updated_response_extraction(
    items: Vec<HashMap<String, AttributeValue>>,
    operations: &[UpdateOperation],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "updated_response_owned_extraction_optimized",
        file!(),
        line!(),
        Some("moved_values"),
    );
    for old_item in items {
        let response = update_item_response(
            operations,
            Some(old_item),
            None,
            Some(&ReturnValuesOldNewUpdated::UpdatedOld),
        )
        .unwrap();
        std::hint::black_box(response.attributes);
    }
    guard.finish()
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

fn measure_updated_old_full_item_clone(
    item: &HashMap<String, AttributeValue>,
    operations: &[UpdateOperation],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "update_apply_updated_old_full_item_clone_baseline",
        file!(),
        line!(),
        Some("updated_old_full_clone"),
    );
    for _ in 0..UPDATE_CLONE_AUDIT_ITERATIONS {
        let item_to_update = item.clone();
        let old_for_response = Some(item_to_update.clone());
        let updated = apply_update_operations(item_to_update.into(), operations).unwrap();
        let response = update_item_response(
            operations,
            old_for_response,
            Some(updated.into_hashmap()),
            Some(&ReturnValuesOldNewUpdated::UpdatedOld),
        )
        .unwrap();
        std::hint::black_box(response.attributes);
    }
    guard.finish()
}

fn measure_updated_old_projected_fields(
    item: &HashMap<String, AttributeValue>,
    operations: &[UpdateOperation],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "update_apply_updated_old_projected_fields",
        file!(),
        line!(),
        Some("updated_old_projected"),
    );
    for _ in 0..UPDATE_CLONE_AUDIT_ITERATIONS {
        let item_to_update = item.clone();
        let attributes = updated_attributes_for_response(operations, &item_to_update);
        let old_for_response = (!attributes.is_empty()).then_some(attributes);
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
            field: Arc::from("counter"),
            value: BoundUpdateOperand::Arithmetic {
                lhs: Box::new(BoundUpdateOperand::Path(Arc::from("counter"))),
                operator: crate::update_logic::ArithmeticOperator::Add,
                rhs: Box::new(BoundUpdateOperand::Path(Arc::from("increment"))),
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
            field: Arc::from("counter"),
            lhs: BoundUpdateOperand::Path(Arc::from("counter")),
            rhs: BoundUpdateOperand::Path(Arc::from("increment")),
            operator: crate::update_logic::ArithmeticOperator::Add,
        };
        std::hint::black_box(operation);
    }
    guard.finish()
}

fn bound_response_field_operations() -> Vec<BoundUpdateOperation<'static>> {
    vec![
        BoundUpdateOperation::Set {
            field: Arc::from("payload_0"),
            value: Cow::Owned(AttributeValue::S("updated".to_string())),
        },
        BoundUpdateOperation::Add {
            field: Arc::from("counter"),
            value: Cow::Owned(AttributeValue::N("1".to_string())),
        },
        BoundUpdateOperation::Remove {
            field: Arc::from("old_payload"),
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
            .map(|operation| operation.field_name().to_string())
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
                    .map(|operation| operation.field_name_arc())
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
            field: Arc::from("payload_0"),
            value: AttributeValue::S("updated".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::from("counter"),
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

#[test]
fn parse_set_arithmetic_in_continuation_clause() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":zero".to_string(), AttributeValue::N("0".to_string()));
    values.insert(":inc".to_string(), AttributeValue::N("2".to_string()));
    values.insert(":name".to_string(), AttributeValue::S("jane".to_string()));

    let operations = parse_update_expression(
        "SET name = :name, version = if_not_exists(version, :zero) + :inc",
        None,
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 2);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "name");
            assert_eq!(value, &AttributeValue::S("jane".to_string()));
        }
        _ => panic!("Expected SET operation for name"),
    }
    match &operations[1] {
        UpdateOperation::SetExpression { field, .. } => {
            assert_eq!(field.as_ref(), "version");
        }
        _ => panic!("Expected SET expression operation for version increment"),
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

#[test]
fn parse_multiple_set_operations() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val1".to_string(), AttributeValue::S("value1".to_string()));
    values.insert(":val2".to_string(), AttributeValue::N("42".to_string()));

    let operations =
        parse_update_expression("SET field1 = :val1, field2 = :val2", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 2);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field1");
            assert_eq!(value, &AttributeValue::S("value1".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
    match &operations[1] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field2");
            assert_eq!(value, &AttributeValue::N("42".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

#[test]
fn parse_set_with_attribute_names() {
    let mut names = storage_types::VecMap::default();
    names.insert("#f".to_string(), "field".to_string());

    let mut values = storage_types::AttributeMap::new();
    values.insert(":v".to_string(), AttributeValue::S("value".to_string()));

    let operations = parse_update_expression("SET #f = :v", Some(&names), Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field");
            assert_eq!(value, &AttributeValue::S("value".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

#[test]
fn parse_set_without_spaces_around_equals() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val".to_string(), AttributeValue::S("value".to_string()));

    let operations = parse_update_expression("SET field=:val", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field");
            assert_eq!(value, &AttributeValue::S("value".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

#[test]
fn parse_set_with_attribute_names_without_spaces_around_equals() {
    let mut names = storage_types::VecMap::default();
    names.insert("#f".to_string(), "field".to_string());

    let mut values = storage_types::AttributeMap::new();
    values.insert(":v".to_string(), AttributeValue::S("value".to_string()));

    let operations = parse_update_expression("SET #f=:v", Some(&names), Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field");
            assert_eq!(value, &AttributeValue::S("value".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

#[test]
fn parse_set_continuation_without_spaces_around_equals() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val1".to_string(), AttributeValue::S("value1".to_string()));
    values.insert(":val2".to_string(), AttributeValue::S("value2".to_string()));

    let operations =
        parse_update_expression("SET field1=:val1, field2=:val2", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 2);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field1");
            assert_eq!(value, &AttributeValue::S("value1".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
    match &operations[1] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field2");
            assert_eq!(value, &AttributeValue::S("value2".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

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
fn apply_add_to_existing_number() {
    let mut item = HashMap::new();
    item.insert("score".to_string(), AttributeValue::N("100".to_string()));

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("score"),
        value: AttributeValue::N("25".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("score").unwrap(),
        &AttributeValue::N("125".to_string())
    );
}

#[test]
fn apply_add_to_nonexistent_number_initializes_to_zero() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("score"),
        value: AttributeValue::N("50".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("score").unwrap(),
        &AttributeValue::N("50".to_string())
    );
}

#[test]
fn given_update_when_plain_item_then_timestamp_metadata_is_not_injected_or_changed() {
    let item = HashMap::from([("payload".to_string(), AttributeValue::S("old".to_string()))]);
    let operations = vec![UpdateOperation::Set {
        field: Arc::from("payload"),
        value: AttributeValue::S("new".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert!(!updated.contains_key(storage_types::single_table_entity::UPDATED_AT_ATTR));
    assert!(!updated.contains_key(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR));

    let item = HashMap::from([
        ("payload".to_string(), AttributeValue::S("old".to_string())),
        (
            storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("1".to_string()),
        ),
    ]);

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR),
        Some(&AttributeValue::N("1".to_string()))
    );
}

#[test]
fn given_existing_timestamp_when_unrelated_bound_update_applied_then_timestamp_is_unchanged() {
    let item = HashMap::from([
        ("payload".to_string(), AttributeValue::S("old".to_string())),
        (
            storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("100".to_string()),
        ),
        (
            storage_types::single_table_entity::UPDATED_AT_ATTR.to_string(),
            AttributeValue::N("200".to_string()),
        ),
        (
            storage_types::single_table_entity::CREATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("300".to_string()),
        ),
        (
            storage_types::single_table_entity::CREATED_AT_ATTR.to_string(),
            AttributeValue::N("400".to_string()),
        ),
    ]);
    let operations = vec![BoundUpdateOperation::Set {
        field: Arc::from("payload"),
        value: Cow::Owned(AttributeValue::S("new".to_string())),
    }];

    let updated = apply_bound_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR),
        Some(&AttributeValue::N("100".to_string()))
    );
    assert_eq!(
        updated.get(storage_types::single_table_entity::UPDATED_AT_ATTR),
        Some(&AttributeValue::N("200".to_string()))
    );
    assert_eq!(
        updated.get(storage_types::single_table_entity::CREATED_AT_ALIAS_ATTR),
        Some(&AttributeValue::N("300".to_string()))
    );
    assert_eq!(
        updated.get(storage_types::single_table_entity::CREATED_AT_ATTR),
        Some(&AttributeValue::N("400".to_string()))
    );
}

#[test]
fn update_respects_explicit_updated_at_assignment() {
    let item = HashMap::from([
        ("payload".to_string(), AttributeValue::S("old".to_string())),
        (
            storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
            AttributeValue::N("1".to_string()),
        ),
    ]);
    let operations = vec![UpdateOperation::Set {
        field: Arc::from(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR),
        value: AttributeValue::N("7".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR),
        Some(&AttributeValue::N("7".to_string()))
    );
}

#[test]
fn apply_add_rejects_invalid_number() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("score"),
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
        field: Arc::from("tags"),
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
fn apply_add_rejects_scalar_operand_for_missing_attribute() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("tags"),
        value: AttributeValue::S("new_tag".to_string()),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("ADD operation requires a number or set operand")
    );
}

#[test]
fn apply_add_to_string_set() {
    let mut item = HashMap::new();
    item.insert(
        "categories".to_string(),
        AttributeValue::SS(vec!["tech".to_string(), "programming".to_string()]),
    );

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("categories"),
        value: AttributeValue::SS(vec!["programming".to_string(), "rust".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("categories").unwrap() {
        AttributeValue::SS(set) => {
            assert_eq!(set.len(), 3);
            assert!(set.contains(&"tech".to_string()));
            assert!(set.contains(&"programming".to_string()));
            assert!(set.contains(&"rust".to_string()));
        }
        _ => panic!("Expected string set"),
    }
}

#[test]
fn apply_add_to_missing_string_set_initializes_to_operand() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("categories"),
        value: AttributeValue::SS(vec!["tech".to_string(), "rust".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("categories").unwrap(),
        &AttributeValue::SS(vec!["tech".to_string(), "rust".to_string()])
    );
}

#[test]
fn apply_add_to_number_set() {
    let mut item = HashMap::new();
    item.insert(
        "scores".to_string(),
        AttributeValue::NS(vec!["85".to_string(), "90".to_string()]),
    );

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("scores"),
        value: AttributeValue::NS(vec!["95".to_string(), "100".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("scores").unwrap() {
        AttributeValue::NS(set) => {
            assert_eq!(set.len(), 4);
            assert!(set.contains(&"85".to_string()));
            assert!(set.contains(&"90".to_string()));
            assert!(set.contains(&"95".to_string()));
            assert!(set.contains(&"100".to_string()));
        }
        _ => panic!("Expected number set"),
    }
}

#[test]
fn apply_add_to_missing_number_set_initializes_to_operand() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("scores"),
        value: AttributeValue::NS(vec!["95".to_string(), "100".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("scores").unwrap(),
        &AttributeValue::NS(vec!["95".to_string(), "100".to_string()])
    );
}

#[test]
fn apply_add_to_binary_set() {
    let mut item = HashMap::new();
    item.insert(
        "data".to_string(),
        AttributeValue::BS(vec![
            "AQID".to_string(), // base64 for [1, 2, 3]
            "BAUG".to_string(), // base64 for [4, 5, 6]
        ]),
    );

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("data"),
        value: AttributeValue::BS(vec![
            "BwgJ".to_string(), // base64 for [7, 8, 9]
        ]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("data").unwrap() {
        AttributeValue::BS(set) => {
            assert_eq!(set.len(), 3);
            assert!(set.contains(&"AQID".to_string()));
            assert!(set.contains(&"BAUG".to_string()));
            assert!(set.contains(&"BwgJ".to_string()));
        }
        _ => panic!("Expected binary set"),
    }
}

#[test]
fn apply_add_to_missing_binary_set_initializes_to_operand() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("data"),
        value: AttributeValue::BS(vec!["BwgJ".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("data").unwrap(),
        &AttributeValue::BS(vec!["BwgJ".to_string()])
    );
}

#[test]
fn apply_add_rejects_empty_set_operand() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("categories"),
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

#[test]
fn parse_multiple_remove_operations() {
    let operations = parse_update_expression("REMOVE field1, field2, field3", None, None).unwrap();

    assert_eq!(operations.len(), 3);
    match &operations[0] {
        UpdateOperation::Remove { field } => assert_eq!(field.as_ref(), "field1"),
        _ => panic!("Expected REMOVE operation"),
    }
    match &operations[1] {
        UpdateOperation::Remove { field } => assert_eq!(field.as_ref(), "field2"),
        _ => panic!("Expected REMOVE operation"),
    }
    match &operations[2] {
        UpdateOperation::Remove { field } => assert_eq!(field.as_ref(), "field3"),
        _ => panic!("Expected REMOVE operation"),
    }
}

#[test]
fn apply_remove_existing_field() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), AttributeValue::S("John".to_string()));
    item.insert("age".to_string(), AttributeValue::N("30".to_string()));
    item.insert("city".to_string(), AttributeValue::S("NYC".to_string()));

    let operations = vec![UpdateOperation::Remove {
        field: Arc::from("age"),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert!(updated.contains_key("name"));
    assert!(!updated.contains_key("age"));
    assert!(updated.contains_key("city"));
}

#[test]
fn apply_remove_nonexistent_field() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), AttributeValue::S("John".to_string()));

    let operations = vec![UpdateOperation::Remove {
        field: Arc::from("nonexistent"),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert!(updated.contains_key("name"));
    assert!(!updated.contains_key("nonexistent"));
}

// ===== DELETE OPERATION TESTS =====

#[test]
fn parse_delete_operation() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(
        ":val".to_string(),
        AttributeValue::SS(vec!["remove_me".to_string()]),
    );

    let operations = parse_update_expression("DELETE tags :val", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Delete { field, value } => {
            assert_eq!(field.as_ref(), "tags");
            assert_eq!(value, &AttributeValue::SS(vec!["remove_me".to_string()]));
        }
        _ => panic!("Expected DELETE operation"),
    }
}

#[test]
fn apply_delete_from_string_set() {
    let mut item = HashMap::new();
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec![
            "rust".to_string(),
            "programming".to_string(),
            "async".to_string(),
        ]),
    );

    let operations = vec![UpdateOperation::Delete {
        field: Arc::from("tags"),
        value: AttributeValue::SS(vec![
            "programming".to_string(),
            "sync".to_string(), // This doesn't exist, should be ignored
        ]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("tags").unwrap() {
        AttributeValue::SS(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&"rust".to_string()));
            assert!(set.contains(&"async".to_string()));
            assert!(!set.contains(&"programming".to_string()));
        }
        _ => panic!("Expected string set"),
    }
}

#[test]
fn apply_delete_removes_attribute_when_string_set_becomes_empty() {
    let mut item = HashMap::new();
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["remove_me".to_string()]),
    );

    let operations = vec![UpdateOperation::Delete {
        field: Arc::from("tags"),
        value: AttributeValue::SS(vec!["remove_me".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert!(!updated.contains_key("tags"));
}

#[test]
fn apply_delete_from_number_set() {
    let mut item = HashMap::new();
    item.insert(
        "scores".to_string(),
        AttributeValue::NS(vec!["85".to_string(), "90".to_string(), "95".to_string()]),
    );

    let operations = vec![UpdateOperation::Delete {
        field: Arc::from("scores"),
        value: AttributeValue::NS(vec!["90".to_string()]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("scores").unwrap() {
        AttributeValue::NS(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&"85".to_string()));
            assert!(set.contains(&"95".to_string()));
            assert!(!set.contains(&"90".to_string()));
        }
        _ => panic!("Expected number set"),
    }
}

#[test]
fn apply_delete_from_binary_set() {
    let mut item = HashMap::new();
    item.insert(
        "data".to_string(),
        AttributeValue::BS(vec![
            "AQID".to_string(), // base64 for [1, 2, 3]
            "BAUG".to_string(), // base64 for [4, 5, 6]
            "BwgJ".to_string(), // base64 for [7, 8, 9]
        ]),
    );

    let operations = vec![UpdateOperation::Delete {
        field: Arc::from("data"),
        value: AttributeValue::BS(vec![
            "BAUG".to_string(), // base64 for [4, 5, 6]
        ]),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    match updated.get("data").unwrap() {
        AttributeValue::BS(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&"AQID".to_string()));
            assert!(set.contains(&"BwgJ".to_string()));
            assert!(!set.contains(&"BAUG".to_string()));
        }
        _ => panic!("Expected binary set"),
    }
}

#[test]
fn apply_delete_on_non_set_type_fails() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), AttributeValue::S("John".to_string()));

    let operations = vec![UpdateOperation::Delete {
        field: Arc::from("name"),
        value: AttributeValue::S("John".to_string()),
    }];

    let result = apply_update_operations(item.into(), &operations);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("An operand in the update expression has an incorrect data type")
    );
}

// ===== MIXED OPERATIONS TESTS =====

#[test]
fn apply_mixed_operations() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), AttributeValue::S("John".to_string()));
    item.insert("score".to_string(), AttributeValue::N("100".to_string()));
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["old".to_string(), "keep".to_string()]),
    );

    let operations = vec![
        UpdateOperation::Set {
            field: Arc::from("name"),
            value: AttributeValue::S("Jane".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::from("score"),
            value: AttributeValue::N("50".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::from("tags"),
            value: AttributeValue::SS(vec!["new".to_string()]),
        },
        UpdateOperation::Remove {
            field: Arc::from("obsolete"), // Doesn't exist, should be ignored
        },
    ];

    let updated = apply_update_operations(item.into(), &operations).unwrap();
    assert_eq!(
        updated.get("name").unwrap(),
        &AttributeValue::S("Jane".to_string())
    );
    assert_eq!(
        updated.get("score").unwrap(),
        &AttributeValue::N("150".to_string())
    );
    match updated.get("tags").unwrap() {
        AttributeValue::SS(set) => {
            assert_eq!(set.len(), 3);
            assert!(set.contains(&"old".to_string()));
            assert!(set.contains(&"keep".to_string()));
            assert!(set.contains(&"new".to_string()));
        }
        _ => panic!("Expected string set"),
    }
}

// ===== COMPLEX EXPRESSION TESTS =====

#[test]
fn parse_complex_expression_with_all_operations() {
    let mut names = storage_types::VecMap::default();
    names.insert("#n".to_string(), "name".to_string());
    names.insert("#s".to_string(), "score".to_string());
    names.insert("#t".to_string(), "tags".to_string());

    let mut values = storage_types::AttributeMap::new();
    values.insert(":name".to_string(), AttributeValue::S("Alice".to_string()));
    values.insert(":points".to_string(), AttributeValue::N("25".to_string()));
    values.insert(
        ":tag".to_string(),
        AttributeValue::SS(vec!["expert".to_string()]),
    );
    values.insert(
        ":old_tags".to_string(),
        AttributeValue::SS(vec!["beginner".to_string()]),
    );

    let operations = parse_update_expression(
        "SET #n = :name, ADD #s :points, #t :tag, REMOVE obsolete, DELETE tags :old_tags",
        Some(&names),
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 5);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "name");
            assert_eq!(value, &AttributeValue::S("Alice".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
    match &operations[1] {
        UpdateOperation::Add { field, value } => {
            assert_eq!(field.as_ref(), "score");
            assert_eq!(value, &AttributeValue::N("25".to_string()));
        }
        _ => panic!("Expected ADD operation"),
    }

    match &operations[2] {
        UpdateOperation::Add { field, value } => {
            assert_eq!(field.as_ref(), "tags");
            assert_eq!(value, &AttributeValue::SS(vec!["expert".to_string()]));
        }
        _ => panic!("Expected ADD operation"),
    }
    match &operations[3] {
        UpdateOperation::Remove { field } => {
            assert_eq!(field.as_ref(), "obsolete");
        }
        _ => panic!("Expected REMOVE operation"),
    }
    match &operations[4] {
        UpdateOperation::Delete { field, value } => {
            assert_eq!(field.as_ref(), "tags");
            assert_eq!(value, &AttributeValue::SS(vec!["beginner".to_string()]));
        }
        _ => panic!("Expected DELETE operation"),
    }
}

// ===== EDGE CASE TESTS =====

#[test]
fn parse_empty_expression() {
    let operations = parse_update_expression("", None, None).unwrap();
    assert_eq!(operations.len(), 0);
}

#[test]
fn parse_expression_with_whitespace() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val".to_string(), AttributeValue::S("test".to_string()));

    let operations =
        parse_update_expression("  SET   field    =    :val   ", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field");
            assert_eq!(value, &AttributeValue::S("test".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

#[test]
fn apply_operations_on_empty_item() {
    let item = HashMap::new();

    let operations = vec![
        UpdateOperation::Set {
            field: Arc::from("new_field"),
            value: AttributeValue::S("value".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::from("counter"),
            value: AttributeValue::N("10".to_string()),
        },
        UpdateOperation::Remove {
            field: Arc::from("nonexistent"),
        },
    ];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(updated.len(), 2);
    assert_eq!(
        updated.get("new_field").unwrap(),
        &AttributeValue::S("value".to_string())
    );
    assert_eq!(
        updated.get("counter").unwrap(),
        &AttributeValue::N("10".to_string())
    );
}

#[test]
fn apply_add_operations_only_on_empty_item() {
    let item = HashMap::new();

    let operations = vec![
        UpdateOperation::Add {
            field: Arc::from("counter"),
            value: AttributeValue::N("25".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::from("tags"),
            value: AttributeValue::SS(vec!["new_tag".to_string()]),
        },
    ];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(updated.len(), 2);
    assert_eq!(
        updated.get("counter").unwrap(),
        &AttributeValue::N("25".to_string())
    );
    assert_eq!(
        updated.get("tags").unwrap(),
        &AttributeValue::SS(vec!["new_tag".to_string()])
    );
}

#[test]
fn all_operations_are_add() {
    let add_only_operations = [
        UpdateOperation::Add {
            field: Arc::from("counter"),
            value: AttributeValue::N("10".to_string()),
        },
        UpdateOperation::Add {
            field: Arc::from("tags"),
            value: AttributeValue::S("tag".to_string()),
        },
    ];

    let mixed_operations = [
        UpdateOperation::Add {
            field: Arc::from("counter"),
            value: AttributeValue::N("10".to_string()),
        },
        UpdateOperation::Set {
            field: Arc::from("name"),
            value: AttributeValue::S("test".to_string()),
        },
    ];

    let set_only_operations = [UpdateOperation::Set {
        field: Arc::from("name"),
        value: AttributeValue::S("test".to_string()),
    }];

    assert!(
        add_only_operations
            .iter()
            .all(|op| matches!(op, UpdateOperation::Add { .. }))
    );
    assert!(
        !mixed_operations
            .iter()
            .all(|op| matches!(op, UpdateOperation::Add { .. }))
    );
    assert!(
        !set_only_operations
            .iter()
            .all(|op| matches!(op, UpdateOperation::Add { .. }))
    );
}

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

#[test]
fn apply_add_rejects_incompatible_existing_type() {
    let mut item = HashMap::new();
    item.insert("field".to_string(), AttributeValue::S("string".to_string()));

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("field"),
        value: AttributeValue::N("5".to_string()),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("An operand in the update expression has an incorrect data type")
    );
}

// ===== SET FUNCTION TESTS =====

#[test]
fn parse_if_not_exists_function() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(
        ":default".to_string(),
        AttributeValue::S("default_value".to_string()),
    );

    let operations = parse_update_expression(
        "SET field = if_not_exists(path, :default)",
        None,
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::SetIfNotExists {
            field,
            path,
            operand,
        } => {
            assert_eq!(field.as_ref(), "field");
            assert_eq!(path.as_ref(), "path");
            assert_eq!(operand, &AttributeValue::S("default_value".to_string()));
        }
        _ => panic!("Expected SetIfNotExists operation"),
    }
}

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
fn apply_if_not_exists_when_path_exists() {
    let mut item = HashMap::new();
    item.insert(
        "existing_path".to_string(),
        AttributeValue::S("existing_value".to_string()),
    );

    let operations = vec![UpdateOperation::SetIfNotExists {
        field: Arc::from("target"),
        path: Arc::from("existing_path"),
        operand: AttributeValue::S("default".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    // Should copy the existing value to the target field
    assert_eq!(
        updated.get("target").unwrap(),
        &AttributeValue::S("existing_value".to_string())
    );
}

#[test]
fn apply_if_not_exists_when_path_does_not_exist() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::SetIfNotExists {
        field: Arc::from("target"),
        path: Arc::from("missing_path"),
        operand: AttributeValue::S("default".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    // Should use the default operand
    assert_eq!(
        updated.get("target").unwrap(),
        &AttributeValue::S("default".to_string())
    );
}

#[test]
fn apply_list_append_with_lists() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::SetListAppend {
        field: Arc::from("result"),
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
        field: Arc::from("result"),
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
fn apply_list_append_reads_path_operands_and_allows_if_not_exists() {
    let mut item = HashMap::new();
    item.insert(
        "existing".to_string(),
        AttributeValue::L(vec![AttributeValue::S("a".to_string())]),
    );

    let mut values = storage_types::AttributeMap::new();
    values.insert(":empty".to_string(), AttributeValue::L(vec![]));
    values.insert(
        ":tail".to_string(),
        AttributeValue::L(vec![AttributeValue::S("b".to_string())]),
    );

    let operations = parse_update_expression(
        "SET result = list_append(existing, :tail), created = list_append(if_not_exists(missing, \
         :empty), :tail)",
        None,
        Some(&values),
    )
    .unwrap();

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("result").unwrap(),
        &AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
        ])
    );
    assert_eq!(
        updated.get("created").unwrap(),
        &AttributeValue::L(vec![AttributeValue::S("b".to_string())])
    );
}

#[test]
fn apply_set_and_remove_list_indexes_in_dynamodb_action_order() {
    let mut item = HashMap::new();
    item.insert(
        "l".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
            AttributeValue::S("c".to_string()),
        ]),
    );

    let mut values = storage_types::AttributeMap::new();
    values.insert(":x".to_string(), AttributeValue::S("X".to_string()));

    let operations =
        parse_update_expression("REMOVE l[0] SET l[1] = :x", None, Some(&values)).unwrap();
    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("l").unwrap(),
        &AttributeValue::L(vec![
            AttributeValue::S("X".to_string()),
            AttributeValue::S("c".to_string()),
        ])
    );
}

#[test]
fn apply_set_list_index_past_end_appends_like_dynamodb() {
    let mut item = HashMap::new();
    item.insert(
        "l".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
            AttributeValue::S("c".to_string()),
        ]),
    );

    let mut values = storage_types::AttributeMap::new();
    values.insert(":z".to_string(), AttributeValue::S("z".to_string()));

    let operations = parse_update_expression("SET l[99] = :z", None, Some(&values)).unwrap();
    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("l").unwrap(),
        &AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
            AttributeValue::S("c".to_string()),
            AttributeValue::S("z".to_string()),
        ])
    );
}

#[test]
fn apply_set_nested_path_rejects_absent_or_wrong_type_parents_like_dynamodb() {
    let mut item = HashMap::new();
    item.insert("s".to_string(), AttributeValue::S("old".to_string()));
    item.insert(
        "m".to_string(),
        AttributeValue::M(HashMap::from([(
            "child".to_string(),
            AttributeValue::S("old".to_string()),
        )])),
    );
    item.insert(
        "l".to_string(),
        AttributeValue::L(vec![AttributeValue::S("a".to_string())]),
    );

    let mut values = storage_types::AttributeMap::new();
    values.insert(":x".to_string(), AttributeValue::S("x".to_string()));

    for update_expression in [
        "SET absentx.child = :x",
        "SET m.absentx.child = :x",
        "SET s.child = :x",
        "SET l[0].child = :x",
    ] {
        let operations = parse_update_expression(update_expression, None, Some(&values)).unwrap();
        let error = apply_update_operations(item.clone().into(), &operations).unwrap_err();
        assert_eq!(
            error.to_string(),
            "The document path provided in the update expression is invalid for update"
        );
    }
}

#[test]
fn apply_remove_nested_path_rejects_absent_intermediate_parent_like_dynamodb() {
    let item = HashMap::from([(
        "m".to_string(),
        AttributeValue::M(HashMap::from([(
            "child".to_string(),
            AttributeValue::S("old".to_string()),
        )])),
    )]);

    let missing_leaf = parse_update_expression("REMOVE m.absent", None, None).unwrap();
    assert_eq!(
        apply_update_operations(item.clone().into(), &missing_leaf).unwrap(),
        AttributeMap::from(&item)
    );

    let missing_parent = parse_update_expression("REMOVE m.absent.child", None, None).unwrap();
    let error = apply_update_operations(item.into(), &missing_parent).unwrap_err();
    assert_eq!(
        error.to_string(),
        "The document path provided in the update expression is invalid for update"
    );
}

#[test]
fn apply_set_arithmetic_rejects_missing_or_wrong_type_operands_like_dynamodb() {
    let mut item = HashMap::new();
    item.insert("n".to_string(), AttributeValue::N("3".to_string()));
    item.insert("s".to_string(), AttributeValue::S("old".to_string()));

    let mut values = storage_types::AttributeMap::new();
    values.insert(":inc".to_string(), AttributeValue::N("2".to_string()));
    values.insert(":s".to_string(), AttributeValue::S("x".to_string()));

    for update_expression in ["SET n = absentx + :inc", "SET n = n + absentx"] {
        let operations = parse_update_expression(update_expression, None, Some(&values)).unwrap();
        let error = apply_update_operations(item.clone().into(), &operations).unwrap_err();
        assert_eq!(
            error.to_string(),
            "The provided expression refers to an attribute that does not exist in the item"
        );
    }

    for update_expression in ["SET n = n + s", "SET n = n + :s"] {
        let operations = parse_update_expression(update_expression, None, Some(&values)).unwrap();
        let error = apply_update_operations(item.clone().into(), &operations).unwrap_err();
        assert_eq!(
            error.to_string(),
            "An operand in the update expression has an incorrect data type"
        );
    }
}

#[test]
fn apply_add_runs_after_set_even_when_add_appears_first() {
    let mut item = HashMap::new();
    item.insert("n".to_string(), AttributeValue::N("3".to_string()));

    let mut values = storage_types::AttributeMap::new();
    values.insert(":inc".to_string(), AttributeValue::N("2".to_string()));

    let operations =
        parse_update_expression("ADD n :inc SET snapx = n", None, Some(&values)).unwrap();
    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("n").unwrap(),
        &AttributeValue::N("5".to_string())
    );
    assert_eq!(
        updated.get("snapx").unwrap(),
        &AttributeValue::N("3".to_string())
    );
}

#[test]
fn apply_set_arithmetic_supports_reverse_add_and_subtract() {
    let mut item = HashMap::new();
    item.insert("n".to_string(), AttributeValue::N("3".to_string()));

    let mut values = storage_types::AttributeMap::new();
    values.insert(":inc".to_string(), AttributeValue::N("2".to_string()));
    values.insert(":dec".to_string(), AttributeValue::N("1".to_string()));

    let operations = parse_update_expression(
        "SET reverse = :inc + n, reduced = n - :dec",
        None,
        Some(&values),
    )
    .unwrap();
    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("reverse").unwrap(),
        &AttributeValue::N("5".to_string())
    );
    assert_eq!(
        updated.get("reduced").unwrap(),
        &AttributeValue::N("2".to_string())
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
fn parse_if_not_exists_with_attribute_names() {
    let mut names = storage_types::VecMap::default();
    names.insert("#p".to_string(), "path_field".to_string());
    names.insert("#t".to_string(), "target_field".to_string());

    let mut values = storage_types::AttributeMap::new();
    values.insert(":def".to_string(), AttributeValue::S("default".to_string()));

    let operations = parse_update_expression(
        "SET #t = if_not_exists(#p, :def)",
        Some(&names),
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::SetIfNotExists {
            field,
            path,
            operand,
        } => {
            assert_eq!(field.as_ref(), "target_field");
            assert_eq!(path.as_ref(), "path_field");
            assert_eq!(operand, &AttributeValue::S("default".to_string()));
        }
        _ => panic!("Expected SetIfNotExists operation"),
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

#[test]
fn parse_set_operation() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val".to_string(), AttributeValue::S("test".to_string()));

    let operations = parse_update_expression("SET field = :val", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Set { field, value } => {
            assert_eq!(field.as_ref(), "field");
            assert_eq!(value, &AttributeValue::S("test".to_string()));
        }
        _ => panic!("Expected SET operation"),
    }
}

#[test]
fn parse_add_operation() {
    let mut values = storage_types::AttributeMap::new();
    values.insert(":val".to_string(), AttributeValue::N("5".to_string()));

    let operations = parse_update_expression("ADD counter :val", None, Some(&values)).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Add { field, value } => {
            assert_eq!(field.as_ref(), "counter");
            assert_eq!(value, &AttributeValue::N("5".to_string()));
        }
        _ => panic!("Expected ADD operation"),
    }
}

#[test]
fn parse_remove_operation() {
    let operations = parse_update_expression("REMOVE field", None, None).unwrap();

    assert_eq!(operations.len(), 1);
    match &operations[0] {
        UpdateOperation::Remove { field } => {
            assert_eq!(field.as_ref(), "field");
        }
        _ => panic!("Expected REMOVE operation"),
    }
}

#[test]
fn apply_set_operation() {
    let mut item = HashMap::new();
    item.insert("existing".to_string(), AttributeValue::S("old".to_string()));

    let operations = vec![
        UpdateOperation::Set {
            field: Arc::from("existing"),
            value: AttributeValue::S("new".to_string()),
        },
        UpdateOperation::Set {
            field: Arc::from("new_field"),
            value: AttributeValue::N("123".to_string()),
        },
    ];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("existing").unwrap(),
        &AttributeValue::S("new".to_string())
    );
    assert_eq!(
        updated.get("new_field").unwrap(),
        &AttributeValue::N("123".to_string())
    );
}

#[test]
fn apply_add_operation_to_number() {
    let mut item = HashMap::new();
    item.insert("counter".to_string(), AttributeValue::N("10".to_string()));

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("counter"),
        value: AttributeValue::N("5".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("counter").unwrap(),
        &AttributeValue::N("15".to_string())
    );
}

#[test]
fn apply_add_operation_to_nonexistent_number() {
    let item = HashMap::new();

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("counter"),
        value: AttributeValue::N("5".to_string()),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert_eq!(
        updated.get("counter").unwrap(),
        &AttributeValue::N("5".to_string())
    );
}

#[test]
fn apply_add_operation_to_list_fails() {
    let mut item = HashMap::new();
    item.insert(
        "list".to_string(),
        AttributeValue::L(vec![AttributeValue::S("item1".to_string())]),
    );

    let operations = vec![UpdateOperation::Add {
        field: Arc::from("list"),
        value: AttributeValue::S("item2".to_string()),
    }];

    let error = apply_update_operations(item.into(), &operations).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("ADD operation requires a number or set operand")
    );
}

#[test]
fn apply_remove_operation() {
    let mut item = HashMap::new();
    item.insert("field".to_string(), AttributeValue::S("value".to_string()));

    let operations = vec![UpdateOperation::Remove {
        field: Arc::from("field"),
    }];

    let updated = apply_update_operations(item.into(), &operations).unwrap();

    assert!(!updated.contains_key("field"));
}

#[test]
fn update_expression_sections_accept_case_and_whitespace_variants() {
    let mut names = storage_types::VecMap::default();
    names.insert("#result".to_string(), "result".to_string());
    names.insert("#old".to_string(), "old".to_string());

    let mut values = storage_types::AttributeMap::new();
    values.insert(":v".to_string(), AttributeValue::S("written".to_string()));

    let operations = parse_update_expression(
        "set #result = :v\nREMOVE\t#old",
        Some(&names),
        Some(&values),
    )
    .unwrap();

    assert_eq!(operations.len(), 2);
    assert!(matches!(operations[0], UpdateOperation::Set { .. }));
    assert!(matches!(operations[1], UpdateOperation::Remove { .. }));
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
                AttributeValue::M(std::collections::HashMap::from([(
                    "value".to_string(),
                    AttributeValue::S(format!("{prefix}-{index}-{}", "x".repeat(64))),
                )]))
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
