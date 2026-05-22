use crate::{
    AttributeValue, DEFAULT_PK_NAME, DEFAULT_SK_NAME, ExprNameRef, ExprValueRef, expr_names_to_map,
    expr_values_to_map,
    lightweight_refs::{AttributeValueRef, KeyRef, ScalarValueRef},
};

#[test]
fn key_ref_to_map_uses_expected_capacity_and_values() {
    let key = KeyRef::pk_sk(ScalarValueRef::S("pk_val"), ScalarValueRef::N("7"));
    let map = key.to_map();

    assert_eq!(map.len(), 2);
    assert_eq!(
        map.get(DEFAULT_PK_NAME),
        Some(&AttributeValue::S("pk_val".to_string()))
    );
    assert_eq!(
        map.get(DEFAULT_SK_NAME),
        Some(&AttributeValue::N("7".to_string()))
    );
}

#[test]
fn expr_helpers_convert_inline_pairs() {
    let names = [ExprNameRef::new("#status", "status")];
    let values = [ExprValueRef::new(":status", AttributeValueRef::S("queued"))];

    let names_map = expr_names_to_map(&names);
    let values_map = expr_values_to_map(&values);

    assert_eq!(names_map.get("#status"), Some(&"status".to_string()));
    assert_eq!(
        values_map.get(":status"),
        Some(&AttributeValue::S("queued".to_string()))
    );
}

#[test]
fn expr_helper_maps_keep_last_duplicate_placeholder_value() {
    let names = [
        ExprNameRef::new("#status", "status"),
        ExprNameRef::new("#status", "state"),
    ];
    let values = [
        ExprValueRef::new(":status", AttributeValueRef::S("queued")),
        ExprValueRef::new(":status", AttributeValueRef::S("running")),
    ];

    let names_map = expr_names_to_map(&names);
    let values_map = expr_values_to_map(&values);

    assert_eq!(names_map.get("#status"), Some(&"state".to_string()));
    assert_eq!(
        values_map.get(":status"),
        Some(&AttributeValue::S("running".to_string()))
    );
}
