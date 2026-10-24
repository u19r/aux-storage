use storage_types::AttributeValue;

use crate::updated_at_apply::{
    inject_updated_at_into_update_expression, resolve_update_attribute_name,
};

#[test]
fn nested_placeholder_document_path_does_not_block_updated_at_injection() {
    let mut expression = "SET #entries.#entry = :entry".to_string();
    let mut names = Some(storage_types::VecMap::from_iter([
        ("#entries".to_string(), "entries".to_string()),
        ("#entry".to_string(), "entry".to_string()),
    ]));
    let mut values = Some(storage_types::AttributeMap::from_iter([(
        ":entry".to_string(),
        AttributeValue::S("value".to_string()),
    )]));

    inject_updated_at_into_update_expression(&mut expression, &mut names, &mut values)
        .expect("nested placeholders should resolve independently");

    assert_eq!(
        expression,
        "SET #entries.#entry = :entry, #__updated_at = :__updated_at"
    );
    assert_eq!(
        names
            .as_ref()
            .and_then(|names| names.get("#__updated_at"))
            .map(String::as_str),
        Some("u_at")
    );
    assert!(
        values
            .as_ref()
            .is_some_and(|values| values.contains_key(":__updated_at"))
    );
}

#[test]
fn mixed_document_path_resolves_each_placeholder_and_preserves_list_indices() {
    let names = storage_types::VecMap::from_iter([
        ("#root".to_string(), "audit.entries".to_string()),
        ("#child".to_string(), "value".to_string()),
    ]);

    let resolved = resolve_update_attribute_name("#root[2].#child[0]", Some(&names))
        .expect("mixed document path should resolve");

    assert_eq!(resolved, "audit.entries[2].value[0]");
}

#[test]
fn document_path_preserves_adjacent_list_indices_and_raw_segments() {
    let names = storage_types::VecMap::from_iter([
        ("#matrix".to_string(), "matrix".to_string()),
        ("#value".to_string(), "value".to_string()),
    ]);

    let resolved =
        resolve_update_attribute_name("payload.#matrix[2][1].items[0].#value", Some(&names))
            .expect("all document path segments should remain intact");

    assert_eq!(resolved, "payload.matrix[2][1].items[0].value");
}

#[test]
fn unaliased_document_path_does_not_require_expression_attribute_names() {
    let resolved = resolve_update_attribute_name("payload.items[3].value", None)
        .expect("unaliased document paths do not need an attribute-name map");

    assert_eq!(resolved, "payload.items[3].value");
}

#[test]
fn nested_document_path_reports_the_missing_segment_placeholder() {
    let names = storage_types::VecMap::from_iter([("#entries".to_string(), "entries".to_string())]);

    let error = resolve_update_attribute_name("#entries.#entry[0]", Some(&names))
        .expect_err("missing nested placeholder should fail");

    assert_eq!(
        error.to_string(),
        "update expression placeholder '#entry' was not found in expression_attribute_names"
    );
}

#[test]
fn given_explicit_updated_at_when_stamping_then_value_is_preserved() {
    let mut expression = "SET #updated_at = :existing".to_string();
    let mut names = Some(storage_types::VecMap::from_iter([(
        "#updated_at".to_string(),
        "updated_at".to_string(),
    )]));
    let mut values = Some(storage_types::AttributeMap::from_iter([(
        ":existing".to_string(),
        AttributeValue::N("1".to_string()),
    )]));

    inject_updated_at_into_update_expression(&mut expression, &mut names, &mut values)
        .expect("existing updated_at assignment should be preserved");

    assert_eq!(expression, "SET #updated_at = :existing");
    assert_eq!(
        names
            .as_ref()
            .and_then(|names| names.get("#updated_at"))
            .map(String::as_str),
        Some("u_at")
    );
    assert_eq!(
        values.as_ref().and_then(|values| values.get(":existing")),
        Some(&AttributeValue::N("1".to_string()))
    );
}

#[test]
fn given_condition_only_timestamp_aliases_when_stamping_update_then_literal_guards_are_preserved() {
    let mut expression = "SET #stamp = if_not_exists(#stamp, :existing)".to_string();
    let mut names = Some(storage_types::VecMap::from_iter([
        ("#stamp".to_string(), "u_at".to_string()),
        ("#legacy_created".to_string(), "created_at".to_string()),
        ("#legacy_updated".to_string(), "updated_at".to_string()),
        ("#legacy_expires".to_string(), "expires_at".to_string()),
    ]));
    let original = names.clone();
    let mut values = Some(storage_types::AttributeMap::from_iter([(
        ":existing".to_string(),
        AttributeValue::N("1".to_string()),
    )]));
    inject_updated_at_into_update_expression(&mut expression, &mut names, &mut values).unwrap();
    assert_eq!(names, original);
    assert_eq!(expression, "SET #stamp = if_not_exists(#stamp, :existing)");
    assert_eq!(values.unwrap().len(), 1);
}

#[test]
fn given_raw_timestamp_assignments_with_other_update_sections_when_stamping_then_only_set_targets_are_canonicalized()
 {
    let mut expression =
        "SET created_at = :created, updated_at = :updated REMOVE #legacy".to_string();
    let mut names = Some(storage_types::VecMap::from_iter([(
        "#legacy".to_string(),
        "expires_at".to_string(),
    )]));
    let mut values = Some(storage_types::AttributeMap::from_iter([
        (":created".to_string(), AttributeValue::N("1".to_string())),
        (":updated".to_string(), AttributeValue::N("2".to_string())),
    ]));
    inject_updated_at_into_update_expression(&mut expression, &mut names, &mut values).unwrap();
    assert_eq!(
        expression,
        "SET c_at = :created, u_at = :updated REMOVE #legacy"
    );
    assert_eq!(
        names.unwrap().get("#legacy").map(String::as_str),
        Some("expires_at")
    );
    assert_eq!(values.unwrap().len(), 2);
}
