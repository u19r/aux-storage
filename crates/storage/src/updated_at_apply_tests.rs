use std::collections::HashMap;

use storage_types::AttributeValue;

use crate::updated_at_apply::{
    inject_updated_at_into_update_expression, resolve_update_attribute_name,
};

#[test]
fn nested_placeholder_document_path_does_not_block_updated_at_injection() {
    let mut expression = "SET #entries.#entry = :entry".to_string();
    let mut names = Some(HashMap::from([
        ("#entries".to_string(), "entries".to_string()),
        ("#entry".to_string(), "entry".to_string()),
    ]));
    let mut values = Some(HashMap::from([(
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
    let names = HashMap::from([
        ("#root".to_string(), "audit.entries".to_string()),
        ("#child".to_string(), "value".to_string()),
    ]);

    let resolved = resolve_update_attribute_name("#root[2].#child[0]", Some(&names))
        .expect("mixed document path should resolve");

    assert_eq!(resolved, "audit.entries[2].value[0]");
}

#[test]
fn document_path_preserves_adjacent_list_indices_and_raw_segments() {
    let names = HashMap::from([
        ("#matrix".to_string(), "matrix".to_string()),
        ("#value".to_string(), "value".to_string()),
    ]);

    let resolved = resolve_update_attribute_name(
        "payload.#matrix[2][1].items[0].#value",
        Some(&names),
    )
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
    let names = HashMap::from([("#entries".to_string(), "entries".to_string())]);

    let error = resolve_update_attribute_name("#entries.#entry[0]", Some(&names))
        .expect_err("missing nested placeholder should fail");

    assert_eq!(
        error.to_string(),
        "update expression placeholder '#entry' was not found in expression_attribute_names"
    );
}

#[test]
fn top_level_updated_at_placeholder_is_still_recognized() {
    let mut expression = "SET #updated_at = :existing".to_string();
    let mut names = Some(HashMap::from([(
        "#updated_at".to_string(),
        "updated_at".to_string(),
    )]));
    let mut values = Some(HashMap::from([(
        ":existing".to_string(),
        AttributeValue::N("1".to_string()),
    )]));

    inject_updated_at_into_update_expression(&mut expression, &mut names, &mut values)
        .expect("existing updated_at assignment should be refreshed");

    assert_eq!(expression, "SET #updated_at = :existing");
    assert_eq!(
        names
            .as_ref()
            .and_then(|names| names.get("#updated_at"))
            .map(String::as_str),
        Some("u_at")
    );
    assert_ne!(
        values
            .as_ref()
            .and_then(|values| values.get(":existing")),
        Some(&AttributeValue::N("1".to_string()))
    );
}
