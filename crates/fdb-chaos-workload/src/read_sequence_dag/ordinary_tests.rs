use std::collections::HashMap;

use super::ordinary::{
    ORDINARY_BATCH_GET_MAX_ATTEMPTS, OrdinaryFailure, merge_batch_get_page, next_batch_get_request,
};
use crate::imports::*;

fn key(value: &str) -> KeyAttributes {
    HashMap::from([("pk".to_string(), AttributeValue::S(value.to_string()))]).into()
}

fn item(value: &str) -> WireItem {
    WireItem::try_from(&HashMap::from([(
        "pk".to_string(),
        AttributeValue::S(value.to_string()),
    )]))
    .expect("test item encodes")
}

#[test]
fn unprocessed_batch_page_is_retried_without_losing_items_or_metadata() {
    let table_name = TableName::new("ordinary-child");
    let mut response = storage_types::BatchGetItemResponse {
        responses: Some(
            HashMap::from([(table_name.clone(), vec![item("first"), item("second")])]).into(),
        ),
        unprocessed_keys: Some(
            HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![key("pending-a"), key("pending-b")],
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )])
            .into(),
        ),
        consumed_capacity: None,
    };
    let mut responses = HashMap::new();

    let retry_items = merge_batch_get_page(&mut responses, &mut response);

    assert_eq!(responses[&table_name].len(), 2);
    let pending = &retry_items[&table_name];
    assert_eq!(pending.keys.len(), 2);
    assert_ne!(pending.keys[0], pending.keys[1]);
    assert_eq!(pending.consistent_read, Some(true));
}

#[test]
fn unprocessed_batch_exhaustion_is_bounded_storage_failure() {
    let table_name = TableName::new("ordinary-child");
    let pending = HashMap::from([(
        table_name,
        KeysAndAttributes {
            keys: vec![key("pending")],
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: Some(false),
        },
    )]);

    for attempt in 0..(ORDINARY_BATCH_GET_MAX_ATTEMPTS - 1) {
        assert!(matches!(
            next_batch_get_request(attempt, pending.clone()),
            Ok(Some(_))
        ));
    }
    let failure = next_batch_get_request(ORDINARY_BATCH_GET_MAX_ATTEMPTS - 1, pending)
        .expect_err("the fourth attempt must be the terminal bounded retry");
    assert!(matches!(
        failure,
        OrdinaryFailure::Storage(message)
            if message.contains("exhausted retries") && !message.contains("fixture")
    ));
}
