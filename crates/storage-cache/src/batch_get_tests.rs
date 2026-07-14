use std::collections::HashMap;

use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, KeyAttributes,
    KeysAndAttributes, TableName, WireItem,
};

use crate::batch_get::{
    BatchGetCachePlanOptions, RuntimeBatchGetCacheOutcome, batch_get_keys_and_attributes_count_map,
    batch_request_has_items, finish_batch_get_request, merge_cached_batch_get_response,
    plan_batch_get_request, plan_batch_get_request_with_options,
};

fn key(value: &str) -> KeyAttributes {
    HashMap::from([("pk".to_string(), AttributeValue::S(value.to_string()))]).into()
}

fn item(value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_string(), AttributeValue::S(value.to_string()))])
}

#[test]
fn plan_batch_get_request_keeps_consistent_reads_cacheable_when_authoritative_flag_is_on() {
    let table_name = TableName::new("strong");
    let request = BatchGetItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            KeysAndAttributes {
                keys: vec![key("1")].into(),
                attributes_to_get: None,
                consistent_read: Some(true),
                projection_expression: None,
                expression_attribute_names: None,
            },
        )]),
        return_consumed_capacity: None,
    };

    let plan = plan_batch_get_request_with_options(
        &request,
        BatchGetCachePlanOptions {
            authoritative_strong_point_reads: true,
        },
    );

    assert_eq!(plan.total_cacheable_keys, 1);
    assert!(
        plan.cacheable_request
            .request_items
            .contains_key(&table_name)
    );
    assert!(plan.db_request.request_items.is_empty());
}

#[test]
fn plan_batch_get_request_splits_consistent_reads_out_of_cacheable_set() {
    let request = BatchGetItemRequest {
        request_items: HashMap::from([
            (
                TableName::new("eventual"),
                KeysAndAttributes {
                    keys: vec![key("1"), key("2")].into(),
                    attributes_to_get: None,
                    consistent_read: Some(false),
                    projection_expression: None,
                    expression_attribute_names: None,
                },
            ),
            (
                TableName::new("strong"),
                KeysAndAttributes {
                    keys: vec![key("3")].into(),
                    attributes_to_get: None,
                    consistent_read: Some(true),
                    projection_expression: None,
                    expression_attribute_names: None,
                },
            ),
        ]),
        return_consumed_capacity: None,
    };

    let plan = plan_batch_get_request(&request);

    assert_eq!(plan.total_cacheable_keys, 2);
    assert!(
        plan.cacheable_request
            .request_items
            .contains_key(&TableName::new("eventual"))
    );
    assert!(
        plan.db_request
            .request_items
            .contains_key(&TableName::new("strong"))
    );
}

#[test]
fn merge_cached_batch_get_response_appends_cached_items_to_existing_table_bucket() {
    let mut response = BatchGetWireItemResponse {
        responses: Some(HashMap::from([(
            TableName::new("tbl"),
            vec![WireItem::from_attribute_map(&item("db")).expect("wire item")],
        )])),
        unprocessed_keys: None,
        consumed_capacity: None,
    };

    merge_cached_batch_get_response(
        &mut response,
        HashMap::from([(
            TableName::new("tbl"),
            vec![WireItem::from_attribute_map(&item("cache")).expect("wire item")],
        )]),
    );

    let merged = response
        .responses
        .and_then(|responses| responses.get(&TableName::new("tbl")).cloned())
        .expect("merged items");
    assert_eq!(merged.len(), 2);
}

#[test]
fn batch_get_helpers_report_empty_and_key_counts() {
    let empty = BatchGetItemRequest {
        request_items: HashMap::new(),
        return_consumed_capacity: None,
    };
    assert!(!batch_request_has_items(&empty));

    let request = BatchGetItemRequest {
        request_items: HashMap::from([(
            TableName::new("tbl"),
            KeysAndAttributes {
                keys: vec![key("1"), key("2"), key("3")].into(),
                attributes_to_get: None,
                consistent_read: None,
                projection_expression: None,
                expression_attribute_names: None,
            },
        )]),
        return_consumed_capacity: None,
    };
    assert!(batch_request_has_items(&request));
    assert_eq!(
        batch_get_keys_and_attributes_count_map(&request.request_items),
        3
    );
}

#[test]
fn finish_batch_get_request_returns_partial_hit_and_db_suffix() {
    let plan = plan_batch_get_request(&BatchGetItemRequest {
        request_items: HashMap::from([(
            TableName::new("users"),
            KeysAndAttributes {
                keys: vec![key("1"), key("2")].into(),
                attributes_to_get: None,
                consistent_read: Some(false),
                projection_expression: None,
                expression_attribute_names: None,
            },
        )]),
        return_consumed_capacity: None,
    });

    let prepared = finish_batch_get_request(
        plan,
        HashMap::from([(
            TableName::new("users"),
            KeysAndAttributes {
                keys: vec![key("2")].into(),
                attributes_to_get: None,
                consistent_read: Some(false),
                projection_expression: None,
                expression_attribute_names: None,
            },
        )]),
        HashMap::from([(
            TableName::new("users"),
            vec![WireItem::from_attribute_map(&item("1")).expect("wire item")],
        )]),
    );

    assert_eq!(
        prepared.cache_outcome,
        Some(RuntimeBatchGetCacheOutcome::HitPartial)
    );
    assert_eq!(prepared.cached_responses[&TableName::new("users")].len(), 1);
    assert_eq!(
        prepared.db_request.request_items[&TableName::new("users")]
            .keys
            .len(),
        1
    );
}
