use std::{collections::HashMap, hint::black_box};

use alloc_counter::AllocationGuard;
use storage_common::{DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT};
use storage_types::{
    AttributeDefinition, AttributeValue, ExclusiveStartKey, IndexName, ItemKey, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, MaxIndexers, QueryRequest, QueryTableRequest,
    ScanTableRequest, StorageError, StorageResult, StoredTableInfo, TableName, TableStatus,
    TimestampMillis, subset_expression_attribute_names_for_expression,
};

use crate::provider::provider_helpers::{
    build_query_request_view, build_scan_request, error_label, exclusive_start_key_from_string,
    extract_operation, remote_key_to_page_token,
};

fn build_query_request(request: &QueryTableRequest) -> QueryRequest {
    let mut remote_request = QueryRequest::builder()
        .table_name(request.table_name.clone())
        .key_condition_expression(request.key_condition_expression.clone())
        .index_name(request.index_name.clone())
        .expression_attribute_names(subset_expression_attribute_names_for_expression(
            &request.key_condition_expression,
            request.expression_attribute_names.as_ref(),
        ))
        .expression_attribute_values(request.expression_attribute_values.clone())
        .limit(Some(
            storage_common::normalize_limit(request.limit, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)
                .expect("valid query limit"),
        ))
        .scan_index_forward(request.scan_index_forward)
        .build();
    remote_request.exclusive_start_key = request
        .exclusive_start_key
        .as_deref()
        .map(exclusive_start_key_from_string);
    remote_request.consistent_read = Some(request.consistent_read);
    remote_request
}

fn table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("tenant_t1"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        max_indexers: MaxIndexers::default(),
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: Default::default(),
        default_item_stream_duration: Default::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn build_scan_and_query_request_when_consistent_read_is_set_then_preserve_it() {
    let scan_request = ScanTableRequest {
        table_name: TableName::new("tenant_t1"),
        index_name: Some(IndexName::new("by_status")),
        limit: Some(25),
        exclusive_start_key: Some("cursor-1".to_string()),
        consistent_read: true,
    };
    let query_request = QueryTableRequest {
        table_name: TableName::new("tenant_t1"),
        index_name: Some(IndexName::new("by_status")),
        key_condition_expression: "#pk = :pk".to_string(),
        expression_attribute_names: Some(
            HashMap::from([("#pk".to_string(), "pk".to_string())]).into(),
        ),
        expression_attribute_values: Some(
            HashMap::from([(":pk".to_string(), AttributeValue::S("tenant#1".to_string()))]).into(),
        ),
        projection_expression: Some("#pk, payload".to_string()),
        limit: Some(10),
        exclusive_start_key: Some("cursor-2".to_string()),
        scan_index_forward: Some(false),
        consistent_read: true,
    };

    let remote_scan = build_scan_request(&scan_request).expect("valid scan limit");
    let remote_query = build_query_request(&query_request);

    assert_eq!(remote_scan.consistent_read, Some(true));
    assert_eq!(remote_scan.index_name, Some(IndexName::new("by_status")));
    assert_eq!(remote_scan.limit, Some(25));
    assert!(matches!(
        remote_scan.exclusive_start_key,
        Some(ExclusiveStartKey::Token(ref token)) if token == "cursor-1"
    ));
    assert_eq!(remote_query.consistent_read, Some(true));
    assert_eq!(remote_query.index_name, Some(IndexName::new("by_status")));
    assert_eq!(remote_query.limit, Some(10));
    assert_eq!(remote_query.scan_index_forward, Some(false));
    assert_eq!(remote_query.projection_expression, None);
    assert!(matches!(
        remote_query.exclusive_start_key,
        Some(ExclusiveStartKey::Token(ref token)) if token == "cursor-2"
    ));
    assert_eq!(
        remote_query
            .expression_attribute_names
            .expect("expression names")
            .get("#pk")
            .map(String::as_str),
        Some("pk")
    );
    assert!(
        remote_query
            .expression_attribute_values
            .expect("expression values")
            .contains_key(":pk")
    );
}

#[test]
fn extract_operation_when_target_has_namespace_then_returns_final_segment() {
    assert_eq!(extract_operation("DynamoDB_20120810.Query"), "Query");
    assert_eq!(extract_operation("Scan"), "Scan");
}

#[test]
fn given_unknown_commit_when_labeling_then_returns_stable_redacted_category() {
    let ambiguous = StorageError::sync_submission_ambiguous(
        Some("proposal-secret".to_string()),
        "request body secret",
    );

    assert_eq!(error_label(&ambiguous), "sync_submission_ambiguous");
}

#[test]
fn given_idempotent_parameter_mismatch_when_labeling_then_uses_stable_category() {
    let mismatch = StorageError::idempotent_parameter_mismatch();

    assert_eq!(error_label(&mismatch), "idempotent_parameter_mismatch");
}

#[test]
fn remote_key_to_page_token_preserves_dynamo_key_types() -> StorageResult<()> {
    let table_info = table_info();
    let key = KeyAttributes::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::N("7".to_string())),
    ]);

    let token = remote_key_to_page_token(&key, &table_info, None)?;
    let decoded = ItemKey::item_key_from_next_page_token(&token, &table_info, &None)?
        .expect("decoded page token");

    assert_eq!(
        decoded.hash_key(),
        &AttributeValue::S("tenant#1".to_string())
    );
    assert_eq!(
        decoded.range_key(),
        Some(&AttributeValue::N("7".to_string()))
    );
    Ok(())
}

#[test]
fn remote_key_to_page_token_rejects_missing_schema_keys() {
    let key = KeyAttributes::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]);

    let error = remote_key_to_page_token(&key, &table_info(), None)
        .expect_err("missing range key should fail closed");

    assert!(error.to_string().contains("Missing key attribute: sk"));
}

#[test]
fn remote_key_to_page_token_preserves_binary_key_types() -> StorageResult<()> {
    let mut table_info = table_info();
    table_info.attribute_definitions[1].attribute_type = KeyAttributeType::B;
    let key = KeyAttributes::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::B("AQI=".to_string())),
    ]);

    let token = remote_key_to_page_token(&key, &table_info, None)?;
    let decoded = ItemKey::item_key_from_next_page_token(&token, &table_info, &None)?
        .expect("decoded page token");

    assert_eq!(
        decoded.range_key(),
        Some(&AttributeValue::B("AQI=".to_string()))
    );
    Ok(())
}

#[test]
fn given_query_with_projection_and_large_bindings_when_view_is_serialized_then_wire_matches_owned()
{
    let request = representative_query_request();
    let owned = serde_json::to_value(build_query_request(&request)).expect("owned query JSON");
    let borrowed =
        serde_json::to_value(build_query_request_view(&request).expect("borrowed query request"))
            .expect("borrowed query JSON");
    assert_eq!(borrowed, owned);
}

#[test]
fn given_query_with_projection_and_large_bindings_when_view_is_reused_then_allocations_drop() {
    let request = representative_query_request();
    let baseline = measure_owned_query_serialization(&request);
    let optimized = measure_borrowed_query_serialization(&request);
    assert!(optimized.allocation_count < baseline.allocation_count);
    assert!(optimized.allocated_bytes < baseline.allocated_bytes);
}

#[test]
fn given_projection_only_aliases_when_query_view_is_built_then_only_key_aliases_are_forwarded() {
    let request = QueryTableRequest {
        table_name: TableName::new("remote-query-table"),
        index_name: None,
        key_condition_expression: "#pk = :pk".to_string(),
        expression_attribute_names: Some(
            HashMap::from([
                ("#pk".to_string(), "partition_key".to_string()),
                ("#payload".to_string(), "payload".to_string()),
            ])
            .into(),
        ),
        expression_attribute_values: None,
        projection_expression: Some("#payload".to_string()),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };

    let json = serde_json::to_value(
        build_query_request_view(&request).expect("query view should validate limit"),
    )
    .expect("query JSON");

    assert_eq!(json.get("ProjectionExpression"), None);
    assert_eq!(json.get("FilterExpression"), None);
    assert_eq!(json.get("Select"), None);
    assert_eq!(json["Limit"], 100);
    assert_eq!(
        json["ExpressionAttributeNames"],
        serde_json::json!({"#pk": "partition_key"})
    );
}

#[test]
fn given_unbounded_scan_when_request_is_built_then_default_limit_is_sent() {
    let request = ScanTableRequest {
        table_name: TableName::new("remote-scan-table"),
        index_name: None,
        limit: None,
        exclusive_start_key: None,
        consistent_read: false,
    };

    let remote = build_scan_request(&request).expect("scan view should validate limit");

    assert_eq!(remote.limit, Some(DEFAULT_SCAN_LIMIT));
    assert!(remote.projection_expression.is_none());
    assert!(remote.filter_expression.is_none());
    assert!(remote.select.is_none());
}

fn representative_query_request() -> QueryTableRequest {
    let large_value = "v".repeat(1_200);
    QueryTableRequest {
        table_name: TableName::new("remote-query-table"),
        index_name: Some(IndexName::new("gsi2")),
        key_condition_expression: "#pk = :pk AND begins_with(#sk, :prefix)".to_string(),
        expression_attribute_names: Some(
            HashMap::from([
                ("#pk".to_string(), "partition_key".to_string()),
                ("#sk".to_string(), "sort_key".to_string()),
                ("#payload".to_string(), "payload".to_string()),
            ])
            .into(),
        ),
        expression_attribute_values: Some(
            HashMap::from([
                (":pk".to_string(), AttributeValue::S("tenant#1".to_string())),
                (
                    ":prefix".to_string(),
                    AttributeValue::S("item#".to_string()),
                ),
                (
                    ":ttl".to_string(),
                    AttributeValue::N("1728000000".to_string()),
                ),
                (":payload".to_string(), AttributeValue::S(large_value)),
            ])
            .into(),
        ),
        projection_expression: Some("#pk, #sk, #payload".to_string()),
        limit: Some(100),
        exclusive_start_key: Some(
            r#"{"partition_key":{"S":"tenant#1"},"sort_key":{"S":"item#25"}}"#.to_string(),
        ),
        scan_index_forward: Some(false),
        consistent_read: false,
    }
}

fn measure_owned_query_serialization(
    request: &QueryTableRequest,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "remote_query_owned_serialization",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..64 {
        let owned = build_query_request(request);
        black_box(serde_json::to_vec(&owned).expect("owned query JSON"));
    }
    guard.finish()
}

fn measure_borrowed_query_serialization(
    request: &QueryTableRequest,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "remote_query_borrowed_serialization",
        file!(),
        line!(),
        Some("optimized"),
    );
    for _ in 0..64 {
        let view = build_query_request_view(request).expect("valid query limit");
        black_box(serde_json::to_vec(&view).expect("borrowed query JSON"));
    }
    guard.finish()
}
