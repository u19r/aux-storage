use std::collections::HashMap;

use storage_types::{
    AttributeValue, ExclusiveStartKey, IndexName, QueryTableRequest, ScanTableRequest, TableName,
};

use crate::provider::provider_helpers::{
    build_query_request, build_scan_request, extract_operation,
};

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
        expression_attribute_names: Some(HashMap::from([("#pk".to_string(), "pk".to_string())])),
        expression_attribute_values: Some(HashMap::from([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        limit: Some(10),
        exclusive_start_key: Some("cursor-2".to_string()),
        scan_index_forward: Some(false),
        consistent_read: true,
    };

    let remote_scan = build_scan_request(&scan_request);
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
