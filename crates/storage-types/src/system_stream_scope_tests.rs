use serde_json::json;

use crate::{
    DescribeSystemStreamRequest, DescribeSystemStreamResponse, SystemStreamScope,
    SystemStreamSourceTableName,
};

#[test]
fn describe_system_stream_request_is_an_empty_strict_object() {
    let request = DescribeSystemStreamRequest::try_from(json!({}))
        .expect("empty object should be a valid discovery request");
    assert_eq!(request, DescribeSystemStreamRequest::default());
    assert_eq!(
        serde_json::to_value(request).expect("request JSON"),
        json!({})
    );

    let error = DescribeSystemStreamRequest::try_from(json!({"ScopeId": "caller-supplied"}))
        .expect_err("callers must not supply a scope");
    assert!(error.contains("unknown field"), "unexpected error: {error}");
}

#[test]
fn describe_system_stream_response_exposes_only_versioned_scope() {
    let response = DescribeSystemStreamResponse::new(
        SystemStreamScope::try_new("database-owned", 4).expect("scope"),
    );
    let encoded = serde_json::to_value(&response).expect("response JSON");
    assert_eq!(
        encoded,
        json!({
            "SourceScope": {
                "Version": 1,
                "ScopeId": "database-owned",
                "Generation": 4,
            }
        })
    );
    assert_eq!(response.source_scope().generation(), 4);
    assert!(encoded.get("Checkpoint").is_none());
    assert!(encoded.get("Lineage").is_none());
}

#[test]
fn describe_system_stream_request_rejects_non_object_payloads() {
    for payload in [json!(null), json!([]), json!("scope")] {
        DescribeSystemStreamRequest::try_from(payload)
            .expect_err("discovery request must be a JSON object");
    }
}

#[test]
fn logical_source_table_name_is_bounded_and_wire_stable() {
    let table_name = SystemStreamSourceTableName::try_new("orders").expect("logical table name");
    assert_eq!(table_name.as_str(), "orders");
    assert_eq!(
        serde_json::to_value(&table_name).expect("source table JSON"),
        json!("orders")
    );
    assert_eq!(
        serde_json::from_value::<SystemStreamSourceTableName>(json!("orders"))
            .expect("source table JSON"),
        table_name
    );
}

#[test]
fn logical_source_table_name_rejects_blank_control_and_physical_values() {
    for value in [
        "",
        "   ",
        "orders\narchive",
        "arn:aws:dynamodb:table/orders",
    ] {
        SystemStreamSourceTableName::try_new(value)
            .expect_err("invalid logical source table name should be rejected");
    }
    let oversized = "x".repeat(crate::constants::SYSTEM_STREAM_SOURCE_TABLE_NAME_MAX_BYTES + 1);
    SystemStreamSourceTableName::try_new(oversized)
        .expect_err("oversized logical source table name should be rejected");
}
