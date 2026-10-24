use crate::manager::storage_manager_impl_run_background_job::TtlSweepPayload;

fn request(
    payload: serde_json::Value,
) -> Result<storage_common::TtlSweepRequest, http_error::HttpApiError> {
    TtlSweepPayload::try_from(payload).map(|payload| payload.request)
}

#[test]
fn ttl_sweep_requires_a_direct_table_target() {
    let error = request(serde_json::json!({
        "JobName": "ttl-sweep",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
    }))
    .err()
    .expect("missing target should be rejected");

    assert!(error.message.contains("TableName"));
}

#[test]
fn ttl_sweep_rejects_namespace_and_proposal_bookkeeping() {
    let error = request(serde_json::json!({
        "JobName": "ttl-sweep",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
        "TableName": "orders",
        "Namespace": "tenant/orders",
    }))
    .err()
    .expect("namespace routing must not be accepted by ttl sweep");

    assert!(error.message.contains("unknown field"));
    let error = request(serde_json::json!({
        "JobName": "ttl-sweep",
        "ProposalId": "proposal-1",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
        "TableName": "orders",
    }))
    .err()
    .expect("proposal identity must not be accepted by ttl sweep");
    assert!(error.message.contains("unknown field"));
}

#[test]
fn ttl_sweep_rejects_empty_attribute_zero_limit_and_blank_cursor() {
    for payload in [
        serde_json::json!({
            "JobName": "ttl-sweep",
            "TTLAttribute": " ",
            "ScanTime": 1_700_000_000_000_i64,
            "Limit": 10,
            "TableName": "orders",
        }),
        serde_json::json!({
            "JobName": "ttl-sweep",
            "TTLAttribute": "expires_at",
            "ScanTime": 1_700_000_000_000_i64,
            "Limit": 0,
            "TableName": "orders",
        }),
        serde_json::json!({
            "JobName": "ttl-sweep",
            "TTLAttribute": "expires_at",
            "ScanTime": 1_700_000_000_000_i64,
            "Limit": 10,
            "Cursor": " ",
            "TableName": "orders",
        }),
    ] {
        request(payload)
            .err()
            .expect("invalid ttl request must be rejected");
    }
}

#[test]
fn ttl_sweep_converts_cursor_token() {
    let request = request(serde_json::json!({
        "JobName": "ttl-sweep",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
        "Cursor": "opaque-page-token",
        "TableName": "orders",
    }))
    .expect("opaque cursor token");
    assert_eq!(
        request.input.cursor.expect("cursor").token,
        "opaque-page-token"
    );
}

#[test]
fn ttl_sweep_rejects_malformed_cursor_token_before_request_creation() {
    let error = request(serde_json::json!({
        "JobName": "ttl-sweep",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
        "Cursor": " ",
        "TableName": "orders",
    }))
    .err()
    .expect("blank opaque cursor token must be rejected");
    assert!(error.message.contains("cursor token must not be empty"));
}

#[test]
fn ttl_sweep_rejects_non_object_payload() {
    let error = TtlSweepPayload::try_from(serde_json::json!(["not-an-object"]))
        .err()
        .expect("non-object ttl request must be rejected");
    assert!(error.message.contains("JSON object"));
}

#[test]
fn ttl_sweep_rejects_unknown_fields() {
    let error = TtlSweepPayload::try_from(serde_json::json!({
        "JobName": "ttl-sweep",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
        "TableName": "orders",
        "Unexpected": "must-not-be-ignored",
    }))
    .err()
    .expect("typed ttl DTO must reject unknown fields");

    assert!(error.message.contains("unknown field"));
}

#[test]
fn given_non_ttl_job_when_payload_is_parsed_then_request_is_rejected() {
    let error = TtlSweepPayload::try_from(serde_json::json!({
        "JobName": "gsi-backfill",
        "TTLAttribute": "expires_at",
        "ScanTime": 1_700_000_000_000_i64,
        "Limit": 10,
        "TableName": "orders",
    }))
    .err()
    .expect("non-TTL work must not enter the TTL handler");

    assert!(error.message.contains("requires a ttl-sweep request"));
}
