use config::DEFAULT_STORAGE_SQLITE_DB_PATH;
use queue::QueueBackend;

use crate::{Args, QueueStorageArg, add_common_headers, queue_config_from_args, queue_url};

fn base_args(storage: QueueStorageArg) -> Args {
    Args {
        storage: Some(storage),
        db_path: None,
        postgres_dsn: None,
        postgres_max_pool_size: 16,
        postgres_tls: true,
        host: None,
        port: None,
        public_base_url: None,
        account_id: "000000000000".to_string(),
        foundationdb_cluster_file: None,
        foundationdb_subspace_prefix: None,
        foundationdb_tenant_name: None,
        foundationdb_cache_read_version_ms: 0,
        foundationdb_report_conflicting_keys: false,
        config: None,
        overrides: Vec::new(),
    }
}

#[test]
fn queue_url_trims_base_slashes() {
    assert_eq!(
        queue_url("http://localhost:9324/", "000000000000", "jobs"),
        "http://localhost:9324/000000000000/jobs"
    );
}

#[test]
fn queue_config_from_args_uses_sqlite_default_path() {
    let config =
        queue_config_from_args(&base_args(QueueStorageArg::SQLite)).expect("sqlite config");

    assert!(matches!(config.backend_type, QueueBackend::SQLite));
    assert_eq!(
        config.connection_string.as_deref(),
        Some(DEFAULT_STORAGE_SQLITE_DB_PATH)
    );
}

#[test]
fn queue_config_from_args_requires_postgres_dsn() {
    let err = queue_config_from_args(&base_args(QueueStorageArg::Postgres))
        .expect_err("postgres config without dsn should fail");

    assert!(
        err.to_string()
            .contains("--postgres-dsn is required when --storage postgres"),
        "unexpected error: {err}"
    );
}

#[test]
fn add_common_headers_sets_request_id_and_optional_error_type() {
    let mut headers = axum::http::HeaderMap::new();

    add_common_headers(
        &mut headers,
        "request-123",
        Some("AWS.SimpleQueueService.Error"),
    );

    assert_eq!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/x-amz-json-1.0")
    );
    assert_eq!(
        headers
            .get("x-amzn-requestid")
            .and_then(|v| v.to_str().ok()),
        Some("request-123")
    );
    assert_eq!(
        headers
            .get("x-amzn-query-error")
            .and_then(|v| v.to_str().ok()),
        Some("AWS.SimpleQueueService.Error;Sender")
    );
}

#[test]
fn add_common_headers_falls_back_to_invalid_for_non_header_safe_values() {
    let mut headers = axum::http::HeaderMap::new();

    add_common_headers(&mut headers, "bad\nrequest-id", Some("bad\nerror-type"));

    assert_eq!(
        headers
            .get("x-amzn-requestid")
            .and_then(|v| v.to_str().ok()),
        Some("invalid")
    );
    assert_eq!(
        headers
            .get("x-amzn-query-error")
            .and_then(|v| v.to_str().ok()),
        Some("invalid")
    );
}
