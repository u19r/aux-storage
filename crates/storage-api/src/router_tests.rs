use axum::http::{HeaderMap, HeaderValue, StatusCode, header};

use crate::router::authorize_prometheus_metrics;

#[test]
fn prometheus_metrics_auth_allows_unsecured_endpoint() {
    let headers = HeaderMap::new();

    assert_eq!(authorize_prometheus_metrics(&headers, None), Ok(()));
}

#[test]
fn prometheus_metrics_auth_requires_bearer_token_when_configured() {
    let headers = HeaderMap::new();

    assert_eq!(
        authorize_prometheus_metrics(&headers, Some("secret")),
        Err(StatusCode::UNAUTHORIZED)
    );
}

#[test]
fn prometheus_metrics_auth_rejects_wrong_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer wrong"),
    );

    assert_eq!(
        authorize_prometheus_metrics(&headers, Some("secret")),
        Err(StatusCode::FORBIDDEN)
    );
}

#[test]
fn prometheus_metrics_auth_accepts_matching_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret"),
    );

    assert_eq!(
        authorize_prometheus_metrics(&headers, Some("secret")),
        Ok(())
    );
}
