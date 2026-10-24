use std::sync::{Arc, atomic::AtomicBool};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use metrics_exporter_prometheus::PrometheusBuilder;
use storage::DatabaseManager;
use tower::ServiceExt;

use crate::{
    AppState,
    router::{
        MetricsEndpointConfig, PrometheusMetricsEndpointConfig, openapi, server_router,
        server_router_with_metrics,
    },
    routes::routes_tests::for_each_conformance_backend,
};

#[tokio::test]
async fn given_no_auth_headers_when_metrics_request_sent_to_router_then_request_is_accepted() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));
        let metrics_handle = PrometheusBuilder::new().build_recorder().handle();
        let app = server_router_with_metrics(
            app_state,
            false,
            MetricsEndpointConfig {
                enabled: true,
                prometheus: Some(PrometheusMetricsEndpointConfig {
                    handle: metrics_handle,
                }),
            },
        );
        let request = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .expect("metrics request");

        let response = app.oneshot(request).await.expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
    })
    .await;
}

#[tokio::test]
async fn given_no_auth_headers_when_dynamodb_request_sent_to_router_then_request_is_accepted() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));
        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("x-amz-target", "DynamoDB_20120810.ListTables")
            .body(Body::from("{}"))
            .expect("DynamoDB request");

        let response = server_router(app_state, false)
            .oneshot(request)
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
    })
    .await;
}

#[tokio::test]
async fn given_startup_is_not_ready_when_dynamodb_request_sent_then_request_is_rejected() {
    for_each_conformance_backend(|_, db| async move {
        let readiness = Arc::new(AtomicBool::new(false));
        let app_state = Arc::new(AppState::new(db).with_startup_readiness(readiness));
        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("x-amz-target", "DynamoDB_20120810.ListTables")
            .body(Body::from("{}"))
            .expect("DynamoDB request");

        let response = server_router(app_state, false)
            .oneshot(request)
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    })
    .await;
}

#[tokio::test]
async fn given_legacy_replication_routes_when_requested_then_routes_are_absent() {
    let db = Arc::new(DatabaseManager::new_for_test().await.expect("database"));
    let state = Arc::new(AppState::new(db));
    for path in [
        "/_internal/storage/replication/heartbeat",
        "/_internal/sync/health",
    ] {
        let response = server_router(state.clone(), true)
            .oneshot(
                Request::builder()
                    .uri(format!("/storage{path}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[test]
fn given_dynamodb_openapi_when_security_surface_is_inspected_then_only_target_header_remains() {
    let spec = serde_json::to_value(openapi()).expect("storage OpenAPI JSON");
    let operation = &spec["paths"]["/storage"]["post"];
    let parameters = operation["parameters"]
        .as_array()
        .expect("DynamoDB OpenAPI parameters");

    assert_eq!(
        parameters
            .iter()
            .map(|parameter| parameter["name"].as_str().expect("parameter name"))
            .collect::<Vec<_>>(),
        ["x-amz-target"]
    );
    for status in ["401", "403"] {
        assert!(operation["responses"].get(status).is_none());
    }
    assert!(operation.get("security").is_none());
}

#[test]
fn storage_openapi_documents_admission_overload_response() {
    let spec = serde_json::to_value(openapi()).expect("storage OpenAPI JSON");
    assert!(
        spec["paths"]["/storage"]["post"]["responses"]
            .get("503")
            .is_some_and(serde_json::Value::is_object)
    );
}

#[test]
fn storage_openapi_does_not_document_scheduled_job_store_routes() {
    let spec = serde_json::to_value(openapi()).expect("storage OpenAPI JSON");
    for path in [
        "/storage/_internal/scheduled-job-store/v1/capabilities",
        "/storage/_internal/scheduled-job-store/v1/get",
        "/storage/_internal/scheduled-job-store/v1/get-many",
        "/storage/_internal/scheduled-job-store/v1/range",
        "/storage/_internal/scheduled-job-store/v1/commit",
    ] {
        assert!(
            !spec["paths"]
                .get(path)
                .is_some_and(serde_json::Value::is_object),
            "scheduled job store route must not be documented: {path}"
        );
    }
}

#[test]
fn storage_openapi_auxiliary_route_status_matches_feature() {
    let spec = serde_json::to_value(openapi()).expect("storage OpenAPI JSON");

    #[cfg(feature = "pubsub")]
    assert!(
        spec["paths"]["/pubsub"]["post"]["responses"]
            .get("200")
            .and_then(serde_json::Value::as_object)
            .is_some(),
        "enabled pubsub route must document success",
    );
    #[cfg(not(feature = "pubsub"))]
    assert!(
        spec["paths"]["/pubsub"]["post"]["responses"]
            .get("404")
            .and_then(serde_json::Value::as_object)
            .is_some(),
        "disabled pubsub route must document not found",
    );

    #[cfg(feature = "queue")]
    assert!(
        spec["paths"]["/queue"]["post"]["responses"]
            .get("200")
            .and_then(serde_json::Value::as_object)
            .is_some(),
        "enabled queue route must document success",
    );
    #[cfg(not(feature = "queue"))]
    assert!(
        spec["paths"]["/queue"]["post"]["responses"]
            .get("404")
            .and_then(serde_json::Value::as_object)
            .is_some(),
        "disabled queue route must document not found",
    );
}

#[cfg(not(any(feature = "queue", feature = "pubsub")))]
#[test]
fn storage_openapi_artifacts_match_the_runtime_document() {
    let runtime = serde_json::to_value(openapi()).expect("runtime OpenAPI JSON");
    let json: serde_json::Value = serde_json::from_str(include_str!("../openapi-storage-api.json"))
        .expect("committed OpenAPI JSON");
    let yaml_text = include_str!("../openapi-storage-api.yaml");
    let yaml: serde_json::Value = yaml_serde::from_str(yaml_text).expect("committed OpenAPI YAML");
    assert!(
        serde_json::from_str::<serde_json::Value>(yaml_text).is_err(),
        "emit the YAML artifact as YAML rather than JSON"
    );

    assert_eq!(json, runtime, "regenerate openapi-storage-api.json");
    assert_eq!(yaml, runtime, "regenerate openapi-storage-api.yaml");
}
