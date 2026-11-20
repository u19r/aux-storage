use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{Json, extract::State, http::StatusCode};
use config::{
    Backends, RemoteBackendConfig, RemoteCredentialsConfig, RemoteStaticCredentials,
    RemoteTimeoutOverrides, Tracing,
};
use serde_json::Value;
use storage::{DatabaseManager, startup::storage_config_from_backends};
use storage_provider::{RemoteCredentialStrategy, StorageBackend};
use storage_types::StorageEnum;

use crate::{
    AppState,
    router::{health_status, ready},
    routes::routes_tests::for_each_conformance_backend,
    runtime_config::{FilterSource, resolve_filter},
};

fn remote_backends(credentials: Option<RemoteCredentialsConfig>) -> Backends {
    Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(RemoteBackendConfig {
            endpoint_urls: vec!["https://storage.example.com".to_string()],
            region: Some("eu-west-2".to_string()),
            tls: true,
            credentials,
            timeout_overrides: Some(RemoteTimeoutOverrides {
                connect_timeout_ms: Some(250),
                request_timeout_ms: Some(1_500),
            }),
        }),
    }
}

#[test]
fn storage_config_from_backends_preserves_remote_credentials_and_timeouts() {
    let backends = remote_backends(Some(RemoteCredentialsConfig {
        r#static: Some(RemoteStaticCredentials {
            access_key: "access-key".to_string(),
            secret_key: "secret-key".to_string(),
            session_token: Some("session-token".to_string()),
        }),
    }));

    let config = storage_config_from_backends(&backends).expect("storage config");

    assert!(matches!(config.backend_type, StorageBackend::Remote));
    let remote = config.remote.expect("remote settings");
    assert_eq!(
        remote.endpoint_urls,
        vec!["https://storage.example.com".to_string()]
    );
    assert_eq!(remote.region.as_deref(), Some("eu-west-2"));
    let timeouts = remote.timeouts.expect("timeout overrides");
    assert_eq!(timeouts.connect_timeout_ms, Some(250));
    assert_eq!(timeouts.request_timeout_ms, Some(1_500));
    match remote.credentials {
        RemoteCredentialStrategy::Static(creds) => {
            assert_eq!(creds.access_key_id, "access-key");
            assert_eq!(creds.secret_access_key, "secret-key");
            assert_eq!(creds.session_token.as_deref(), Some("session-token"));
        }
        other => panic!("expected static credentials, got {other:?}"),
    }
}

#[test]
fn storage_config_from_backends_uses_default_chain_without_static_credentials() {
    let backends = remote_backends(Some(RemoteCredentialsConfig { r#static: None }));

    let config = storage_config_from_backends(&backends).expect("storage config");
    let remote = config.remote.expect("remote settings");

    assert!(matches!(
        remote.credentials,
        RemoteCredentialStrategy::DefaultChain
    ));
}

#[test]
fn storage_config_from_backends_rejects_remote_without_endpoint_urls() {
    let mut backends = remote_backends(None);
    backends
        .remote
        .as_mut()
        .expect("remote backend")
        .endpoint_urls = Vec::new();

    let err = storage_config_from_backends(&backends).expect_err("empty remote endpoints");

    assert!(matches!(
        err.as_ref(),
        StorageEnum::Validation { message, .. }
            if message.contains("remote storage requires at least one endpoint URL")
    ));
}

#[test]
fn resolve_filter_uses_config_when_log_level_is_valid() {
    let tracing = Tracing {
        log_level: Some("info,storage_api=debug".to_string()),
    };

    let (filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), FilterSource::Config.to_string());
    let filter = filter.to_string();
    assert!(filter.contains("info"));
    assert!(filter.contains("storage_api=debug"));
}

#[test]
fn resolve_filter_falls_back_to_default_when_log_level_is_invalid() {
    let tracing = Tracing {
        log_level: Some("[".to_string()),
    };

    let (filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), FilterSource::Default.to_string());
    assert_eq!(filter.to_string(), "warn");
}

fn app_state(db: Arc<DatabaseManager>) -> Arc<AppState> {
    Arc::new(AppState::new(db))
}

#[tokio::test]
async fn health_status_only_turns_unhealthy_after_sustained_failure_threshold() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = app_state(db);

        for index in 0..5 {
            app_state
                .health
                .record_failure(format!("synthetic failure {index}"));
        }

        let (status_before_window, Json(body_before_window)) =
            health_status(State(app_state.clone())).await;
        assert_eq!(status_before_window, StatusCode::OK);
        assert_eq!(
            body_before_window["status"],
            Value::String("healthy".to_string())
        );

        let window_start = Instant::now() - Duration::from_secs(5);
        for index in 0..5 {
            app_state
                .health
                .record_failure_at(window_start, format!("synthetic failure {index}"));
        }

        let (status_after_window, Json(body_after_window)) = health_status(State(app_state)).await;
        assert_eq!(status_after_window, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body_after_window["status"],
            Value::String("unhealthy".to_string())
        );
        assert_eq!(
            body_after_window["reason"],
            Value::String("synthetic failure 4".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn ready_success_clears_accumulated_health_failures() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = app_state(db);

        let window_start = Instant::now() - Duration::from_secs(5);
        for index in 0..5 {
            app_state
                .health
                .record_failure_at(window_start, format!("transient failure {index}"));
        }

        let (status_before_ready, _) = health_status(State(app_state.clone())).await;
        assert_eq!(status_before_ready, StatusCode::SERVICE_UNAVAILABLE);

        let (ready_status, Json(ready_body)) = ready(State(app_state.clone())).await;
        assert_eq!(ready_status, StatusCode::OK);
        assert_eq!(ready_body["status"], Value::String("ready".to_string()));

        let (status_after_ready, Json(body_after_ready)) = health_status(State(app_state)).await;
        assert_eq!(status_after_ready, StatusCode::OK);
        assert_eq!(
            body_after_ready["status"],
            Value::String("healthy".to_string())
        );
    })
    .await;
}

#[test]
fn given_a_remote_connection_when_storage_api_starts_then_it_is_rejected() {
    use crate::runtime_config::require_local_connections;

    let local = storage_config_from_backends(&Backends::sqlite_default()).unwrap();
    let remote = storage_config_from_backends(&remote_backends(None)).unwrap();
    let registry =
        |named: storage_provider::StorageConfig| storage_provider::StorageConnectionRegistry {
            default_connection_id: "default".to_owned(),
            connections: std::collections::HashMap::from([
                ("default".to_owned(), local.clone()),
                ("named".to_owned(), named),
            ]),
        };

    assert!(require_local_connections(&registry(local.clone())).is_ok());
    let error = require_local_connections(&registry(remote)).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("storage-api does not support remote storage connections"),
        "{error}"
    );
}
