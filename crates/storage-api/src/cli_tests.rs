use std::{
    ffi::OsString,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Json, extract::State, http::StatusCode};
use config::{
    Backends, RemoteBackendConfig, RemoteCredentialsConfig, RemoteStaticCredentials,
    RemoteTimeoutOverrides, Tracing,
};
use serde_json::Value;
use storage::DatabaseManager;
use storage_provider::{RemoteCredentialStrategy, StorageBackend};
use storage_types::StorageEnum;

use crate::{
    AppState, FilterSource, StorageApiManagerOptions, ensure_backend_matches, health_status, ready,
    resolve_filter, shutdown_grace_period, storage_config_from_backends,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn with_env_var<T>(name: &str, value: Option<&str>, run: impl FnOnce() -> T) -> T {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let previous = std::env::var_os(name);
    unsafe {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }
    let result = run();
    unsafe {
        match previous {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }
    result
}

fn remote_backends(credentials: Option<RemoteCredentialsConfig>) -> Backends {
    Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(RemoteBackendConfig {
            endpoint_urls: vec!["https://storage.example.com".to_string()],
            region: Some("eu-west-2".to_string()),
            tls: true,
            default_storage_mode: config::RemoteDefaultStorageMode::Dedicated,
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
        instance_keys: None,
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
fn storage_config_from_backends_uses_default_chain_when_instance_keys_enabled() {
    let backends = remote_backends(Some(RemoteCredentialsConfig {
        r#static: None,
        instance_keys: Some(true),
    }));

    let config = storage_config_from_backends(&backends).expect("storage config");
    let remote = config.remote.expect("remote settings");

    assert!(matches!(
        remote.credentials,
        RemoteCredentialStrategy::DefaultChain
    ));
}

#[test]
fn storage_config_from_backends_uses_default_chain_when_instance_keys_disabled() {
    let backends = remote_backends(Some(RemoteCredentialsConfig {
        r#static: None,
        instance_keys: Some(false),
    }));

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
        StorageEnum::Validation { message }
            if message.contains("remote storage requires at least one endpoint URL")
    ));
}

#[test]
fn storage_config_from_backends_rejects_conflicting_remote_credentials() {
    let backends = remote_backends(Some(RemoteCredentialsConfig {
        r#static: Some(RemoteStaticCredentials {
            access_key: "access-key".to_string(),
            secret_key: "secret-key".to_string(),
            session_token: None,
        }),
        instance_keys: Some(true),
    }));

    let err = storage_config_from_backends(&backends).expect_err("conflicting credentials");

    assert!(matches!(
        err.as_ref(),
        StorageEnum::Validation { message }
            if message.contains("invalid remote credentials config")
    ));
}

#[test]
fn ensure_backend_matches_rejects_remote_backend_in_standalone_mode() {
    let err = ensure_backend_matches(&remote_backends(None)).expect_err("remote mode");

    assert!(matches!(
        err.as_ref(),
        StorageEnum::InternalServerError { message }
            if message.contains("storage-api does not support remote backend in standalone mode")
    ));
}

#[test]
fn resolve_filter_uses_config_when_log_level_is_valid() {
    let tracing = Tracing {
        log_level: Some("info,storage_api=debug".to_string()),
        log_destination: "stdout".to_string(),
        traces: Vec::new(),
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
        log_destination: "stdout".to_string(),
        traces: Vec::new(),
    };

    let (filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), FilterSource::Default.to_string());
    assert_eq!(filter.to_string(), "warn");
}

#[test]
fn shutdown_grace_period_uses_default_when_env_missing_or_invalid() {
    with_env_var("APP_SHUTDOWN_GRACE_SECONDS", None, || {
        assert_eq!(shutdown_grace_period(), Duration::from_secs(5));
    });
    with_env_var("APP_SHUTDOWN_GRACE_SECONDS", Some("not-a-number"), || {
        assert_eq!(shutdown_grace_period(), Duration::from_secs(5));
    });
    with_env_var("APP_SHUTDOWN_GRACE_SECONDS", Some("27"), || {
        assert_eq!(shutdown_grace_period(), Duration::from_secs(27));
    });
}

#[test]
fn with_env_var_restores_original_value() {
    let original = OsString::from("original");
    unsafe {
        std::env::set_var("STORAGE_API_TEST_ENV", &original);
    }

    with_env_var("STORAGE_API_TEST_ENV", Some("temporary"), || {
        assert_eq!(
            std::env::var("STORAGE_API_TEST_ENV").as_deref(),
            Ok("temporary")
        );
    });

    assert_eq!(std::env::var_os("STORAGE_API_TEST_ENV"), Some(original));
    unsafe {
        std::env::remove_var("STORAGE_API_TEST_ENV");
    }
}

async fn app_state() -> Arc<AppState> {
    let db = Arc::new(DatabaseManager::new_for_test().await.expect("db"));
    Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ))
}

#[tokio::test]
async fn health_status_only_turns_unhealthy_after_sustained_failure_threshold() {
    let app_state = app_state().await;

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

    tokio::time::sleep(Duration::from_secs(5)).await;

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
}

#[tokio::test]
async fn ready_success_clears_accumulated_health_failures() {
    let app_state = app_state().await;

    for index in 0..5 {
        app_state
            .health
            .record_failure(format!("transient failure {index}"));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

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
}
