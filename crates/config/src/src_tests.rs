use std::{fs, sync::Mutex};

use serde_json::json;

use crate::{
    AppRole, Backends, ConfigError, DEFAULT_STORAGE_SQLITE_DB_PATH, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig, RootConfig, StorageApiLaunchConfig, load,
    load_optional_with_overrides,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn write_temp_config(value: serde_json::Value) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("create temporary config file");
    fs::write(
        file.path(),
        serde_json::to_vec_pretty(&value).expect("serialize config"),
    )
    .expect("write config");
    file
}

#[test]
fn loads_storage_defaults() {
    let loaded = load_optional_with_overrides(None, &[]).expect("load defaults");
    assert_eq!(loaded.root.version, "1.0");
    assert_eq!(loaded.root.roles, vec![AppRole::Api, AppRole::DatabaseJobs]);
    assert_eq!(
        loaded
            .root
            .features
            .backends
            .sqlite
            .as_ref()
            .expect("sqlite default")
            .db_path,
        DEFAULT_STORAGE_SQLITE_DB_PATH
    );
}

#[test]
fn config_schema_can_be_written_for_runtime_discovery() {
    let dir = tempfile::tempdir().expect("create temporary directory");
    let schema_path = dir.path().join("config.schema.json");

    crate::Config::write_schema_to(&schema_path).expect("write schema");

    let written = fs::read_to_string(schema_path).expect("schema file exists");
    assert!(written.contains("\"$schema\""));
    assert!(written.contains("\"RootConfig\""));
}

#[test]
fn effective_pretty_reports_the_merged_config_without_requiring_callers_to_serialize_it() {
    let loaded = load_optional_with_overrides(
        None,
        &[("description".to_string(), "\"business config\"".to_string())],
    )
    .expect("load config with description override");

    let pretty = loaded.effective_pretty();

    assert!(pretty.contains("\"description\": \"business config\""));
    assert!(pretty.contains("\"features\""));
}

#[test]
fn required_config_file_must_exist() {
    let dir = tempfile::tempdir().expect("create temporary directory");
    let missing_path = dir.path().join("missing.json");

    let error = load(&missing_path).expect_err("required config file should fail");

    assert!(matches!(error, ConfigError::Io { .. }));
}

#[test]
fn optional_config_file_can_be_missing_when_process_defaults_are_acceptable() {
    let dir = tempfile::tempdir().expect("create temporary directory");
    let missing_path = dir.path().join("missing.json");

    let loaded =
        load_optional_with_overrides(Some(&missing_path), &[]).expect("missing optional config");

    assert!(loaded.root.features.backends.sqlite.is_some());
}

#[test]
fn loads_postgres_backend() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "postgres": {
                    "dsn": "postgres://localhost/aux_storage",
                    "max_pool_size": 4
                }
            }
        }
    }));

    let loaded = load(file.path()).expect("load postgres config");
    let postgres = loaded
        .root
        .features
        .backends
        .postgres
        .as_ref()
        .expect("postgres backend");
    assert_eq!(postgres.dsn, "postgres://localhost/aux_storage");
    assert_eq!(postgres.max_pool_size, 4);
}

#[test]
fn rejects_multiple_backends() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": {},
                "rocksdb": {}
            }
        }
    }));

    let error = load(file.path()).expect_err("multiple backends should fail");
    assert!(matches!(error, ConfigError::Validation { .. }));
}

#[test]
fn rejects_no_storage_backend_after_overlay_disables_the_default_backend() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null
            }
        }
    }));

    let error = load(file.path()).expect_err("zero backends should fail");

    assert!(matches!(error, ConfigError::Validation { .. }));
}

#[test]
fn rejects_postgres_backend_without_a_dsn() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "postgres": {
                    "dsn": " ",
                    "max_pool_size": 4
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("blank postgres dsn should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.backends.postgres.dsn must not be empty")));
}

#[test]
fn rejects_postgres_backend_without_a_pool() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "postgres": {
                    "dsn": "postgres://localhost/aux_storage",
                    "max_pool_size": 0
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("zero postgres pool should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.backends.postgres.max_pool_size must be greater than 0")));
}

#[test]
fn rejects_remote_backend_without_endpoint_urls() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "remote": {}
            }
        }
    }));

    let error = load(file.path()).expect_err("remote backend without endpoints should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.backends.remote.endpoint_urls must not be empty")));
}

#[test]
fn rejects_remote_backend_with_blank_region() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "remote": {
                    "endpoint_urls": ["http://127.0.0.1:8080"],
                    "region": ""
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("blank remote region should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.backends.remote.region must not be empty")));
}

#[test]
fn rejects_remote_backend_with_conflicting_credential_sources() {
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "remote": {
                    "endpoint_urls": ["http://127.0.0.1:8080"],
                    "credentials": {
                        "static": {
                            "access_key": "access",
                            "secret_key": "secret"
                        },
                        "instance_keys": true
                    }
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("conflicting remote credentials should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("remote credentials must use either static credentials or instance keys")));
}

#[test]
fn cli_overrides_apply() {
    let loaded = load_optional_with_overrides(
        None,
        &[(
            "features.backends.sqlite.db_path".to_string(),
            "\"/tmp/aux-storage.db\"".to_string(),
        )],
    )
    .expect("load override");

    assert_eq!(
        loaded
            .root
            .features
            .backends
            .sqlite
            .as_ref()
            .expect("sqlite backend")
            .db_path,
        "/tmp/aux-storage.db"
    );
}

#[test]
fn cli_overrides_parse_json_values_before_merging() {
    let loaded = load_optional_with_overrides(
        None,
        &[
            (
                "features.storage_point_read_cache.enabled".to_string(),
                "true".to_string(),
            ),
            (
                "features.storage_point_read_cache.capacity".to_string(),
                "12".to_string(),
            ),
        ],
    )
    .expect("load override");

    assert!(loaded.root.features.storage_point_read_cache.enabled);
    assert_eq!(loaded.root.features.storage_point_read_cache.capacity, 12);
}

#[test]
fn cli_overrides_reject_empty_path_segments() {
    let error = load_optional_with_overrides(
        None,
        &[(
            "features..backends.sqlite.db_path".to_string(),
            "x".to_string(),
        )],
    )
    .expect_err("empty override path segment should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("override path contains an empty segment")));
}

#[test]
fn remote_credentials_reject_conflicting_modes() {
    let credentials = RemoteCredentialsConfig {
        r#static: Some(RemoteStaticCredentialsConfig {
            access_key: "key".to_string(),
            secret_key: "secret".to_string(),
            session_token: None,
        }),
        instance_keys: Some(true),
    };

    assert!(credentials.validate().is_err());
}

#[test]
fn root_config_reports_background_worker_flag() {
    let mut root = RootConfig::default();
    assert!(root.background_workers_enabled());
    root.features.runtime.enable_background_workers = false;
    assert!(!root.background_workers_enabled());
}

#[test]
fn metrics_config_defaults_to_enabled() {
    let loaded = load_optional_with_overrides(None, &[]).expect("load defaults");

    assert!(loaded.root.features.metrics.enabled);
    assert!(
        loaded
            .root
            .features
            .metrics
            .prometheus
            .bearer_token
            .is_none()
    );
}

#[test]
fn metrics_config_accepts_prometheus_bearer_token() {
    let file = write_temp_config(json!({
        "features": {
            "metrics": {
                "enabled": true,
                "prometheus": {
                    "bearer_token": "metrics-token"
                }
            }
        }
    }));

    let loaded = load(file.path()).expect("load metrics config");

    assert_eq!(
        loaded
            .root
            .features
            .metrics
            .prometheus
            .bearer_token
            .as_deref(),
        Some("metrics-token")
    );
}

#[test]
fn metrics_config_rejects_empty_prometheus_bearer_token() {
    let file = write_temp_config(json!({
        "features": {
            "metrics": {
                "prometheus": {
                    "bearer_token": ""
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("empty metrics token should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.metrics.prometheus.bearer_token")));
}

#[test]
fn queue_config_accepts_runtime_settings_without_backend_selector() {
    let file = write_temp_config(json!({
        "queue": {
            "account_id": "123456789012",
            "public_base_url": "http://127.0.0.1:3001/queue",
            "visibility_timeout_seconds": 45
        }
    }));

    let loaded = load(file.path()).expect("load queue runtime config");
    assert_eq!(loaded.root.queue.account_id, "123456789012");
    assert_eq!(
        loaded.root.queue.public_base_url.as_deref(),
        Some("http://127.0.0.1:3001/queue")
    );
    assert_eq!(loaded.root.queue.visibility_timeout_seconds, 45);
}

#[test]
fn service_routes_load_from_http_config() {
    let file = write_temp_config(json!({
        "http": {
            "routes": {
                "storage": "/storage",
                "queue": "/queue",
                "pubsub": "/pubsub"
            }
        }
    }));

    let loaded = load(file.path()).expect("load service route config");

    assert_eq!(loaded.root.http.routes.storage, "/storage");
    assert_eq!(loaded.root.http.routes.queue, "/queue");
    assert_eq!(loaded.root.http.routes.pubsub, "/pubsub");
}

#[test]
fn queue_config_rejects_empty_public_base_url() {
    let file = write_temp_config(json!({
        "queue": {
            "public_base_url": ""
        }
    }));

    let error = load(file.path()).expect_err("empty queue public base URL should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("queue.public_base_url must not be empty")));
}

#[test]
fn queue_config_rejects_empty_account_id() {
    let file = write_temp_config(json!({
        "queue": {
            "account_id": " "
        }
    }));

    let error = load(file.path()).expect_err("empty queue account id should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("queue.account_id must not be empty")));
}

#[test]
fn job_jitter_is_capped_at_one_hundred_percent() {
    let file = write_temp_config(json!({
        "jobs": {
            "jitter_percent": 101
        }
    }));

    let error = load(file.path()).expect_err("job jitter over 100 should fail");

    assert!(matches!(error, ConfigError::Validation { .. }));
}

#[test]
fn point_read_cache_capacity_must_leave_room_for_entries() {
    let file = write_temp_config(json!({
        "features": {
            "storage_point_read_cache": {
                "capacity": 0
            }
        }
    }));

    let error = load(file.path()).expect_err("zero cache capacity should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.storage_point_read_cache.capacity must be greater than 0")));
}

#[test]
fn point_read_cache_byte_limit_must_be_positive_when_set() {
    let file = write_temp_config(json!({
        "features": {
            "storage_point_read_cache": {
                "max_bytes": 0
            }
        }
    }));

    let error = load(file.path()).expect_err("zero cache byte limit should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.storage_point_read_cache.max_bytes must be greater than 0")));
}

#[test]
fn storage_connection_registry_must_name_an_existing_default_connection() {
    let file = write_temp_config(json!({
        "features": {
            "storage_connections": {
                "default_connection": "replica",
                "connections": {
                    "primary": {
                        "sqlite": {}
                    }
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("missing default connection should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("default_connection 'replica' not found in connections")));
}

#[test]
fn storage_connection_registry_requires_a_non_empty_default_connection_id() {
    let file = write_temp_config(json!({
        "features": {
            "storage_connections": {
                "default_connection": " ",
                "connections": {
                    "primary": {
                        "sqlite": {}
                    }
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("blank default connection should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.storage_connections.default_connection must not be empty")));
}

#[test]
fn storage_connection_registry_requires_at_least_one_connection() {
    let file = write_temp_config(json!({
        "features": {
            "storage_connections": {
                "default_connection": "primary",
                "connections": {}
            }
        }
    }));

    let error = load(file.path()).expect_err("empty connection registry should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.storage_connections.connections must not be empty")));
}

#[test]
fn storage_connection_registry_validates_each_connection_backend_selection() {
    let file = write_temp_config(json!({
        "features": {
            "storage_connections": {
                "default_connection": "primary",
                "connections": {
                    "primary": {
                        "sqlite": null
                    }
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("connection without backend should fail");

    assert!(matches!(error, ConfigError::Validation { .. }));
}

#[test]
fn point_read_cache_budget_loads_fixed_bytes_inside_mode() {
    let file = write_temp_config(json!({
        "features": {
            "storage_point_read_cache": {
                "memory_budget": {
                    "mode": {
                        "fixed_bytes": {
                            "bytes": 4096
                        }
                    }
                }
            }
        }
    }));

    let loaded = load(file.path()).expect("load fixed byte budget");

    assert_eq!(
        loaded
            .root
            .features
            .storage_point_read_cache
            .memory_budget
            .mode,
        crate::StoragePointReadCacheMemoryBudgetMode::FixedBytes { bytes: 4096 }
    );
}

#[test]
fn empty_backends_keep_default_sqlite() {
    let file = write_temp_config(json!({}));
    let loaded = load(file.path()).expect("load empty config");
    assert!(matches!(
        loaded.root.features.backends,
        Backends {
            sqlite: Some(_),
            turso: None,
            postgres: None,
            rocksdb: None,
            foundationdb: None,
            remote: None
        }
    ));
}

#[test]
fn storage_api_launch_config_uses_defaults_without_config_file() {
    let launch = StorageApiLaunchConfig::from_args(["storage-api"]).expect("default launch config");

    assert_eq!(launch.effective.bind_addr, "0.0.0.0:8080");
    assert!(launch.effective.backends.sqlite.is_some());
    assert!(launch.inputs.config_path.is_none());
}

#[test]
fn storage_api_launch_config_maps_top_level_flags_to_config_paths() {
    let launch = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--storage",
        "postgres",
        "--postgres-dsn",
        "postgres://localhost/aux_storage",
        "--postgres-max-pool-size",
        "7",
        "--postgres-tls",
        "false",
        "--port",
        "3010",
    ])
    .expect("launch config from flags");

    assert_eq!(launch.effective.bind_addr, "0.0.0.0:3010");
    assert!(launch.effective.backends.sqlite.is_none());
    let postgres = launch
        .effective
        .backends
        .postgres
        .as_ref()
        .expect("postgres backend");
    assert_eq!(postgres.dsn, "postgres://localhost/aux_storage");
    assert_eq!(postgres.max_pool_size, 7);
    assert!(!postgres.tls);
}

#[test]
fn storage_api_launch_config_overrides_win_over_top_level_flags() {
    let launch = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--port",
        "3010",
        "--overrides",
        "http.bind_addr=127.0.0.1:9000",
    ])
    .expect("launch config with overrides");

    assert_eq!(launch.effective.bind_addr, "127.0.0.1:9000");
}

#[test]
fn storage_api_launch_config_splits_escaped_override_assignments() {
    let launch = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--overrides",
        r#"description=one\,two,http.bind_addr=127.0.0.1:9001"#,
    ])
    .expect("launch config with escaped comma");

    assert_eq!(
        launch.effective.root.description.as_deref(),
        Some("one,two")
    );
    assert_eq!(launch.effective.bind_addr, "127.0.0.1:9001");
}

#[test]
fn storage_api_launch_config_supports_escaped_equals_in_override_values() {
    let launch = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--overrides",
        r#"description=left\=right"#,
    ])
    .expect("launch config with escaped equals");

    assert_eq!(
        launch.effective.root.description.as_deref(),
        Some("left=right")
    );
}

#[test]
fn storage_api_launch_config_rejects_empty_override_paths() {
    let error = StorageApiLaunchConfig::from_args(["storage-api", "--overrides", "=value"])
        .expect_err("empty override path should fail");

    assert!(matches!(error, ConfigError::OverrideParse { .. }));
}

#[test]
fn committed_schema_matches_current_model() {
    let generated = crate::Config::schema_json().expect("generate schema");
    let committed = include_str!("../config.schema.json");

    assert_eq!(generated.trim(), committed.trim());
}

#[test]
fn interpolation_resolves_partial_environment_values() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    unsafe {
        std::env::set_var("AUX_CONFIG_TEST_DB_NAME", "interpolated");
    }
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": {
                    "db_path": "/tmp/${AUX_CONFIG_TEST_DB_NAME}.db"
                }
            }
        }
    }));

    let loaded = load(file.path()).expect("load interpolated config");

    assert_eq!(
        loaded
            .root
            .features
            .backends
            .sqlite
            .as_ref()
            .expect("sqlite")
            .db_path,
        "/tmp/interpolated.db"
    );
    unsafe {
        std::env::remove_var("AUX_CONFIG_TEST_DB_NAME");
    }
}

#[test]
fn interpolation_resolves_files_relative_to_config_file() {
    let dir = tempfile::tempdir().expect("create temporary directory");
    fs::write(dir.path().join("dsn.txt"), "postgres://localhost/from-file")
        .expect("write dsn file");
    let config_path = dir.path().join("config.json");
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&json!({
            "features": {
                "backends": {
                    "sqlite": null,
                    "postgres": {
                        "dsn": "file::dsn.txt::"
                    }
                }
            }
        }))
        .expect("serialize config"),
    )
    .expect("write config");

    let loaded = load(&config_path).expect("load config with file interpolation");

    assert_eq!(
        loaded
            .root
            .features
            .backends
            .postgres
            .as_ref()
            .expect("postgres")
            .dsn,
        "postgres://localhost/from-file"
    );
}

#[test]
fn interpolation_reports_missing_environment_values() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    unsafe {
        std::env::remove_var("AUX_CONFIG_TEST_MISSING");
    }
    let file = write_temp_config(json!({
        "features": {
            "backends": {
                "sqlite": {
                    "db_path": "${AUX_CONFIG_TEST_MISSING}"
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("missing env should fail");

    assert!(matches!(error, ConfigError::Interpolation { path, message }
        if path.contains("db_path") && message.contains("AUX_CONFIG_TEST_MISSING")));
}
