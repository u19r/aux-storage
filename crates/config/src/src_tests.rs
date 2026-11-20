use std::{fs, sync::Mutex};

use serde_json::json;

use crate::{
    Backends, ConfigError, DEFAULT_STORAGE_SQLITE_DB_PATH, RootConfig, StorageApiLaunchConfig,
    config_test_support_tests::{temp_dir, write_config},
    launch::collect_storage_admission_environment_overrides,
    load, load_optional_with_overrides,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn loads_storage_defaults() {
    let loaded = load_optional_with_overrides(None, &[]).expect("load defaults");
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
fn queue_and_pubsub_are_off_by_default_and_switch_on_by_override() {
    let loaded = load_optional_with_overrides(None, &[]).expect("load defaults");
    assert!(!loaded.root.queue.enabled);
    assert!(!loaded.root.pubsub.enabled);

    let loaded = load_optional_with_overrides(
        None,
        &[
            ("queue.enabled".to_string(), "true".to_string()),
            ("pubsub.enabled".to_string(), "true".to_string()),
        ],
    )
    .expect("load with switches on");
    assert!(loaded.root.queue.enabled && loaded.root.pubsub.enabled);

    let dir = temp_dir("config-");
    let schema_path = dir.path().join("config.schema.json");
    crate::Config::write_schema_to(&schema_path).expect("write schema");
    let schema: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(schema_path).expect("schema file")).unwrap();
    for (definition, default_off) in [("QueueConfig", true), ("PubsubConfig", true)] {
        let enabled = &schema["$defs"][definition]["properties"]["enabled"];
        assert_eq!(enabled["type"], "boolean", "{definition}");
        assert_eq!(enabled["default"] == false, default_off, "{definition}");
    }
}

#[test]
fn foundationdb_cache_read_version_is_disabled_by_default() {
    let file = write_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "foundationdb": {}
            }
        }
    }));

    let loaded = load(file.path()).expect("load FoundationDB defaults");
    assert_eq!(
        loaded
            .root
            .features
            .backends
            .foundationdb
            .as_ref()
            .expect("FoundationDB backend")
            .cache_read_version_ms,
        0
    );
}

#[test]
fn given_foundationdb_reply_byte_limit_when_loaded_then_default_is_one_mib_and_range_is_enforced() {
    let reply_limit = |settings: serde_json::Value| {
        let file = write_config(json!({
            "features": {"backends": {"sqlite": null, "foundationdb": settings}}
        }));
        load(file.path()).map(|loaded| {
            loaded
                .root
                .features
                .backends
                .foundationdb
                .as_ref()
                .expect("FoundationDB backend")
                .reply_byte_limit
        })
    };
    assert_eq!(reply_limit(json!({})).unwrap(), 1_048_576);
    for accepted in [80_000, 524_288, 4_194_304] {
        assert_eq!(
            reply_limit(json!({"reply_byte_limit": accepted})).unwrap(),
            accepted
        );
    }
    for rejected in [79_999, 4_194_305] {
        assert!(
            reply_limit(json!({"reply_byte_limit": rejected})).is_err(),
            "{rejected}"
        );
    }
}

#[test]
fn foundationdb_cache_read_version_preserves_explicit_values_including_zero() {
    for expected in [50, 25, 0] {
        let file = write_config(json!({
            "features": {
                "backends": {
                    "sqlite": null,
                    "foundationdb": {
                        "cache_read_version_ms": expected
                    }
                }
            }
        }));

        let loaded = load(file.path()).expect("load FoundationDB cache setting");
        assert_eq!(
            loaded
                .root
                .features
                .backends
                .foundationdb
                .as_ref()
                .expect("FoundationDB backend")
                .cache_read_version_ms,
            expected
        );
    }
}

#[test]
fn config_schema_can_be_written_for_runtime_discovery() {
    let dir = temp_dir("config-");
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
    let dir = temp_dir("config-");
    let missing_path = dir.path().join("missing.json");

    let error = load(&missing_path).expect_err("required config file should fail");

    assert!(matches!(error, ConfigError::Io { .. }));
}

#[test]
fn optional_config_file_can_be_missing_when_process_defaults_are_acceptable() {
    let dir = temp_dir("config-");
    let missing_path = dir.path().join("missing.json");

    let loaded =
        load_optional_with_overrides(Some(&missing_path), &[]).expect("missing optional config");

    assert!(loaded.root.features.backends.sqlite.is_some());
}

#[test]
fn loads_postgres_backend() {
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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

fn remote_config_with_timeouts(connect: u64, request: u64) -> tempfile::NamedTempFile {
    write_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "remote": {
                    "endpoint_urls": ["http://127.0.0.1:8080"],
                    "timeout_overrides": {
                        "connect_timeout_ms": connect,
                        "request_timeout_ms": request
                    }
                }
            }
        }
    }))
}

#[test]
fn rejects_remote_connect_timeout_not_below_request_timeout() {
    for (connect, request) in [(500, 500), (900, 500)] {
        let file = remote_config_with_timeouts(connect, request);

        let error = load(file.path()).expect_err("connect >= request should fail");

        let expected = format!(
            "features.backends.remote.timeout_overrides.connect_timeout_ms ({connect}) must be \
             less than request_timeout_ms ({request})"
        );
        assert!(matches!(error, ConfigError::Validation { message }
            if message.contains(&expected)));
    }
}

#[test]
fn accepts_remote_connect_timeout_below_request_timeout() {
    let file = remote_config_with_timeouts(499, 500);

    load(file.path()).expect("connect < request should load");
}

#[test]
fn rejects_remote_credentials_instance_keys() {
    let file = write_config(json!({
        "features": {
            "backends": {
                "sqlite": null,
                "remote": {
                    "endpoint_urls": ["http://127.0.0.1:8080"],
                    "credentials": {
                        "instance_keys": true
                    }
                }
            }
        }
    }));

    let error = load(file.path()).expect_err("instance_keys is not a setting");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("instance_keys")));
}

#[test]
fn cli_overrides_apply() {
    let loaded = load_optional_with_overrides(
        None,
        &[(
            "features.backends.sqlite.db_path".to_string(),
            "\"run-artifacts/config-data/aux-storage.db\"".to_string(),
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
        "run-artifacts/config-data/aux-storage.db"
    );
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
}

#[test]
fn queue_config_accepts_runtime_settings_without_backend_selector() {
    let file = write_config(json!({
        "queue": {
            "account_id": "123456789012",
            "public_base_url": "http://127.0.0.1:3001/queue"
        }
    }));

    let loaded = load(file.path()).expect("load queue runtime config");
    assert_eq!(loaded.root.queue.account_id, "123456789012");
    assert_eq!(
        loaded.root.queue.public_base_url.as_deref(),
        Some("http://127.0.0.1:3001/queue")
    );
}

#[test]
fn service_routes_load_from_http_config() {
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
        "queue": {
            "account_id": " "
        }
    }));

    let error = load(file.path()).expect_err("empty queue account id should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("queue.account_id must not be empty")));
}

#[test]
fn storage_connection_registry_must_name_an_existing_default_connection() {
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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
    let file = write_config(json!({
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
fn empty_backends_keep_default_sqlite() {
    let file = write_config(json!({}));
    let loaded = load(file.path()).expect("load empty config");
    assert!(matches!(
        loaded.root.features.backends,
        Backends {
            initial_stream_lineages: None,
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

    assert_eq!(launch.effective.config.root.http.bind_addr, "0.0.0.0:8080");
    assert!(
        launch
            .effective
            .config
            .root
            .features
            .backends
            .sqlite
            .is_some()
    );
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

    assert_eq!(launch.effective.config.root.http.bind_addr, "0.0.0.0:3010");
    assert!(
        launch
            .effective
            .config
            .root
            .features
            .backends
            .sqlite
            .is_none()
    );
    let postgres = launch
        .effective
        .config
        .root
        .features
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

    assert_eq!(
        launch.effective.config.root.http.bind_addr,
        "127.0.0.1:9000"
    );
}

#[test]
fn storage_admission_flags_and_explicit_overrides_are_applied() {
    let launch = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--storage-admission-enabled",
        "false",
        "--storage-admission-maximum-concurrency",
        "8",
        "--overrides",
        "features.storage_admission.maximum_concurrency=12",
    ])
    .expect("storage admission launch config");

    assert!(
        !launch
            .effective
            .config
            .root
            .features
            .storage_admission
            .enabled
    );
    assert_eq!(
        launch
            .effective
            .config
            .root
            .features
            .storage_admission
            .maximum_concurrency,
        12
    );
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
        launch.effective.config.root.description.as_deref(),
        Some("one,two")
    );
    assert_eq!(
        launch.effective.config.root.http.bind_addr,
        "127.0.0.1:9001"
    );
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
        launch.effective.config.root.description.as_deref(),
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
fn given_default_launch_when_loaded_then_runtime_settings_have_defaults() {
    let launch = StorageApiLaunchConfig::from_args(["storage-api"]).expect("default launch config");
    let runtime = &launch.effective.config.root.features.runtime;

    assert_eq!(runtime.shutdown_grace_seconds, 5);
    assert!(runtime.worker_threads.is_none());
    assert!(!runtime.tokio_console);
}

#[test]
fn given_runtime_overrides_when_launched_then_runtime_settings_are_set() {
    let launch = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--overrides",
        "features.runtime.worker_threads=3,features.runtime.shutdown_grace_seconds=9,features.\
         runtime.tokio_console=true",
    ])
    .expect("launch config with runtime overrides");
    let runtime = &launch.effective.config.root.features.runtime;

    assert_eq!(runtime.worker_threads, std::num::NonZeroUsize::new(3));
    assert_eq!(runtime.shutdown_grace_seconds, 9);
    assert!(runtime.tokio_console);
}

#[test]
fn given_zero_worker_threads_when_launched_then_rejected() {
    let result = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--overrides",
        "features.runtime.worker_threads=0",
    ]);

    assert!(result.is_err());
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
    let file = write_config(json!({
        "features": {
            "backends": {
                "sqlite": {
                    "db_path": "run-artifacts/config-data/${AUX_CONFIG_TEST_DB_NAME}.db"
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
        "run-artifacts/config-data/interpolated.db"
    );
    unsafe {
        std::env::remove_var("AUX_CONFIG_TEST_DB_NAME");
    }
}

#[test]
fn interpolation_resolves_files_relative_to_config_file() {
    let dir = temp_dir("config-");
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
    let file = write_config(json!({
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

#[test]
fn storage_admission_defaults_and_environment_override_are_stable() {
    let loaded = load_optional_with_overrides(None, &[]).expect("load defaults");
    assert_eq!(
        loaded
            .root
            .features
            .storage_admission
            .initial_sustainable_throughput_rps,
        20_000
    );
    assert_eq!(
        loaded.root.features.storage_admission.maximum_concurrency,
        1_024
    );
    let partial = load_optional_with_overrides(
        None,
        &[("features.storage_admission".to_string(), "{}".to_string())],
    )
    .expect("load partial admission defaults");
    assert_eq!(partial.root.features.storage_admission.queue_capacity, 256);

    let overrides = collect_storage_admission_environment_overrides(|name| match name {
        "AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS" => Ok("123".to_string()),
        _ => Err(std::env::VarError::NotPresent),
    })
    .expect("environment override");
    let loaded = load_optional_with_overrides(None, &overrides).expect("load env override");
    assert_eq!(
        loaded
            .root
            .features
            .storage_admission
            .initial_sustainable_throughput_rps,
        123
    );

    let overrides = collect_storage_admission_environment_overrides(|name| match name {
        "AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS" => Ok("123".to_string()),
        "AUX_STORAGE_ADMISSION_INITIAL_SUSTAINABLE_THROUGHPUT_RPS" => Ok("456".to_string()),
        _ => Err(std::env::VarError::NotPresent),
    })
    .expect("both throughput aliases are accepted");
    let loaded = load_optional_with_overrides(None, &overrides).expect("load both aliases");
    assert_eq!(
        loaded
            .root
            .features
            .storage_admission
            .initial_sustainable_throughput_rps,
        456,
        "the canonical admission environment name wins over the legacy shorthand"
    );
}

#[test]
fn storage_admission_environment_rejects_zero_except_queue_capacity() {
    let error = collect_storage_admission_environment_overrides(|name| {
        if name == "AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS" {
            Ok("0".to_string())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    })
    .expect_err("zero throughput must fail");
    assert!(
        error
            .to_string()
            .contains("AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS")
    );

    let overrides = collect_storage_admission_environment_overrides(|name| {
        if name == "AUX_STORAGE_ADMISSION_QUEUE_CAPACITY" {
            Ok("0".to_string())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    })
    .expect("zero queue capacity is valid");
    assert_eq!(overrides[0].1, "0");
}

#[test]
fn storage_deadlines_defaults_override_and_checks_are_stable() {
    let loaded = load_optional_with_overrides(None, &[]).expect("load defaults");
    let deadlines = loaded.root.features.storage_deadlines;
    assert_eq!(
        [
            deadlines.point_read_ms,
            deadlines.range_read_ms,
            deadlines.write_ms,
            deadlines.transaction_ms,
            deadlines.schema_change_ms,
            deadlines.background_page_ms,
            deadlines.long_poll_probe_ms,
            deadlines.commit_grace_ms,
        ],
        [5_000, 10_000, 10_000, 15_000, 30_000, 10_000, 1_000, 2_000]
    );

    let overrides = collect_storage_admission_environment_overrides(|name| match name {
        "AUX_STORAGE_DEADLINES_WRITE_MS" => Ok("750".to_string()),
        _ => Err(std::env::VarError::NotPresent),
    })
    .expect("environment override");
    let loaded = load_optional_with_overrides(None, &overrides).expect("load env override");
    assert_eq!(loaded.root.features.storage_deadlines.write_ms, 750);

    let zero = collect_storage_admission_environment_overrides(|name| {
        if name == "AUX_STORAGE_DEADLINES_COMMIT_GRACE_MS" {
            Ok("0".to_string())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    })
    .expect_err("zero grace must fail");
    assert!(
        zero.to_string()
            .contains("AUX_STORAGE_DEADLINES_COMMIT_GRACE_MS")
    );

    for (path, value) in [
        ("features.storage_deadlines.range_read_ms", "0"),
        ("features.storage_deadlines.long_poll_probe_ms", "5001"),
    ] {
        let error = load_optional_with_overrides(None, &[(path.to_string(), value.to_string())])
            .expect_err("invalid deadline setting must fail");
        assert!(error.to_string().contains("must"), "{path}: {error}");
    }
}

#[test]
fn given_retired_replication_config_when_loaded_then_rejected() {
    for value in [
        json!({"features": {"storage_sync_replication": {"enabled": false}}}),
        json!({"jobs": {"storage_replication": {"enabled": false}}}),
    ] {
        let file = write_config(value);
        assert!(load(file.path()).is_err());
    }
}

/// Review D12: the delivery runner settings load from `pubsub.delivery`.
#[test]
fn pubsub_delivery_config_loads_and_rejects_zero_limits() {
    let file = write_config(json!({
        "pubsub": {
            "public_base_url": "https://sns.example.test/pubsub",
            "delivery": {
                "max_in_flight": 16,
                "per_destination": 2,
                "request_timeout_seconds": 5,
                "lease_seconds": 20,
                "idle_probe_millis": 250
            }
        }
    }));
    let loaded = load(file.path()).expect("load pubsub delivery config");
    let delivery = &loaded.root.pubsub.delivery;
    assert_eq!(
        (
            delivery.max_in_flight,
            delivery.per_destination,
            delivery.request_timeout_seconds,
            delivery.lease_seconds,
            delivery.idle_probe_millis
        ),
        (16, 2, 5, 20, 250)
    );
    assert_eq!(
        loaded.root.pubsub.public_base_url.as_deref(),
        Some("https://sns.example.test/pubsub")
    );
}

/// Finding 9: every delivery rule is checked at load, so no runner starts
/// with a rule broken. Before the change the loader accepted a lease of 20 s
/// with a 10 s request timeout and the service failed later at startup.
#[test]
fn pubsub_delivery_rules_reject_at_load() {
    let cases = [
        (
            json!({"delivery": {"lease_seconds": 20, "request_timeout_seconds": 10}}),
            "lease_seconds must be at least 2 x request_timeout_seconds + 5",
        ),
        (
            json!({"delivery": {"request_timeout_seconds": 0}}),
            "request_timeout_seconds must be 1 to 60",
        ),
        (
            json!({"delivery": {"request_timeout_seconds": 61, "lease_seconds": 200}}),
            "request_timeout_seconds must be 1 to 60",
        ),
        (
            json!({"delivery": {"lease_seconds": 901}}),
            "lease_seconds must be at most 900",
        ),
        (
            json!({"delivery": {"idle_probe_millis": 9}}),
            "idle_probe_millis must be 10 to 60000",
        ),
        (
            json!({"delivery": {"max_in_flight": 1025}}),
            "max_in_flight must be 1 to 1024",
        ),
        (
            json!({"delivery": {"max_in_flight": 0}}),
            "max_in_flight must be 1 to 1024",
        ),
        (
            json!({"delivery": {"max_in_flight": 4, "per_destination": 5}}),
            "per_destination must be 1 to max_in_flight",
        ),
        (
            json!({"delivery": {"per_destination": 0}}),
            "per_destination must be 1 to max_in_flight",
        ),
        (
            json!({"public_base_url": "sns.example.test/pubsub"}),
            "public_base_url must be an http or https URL",
        ),
        (
            json!({"public_base_url": "ftp://sns.example.test"}),
            "public_base_url must be an http or https URL",
        ),
    ];
    for (pubsub, rule) in cases {
        let file = write_config(json!({ "pubsub": pubsub }));
        let error = load(file.path()).expect_err(rule);
        assert!(
            matches!(&error, ConfigError::Validation { message } if message.contains(rule)),
            "{rule}: {error:?}"
        );
    }
}

#[test]
fn given_initial_lineage_configuration_when_deserialized_then_count_is_bounded_and_optional() {
    for count in [0, 129, 65536] {
        let value = serde_json::json!({"initial_stream_lineages": count});
        assert!(serde_json::from_value::<crate::Backends>(value).is_err());
    }
    for count in [1, 4, 128] {
        let value = serde_json::json!({"initial_stream_lineages": count});
        let backends = serde_json::from_value::<crate::Backends>(value).unwrap();
        assert_eq!(backends.initial_stream_lineages.unwrap().get(), count);
    }
    let backends = serde_json::from_str::<crate::Backends>("{}").unwrap();
    assert!(backends.initial_stream_lineages.is_none());
}

#[test]
fn given_lineage_count_schema_when_generated_then_bounds_match_deserialization() {
    let schema = schemars::schema_for!(storage_types::InitialStreamLineages);
    let value = serde_json::to_value(schema).unwrap();
    assert_eq!(value["minimum"], 1);
    assert_eq!(
        value["maximum"],
        storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES
    );
}

fn postgres_config_with(extra: Option<(&str, serde_json::Value)>) -> serde_json::Value {
    let mut postgres = json!({"dsn": "postgres://localhost/db"});
    if let Some((key, value)) = extra {
        postgres[key] = value;
    }
    json!({"features": {"backends": {"sqlite": null, "postgres": postgres}}})
}

#[test]
fn given_deleted_settings_when_loaded_then_rejected() {
    let mut cases = vec![
        json!({"version": "1.0"}),
        json!({"roles": ["api"]}),
        json!({"http": {"trusted_proxy_ips": ["127.0.0.1"]}}),
        json!({"http": {"slow_operation_log_ms": {"default": 500}}}),
        json!({"features": {"tracing": {"log_destination": "stdout"}}}),
        json!({"features": {"tracing": {"traces": []}}}),
        json!({"features": {"metrics": {"prometheus": {}}}}),
        json!({"features": {"runtime": {"environment": "production"}}}),
        json!({"jobs": {"max_immediate_workers": 8}}),
        json!({"jobs": {"jitter_percent": 10}}),
        json!({"queue": {"visibility_timeout_seconds": 30}}),
        json!({"queue": {"message_retention_seconds": 345600}}),
        json!({"features": {"backends": {"sqlite": {"busy_timeout_ms": 5000}}}}),
        json!({"features": {"backends": {"sqlite": null, "turso": {"busy_timeout_ms": 5000}}}}),
        json!({"features": {"backends": {"sqlite": {"immediate_gsi_consistency": true}}}}),
        json!({"features": {"backends": {"sqlite": null, "rocksdb": {"immediate_gsi_consistency": true}}}}),
        json!({"features": {"backends": {"sqlite": null, "foundationdb": {"report_conflicting_keys": true}}}}),
        json!({"features": {"backends": {"sqlite": null, "foundationdb": {"immediate_gsi_consistency": true}}}}),
        json!({"features": {"backends": {"sqlite": null, "remote": {"endpoint_urls": ["http://127.0.0.1:1"], "default_storage_mode": "dedicated"}}}}),
    ];
    for (key, value) in [
        ("connect_timeout_ms", json!(3000)),
        ("keepalives_idle_ms", json!(5000)),
        ("keepalives_interval_ms", json!(2000)),
        ("keepalives_retries", json!(3)),
        ("tcp_user_timeout_ms", json!(10000)),
        ("background_max_pool_size", json!(4)),
        ("immediate_gsi_consistency", json!(true)),
    ] {
        cases.push(postgres_config_with(Some((key, value))));
    }

    let control = write_config(postgres_config_with(None));
    assert!(
        load(control.path()).is_ok(),
        "postgres base config must load so rejection is the key"
    );

    for case in cases {
        let file = write_config(case.clone());
        let error = load(file.path()).err();
        assert!(
            matches!(error, Some(ConfigError::Validation { .. })),
            "expected validation error for {case}, got {error:?}"
        );
    }
}

#[test]
fn given_deleted_setting_override_when_loaded_then_rejected() {
    for path in [
        "features.backends.foundationdb.report_conflicting_keys",
        "features.backends.sqlite.immediate_gsi_consistency",
    ] {
        let result = load_optional_with_overrides(None, &[(path.to_string(), "true".to_string())]);
        assert!(result.is_err(), "override {path} must be rejected");
    }
}

#[test]
fn given_removed_background_pool_flag_when_parsed_then_rejected() {
    let error = StorageApiLaunchConfig::from_args([
        "storage-api",
        "--postgres-background-max-pool-size",
        "2",
    ])
    .err();

    assert!(matches!(error, Some(ConfigError::Argument { .. })));
}

#[test]
fn committed_profiles_load() {
    for path in [
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/dev/remote/storage-api.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/prod/k8s/foundationdb/storage-api.json"
        ),
    ] {
        let result = load(std::path::Path::new(path));
        assert!(result.is_ok(), "{path} must load: {:?}", result.err());
    }
}
