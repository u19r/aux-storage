use config::{
    Backends, FoundationdbBackendConfig, PostgresBackendConfig, RemoteBackendConfig,
    RemoteDefaultStorageMode, RocksdbBackendConfig, SqliteBackendConfig, TursoBackendConfig,
};
use pubsub_provider::{SubscribeRequest, SubscriptionProtocol};

#[cfg(feature = "rocksdb")]
use super::app::rocksdb_pubsub_db_path;
#[cfg(feature = "turso")]
use super::app::turso_pubsub_db_path;
use super::app::{
    Args, PubsubStorageArg, create_pubsub_runtime_providers, ensure_no_remote_pubsub_backend,
    initialize_pubsub_runtime, parse_override_args, pubsub_backend_override_path, pubsub_bind_addr,
    pubsub_config_overrides, pubsub_db_path_override_path, selected_pubsub_backend,
    sqlite_pubsub_db_path,
};

fn base_args(storage: Option<PubsubStorageArg>) -> Args {
    Args {
        storage,
        db_path: None,
        host: None,
        port: None,
        foundationdb_cluster_file: None,
        foundationdb_subspace_prefix: None,
        foundationdb_tenant_name: None,
        foundationdb_cache_read_version_ms: None,
        foundationdb_report_conflicting_keys: false,
        config: None,
        overrides: Vec::new(),
    }
}

#[tokio::test]
async fn given_standalone_runtime_when_subscribing_queue_then_protocol_is_available() {
    let backends = standalone_fixture_backends();
    let providers = create_pubsub_runtime_providers(&backends)
        .await
        .expect("standalone providers should initialize");
    let manager = initialize_pubsub_runtime(providers)
        .await
        .expect("standalone manager should initialize");
    let topic = manager
        .create_topic("standalone-queue-subscription")
        .await
        .expect("topic should be created");

    let subscription = manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn,
            protocol: SubscriptionProtocol::Queue,
            endpoint: "queue://standalone-subscriber".to_string(),
            attributes: Default::default(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .expect("queue protocol should be available");

    assert!(!subscription.subscription_arn.is_empty());
}

#[cfg(all(feature = "turso", not(feature = "sqlite")))]
fn standalone_fixture_backends() -> Backends {
    Backends {
        sqlite: None,
        turso: Some(TursoBackendConfig {
            db_path: ":memory:".to_string(),
            ..TursoBackendConfig::default()
        }),
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(all(
    feature = "postgres",
    not(feature = "sqlite"),
    not(feature = "turso"),
    not(feature = "rocksdb"),
    not(feature = "foundationdb")
))]
fn standalone_fixture_backends() -> Backends {
    let dsn = std::env::var("TEST_POSTGRES_DSN")
        .expect("TEST_POSTGRES_DSN is required for PostgreSQL pubsub fixture");
    Backends {
        sqlite: None,
        turso: None,
        postgres: Some(PostgresBackendConfig {
            dsn,
            max_pool_size: 4,
            background_max_pool_size: 4,
            tls: false,
            immediate_gsi_consistency: false,
        }),
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(not(any(
    all(feature = "turso", not(feature = "sqlite")),
    all(
        feature = "postgres",
        not(feature = "sqlite"),
        not(feature = "turso"),
        not(feature = "rocksdb"),
        not(feature = "foundationdb")
    )
)))]
fn standalone_fixture_backends() -> Backends {
    Backends {
        sqlite: Some(SqliteBackendConfig {
            db_path: ":memory:".to_string(),
            immediate_gsi_consistency: false,
        }),
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[test]
fn standalone_fixture_selects_the_only_compiled_sql_backend() {
    let backends = standalone_fixture_backends();
    let turso_only = cfg!(all(feature = "turso", not(feature = "sqlite")));
    let postgres_only = cfg!(all(
        feature = "postgres",
        not(feature = "sqlite"),
        not(feature = "turso"),
        not(feature = "rocksdb"),
        not(feature = "foundationdb")
    ));
    assert_eq!(backends.turso.is_some(), turso_only);
    assert_eq!(backends.sqlite.is_some(), !(turso_only || postgres_only));
    assert_eq!(backends.postgres.is_some(), postgres_only);
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocksdb_runtime_when_creating_topic_then_topic_is_persisted() {
    let directory = std::env::temp_dir().join(format!("pubsub-rocksdb-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&directory).expect("temporary RocksDB directory");
    let backends = Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: Some(RocksdbBackendConfig {
            db_path: directory.display().to_string(),
            immediate_gsi_consistency: false,
        }),
        foundationdb: None,
        remote: None,
    };
    let providers = create_pubsub_runtime_providers(&backends)
        .await
        .expect("RocksDB providers should initialize");
    let manager = initialize_pubsub_runtime(providers)
        .await
        .expect("RocksDB manager should initialize");

    manager
        .create_topic("rocksdb-topic")
        .await
        .expect("RocksDB topic creation should succeed");
    std::fs::remove_dir_all(directory).expect("remove temporary RocksDB directory");
}

#[test]
fn pubsub_bind_addr_prefers_explicit_host_and_port_over_config() {
    let mut args = base_args(None);
    args.host = Some("127.0.0.1".to_string());
    args.port = Some(9999);

    let addr = pubsub_bind_addr(&args, "0.0.0.0:9466").expect("bind addr should parse");

    assert_eq!(addr.to_string(), "127.0.0.1:9999");
}

#[test]
fn pubsub_bind_addr_uses_service_default_port_for_host_only() {
    let mut args = base_args(None);
    args.host = Some("127.0.0.1".to_string());

    let addr = pubsub_bind_addr(&args, "0.0.0.0:1234").expect("bind addr should parse");

    assert_eq!(addr.to_string(), "127.0.0.1:9466");
}

#[test]
fn pubsub_storage_selection_nulls_other_backends_then_enables_selected_backend() {
    let args = base_args(Some(PubsubStorageArg::RocksDb));

    let overrides = pubsub_config_overrides(&args);

    assert!(overrides.contains(&("features.backends.remote".to_string(), "null".to_string())));
    assert!(overrides.contains(&("features.backends.rocksdb".to_string(), "{}".to_string())));
}

#[test]
fn pubsub_db_path_override_targets_backend_specific_path_when_supported() {
    assert_eq!(
        pubsub_db_path_override_path(Some(&PubsubStorageArg::SQLite)),
        "features.backends.sqlite.db_path"
    );
    assert_eq!(
        pubsub_db_path_override_path(Some(&PubsubStorageArg::Turso)),
        "features.backends.turso.db_path"
    );
    assert_eq!(
        pubsub_db_path_override_path(Some(&PubsubStorageArg::RocksDb)),
        "features.backends.rocksdb.db_path"
    );
}

#[test]
fn pubsub_config_overrides_include_foundationdb_runtime_options() {
    let mut args = base_args(Some(PubsubStorageArg::FoundationDb));
    args.foundationdb_cluster_file = Some("cluster-file".to_string());
    args.foundationdb_subspace_prefix = Some("pubsub".to_string());
    args.foundationdb_tenant_name = Some("tenant".to_string());
    args.foundationdb_cache_read_version_ms = Some(25);
    args.foundationdb_report_conflicting_keys = true;

    let overrides = pubsub_config_overrides(&args);

    assert!(overrides.contains(&(
        "features.backends.foundationdb.cluster_file".to_string(),
        "cluster-file".to_string(),
    )));
    assert!(overrides.contains(&(
        "features.backends.foundationdb.subspace_prefix".to_string(),
        "pubsub".to_string(),
    )));
    assert!(overrides.contains(&(
        "features.backends.foundationdb.tenant_name".to_string(),
        "tenant".to_string(),
    )));
    assert!(overrides.contains(&(
        "features.backends.foundationdb.cache_read_version_ms".to_string(),
        "25".to_string(),
    )));
    assert!(overrides.contains(&(
        "features.backends.foundationdb.report_conflicting_keys".to_string(),
        "true".to_string(),
    )));
}

#[test]
fn pubsub_cli_explicit_zero_preserves_the_grv_cache_opt_out() {
    let mut args = base_args(Some(PubsubStorageArg::FoundationDb));
    args.foundationdb_cache_read_version_ms = Some(0);

    let overrides = pubsub_config_overrides(&args);

    assert!(overrides.contains(&(
        "features.backends.foundationdb.cache_read_version_ms".to_string(),
        "0".to_string(),
    )));
}

#[test]
fn pubsub_parse_override_args_requires_path_value_pairs() {
    let parsed = parse_override_args(&["features.backends.sqlite=null".to_string()])
        .expect("override should parse");
    let missing_equals = parse_override_args(&["features.backends.sqlite".to_string()])
        .expect_err("missing equals should fail");
    let empty_path =
        parse_override_args(&["=null".to_string()]).expect_err("empty path should fail");

    assert_eq!(
        parsed,
        vec![("features.backends.sqlite".to_string(), "null".to_string())]
    );
    assert!(missing_equals.to_string().contains("PATH=VALUE"));
    assert!(empty_path.to_string().contains("path must not be empty"));
}

#[test]
fn selected_pubsub_backend_prefers_explicit_non_default_backends() {
    let mut backends = Backends::default();
    assert!(matches!(
        selected_pubsub_backend(&backends),
        PubsubStorageArg::SQLite
    ));

    backends.turso = Some(TursoBackendConfig::default());
    assert!(matches!(
        selected_pubsub_backend(&backends),
        PubsubStorageArg::Turso
    ));

    backends.turso = None;
    backends.postgres = Some(PostgresBackendConfig {
        dsn: "postgres://localhost/pubsub".to_string(),
        max_pool_size: 4,
        background_max_pool_size: 2,
        tls: false,
        immediate_gsi_consistency: false,
    });
    assert!(matches!(
        selected_pubsub_backend(&backends),
        PubsubStorageArg::Postgres
    ));

    backends.postgres = None;
    backends.rocksdb = Some(RocksdbBackendConfig::default());
    assert!(matches!(
        selected_pubsub_backend(&backends),
        PubsubStorageArg::RocksDb
    ));

    backends.rocksdb = None;
    backends.foundationdb = Some(FoundationdbBackendConfig::default());
    assert!(matches!(
        selected_pubsub_backend(&backends),
        PubsubStorageArg::FoundationDb
    ));
}

#[test]
fn pubsub_backend_override_path_matches_cli_storage_variants() {
    assert_eq!(
        pubsub_backend_override_path(&PubsubStorageArg::Postgres),
        "features.backends.postgres"
    );
}

#[test]
fn pubsub_backend_build_helpers_resolve_backend_paths_without_opening_providers() {
    let backends = Backends {
        sqlite: Some(SqliteBackendConfig {
            db_path: "pubsub.sqlite".to_string(),
            immediate_gsi_consistency: false,
        }),
        turso: Some(TursoBackendConfig {
            db_path: "pubsub.turso".to_string(),
            immediate_gsi_consistency: false,
        }),
        postgres: None,
        rocksdb: Some(RocksdbBackendConfig {
            db_path: "pubsub.rocks".to_string(),
            immediate_gsi_consistency: false,
        }),
        foundationdb: None,
        remote: None,
    };

    assert_eq!(sqlite_pubsub_db_path(&backends), "pubsub.sqlite");
    #[cfg(feature = "turso")]
    assert_eq!(turso_pubsub_db_path(&backends), "pubsub.turso");
    #[cfg(feature = "rocksdb")]
    assert_eq!(rocksdb_pubsub_db_path(&backends), "pubsub.rocks");
}

#[test]
fn pubsub_backend_build_helpers_fall_back_to_default_paths() {
    let backends = Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    assert_eq!(sqlite_pubsub_db_path(&backends), "storage.sqlite3");
    #[cfg(feature = "turso")]
    assert_eq!(turso_pubsub_db_path(&backends), "storage.turso.db");
    #[cfg(feature = "rocksdb")]
    assert_eq!(rocksdb_pubsub_db_path(&backends), "storage.rocksdb");
}

#[test]
fn pubsub_backend_builder_rejects_remote_backend_without_linked_provider() {
    let backends = Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(RemoteBackendConfig {
            endpoint_urls: vec!["https://pubsub.example.test".to_string()],
            region: Some("us-east-1".to_string()),
            tls: true,
            credentials: None,
            default_storage_mode: RemoteDefaultStorageMode::Dedicated,
            timeout_overrides: None,
        }),
    };

    let error =
        ensure_no_remote_pubsub_backend(&backends).expect_err("remote pubsub backend should fail");

    assert!(
        error
            .to_string()
            .contains("no remote SNS pubsub provider is linked"),
        "unexpected error: {error}"
    );
}
