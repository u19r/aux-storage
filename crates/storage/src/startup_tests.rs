use std::collections::HashMap;

use storage_provider::{RemoteCredentialStrategy, StorageBackend};
#[cfg(feature = "sqlite")]
use storage_types::TableName;

use crate::startup::{storage_config_from_backends, storage_connection_registry_from_features};

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_startup_when_factory_initializes_then_system_tables_exist_and_customer_tables_do_not()
 {
    let features = config::Features {
        backends: config::Backends {
            initial_stream_lineages: Some(storage_types::InitialStreamLineages::FOUR),
            sqlite: Some(config::SqliteBackendConfig {
                db_path: ":memory:".to_string(),
            }),
            turso: None,
            postgres: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        },
        ..config::Features::default()
    };

    let root = config::RootConfig {
        features,
        ..config::RootConfig::default()
    };
    let manager = crate::startup::RuntimeConfig::from_file(&root)
        .expect("file translation")
        .open()
        .await
        .expect("native sqlite startup");

    let scope = manager.get_system_stream_scope_for_control().await.unwrap();
    let page = manager
        .get_system_stream_page_for_control(
            stream_provider::SystemStreamPageRequest::try_new(scope, None, 100).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.issued_checkpoint().checkpoint().frontier().len(), 4);

    for table_name in [
        crate::Tables::sys_namespaces(),
        crate::Tables::sys_analytics(),
    ] {
        assert!(
            manager
                .table_exists(&table_name)
                .await
                .expect("system table lookup")
        );
    }
    assert!(
        !manager
            .table_exists(&TableName::new("ordinary_table"))
            .await
            .expect("customer table lookup")
    );
}

#[test]
fn turso_backend_maps_to_turso_storage_config() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: Some(config::TursoBackendConfig {
            db_path: "turso-main.db".to_string(),
        }),
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    let storage_cfg = storage_config_from_backends(&backends).expect("storage config");
    assert!(
        matches!(storage_cfg.backend_type, StorageBackend::Turso),
        "expected turso backend, got {:?}",
        storage_cfg.backend_type
    );
    assert_eq!(
        storage_cfg.connection_string.as_deref(),
        Some("turso-main.db")
    );
    assert!(storage_cfg.turso.is_some());
}

#[test]
fn remote_backend_propagates_settings() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(config::RemoteBackendConfig {
            endpoint_urls: vec!["http://127.0.0.1:8123".to_string()],
            region: None,
            tls: false,
            credentials: None,
            timeout_overrides: Some(config::RemoteTimeoutOverrides {
                connect_timeout_ms: Some(150),
                request_timeout_ms: Some(900),
            }),
        }),
    };

    let storage_cfg = storage_config_from_backends(&backends).expect("storage config");
    assert!(
        matches!(storage_cfg.backend_type, StorageBackend::Remote),
        "expected remote backend, got {:?}",
        storage_cfg.backend_type
    );
    let remote = storage_cfg.remote.expect("remote settings");
    assert_eq!(
        remote.endpoint_urls,
        vec!["http://127.0.0.1:8123".to_string()]
    );
    assert!(!remote.tls, "expected TLS disabled from config");
    assert!(
        matches!(remote.credentials, RemoteCredentialStrategy::DefaultChain),
        "expected default credential strategy"
    );
    let timeouts_cfg = remote.timeouts.expect("timeouts populated");
    assert_eq!(
        timeouts_cfg.connect_timeout_ms,
        Some(150),
        "connect timeout propagated"
    );
    assert_eq!(
        timeouts_cfg.request_timeout_ms,
        Some(900),
        "request timeout propagated"
    );
}

#[test]
fn sqlite_backend_maps_path_to_sqlite_storage_config() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: Some(config::SqliteBackendConfig {
            db_path: "storage.db".to_string(),
        }),
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    let storage_cfg = storage_config_from_backends(&backends).expect("storage config");

    assert!(matches!(storage_cfg.backend_type, StorageBackend::SQLite));
    assert_eq!(storage_cfg.connection_string.as_deref(), Some("storage.db"));
    assert!(storage_cfg.sqlite.is_some());
}

#[test]
fn postgres_backend_propagates_pool_and_tls_settings_without_connecting() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: Some(config::PostgresBackendConfig {
            dsn: "postgres://localhost/aux_storage".to_string(),
            max_pool_size: 7,
            tls: false,
        }),
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    let storage_cfg = storage_config_from_backends(&backends).expect("storage config");

    assert!(matches!(storage_cfg.backend_type, StorageBackend::Postgres));
    assert_eq!(
        storage_cfg.connection_string.as_deref(),
        Some("postgres://localhost/aux_storage")
    );
    let postgres = storage_cfg.postgres.expect("postgres settings");
    assert_eq!(postgres.dsn, "postgres://localhost/aux_storage");
    assert_eq!(postgres.max_pool_size, 7);
    assert!(!postgres.tls);
}

#[test]
fn rocksdb_backend_propagates_path_without_opening_the_store() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: Some(config::RocksdbBackendConfig {
            db_path: "rocks-storage".to_string(),
            ..Default::default()
        }),
        foundationdb: None,
        remote: None,
    };

    let storage_cfg = storage_config_from_backends(&backends).expect("storage config");

    assert!(matches!(storage_cfg.backend_type, StorageBackend::RocksDB));
    assert_eq!(
        storage_cfg.connection_string.as_deref(),
        Some("rocks-storage")
    );
    assert!(storage_cfg.rocksdb.is_some());
}

#[test]
fn remote_backend_uses_static_credentials_when_the_config_supplies_them() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(config::RemoteBackendConfig {
            endpoint_urls: vec!["http://127.0.0.1:8123".to_string()],
            region: Some("us-east-1".to_string()),
            tls: true,
            credentials: Some(config::RemoteCredentialsConfig {
                r#static: Some(config::RemoteStaticCredentialsConfig {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: Some("session".to_string()),
                }),
            }),
            timeout_overrides: None,
        }),
    };

    let storage_cfg = storage_config_from_backends(&backends).expect("storage config");
    let remote = storage_cfg.remote.expect("remote settings");

    assert!(matches!(storage_cfg.backend_type, StorageBackend::Remote));
    assert!(remote.tls);
    match remote.credentials {
        RemoteCredentialStrategy::Static(credentials) => {
            assert_eq!(credentials.access_key_id, "access");
            assert_eq!(credentials.secret_access_key, "secret");
            assert_eq!(credentials.session_token.as_deref(), Some("session"));
        }
        RemoteCredentialStrategy::DefaultChain => panic!("expected static credentials"),
    }
}

#[test]
fn remote_backend_rejects_an_endpointless_remote_configuration_before_runtime_startup() {
    let backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(config::RemoteBackendConfig {
            endpoint_urls: vec![],
            region: Some("us-east-1".to_string()),
            tls: true,
            credentials: None,
            timeout_overrides: None,
        }),
    };

    let error =
        storage_config_from_backends(&backends).expect_err("endpointless remote should fail");

    assert!(format!("{error}").contains("remote storage requires at least one endpoint URL"));
}

#[test]
fn storage_registry_uses_the_default_backend_when_no_named_connections_are_configured() {
    let features = config::Features {
        backends: config::Backends {
            initial_stream_lineages: None,
            sqlite: Some(config::SqliteBackendConfig {
                db_path: "default.db".to_string(),
            }),
            turso: None,
            postgres: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        },
        ..config::Features::default()
    };

    let registry = storage_connection_registry_from_features(&features).expect("registry");

    assert_eq!(registry.default_connection_id, "default");
    assert!(matches!(
        registry.connections["default"].backend_type,
        StorageBackend::SQLite
    ));
}

#[test]
fn storage_registry_uses_named_connections_and_preserves_the_declared_default() {
    let features = config::Features {
        storage_connections: Some(config::StorageConnectionsConfig {
            default_connection: "tenant_b".to_string(),
            connections: HashMap::from([
                (
                    "tenant_a".to_string(),
                    config::Backends {
                        initial_stream_lineages: None,
                        sqlite: Some(config::SqliteBackendConfig {
                            db_path: "tenant-a.db".to_string(),
                        }),
                        turso: None,
                        postgres: None,
                        rocksdb: None,
                        foundationdb: None,
                        remote: None,
                    },
                ),
                (
                    "tenant_b".to_string(),
                    config::Backends {
                        initial_stream_lineages: None,
                        sqlite: Some(config::SqliteBackendConfig {
                            db_path: "tenant-b.db".to_string(),
                        }),
                        turso: None,
                        postgres: None,
                        rocksdb: None,
                        foundationdb: None,
                        remote: None,
                    },
                ),
            ]),
        }),
        ..config::Features::default()
    };

    let registry = storage_connection_registry_from_features(&features).expect("registry");

    assert_eq!(registry.default_connection_id, "tenant_b");
    assert_eq!(registry.connections.len(), 2);
    assert_eq!(
        registry.connections["tenant_b"]
            .connection_string
            .as_deref(),
        Some("tenant-b.db")
    );
}

#[test]
fn storage_registry_rejects_a_default_connection_that_is_not_declared() {
    let features = config::Features {
        storage_connections: Some(config::StorageConnectionsConfig {
            default_connection: "missing".to_string(),
            connections: HashMap::from([(
                "tenant_a".to_string(),
                config::Backends {
                    initial_stream_lineages: None,
                    sqlite: Some(config::SqliteBackendConfig {
                        db_path: "tenant-a.db".to_string(),
                    }),
                    turso: None,
                    postgres: None,
                    rocksdb: None,
                    foundationdb: None,
                    remote: None,
                },
            )]),
        }),
        ..config::Features::default()
    };

    let error =
        storage_connection_registry_from_features(&features).expect_err("missing default fails");

    assert!(format!("{error}").contains(
        "default connection 'missing' missing in features.storage_connections.connections"
    ));
}

#[test]
fn given_provider_lineage_defaults_when_connection_override_is_resolved_then_policy_is_shared() {
    use storage_types::InitialStreamLineages;
    for (backend, expected) in [
        (StorageBackend::SQLite, 1),
        (StorageBackend::Turso, 1),
        (StorageBackend::RocksDB, 1),
        (StorageBackend::Postgres, 4),
        (StorageBackend::FoundationDb, 4),
    ] {
        assert_eq!(backend.default_initial_stream_lineages().get(), expected);
    }
    let mut backends = config::Backends::sqlite_default();
    backends.initial_stream_lineages = Some(InitialStreamLineages::FOUR);
    let config = storage_config_from_backends(&backends).unwrap();
    assert_eq!(
        config.initial_stream_lineages,
        Some(InitialStreamLineages::FOUR)
    );
}
