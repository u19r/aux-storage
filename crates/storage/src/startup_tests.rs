use std::{collections::HashMap, num::NonZeroUsize};

use storage_provider::{RemoteCredentialStrategy, StorageBackend};

use crate::startup::{
    authoritative_cache_options_from_features, point_read_cache_from_features,
    query_proof_cache_from_features, resolve_point_read_cache_max_bytes_for,
    storage_config_from_backends, storage_connection_registry_from_features,
};

#[cfg(feature = "turso")]
#[test]
fn turso_backend_maps_to_turso_storage_config() {
    let backends = config::Backends {
        sqlite: None,
        turso: Some(config::TursoBackendConfig {
            db_path: "turso-main.db".to_string(),
            immediate_gsi_consistency: true,
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
    assert!(
        storage_cfg
            .turso
            .as_ref()
            .is_some_and(|settings| settings.immediate_gsi_consistency)
    );
}

#[cfg(not(feature = "turso"))]
#[test]
fn turso_backend_requires_turso_feature() {
    let backends = config::Backends {
        sqlite: None,
        turso: Some(config::TursoBackendConfig {
            db_path: "turso-main.db".to_string(),
            immediate_gsi_consistency: false,
        }),
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    let err = storage_config_from_backends(&backends).expect_err("missing turso feature");

    assert!(err.to_string().contains(
        "configuration selects turso backend but binary was built without turso support"
    ));
}

#[test]
fn remote_backend_propagates_settings() {
    let backends = config::Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(config::RemoteBackendConfig {
            endpoint_urls: vec!["http://127.0.0.1:8123".to_string()],
            region: None,
            tls: false,
            default_storage_mode: config::RemoteDefaultStorageMode::Dedicated,
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
fn sqlite_backend_propagates_the_immediate_gsi_consistency_rule() {
    let backends = config::Backends {
        sqlite: Some(config::SqliteBackendConfig {
            db_path: "storage.db".to_string(),
            immediate_gsi_consistency: true,
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
    assert!(
        storage_cfg
            .sqlite
            .expect("sqlite settings")
            .immediate_gsi_consistency
    );
}

#[test]
fn postgres_backend_propagates_pool_tls_and_consistency_settings_without_connecting() {
    let backends = config::Backends {
        sqlite: None,
        turso: None,
        postgres: Some(config::PostgresBackendConfig {
            dsn: "postgres://localhost/aux_storage".to_string(),
            max_pool_size: 7,
            tls: false,
            immediate_gsi_consistency: true,
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
    assert!(postgres.immediate_gsi_consistency);
}

#[test]
fn rocksdb_backend_propagates_path_and_consistency_settings_without_opening_the_store() {
    let backends = config::Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: Some(config::RocksdbBackendConfig {
            db_path: "rocks-storage".to_string(),
            immediate_gsi_consistency: true,
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
    assert!(
        storage_cfg
            .rocksdb
            .expect("rocksdb settings")
            .immediate_gsi_consistency
    );
}

#[test]
fn remote_backend_uses_static_credentials_when_the_config_supplies_them() {
    let backends = config::Backends {
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(config::RemoteBackendConfig {
            endpoint_urls: vec!["http://127.0.0.1:8123".to_string()],
            region: Some("us-east-1".to_string()),
            tls: true,
            default_storage_mode: config::RemoteDefaultStorageMode::Dedicated,
            credentials: Some(config::RemoteCredentialsConfig {
                r#static: Some(config::RemoteStaticCredentialsConfig {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: Some("session".to_string()),
                }),
                instance_keys: None,
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
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(config::RemoteBackendConfig {
            endpoint_urls: vec![],
            region: Some("us-east-1".to_string()),
            tls: true,
            default_storage_mode: config::RemoteDefaultStorageMode::Dedicated,
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
            sqlite: Some(config::SqliteBackendConfig {
                db_path: "default.db".to_string(),
                immediate_gsi_consistency: false,
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
                        sqlite: Some(config::SqliteBackendConfig {
                            db_path: "tenant-a.db".to_string(),
                            immediate_gsi_consistency: false,
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
                        sqlite: Some(config::SqliteBackendConfig {
                            db_path: "tenant-b.db".to_string(),
                            immediate_gsi_consistency: true,
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
                    sqlite: Some(config::SqliteBackendConfig {
                        db_path: "tenant-a.db".to_string(),
                        immediate_gsi_consistency: false,
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
fn storage_point_read_cache_defaults_to_disabled() {
    let cache = point_read_cache_from_features(&config::Features::default());

    assert!(!cache.is_enabled());
}

#[test]
fn storage_point_read_cache_can_be_enabled_from_features() {
    let features = config::Features {
        storage_point_read_cache: config::StoragePointReadCacheConfig {
            enabled: true,
            capacity: 123,
            max_bytes: Some(456),
            memory_budget: config::StoragePointReadCacheMemoryBudgetConfig {
                mode: config::StoragePointReadCacheMemoryBudgetMode::AutoPerCore {
                    megabytes_per_core: 100,
                },
            },
            ttl_seconds: 45,
            eviction_policy: config::StoragePointReadCacheEvictionPolicy::TwoQueue,
            ..config::StoragePointReadCacheConfig::default()
        },
        ..config::Features::default()
    };

    let cache = point_read_cache_from_features(&features);

    assert!(cache.is_enabled());
}

#[test]
fn point_read_cache_auto_budget_scales_by_available_core_count() {
    let cache_config = config::StoragePointReadCacheConfig {
        enabled: true,
        capacity: 1_000_000,
        max_bytes: None,
        memory_budget: config::StoragePointReadCacheMemoryBudgetConfig {
            mode: config::StoragePointReadCacheMemoryBudgetMode::AutoPerCore {
                megabytes_per_core: 100,
            },
        },
        ttl_seconds: 60,
        eviction_policy: config::StoragePointReadCacheEvictionPolicy::TwoQueue,
        ..config::StoragePointReadCacheConfig::default()
    };

    let resolved = resolve_point_read_cache_max_bytes_for(
        &cache_config,
        std::num::NonZeroUsize::new(4).expect("4 is non-zero"),
        Some(8 * 1024 * 1024 * 1024),
    );

    assert_eq!(resolved, 400 * 1024 * 1024);
}

#[test]
fn point_read_cache_percent_budget_uses_effective_memory_limit() {
    let cache_config = config::StoragePointReadCacheConfig {
        enabled: true,
        capacity: 1_000_000,
        max_bytes: None,
        memory_budget: config::StoragePointReadCacheMemoryBudgetConfig {
            mode: config::StoragePointReadCacheMemoryBudgetMode::PercentOfAvailableMemory {
                percent: 25,
            },
        },
        ttl_seconds: 60,
        eviction_policy: config::StoragePointReadCacheEvictionPolicy::TwoQueue,
        ..config::StoragePointReadCacheConfig::default()
    };

    let resolved = resolve_point_read_cache_max_bytes_for(
        &cache_config,
        std::num::NonZeroUsize::new(8).expect("8 is non-zero"),
        Some(2 * 1024 * 1024 * 1024),
    );

    assert_eq!(resolved, 512 * 1024 * 1024);
}

#[test]
fn point_read_cache_percent_budget_falls_back_to_per_core_budget_without_memory_limit() {
    let cache_config = config::StoragePointReadCacheConfig {
        enabled: true,
        capacity: 1_000_000,
        max_bytes: None,
        memory_budget: config::StoragePointReadCacheMemoryBudgetConfig {
            mode: config::StoragePointReadCacheMemoryBudgetMode::PercentOfAvailableMemory {
                percent: 25,
            },
        },
        ttl_seconds: 60,
        eviction_policy: config::StoragePointReadCacheEvictionPolicy::TwoQueue,
        ..config::StoragePointReadCacheConfig::default()
    };

    let resolved = resolve_point_read_cache_max_bytes_for(
        &cache_config,
        NonZeroUsize::new(2).expect("2 is non-zero"),
        None,
    );

    assert_eq!(resolved, 200 * 1024 * 1024);
}

#[test]
fn point_read_cache_fixed_byte_budget_is_never_zero() {
    let cache_config = config::StoragePointReadCacheConfig {
        enabled: true,
        capacity: 1_000_000,
        max_bytes: None,
        memory_budget: config::StoragePointReadCacheMemoryBudgetConfig {
            mode: config::StoragePointReadCacheMemoryBudgetMode::FixedBytes { bytes: 0 },
        },
        ttl_seconds: 60,
        eviction_policy: config::StoragePointReadCacheEvictionPolicy::TwoQueue,
        ..config::StoragePointReadCacheConfig::default()
    };

    let resolved = resolve_point_read_cache_max_bytes_for(
        &cache_config,
        NonZeroUsize::new(8).expect("8 is non-zero"),
        Some(2 * 1024 * 1024 * 1024),
    );

    assert_eq!(resolved, 1);
}

#[test]
fn point_read_cache_legacy_max_bytes_overrides_budget_policy() {
    let features = config::Features {
        storage_point_read_cache: config::StoragePointReadCacheConfig {
            enabled: true,
            capacity: 123,
            max_bytes: Some(16 * 1024 * 1024),
            memory_budget: config::StoragePointReadCacheMemoryBudgetConfig {
                mode: config::StoragePointReadCacheMemoryBudgetMode::PercentOfAvailableMemory {
                    percent: 80,
                },
            },
            ttl_seconds: 45,
            eviction_policy: config::StoragePointReadCacheEvictionPolicy::TwoQueue,
            ..config::StoragePointReadCacheConfig::default()
        },
        ..config::Features::default()
    };

    let cache = point_read_cache_from_features(&features);

    assert!(cache.is_enabled());
}

#[test]
fn storage_query_proof_cache_defaults_to_disabled() {
    let cache = query_proof_cache_from_features(&config::Features::default());

    assert!(!cache.is_enabled());
}

#[test]
fn storage_query_proof_cache_can_be_enabled_from_features() {
    let features = config::Features {
        storage_query_proof_cache: config::StorageQueryProofCacheConfig {
            enabled: true,
            max_query_spaces: 32,
            max_manifest_entries: 256,
            max_coverage_ranges: 64,
            eviction_policy: config::StorageQueryProofCacheEvictionPolicy::PartitionLru,
        },
        ..config::Features::default()
    };

    let cache = query_proof_cache_from_features(&features);

    assert!(cache.is_enabled());
}

#[test]
fn authoritative_cache_options_follow_the_point_read_cache_feature_flags() {
    let features = config::Features {
        storage_point_read_cache: config::StoragePointReadCacheConfig {
            authoritative_strong_point_reads: true,
            authoritative_write_preimages: true,
            strong_read_through_warming: true,
            ..config::StoragePointReadCacheConfig::default()
        },
        ..config::Features::default()
    };

    let options = authoritative_cache_options_from_features(&features);

    assert!(options.authoritative_strong_point_reads);
    assert!(options.authoritative_write_preimages);
    assert!(options.strong_read_through_warming);
}
