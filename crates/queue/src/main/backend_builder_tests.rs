use config::{
    FoundationdbBackendConfig, PostgresBackendConfig, RemoteBackendConfig, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig, RemoteTimeoutOverrides, RocksdbBackendConfig,
    SqliteBackendConfig, TursoBackendConfig,
};
use queue::{QueueBackend, RemoteCredentialStrategy};

use super::app::{
    foundationdb_queue_provider_config, postgres_queue_provider_config,
    remote_queue_provider_config, rocksdb_queue_provider_config, sqlite_queue_provider_config,
    turso_queue_provider_config,
};

#[test]
fn queue_sqlite_turso_and_rocksdb_builders_use_backend_db_paths() {
    let sqlite = sqlite_queue_provider_config(&SqliteBackendConfig {
        db_path: "queue.sqlite".to_string(),
        immediate_gsi_consistency: false,
    });
    let turso = turso_queue_provider_config(&TursoBackendConfig {
        db_path: "queue.turso".to_string(),
        immediate_gsi_consistency: false,
    });
    let rocksdb = rocksdb_queue_provider_config(&RocksdbBackendConfig {
        db_path: "queue.rocks".to_string(),
        immediate_gsi_consistency: false,
    });

    assert!(matches!(sqlite.backend_type, QueueBackend::SQLite));
    assert_eq!(sqlite.connection_string.as_deref(), Some("queue.sqlite"));
    assert!(matches!(turso.backend_type, QueueBackend::Turso));
    assert_eq!(turso.connection_string.as_deref(), Some("queue.turso"));
    assert!(matches!(rocksdb.backend_type, QueueBackend::RocksDB));
    assert_eq!(rocksdb.connection_string.as_deref(), Some("queue.rocks"));
}

#[test]
fn queue_postgres_builder_preserves_pool_and_tls_settings() {
    let config = postgres_queue_provider_config(&PostgresBackendConfig {
        dsn: "postgres://localhost/queue".to_string(),
        max_pool_size: 8,
        background_max_pool_size: 2,
        tls: false,
        immediate_gsi_consistency: false,
    });

    let postgres = config.postgres.expect("postgres settings");
    assert!(matches!(config.backend_type, QueueBackend::Postgres));
    assert_eq!(
        config.connection_string.as_deref(),
        Some("postgres://localhost/queue")
    );
    assert_eq!(postgres.dsn, "postgres://localhost/queue");
    assert_eq!(postgres.max_pool_size, 8);
    assert!(!postgres.tls);
}

#[test]
fn queue_foundationdb_builder_preserves_optional_cluster_settings() {
    let config = foundationdb_queue_provider_config(&FoundationdbBackendConfig {
        cluster_file: Some("cluster-file".to_string()),
        tenant_name: Some("tenant".to_string()),
        subspace_prefix: Some("queue".to_string()),
        cache_read_version_ms: 10,
        immediate_gsi_consistency: false,
        report_conflicting_keys: true,
    });

    let foundationdb = config.foundationdb.expect("foundationdb settings");
    assert!(matches!(config.backend_type, QueueBackend::FoundationDb));
    assert_eq!(foundationdb.cluster_file.as_deref(), Some("cluster-file"));
    assert_eq!(foundationdb.tenant_name.as_deref(), Some("tenant"));
    assert_eq!(foundationdb.subspace_prefix.as_deref(), Some("queue"));
    assert_eq!(foundationdb.cache_read_version_ms, 10);
    assert!(foundationdb.report_conflicting_keys);
}

#[test]
fn queue_foundationdb_builder_uses_the_shared_cache_default() {
    let config = foundationdb_queue_provider_config(&FoundationdbBackendConfig::default());

    assert_eq!(
        config
            .foundationdb
            .expect("foundationdb settings")
            .cache_read_version_ms,
        50
    );
}

#[test]
fn queue_remote_builder_preserves_endpoint_credentials_and_timeouts() {
    let config = remote_queue_provider_config(&RemoteBackendConfig {
        endpoint_urls: vec!["https://queue.example.test".to_string()],
        region: Some("us-east-1".to_string()),
        tls: true,
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: Some("session".to_string()),
            }),
            instance_keys: None,
        }),
        default_storage_mode: config::RemoteDefaultStorageMode::Dedicated,
        timeout_overrides: Some(RemoteTimeoutOverrides {
            connect_timeout_ms: Some(1000),
            request_timeout_ms: Some(2000),
        }),
    })
    .expect("remote config should build");

    let remote = config.remote.expect("remote settings");
    assert!(matches!(config.backend_type, QueueBackend::Remote));
    assert_eq!(remote.endpoint_urls, vec!["https://queue.example.test"]);
    assert_eq!(remote.region.as_deref(), Some("us-east-1"));
    assert!(matches!(
        remote.credentials,
        RemoteCredentialStrategy::Static(ref credentials)
            if credentials.access_key_id == "access"
                && credentials.secret_access_key == "secret"
                && credentials.session_token.as_deref() == Some("session")
    ));
    assert_eq!(
        remote
            .timeouts
            .as_ref()
            .and_then(|timeouts| timeouts.connect_timeout_ms),
        Some(1000)
    );
    assert_eq!(
        remote
            .timeouts
            .as_ref()
            .and_then(|timeouts| timeouts.request_timeout_ms),
        Some(2000)
    );
}
