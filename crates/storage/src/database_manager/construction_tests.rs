use storage_provider::{
    FoundationDbSettings, PostgresSettings, RemoteCredentialStrategy, RemoteStorageSettings,
    RocksdbSettings, SqliteSettings, StorageBackend, StorageConnectionConfig, TursoSettings,
};

use super::construction::read_sequence_capabilities_for_connection;

#[test]
fn sqlite_read_sequence_capabilities_follow_immediate_gsi_setting() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            immediate_gsi_consistency: true,
            force_file_backed_database: false,
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    });

    assert!(capabilities.eventual_reads);
    assert!(capabilities.strong_reads);
    assert!(capabilities.transactional_reads);
    assert!(capabilities.immediate_gsi_consistency);
    assert!(!capabilities.transactional_snapshots);
}

#[test]
fn sqlite_read_sequence_file_backed_capabilities_enable_transactional_snapshots() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some("read-sequence-file-backed.db".to_string()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            immediate_gsi_consistency: false,
            force_file_backed_database: true,
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    });

    assert!(capabilities.transactional_snapshots);
}

#[test]
fn remote_read_sequence_capabilities_support_strong_base_reads_and_reject_transactional() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::Remote,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: Some(RemoteStorageSettings {
            endpoint_urls: vec!["http://localhost:8000".to_string()],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::DefaultChain,
            timeouts: None,
        }),
    });

    assert!(capabilities.eventual_reads);
    assert!(capabilities.strong_reads);
    assert!(!capabilities.transactional_reads);
    assert!(!capabilities.immediate_gsi_consistency);
    assert!(!capabilities.transactional_snapshots);
}

#[test]
fn postgres_read_sequence_capabilities_enable_transactional_snapshots() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::Postgres,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: Some(PostgresSettings {
            immediate_gsi_consistency: true,
            ..PostgresSettings::default()
        }),
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    });

    assert!(capabilities.eventual_reads);
    assert!(capabilities.strong_reads);
    assert!(capabilities.transactional_reads);
    assert!(capabilities.immediate_gsi_consistency);
    assert!(capabilities.transactional_snapshots);
}

#[test]
fn turso_read_sequence_capabilities_enable_transactional_snapshots() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::Turso,
        connection_string: Some("file:read-sequence-turso.db".to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: Some(TursoSettings {
            immediate_gsi_consistency: true,
        }),
        rocksdb: None,
        foundationdb: None,
        remote: None,
    });

    assert!(capabilities.eventual_reads);
    assert!(capabilities.strong_reads);
    assert!(capabilities.transactional_reads);
    assert!(capabilities.immediate_gsi_consistency);
    assert!(capabilities.transactional_snapshots);
}

#[test]
fn rocksdb_read_sequence_capabilities_enable_transactional_snapshots() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::RocksDB,
        connection_string: Some("read-sequence-rocksdb".to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: Some(RocksdbSettings {
            immediate_gsi_consistency: true,
        }),
        foundationdb: None,
        remote: None,
    });

    assert!(capabilities.eventual_reads);
    assert!(capabilities.strong_reads);
    assert!(capabilities.transactional_reads);
    assert!(capabilities.immediate_gsi_consistency);
    assert!(capabilities.transactional_snapshots);
}

#[test]
fn foundationdb_read_sequence_capabilities_enable_transactional_snapshots() {
    let capabilities = read_sequence_capabilities_for_connection(&StorageConnectionConfig {
        backend_type: StorageBackend::FoundationDb,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: Some(FoundationDbSettings {
            immediate_gsi_consistency: true,
            ..FoundationDbSettings::default()
        }),
        remote: None,
    });

    assert!(capabilities.eventual_reads);
    assert!(capabilities.strong_reads);
    assert!(capabilities.transactional_reads);
    assert!(capabilities.immediate_gsi_consistency);
    assert!(capabilities.transactional_snapshots);
}
