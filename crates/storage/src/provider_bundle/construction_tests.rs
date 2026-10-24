use storage_provider::{StorageBackend, StorageConfig};

use super::create_storage_provider_bundle;

fn config(backend_type: StorageBackend) -> StorageConfig {
    StorageConfig {
        backend_type,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "rocksdb"))]
#[tokio::test]
async fn given_local_bundles_when_constructed_then_initialized_capabilities_are_present() {
    let directory = std::env::temp_dir().join(fixture_scope());
    std::fs::create_dir(&directory).expect("isolated local databases");
    for (backend, name) in [
        #[cfg(feature = "sqlite")]
        (StorageBackend::SQLite, "sqlite.db"),
        #[cfg(feature = "turso")]
        (StorageBackend::Turso, "turso.db"),
        #[cfg(feature = "rocksdb")]
        (StorageBackend::RocksDB, "rocksdb"),
    ] {
        let mut config = config(backend);
        config.connection_string = Some(directory.join(name).to_string_lossy().into_owned());
        let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
            .await
            .expect("construct bundle");
        assert_initialized_capabilities(bundle);
    }
    std::fs::remove_dir_all(directory).expect("remove owned local fixture");
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires a configured PostgreSQL service"]
async fn given_postgres_bundle_when_constructed_then_initialized_capabilities_are_present() {
    let mut config = config(StorageBackend::Postgres);
    config.postgres = Some(storage_provider::PostgresSettings {
        dsn: std::env::var("TEST_POSTGRES_DSN")
            .or_else(|_| std::env::var("CUCUMBER_POSTGRES_DSN"))
            .expect("TEST_POSTGRES_DSN or CUCUMBER_POSTGRES_DSN required"),
        tls: false,
        ..Default::default()
    });
    let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
        .await
        .expect("construct PostgreSQL bundle");
    assert_initialized_capabilities(bundle);
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a configured FoundationDB service"]
async fn given_foundationdb_bundle_when_constructed_then_initialized_capabilities_are_present() {
    let mut config = config(StorageBackend::FoundationDb);
    config.foundationdb = Some(storage_provider::FoundationDbSettings {
        cluster_file: Some(
            std::env::var("TEST_FDB_CLUSTER_FILE")
                .or_else(|_| std::env::var("FDB_CLUSTER_FILE"))
                .expect("TEST_FDB_CLUSTER_FILE or FDB_CLUSTER_FILE required"),
        ),
        subspace_prefix: Some(fixture_scope()),
        ..Default::default()
    });
    let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
        .await
        .expect("construct FDB bundle");
    assert_initialized_capabilities(bundle);
}

#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "rocksdb",
    feature = "foundationdb"
))]
fn fixture_scope() -> String {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    format!("partition-capability-{}-{time}", std::process::id())
}

fn assert_initialized_capabilities(bundle: super::StorageProviderBundle) {
    assert!(bundle.stream.is_some());
    assert!(bundle.queue.is_some());
    assert!(bundle.pubsub.is_some());
    assert!(bundle.native_attempt_capacity.is_some());
    assert!(bundle.ttl_sweep_repository.is_some());
    assert!(bundle.system_stream_trim_repository.is_some());
    assert!(bundle.table_reclaim_repository.is_some());
    let maintenance = bundle
        .maintenance
        .expect("initialized maintenance capabilities");
    assert!(maintenance.queue_message_cleanup_repository.is_some());
}
