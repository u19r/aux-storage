use storage_provider::{StorageBackend, StorageConfig};

use super::open_store;

fn config(backend_type: StorageBackend) -> StorageConfig {
    StorageConfig {
        initial_stream_lineages: None,
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
        let bundle = open_store(config, &Default::default(), "default")
            .await
            .expect("construct bundle");
        assert_initialized_capabilities(bundle);
    }
    std::fs::remove_dir_all(directory).expect("remove owned local fixture");
}

#[cfg(all(feature = "postgres", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires a configured PostgreSQL service"]
async fn given_postgres_bundle_when_constructed_then_initialized_capabilities_are_present() {
    let base = std::env::var("TEST_POSTGRES_DSN").expect("TEST_POSTGRES_DSN required");
    let (dsn, schema) =
        sql_backend::postgres::test_schema::isolated_test_dsn(&base, "storage_bundle");
    sql_backend::postgres::test_schema::create_test_schema(&dsn, &schema)
        .await
        .unwrap();
    let mut config = config(StorageBackend::Postgres);
    config.postgres = Some(storage_provider::PostgresSettings {
        dsn,
        tls: false,
        ..Default::default()
    });
    let bundle = open_store(config, &Default::default(), "default")
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
            std::env::var("TEST_FDB_CLUSTER_FILE").expect("TEST_FDB_CLUSTER_FILE required"),
        ),
        subspace_prefix: Some(fixture_scope()),
        ..Default::default()
    });
    let bundle = open_store(config, &Default::default(), "default")
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

fn assert_initialized_capabilities(store: super::InitializedStore) {
    let native = store.native_store().expect("a native store");
    assert!(native.attempt_capacity().is_ok());
    assert!(native.scheduled_job_store(Default::default()).is_ok());
}
