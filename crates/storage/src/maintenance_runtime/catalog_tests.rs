use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use scheduled_bg_jobs::{JobCatalog, Timestamp};
use storage_common::{DatabaseJobIntervals, StorageJob};
use storage_provider::{StorageBackend, StorageConfig};

use super::catalog::MaintenanceJobCatalog;
use crate::provider_bundle::{CallerMaintenance, open_store};

#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "rocksdb",
    feature = "foundationdb"
))]
fn fixture_scope() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("job-catalog-{}-{now}", std::process::id())
}

fn build_config(backend: &StorageBackend, directory: &std::path::Path) -> StorageConfig {
    StorageConfig {
        initial_stream_lineages: None,
        backend_type: backend.clone(),
        connection_string: Some(
            directory
                .join(format!("{backend:?}"))
                .to_string_lossy()
                .into(),
        ),
        file_path: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

fn registered_identities(catalog: &JobCatalog) -> BTreeSet<(String, u16)> {
    catalog
        .test_handler_identities()
        .map(|(kind, version)| (kind.as_str().into(), version.get()))
        .collect()
}

fn assert_catalog(bundle: CallerMaintenance, backend: &StorageBackend) {
    let catalog = MaintenanceJobCatalog::try_new(
        bundle.maintenance.unwrap(),
        DatabaseJobIntervals::default(),
        Timestamp::ZERO,
        None,
    )
    .unwrap()
    .0;
    let expected = StorageJob::ALL
        .into_iter()
        .filter(|job| {
            *job != StorageJob::PartitionReconciliation
                || matches!(
                    backend,
                    StorageBackend::RocksDB | StorageBackend::FoundationDb
                )
        })
        .map(|job| (job.as_str().into(), 1))
        .collect();
    assert_eq!(registered_identities(&catalog), expected, "{backend:?}");
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "rocksdb"))]
#[tokio::test]
async fn given_local_native_bundles_when_maintenance_catalogs_are_assembled_then_every_supported_family_is_registered()
 {
    let directory = std::env::temp_dir().join(fixture_scope());
    std::fs::create_dir(&directory).unwrap();
    for backend in [
        #[cfg(feature = "sqlite")]
        StorageBackend::SQLite,
        #[cfg(feature = "turso")]
        StorageBackend::Turso,
        #[cfg(feature = "rocksdb")]
        StorageBackend::RocksDB,
    ] {
        let config = build_config(&backend, &directory);
        let bundle = open_store(config, &Default::default(), "default")
            .await
            .unwrap()
            .caller_maintenance();
        assert_catalog(bundle, &backend);
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_absent_optional_maintenance_repositories_when_catalog_is_assembled_then_only_supported_handlers_exist()
 {
    let directory = std::env::temp_dir().join(fixture_scope());
    std::fs::create_dir(&directory).unwrap();
    let config = build_config(&StorageBackend::SQLite, &directory);
    let bundle = open_store(config, &Default::default(), "default")
        .await
        .unwrap()
        .caller_maintenance();
    let mut repositories = bundle.maintenance.unwrap();
    repositories.partition_reconciliation_repository = None;
    repositories.queue_message_cleanup_repository = None;
    repositories.pubsub_stale_subscriptions = None;
    let catalog = MaintenanceJobCatalog::try_new(
        repositories,
        DatabaseJobIntervals::default(),
        Timestamp::ZERO,
        None,
    )
    .unwrap()
    .0;
    let expected = StorageJob::ALL
        .into_iter()
        .filter(|job| {
            matches!(
                job,
                StorageJob::GsiBackfill
                    | StorageJob::TransactReceiptTrim
                    | StorageJob::TtlSweep
                    | StorageJob::StreamTrim
                    | StorageJob::TableReclaim
            )
        })
        .map(|job| (job.as_str().into(), 1))
        .collect();
    assert_eq!(registered_identities(&catalog), expected);
    let manifest = catalog.manifest().expect("complete namespace manifest");
    assert_eq!(manifest.seed_ids().len(), StorageJob::ALL.len());
    for job in StorageJob::ALL {
        assert!(manifest.seed_ids().contains(&job.job_id()));
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(all(feature = "postgres", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires a configured PostgreSQL service"]
async fn given_native_postgres_when_maintenance_catalog_is_assembled_then_sql_families_are_registered()
 {
    let base = std::env::var("TEST_JOB_EFFECT_POSTGRES_DSN")
        .or_else(|_| std::env::var("TEST_POSTGRES_DSN"))
        .unwrap();
    let (dsn, schema) =
        sql_backend::postgres::test_schema::isolated_test_dsn(&base, "storage_catalog");
    sql_backend::postgres::test_schema::create_test_schema(&dsn, &schema)
        .await
        .unwrap();
    let mut config = build_config(&StorageBackend::Postgres, &std::env::temp_dir());
    config.postgres = Some(storage_provider::PostgresSettings {
        dsn,
        tls: false,
        ..Default::default()
    });
    let bundle = open_store(config, &Default::default(), "default")
        .await
        .unwrap()
        .caller_maintenance();
    assert_catalog(bundle, &StorageBackend::Postgres);
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a configured FoundationDB service"]
async fn given_native_foundationdb_when_maintenance_catalog_is_assembled_then_kv_families_are_registered()
 {
    let mut config = build_config(&StorageBackend::FoundationDb, &std::env::temp_dir());
    config.foundationdb = Some(storage_provider::FoundationDbSettings {
        cluster_file: Some(std::env::var("TEST_FDB_CLUSTER_FILE").unwrap()),
        subspace_prefix: Some(fixture_scope()),
        ..Default::default()
    });
    let bundle = open_store(config, &Default::default(), "default")
        .await
        .unwrap()
        .caller_maintenance();
    assert_catalog(bundle, &StorageBackend::FoundationDb);
}
