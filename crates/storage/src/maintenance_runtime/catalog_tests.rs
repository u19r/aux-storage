use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use scheduled_bg_jobs::{JobCatalog, Timestamp};
use storage_common::{DatabaseJobIntervals, StorageJob};
use storage_provider::{StorageBackend, StorageConfig};

use super::catalog::MaintenanceJobCatalog;
use crate::{StorageProviderBundle, provider_bundle::create_storage_provider_bundle};

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

fn assert_catalog(bundle: StorageProviderBundle, backend: &StorageBackend) {
    let catalog = MaintenanceJobCatalog::try_new(
        bundle.maintenance.unwrap(),
        bundle.table_reclaim_repository,
        DatabaseJobIntervals::default(),
        Timestamp::ZERO,
    )
    .unwrap();
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
        let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
            .await
            .unwrap();
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
    let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
        .await
        .unwrap();
    let mut repositories = bundle.maintenance.unwrap();
    repositories.partition_reconciliation_repository = None;
    repositories.queue_message_cleanup_repository = None;
    let catalog = MaintenanceJobCatalog::try_new(
        repositories,
        None,
        DatabaseJobIntervals::default(),
        Timestamp::ZERO,
    )
    .unwrap();
    let expected = StorageJob::ALL
        .into_iter()
        .filter(|job| {
            matches!(
                job,
                StorageJob::GsiBackfill | StorageJob::TtlSweep | StorageJob::StreamTrim
            )
        })
        .map(|job| (job.as_str().into(), 1))
        .collect();
    assert_eq!(registered_identities(&catalog), expected);
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires a configured PostgreSQL service"]
async fn given_native_postgres_when_maintenance_catalog_is_assembled_then_sql_families_are_registered()
 {
    let mut config = build_config(&StorageBackend::Postgres, &std::env::temp_dir());
    config.postgres = Some(storage_provider::PostgresSettings {
        dsn: std::env::var("TEST_JOB_EFFECT_POSTGRES_DSN").unwrap(),
        tls: false,
        ..Default::default()
    });
    let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
        .await
        .unwrap();
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
    let bundle = create_storage_provider_bundle(config, &Default::default(), "default")
        .await
        .unwrap();
    assert_catalog(bundle, &StorageBackend::FoundationDb);
}
