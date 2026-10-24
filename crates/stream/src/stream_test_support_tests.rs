use std::{ops::Deref, sync::Arc};

use storage_provider::{
    FoundationDbSettings, PostgresSettings, RocksdbSettings, SqliteSettings, StorageBackend,
    StorageConfig, TursoSettings,
};
use stream_provider::{SubscriptionMessageSender, SystemStreamPageRequest};
use tempfile::{TempDir, tempdir};

use crate::{StreamManager, create_stream_provider};

pub(crate) struct TestStreamManager {
    manager: StreamManager,
    _temporary_directory: TempDir,
}

impl Deref for TestStreamManager {
    type Target = StreamManager;

    fn deref(&self) -> &Self::Target {
        &self.manager
    }
}

pub(crate) async fn create_test_manager() -> TestStreamManager {
    create_test_manager_with_sender(None).await
}

pub(crate) async fn create_test_manager_with_sender(
    sender: Option<Arc<dyn SubscriptionMessageSender>>,
) -> TestStreamManager {
    let temporary_directory = tempdir().expect("create stream test temporary directory");
    let config = selected_backend_config(&temporary_directory).await;
    let provider = create_stream_provider(config)
        .await
        .expect("create selected stream test provider");
    let manager = match sender {
        Some(sender) => {
            StreamManager::new_with_subscription_message_sender(provider.into(), sender)
        }
        None => StreamManager::new(provider.into()),
    };
    TestStreamManager {
        manager,
        _temporary_directory: temporary_directory,
    }
}

#[tokio::test]
async fn given_issued_system_checkpoint_when_reading_next_page_then_same_frontier_is_returned() {
    let temporary_directory = tempdir().expect("create stream test temporary directory");
    let config = selected_backend_config(&temporary_directory).await;
    let provider = create_stream_provider(config)
        .await
        .expect("create stream provider");
    let scope = provider
        .system_stream_scope()
        .await
        .expect("read system scope");
    let first = provider
        .get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 1).expect("first page request"),
        )
        .await
        .expect("read first page");
    let next = provider
        .get_system_stream_page(
            SystemStreamPageRequest::try_new(
                scope,
                Some(first.issued_checkpoint().token().clone()),
                1,
            )
            .expect("next page request"),
        )
        .await
        .expect("read resumed page");
    assert!(first.records().is_empty());
    assert!(next.records().is_empty());
    assert_eq!(next.issued_checkpoint(), first.issued_checkpoint());
}

async fn selected_backend_config(temporary_directory: &TempDir) -> StorageConfig {
    #[cfg(feature = "sqlite")]
    return sqlite_config(temporary_directory);

    #[cfg(all(not(feature = "sqlite"), feature = "turso"))]
    return turso_config(temporary_directory);

    #[cfg(all(not(feature = "sqlite"), not(feature = "turso"), feature = "postgres"))]
    return postgres_config(temporary_directory).await;

    #[cfg(all(
        not(feature = "sqlite"),
        not(feature = "turso"),
        not(feature = "postgres"),
        feature = "rocksdb"
    ))]
    return rocksdb_config(temporary_directory);

    #[cfg(all(
        not(feature = "sqlite"),
        not(feature = "turso"),
        not(feature = "postgres"),
        not(feature = "rocksdb"),
        feature = "foundationdb"
    ))]
    return foundationdb_config(temporary_directory);

    #[cfg(not(any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )))]
    panic!("stream tests require one native storage backend feature");
}

#[cfg(feature = "sqlite")]
fn sqlite_config(temporary_directory: &TempDir) -> StorageConfig {
    config_for_path(
        StorageBackend::SQLite,
        temporary_directory.path().join("stream.sqlite"),
        None,
    )
}

#[cfg(all(not(feature = "sqlite"), feature = "turso"))]
fn turso_config(temporary_directory: &TempDir) -> StorageConfig {
    config_for_path(
        StorageBackend::Turso,
        temporary_directory.path().join("stream.turso.db"),
        None,
    )
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "turso"),
    not(feature = "postgres"),
    feature = "rocksdb"
))]
fn rocksdb_config(temporary_directory: &TempDir) -> StorageConfig {
    config_for_path(
        StorageBackend::RocksDB,
        temporary_directory.path().join("stream.rocksdb"),
        None,
    )
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "turso"),
    not(feature = "postgres"),
    not(feature = "rocksdb"),
    feature = "foundationdb"
))]
fn foundationdb_config(temporary_directory: &TempDir) -> StorageConfig {
    let prefix = format!("stream-tests/{}/", directory_token(temporary_directory));
    config_for_path(
        StorageBackend::FoundationDb,
        std::path::PathBuf::from("/usr/local/etc/foundationdb/fdb.cluster"),
        Some(FoundationDbSettings {
            cluster_file: Some("/usr/local/etc/foundationdb/fdb.cluster".to_string()),
            subspace_prefix: Some(prefix),
            ..FoundationDbSettings::default()
        }),
    )
}

fn config_for_path(
    backend_type: StorageBackend,
    path: std::path::PathBuf,
    foundationdb: Option<FoundationDbSettings>,
) -> StorageConfig {
    let path = path.to_string_lossy().into_owned();
    StorageConfig {
        backend_type,
        connection_string: Some(path.clone()),
        file_path: Some(path),
        sqlite: Some(SqliteSettings::default()),
        turso: Some(TursoSettings::default()),
        postgres: Some(PostgresSettings {
            tls: false,
            ..PostgresSettings::default()
        }),
        rocksdb: Some(RocksdbSettings::default()),
        foundationdb,
        remote: None,
    }
}

#[cfg(all(not(feature = "sqlite"), not(feature = "turso"), feature = "postgres"))]
async fn postgres_config(temporary_directory: &TempDir) -> StorageConfig {
    let base_dsn = std::env::var("TEST_POSTGRES_DSN")
        .unwrap_or_else(|_| "postgresql://ross@localhost/postgres".to_string());
    let schema = format!("stream_test_{}", directory_token(temporary_directory));
    let (client, connection) = tokio_postgres::connect(&base_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect PostgreSQL test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("create isolated PostgreSQL test schema");
    let separator = if base_dsn.contains('?') { '&' } else { '?' };
    let dsn = format!("{base_dsn}{separator}options=-csearch_path%3D{schema}");
    config_for_path(
        StorageBackend::Postgres,
        std::path::PathBuf::from(dsn),
        None,
    )
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "turso"),
    any(feature = "postgres", feature = "foundationdb")
))]
fn directory_token(directory: &TempDir) -> String {
    directory
        .path()
        .file_name()
        .expect("temporary directory should have a name")
        .to_string_lossy()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}
