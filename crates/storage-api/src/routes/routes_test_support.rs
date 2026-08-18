#![allow(dead_code)]

use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::sync::Mutex;

use http_error::HttpApiError;
use serde_json::Value;
use storage::DatabaseManager;
use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DescribeTableRequest, DescribeTimeToLiveRequest, GetItemRequest, ListTablesRequest,
    PutItemRequest, QueryRequest, ScanRequest, TransactGetItemsRequest, TransactWriteItemsRequest,
    UpdateItemRequest, UpdateTableRequest,
};

use crate::{
    manager::{StorageApiManager, StorageApiManagerImpl, StorageApiManagerOptions},
    types::Response,
};

pub async fn create_test_db() -> Arc<DatabaseManager> {
    Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("create test database manager"),
    )
}

#[derive(Clone, Debug)]
pub struct ConformanceTestBackend {
    pub name: &'static str,
    config: StorageConfig,
    #[cfg(feature = "postgres")]
    postgres_cleanup: Option<Arc<PostgresSchemaCleanupRegistry>>,
}

impl ConformanceTestBackend {
    pub async fn create_db(&self) -> Arc<DatabaseManager> {
        let config = self.isolated_config();
        Arc::new(
            DatabaseManager::new_for_test_with_config(config)
                .await
                .unwrap_or_else(|err| panic!("create {} conformance database: {err}", self.name)),
        )
    }

    pub async fn create_transactional_db(&self) -> Arc<DatabaseManager> {
        let mut config = self.isolated_config();
        if matches!(config.backend_type, StorageBackend::SQLite) {
            config.connection_string = Some(
                crate::storage_api_test_support::unique_path("sqlite-transactional")
                    .to_string_lossy()
                    .into_owned(),
            );
            config
                .sqlite
                .get_or_insert_with(storage_provider::SqliteSettings::default)
                .force_file_backed_database = true;
        }
        Arc::new(
            DatabaseManager::new_for_test_with_config(config)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "create transactional {} conformance database: {err}",
                        self.name
                    )
                }),
        )
    }

    fn isolated_config(&self) -> StorageConfig {
        match self.config.backend_type {
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres => {
                let isolated = isolated_postgres_config(&self.config);
                if let Some(cleanup) = self.postgres_cleanup.as_ref() {
                    cleanup.register(PostgresSchemaCleanup {
                        dsn: isolated.base_dsn,
                        schema: isolated.schema,
                    });
                }
                isolated.config
            }
            _ => self.config.clone(),
        }
    }
}

pub async fn create_transactional_test_db() -> Arc<DatabaseManager> {
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(
            crate::storage_api_test_support::unique_path("sqlite-transactional")
                .to_string_lossy()
                .into_owned(),
        ),
        file_path: None,
        sqlite: Some(storage_provider::SqliteSettings {
            force_file_backed_database: true,
            ..storage_provider::SqliteSettings::default()
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    Arc::new(
        DatabaseManager::new_for_test_with_config(config)
            .await
            .expect("create transactional test database manager"),
    )
}

pub fn default_conformance_backends() -> Vec<ConformanceTestBackend> {
    let backends = vec![
        #[cfg(feature = "sqlite")]
        ConformanceTestBackend {
            name: "sqlite",
            config: StorageConfig {
                backend_type: StorageBackend::SQLite,
                connection_string: Some(":memory:".to_string()),
                file_path: None,
                sqlite: Some(storage_provider::SqliteSettings {
                    immediate_gsi_consistency: true,
                    ..storage_provider::SqliteSettings::default()
                }),
                postgres: None,
                turso: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            #[cfg(feature = "postgres")]
            postgres_cleanup: None,
        },
        #[cfg(feature = "turso")]
        ConformanceTestBackend {
            name: "turso",
            config: StorageConfig {
                backend_type: StorageBackend::Turso,
                connection_string: Some(":memory:".to_string()),
                file_path: None,
                sqlite: None,
                postgres: None,
                turso: Some(storage_provider::TursoSettings {
                    immediate_gsi_consistency: true,
                }),
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            #[cfg(feature = "postgres")]
            postgres_cleanup: None,
        },
        #[cfg(feature = "rocksdb")]
        ConformanceTestBackend {
            name: "rocksdb",
            config: StorageConfig {
                backend_type: StorageBackend::RocksDB,
                connection_string: None,
                file_path: Some(
                    crate::storage_api_test_support::unique_path("rocksdb-conformance")
                        .to_string_lossy()
                        .into_owned(),
                ),
                sqlite: None,
                postgres: None,
                turso: None,
                rocksdb: Some(storage_provider::RocksdbSettings {
                    immediate_gsi_consistency: true,
                }),
                foundationdb: None,
                remote: None,
            },
            #[cfg(feature = "postgres")]
            postgres_cleanup: None,
        },
    ];
    #[cfg(any(feature = "postgres", feature = "foundationdb"))]
    let mut backends = backends;

    #[cfg(feature = "postgres")]
    if let Ok(dsn) = std::env::var("AUX_STORAGE_CONFORMANCE_POSTGRES_DSN") {
        backends.push(ConformanceTestBackend {
            name: "postgres",
            config: StorageConfig {
                backend_type: StorageBackend::Postgres,
                connection_string: Some(dsn.clone()),
                file_path: None,
                sqlite: None,
                postgres: Some(storage_provider::PostgresSettings {
                    dsn,
                    max_pool_size: 4,
                    background_max_pool_size: 2,
                    tls: false,
                    immediate_gsi_consistency: true,
                }),
                turso: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            postgres_cleanup: Some(Arc::new(PostgresSchemaCleanupRegistry::default())),
        });
    }

    #[cfg(feature = "foundationdb")]
    if std::env::var("AUX_STORAGE_CONFORMANCE_FOUNDATIONDB").as_deref() == Ok("1") {
        backends.push(ConformanceTestBackend {
            name: "foundationdb",
            config: StorageConfig {
                backend_type: StorageBackend::FoundationDb,
                connection_string: None,
                file_path: None,
                sqlite: None,
                postgres: None,
                turso: None,
                rocksdb: None,
                foundationdb: Some(storage_provider::FoundationDbSettings {
                    subspace_prefix: Some(
                        crate::storage_api_test_support::unique_path("fdb-conformance")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    immediate_gsi_consistency: true,
                    ..storage_provider::FoundationDbSettings::default()
                }),
                remote: None,
            },
            #[cfg(feature = "postgres")]
            postgres_cleanup: None,
        });
    }

    backends
}

#[cfg(feature = "postgres")]
struct IsolatedPostgresConfig {
    config: StorageConfig,
    base_dsn: String,
    schema: String,
}

#[cfg(feature = "postgres")]
fn isolated_postgres_config(config: &StorageConfig) -> IsolatedPostgresConfig {
    let mut config = config.clone();
    let schema = format!("aux_storage_conformance_{}", uuid::Uuid::new_v4().simple());
    let base_dsn = config
        .postgres
        .as_ref()
        .map(|postgres| postgres.dsn.as_str())
        .or(config.connection_string.as_deref())
        .expect("postgres conformance DSN")
        .to_string();
    create_postgres_schema(&base_dsn, &schema);
    let schema_dsn = postgres_dsn_with_search_path(&base_dsn, &schema);
    config.connection_string = Some(schema_dsn.clone());
    if let Some(postgres) = config.postgres.as_mut() {
        postgres.dsn = schema_dsn;
    }
    IsolatedPostgresConfig {
        config,
        base_dsn,
        schema,
    }
}

#[cfg(feature = "postgres")]
#[derive(Debug, Default)]
struct PostgresSchemaCleanupRegistry {
    schemas: Mutex<Vec<PostgresSchemaCleanup>>,
}

#[cfg(feature = "postgres")]
impl PostgresSchemaCleanupRegistry {
    fn register(&self, cleanup: PostgresSchemaCleanup) {
        self.schemas
            .lock()
            .expect("postgres schema cleanup registry lock")
            .push(cleanup);
    }
}

#[cfg(feature = "postgres")]
impl Drop for PostgresSchemaCleanupRegistry {
    fn drop(&mut self) {
        let Ok(schemas) = self.schemas.get_mut() else {
            eprintln!("postgres conformance cleanup registry was poisoned; schemas may remain");
            return;
        };
        for cleanup in schemas.drain(..).rev() {
            cleanup.drop_schema();
        }
    }
}

#[cfg(feature = "postgres")]
#[derive(Debug)]
struct PostgresSchemaCleanup {
    dsn: String,
    schema: String,
}

#[cfg(feature = "postgres")]
impl PostgresSchemaCleanup {
    fn drop_schema(&self) {
        let status = std::process::Command::new("psql")
            .arg(&self.dsn)
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-q")
            .arg("-c")
            .arg(format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.schema))
            .status();
        match status {
            Ok(status) if status.success() => {}
            Ok(status) => eprintln!(
                "failed to drop postgres conformance schema {}: psql exited with {status}",
                self.schema
            ),
            Err(err) => eprintln!(
                "failed to run psql to drop postgres conformance schema {}: {err}",
                self.schema
            ),
        }
    }
}

#[cfg(feature = "postgres")]
fn create_postgres_schema(dsn: &str, schema: &str) {
    let status = std::process::Command::new("psql")
        .arg(dsn)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-c")
        .arg(format!("CREATE SCHEMA \"{schema}\""))
        .status()
        .expect("run psql to create postgres conformance schema");
    assert!(
        status.success(),
        "create postgres conformance schema {schema}"
    );
}

#[cfg(feature = "postgres")]
fn postgres_dsn_with_search_path(dsn: &str, schema: &str) -> String {
    let mut url = url::Url::parse(dsn).expect("postgres conformance DSN should be a URL");
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    url.into()
}

pub async fn handle_create_table(
    db: Arc<DatabaseManager>,
    request: CreateTableRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .create_table(request)
        .await
}

pub async fn handle_list_tables(
    db: Arc<DatabaseManager>,
    request: ListTablesRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .list_tables(request)
        .await
}

pub async fn handle_put_item(
    db: Arc<DatabaseManager>,
    request: PutItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .put_item(request)
        .await
}

pub async fn handle_get_item(
    db: Arc<DatabaseManager>,
    request: GetItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .get_item(request)
        .await
}

pub async fn handle_delete_item(
    db: Arc<DatabaseManager>,
    request: DeleteItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .delete_item(request)
        .await
}

pub async fn handle_update_item(
    db: Arc<DatabaseManager>,
    request: UpdateItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .update_item(request)
        .await
}

pub async fn handle_describe_table(
    db: Arc<DatabaseManager>,
    request: DescribeTableRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .describe_table(request)
        .await
}

pub async fn handle_update_table(
    db: Arc<DatabaseManager>,
    request: UpdateTableRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .update_table(request)
        .await
}

pub async fn handle_batch_write_item(
    db: Arc<DatabaseManager>,
    request: BatchWriteItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .batch_write_item(request)
        .await
}

pub async fn handle_batch_get_item(
    db: Arc<DatabaseManager>,
    request: BatchGetItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .batch_get_item(request)
        .await
}

pub async fn handle_transact_write_items(
    db: Arc<DatabaseManager>,
    request: TransactWriteItemsRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .transact_write_items(request)
        .await
}

pub async fn handle_transact_get_items(
    db: Arc<DatabaseManager>,
    request: TransactGetItemsRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .transact_get_items(request)
        .await
}

pub async fn handle_scan(
    db: Arc<DatabaseManager>,
    request: ScanRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .scan(request)
        .await
}

pub async fn handle_query(
    db: Arc<DatabaseManager>,
    request: QueryRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .query(request)
        .await
}

pub async fn handle_describe_time_to_live(
    db: Arc<DatabaseManager>,
    request: DescribeTimeToLiveRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .describe_time_to_live(request)
        .await
}

pub async fn handle_clear_all_tables(
    db: Arc<DatabaseManager>,
    payload: Value,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default())
        .clear_all_tables(payload)
        .await
}
