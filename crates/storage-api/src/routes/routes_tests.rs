#![allow(dead_code)]

#[cfg(feature = "postgres")]
use std::sync::Mutex;
use std::{future::Future, sync::Arc};

use http_error::HttpApiError;
use serde_json::Value;
use storage::{AdmissionClass, DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput};
use storage_provider::{StorageBackend, StorageConfig};
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DescribeTableRequest, DescribeTimeToLiveRequest, GetItemRequest, ListTablesRequest,
    PutItemRequest, PutItemResponse, QueryRequest, ScanRequest, StorageResult,
    TransactGetItemsRequest, TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest,
};

use crate::{
    manager::{StorageApiManager, StorageApiManagerImpl},
    types::Response,
};

#[test]
fn given_request_path_sources_when_scanned_then_provider_calls_are_admitted() {
    let source_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    for directory in ["manager", "routes"] {
        let mut directories = vec![source_root.join(directory)];
        while let Some(directory) = directories.pop() {
            let entries = std::fs::read_dir(&directory).expect("request-path source directory");
            for entry in entries {
                let path = entry.expect("request-path source entry").path();
                if path.is_dir() {
                    directories.push(path);
                    continue;
                }
                if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
                    continue;
                }
                let file_name = path
                    .file_name()
                    .and_then(std::ffi::OsStr::to_str)
                    .unwrap_or_default();
                if file_name.ends_with("_tests.rs")
                    || path.components().any(|component| {
                        component.as_os_str().to_string_lossy().ends_with("_tests")
                    })
                {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("request-path source file");
                for accessor in ["stream_provider()", "queue_provider()", "pubsub_provider()"] {
                    if source.contains(accessor) {
                        violations.push(format!("{} contains {accessor}", path.display()));
                    }
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "request paths must use an admitted provider handle:\n{}",
        violations.join("\n")
    );
}

pub async fn create_sqlite_test_db() -> Arc<DatabaseManager> {
    Arc::new(
        DatabaseManager::new_for_test()
            .await
            .expect("create test database manager"),
    )
}

pub async fn seed_item(
    db: &DatabaseManager,
    input: PutItemInput,
) -> StorageResult<PutItemResponse> {
    let item = storage_types::WireItem::try_from(input.item)?;
    let request = PutItemRequest {
        table_name: input.table_name,
        item,
        indexers: input.indexers,
        condition_expression: input.condition_expression,
        expression_attribute_names: input.expression_attribute_names,
        expression_attribute_values: input.expression_attribute_values,
        expected: None,
        conditional_operator: None,
        return_values: input.return_values,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
        return_values_on_condition_check_failure: input
            .return_old_on_condition_failure
            .then(|| "ALL_OLD".to_string()),
        aux_item_stream_ttl_hours: input.aux_item_stream_ttl_hours,
    };
    db.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
        dynamo.put_item(request).await
    })
    .await
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
            DatabaseManager::new_with_config_and_runtime_options(
                config,
                conformance_runtime_options(),
            )
            .await
            .unwrap_or_else(|err| panic!("create {} conformance database: {err}", self.name)),
        )
    }

    pub async fn create_transactional_db(&self) -> Arc<DatabaseManager> {
        let mut config = self.isolated_config();
        if matches!(config.backend_type, StorageBackend::SQLite) {
            config.connection_string = Some(
                crate::storage_api_test_support_tests::unique_path("sqlite-transactional")
                    .to_string_lossy()
                    .into_owned(),
            );
            config
                .sqlite
                .get_or_insert_with(storage_provider::SqliteSettings::default)
                .force_file_backed_database = true;
        }
        Arc::new(
            DatabaseManager::new_with_config_and_runtime_options(
                config,
                conformance_runtime_options(),
            )
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
            StorageBackend::RocksDB => {
                let mut config = self.config.clone();
                config.file_path = Some(
                    crate::storage_api_test_support_tests::unique_path("rocksdb-conformance")
                        .to_string_lossy()
                        .into_owned(),
                );
                config
            }
            StorageBackend::FoundationDb => {
                let mut config = self.config.clone();
                config
                    .foundationdb
                    .as_mut()
                    .expect("FoundationDB settings")
                    .subspace_prefix = Some(
                    crate::storage_api_test_support_tests::unique_path("fdb-conformance")
                        .to_string_lossy()
                        .into_owned(),
                );
                config
            }
            _ => self.config.clone(),
        }
    }
}

pub async fn for_each_conformance_backend<F, Fut>(mut test: F)
where
    F: FnMut(&'static str, Arc<DatabaseManager>) -> Fut,
    Fut: Future<Output = ()>,
{
    for backend in default_conformance_backends() {
        let name = backend.name;
        let db = backend.create_db().await;
        test(name, db).await;
    }
}

pub async fn for_each_transactional_conformance_backend<F, Fut>(mut test: F)
where
    F: FnMut(&'static str, Arc<DatabaseManager>) -> Fut,
    Fut: Future<Output = ()>,
{
    for backend in default_conformance_backends() {
        let name = backend.name;
        let db = backend.create_transactional_db().await;
        test(name, db).await;
    }
}

fn conformance_runtime_options() -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build()
}

pub async fn create_sqlite_transactional_test_db() -> Arc<DatabaseManager> {
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(
            crate::storage_api_test_support_tests::unique_path("sqlite-transactional")
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
    reject_uncompiled_external_selection();
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
                    crate::storage_api_test_support_tests::unique_path("rocksdb-conformance")
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
    if let Some(dsn) = postgres_conformance_dsn() {
        backends.push(ConformanceTestBackend {
            name: "postgres",
            config: StorageConfig {
                backend_type: StorageBackend::Postgres,
                connection_string: Some(dsn.clone()),
                file_path: None,
                sqlite: None,
                postgres: Some(storage_provider::PostgresSettings {
                    dsn,
                    max_pool_size: 8,
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
    if foundationdb_conformance_selected() {
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
                    cluster_file: std::env::var("TEST_FDB_CLUSTER_FILE").ok(),
                    subspace_prefix: Some(
                        crate::storage_api_test_support_tests::unique_path("fdb-conformance")
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

    require_selected_conformance_backends(backends)
}

fn reject_uncompiled_external_selection() {
    #[cfg(not(feature = "postgres"))]
    if std::env::var_os("AUX_STORAGE_CONFORMANCE_POSTGRES_DSN").is_some() {
        panic!("AUX_STORAGE_CONFORMANCE_POSTGRES_DSN requires the postgres feature");
    }

    #[cfg(not(feature = "foundationdb"))]
    if std::env::var_os("AUX_STORAGE_CONFORMANCE_FOUNDATIONDB").is_some() {
        panic!("AUX_STORAGE_CONFORMANCE_FOUNDATIONDB requires the foundationdb feature");
    }
}

fn require_selected_conformance_backends(
    backends: Vec<ConformanceTestBackend>,
) -> Vec<ConformanceTestBackend> {
    assert!(
        !backends.is_empty(),
        concat!(
            "no storage-api conformance backend selected; compile a provider feature or configure ",
            "AUX_STORAGE_CONFORMANCE_POSTGRES_DSN/AUX_STORAGE_CONFORMANCE_FOUNDATIONDB"
        )
    );
    backends
}

#[cfg(feature = "postgres")]
fn postgres_conformance_dsn() -> Option<String> {
    match std::env::var("AUX_STORAGE_CONFORMANCE_POSTGRES_DSN") {
        Ok(dsn) => {
            assert!(
                !dsn.trim().is_empty(),
                "AUX_STORAGE_CONFORMANCE_POSTGRES_DSN must not be empty"
            );
            Some(dsn)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("AUX_STORAGE_CONFORMANCE_POSTGRES_DSN must be valid Unicode")
        }
    }
}

#[cfg(feature = "foundationdb")]
fn foundationdb_conformance_selected() -> bool {
    match std::env::var("AUX_STORAGE_CONFORMANCE_FOUNDATIONDB") {
        Ok(value) => parse_foundationdb_selector(Some(&value))
            .unwrap_or_else(|message| panic!("{message}, got {value:?}")),
        Err(std::env::VarError::NotPresent) => false,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("AUX_STORAGE_CONFORMANCE_FOUNDATIONDB must be valid Unicode")
        }
    }
}

fn parse_foundationdb_selector(value: Option<&str>) -> Result<bool, &'static str> {
    match value {
        None => Ok(false),
        Some("1") => Ok(true),
        Some(_) => Err("AUX_STORAGE_CONFORMANCE_FOUNDATIONDB must be exactly 1 when set"),
    }
}

mod conformance_registry_tests;
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
        let schemas = schemas.drain(..).rev().collect::<Vec<_>>();
        if schemas.is_empty() {
            return;
        }

        if tokio::runtime::Handle::try_current().is_ok() {
            for cleanup in schemas {
                cleanup.drop_schema_detached();
            }
        } else {
            for cleanup in schemas {
                cleanup.drop_schema_blocking();
            }
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
    fn drop_schema_detached(self) {
        let result = self.drop_schema_command().spawn();
        if let Err(err) = result {
            eprintln!(
                "failed to run psql to drop postgres conformance schema {}: {err}",
                self.schema
            );
        }
    }

    fn drop_schema_blocking(self) {
        let status = self.drop_schema_command().status();
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

    fn drop_schema_command(&self) -> std::process::Command {
        let mut command = std::process::Command::new("psql");
        command
            .arg(&self.dsn)
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-q")
            .arg("-c")
            .arg(format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.schema));
        command
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
    StorageApiManagerImpl::new(db).create_table(request).await
}

pub async fn handle_list_tables(
    db: Arc<DatabaseManager>,
    request: ListTablesRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).list_tables(request).await
}

pub async fn handle_put_item(
    db: Arc<DatabaseManager>,
    request: PutItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).put_item(request).await
}

pub async fn handle_get_item(
    db: Arc<DatabaseManager>,
    request: GetItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).get_item(request).await
}

pub async fn handle_delete_item(
    db: Arc<DatabaseManager>,
    request: DeleteItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).delete_item(request).await
}

pub async fn handle_update_item(
    db: Arc<DatabaseManager>,
    request: UpdateItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).update_item(request).await
}

pub async fn handle_describe_table(
    db: Arc<DatabaseManager>,
    request: DescribeTableRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).describe_table(request).await
}

pub async fn handle_update_table(
    db: Arc<DatabaseManager>,
    request: UpdateTableRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).update_table(request).await
}

pub async fn handle_batch_write_item(
    db: Arc<DatabaseManager>,
    request: BatchWriteItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db)
        .batch_write_item(request)
        .await
}

pub async fn handle_batch_get_item(
    db: Arc<DatabaseManager>,
    request: BatchGetItemRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).batch_get_item(request).await
}

pub async fn handle_transact_write_items(
    db: Arc<DatabaseManager>,
    request: TransactWriteItemsRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db)
        .transact_write_items(request)
        .await
}

pub async fn handle_transact_get_items(
    db: Arc<DatabaseManager>,
    request: TransactGetItemsRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db)
        .transact_get_items(request)
        .await
}

pub async fn handle_scan(
    db: Arc<DatabaseManager>,
    request: ScanRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).scan(request).await
}

pub async fn handle_query(
    db: Arc<DatabaseManager>,
    request: QueryRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db).query(request).await
}

pub async fn handle_describe_time_to_live(
    db: Arc<DatabaseManager>,
    request: DescribeTimeToLiveRequest,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db)
        .describe_time_to_live(request)
        .await
}

pub async fn handle_clear_all_tables(
    db: Arc<DatabaseManager>,
    payload: Value,
) -> Result<Response, HttpApiError> {
    StorageApiManagerImpl::new(db)
        .clear_all_tables(payload)
        .await
}
