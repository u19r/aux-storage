//! Isolated replacement-backend fixture shared by operation tests.

use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use bg_job_store::ScheduledJobStoreSource as _;
use queue_provider::QueueOperations;
use storage_provider::{
    DynamoOperations, StreamRetentionRepository, SystemStreamTrimRepository,
    TableReclaimRepository,
    maintenance::{ExpirationRepository, GsiBuildRepository},
};
use storage_types::{
    AttributeDefinition, BillingMode, Clock, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, StorageError, TableName, TimestampMillis,
    context::WrappedError as _,
};
use stream_provider::StreamOperations;

struct TestClock {
    millis: AtomicI64,
    system_time: AtomicBool,
}

impl TestClock {
    fn new() -> Self {
        Self {
            millis: AtomicI64::new(TimestampMillis::now().timestamp_millis()),
            system_time: AtomicBool::new(false),
        }
    }

    fn advance(&self, duration: Duration) -> TimestampMillis {
        assert!(
            !self.system_time.load(Ordering::SeqCst),
            "cannot advance wall time"
        );
        let millis = i64::try_from(duration.as_millis()).unwrap();
        let before = self.millis.fetch_add(millis, Ordering::SeqCst);
        TimestampMillis::from_timestamp(before.checked_add(millis).unwrap())
    }
}

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        if self.system_time.load(Ordering::SeqCst) {
            return TimestampMillis::now();
        }
        TimestampMillis::from_timestamp(self.millis.load(Ordering::SeqCst))
    }
}

pub(crate) struct ConformanceFixtures(OnceLock<HashMap<String, &'static str>>);

impl ConformanceFixtures {
    pub(crate) const fn new() -> Self {
        Self(OnceLock::new())
    }

    pub(crate) fn case(&self, contents: &'static str, source: &str) -> serde_json::Value {
        let cases = self.0.get_or_init(|| {
            contents
                .lines()
                .map(|line| {
                    let value: serde_json::Value = serde_json::from_str(line).unwrap();
                    let source = value["source"].as_str().unwrap().to_owned();
                    (source, line)
                })
                .collect()
        });
        let line = cases
            .get(source)
            .unwrap_or_else(|| panic!("missing conformance fixture: {source}"));
        let seed = uuid::Uuid::new_v4().simple().to_string();
        serde_json::from_str(&line.replace("<<seed>>", &seed)).unwrap()
    }
}

pub(crate) fn assert_dynamo_error_response_exact(
    error: StorageError,
    expected_status: axum::http::StatusCode,
    expected_body: &str,
) {
    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::from(error).into();
    assert_eq!(status, expected_status);
    assert_eq!(serde_json::to_string(&body).unwrap(), expected_body);
}

/// Match the conformance runner's comparison of Dynamo sets, whose wire order
/// is not defined.
pub(crate) fn assert_conformance_body(
    actual: &serde_json::Value,
    expected: &serde_json::Value,
    source: &str,
) {
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    normalize_attribute_sets(&mut actual);
    normalize_attribute_sets(&mut expected);
    assert_eq!(actual, expected, "{source}");
}

pub(crate) fn normalize_attribute_sets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for set_key in ["SS", "NS", "BS"] {
                if let Some(serde_json::Value::Array(values)) = map.get_mut(set_key) {
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
            }
            for child in map.values_mut() {
                normalize_attribute_sets(child);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                normalize_attribute_sets(child);
            }
        }
        _ => {}
    }
}

pub(crate) fn hash_table_request(table: TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table,
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
}

pub(crate) fn unique_table_name() -> TableName {
    TableName::new(&format!("test_{}", uuid::Uuid::new_v4().simple()))
}

#[cfg(feature = "postgres")]
fn isolated_postgres_dsn(dsn: &str, schema: &str) -> String {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        let separator = if dsn.contains('?') { '&' } else { '?' };
        format!("{dsn}{separator}options=-csearch_path%3D{schema}")
    } else {
        format!("{dsn} options='-c search_path={schema}'")
    }
}

pub(crate) struct TestStore {
    pub(crate) operations: Arc<dyn DynamoOperations>,
    pub(crate) scheduled_job_store: bg_job_store::ScheduledJobStore,
    pub(crate) gsi_build_repository: Arc<dyn GsiBuildRepository>,
    pub(crate) expiration_repository: Arc<dyn ExpirationRepository>,
    pub(crate) ttl_sweep_repository: Arc<dyn storage_provider::TtlSweepRepository>,
    pub(crate) stream_operations: Arc<dyn StreamOperations>,
    pub(crate) queue_operations: Arc<dyn QueueOperations>,
    pub(crate) pubsub_provider: Arc<dyn pubsub_provider::PubsubProvider>,
    pub(crate) stream_retention_repository: Arc<dyn StreamRetentionRepository>,
    pub(crate) system_stream_trim_repository: Arc<dyn SystemStreamTrimRepository>,
    pub(crate) table_reclaim_repository: Arc<dyn TableReclaimRepository>,
    clock: Arc<TestClock>,
    _directory: Option<tempfile::TempDir>,
    location: DatabaseLocation,
}

#[derive(Clone)]
enum DatabaseLocation {
    #[cfg(feature = "sqlite")]
    Sqlite { path: String },
    #[cfg(feature = "turso")]
    Turso { path: String },
    #[cfg(feature = "rocksdb")]
    RocksDb { path: std::path::PathBuf },
    #[cfg(feature = "postgres")]
    Postgres { dsn: String, schema: String },
    #[cfg(feature = "foundationdb")]
    FoundationDb {
        cluster_file: String,
        prefix: String,
    },
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
pub(crate) fn required_backend_setting(
    name: &str,
    value: Option<&str>,
    example: &str,
) -> Result<String, String> {
    match value.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value.to_owned()),
        _ => Err(format!(
            "{name} is required for this backend; set it before running tests, for example: \
             {example}"
        )),
    }
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn required_env(name: &str, example: &str) -> String {
    let value = std::env::var(name).ok();
    required_backend_setting(name, value.as_deref(), example)
        .unwrap_or_else(|message| panic!("{message}"))
}

#[derive(Default)]
struct FaultProbes {
    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
    sql: Option<Arc<dyn sql_backend::SqlFaultProbe>>,
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    kv: Option<Arc<dyn kv_backend::KvFaultProbe>>,
}

impl TestStore {
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    pub(crate) async fn new_kv_with_fault_probe(probe: Arc<dyn kv_backend::KvFaultProbe>) -> Self {
        Self::new_with_fault_probes(FaultProbes {
            kv: Some(probe),
            ..FaultProbes::default()
        })
        .await
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
    pub(crate) async fn new_sql_with_fault_probe(
        probe: Arc<dyn sql_backend::SqlFaultProbe>,
    ) -> Self {
        Self::new_with_fault_probes(FaultProbes {
            sql: Some(probe),
            ..FaultProbes::default()
        })
        .await
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_dsn(&self) -> String {
        match &self.location {
            DatabaseLocation::Postgres { dsn, schema } => isolated_postgres_dsn(dsn, schema),
            #[cfg(any(
                feature = "sqlite",
                feature = "turso",
                feature = "rocksdb",
                feature = "foundationdb"
            ))]
            _ => panic!("Postgres test requires an isolated Postgres schema"),
        }
    }

    fn from_store<S>(
        store: S,
        scheduled_job_store: bg_job_store::ScheduledJobStore,
        clock: Arc<TestClock>,
        directory: Option<tempfile::TempDir>,
        location: DatabaseLocation,
    ) -> Self
    where
        S: DynamoOperations
            + GsiBuildRepository
            + ExpirationRepository
            + storage_provider::TtlSweepRepository
            + StreamOperations
            + StreamRetentionRepository
            + SystemStreamTrimRepository
            + TableReclaimRepository
            + QueueOperations
            + pubsub_provider::PubsubProvider
            + 'static,
    {
        let store = Arc::new(store);
        Self {
            operations: store.clone(),
            scheduled_job_store,
            gsi_build_repository: store.clone(),
            expiration_repository: store.clone(),
            ttl_sweep_repository: store.clone(),
            stream_retention_repository: store.clone(),
            system_stream_trim_repository: store.clone(),
            table_reclaim_repository: store.clone(),
            stream_operations: store.clone(),
            queue_operations: store.clone(),
            pubsub_provider: store,
            clock,
            _directory: directory,
            location,
        }
    }

    pub(crate) fn use_system_time(&self) {
        self.clock.system_time.store(true, Ordering::SeqCst);
    }

    pub(crate) fn advance_time(&self, duration: Duration) -> TimestampMillis {
        self.clock.advance(duration)
    }

    pub(crate) fn now(&self) -> TimestampMillis {
        self.clock.now()
    }

    pub(crate) async fn create_conformance_table(&self, kind: &str, table: TableName) {
        match kind {
            "basic_pk" => self.create_hash_table(table).await,
            "basic_pk_sk" => {
                self.create_hash_range_table(table, KeyAttributeType::S)
                    .await
            }
            "basic_number_pk_sk" => {
                self.create_hash_range_table(table, KeyAttributeType::N)
                    .await
            }
            "basic_binary_pk_sk" => {
                self.create_hash_range_table(table, KeyAttributeType::B)
                    .await
            }
            "count_hash_key" => {
                let request = CreateTableRequest::new(
                    table,
                    vec![AttributeDefinition {
                        attribute_name: "COUNT".to_owned(),
                        attribute_type: KeyAttributeType::S,
                    }],
                    vec![KeySchemaElement {
                        attribute_name: "COUNT".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    BillingMode::PayPerRequest,
                );
                self.operations.create_table(request).await.unwrap();
            }
            other => panic!("unsupported conformance table: {other}"),
        }
    }

    pub(crate) async fn create_hash_table(&self, table: TableName) {
        self.operations
            .create_table(hash_table_request(table))
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "create table through replacement store: {:?}",
                    error.to_enum()
                )
            });
    }

    pub(crate) async fn create_standard_table(&self) -> TableName {
        let table = unique_table_name();
        self.create_hash_table(table.clone()).await;
        table
    }

    pub(crate) async fn create_hash_range_table(
        &self,
        table: TableName,
        key_type: KeyAttributeType,
    ) {
        let request = CreateTableRequest::new(
            table,
            [
                AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: key_type,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_owned(),
                    attribute_type: key_type,
                },
            ]
            .to_vec(),
            [
                KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_owned(),
                    key_type: KeyType::Range,
                },
            ]
            .to_vec(),
            BillingMode::PayPerRequest,
        );
        self.operations
            .create_table(request)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "create hash-range table through replacement store: {:?}",
                    error.to_enum()
                )
            });
    }

    pub(crate) async fn finish(self) {
        let Self {
            operations,
            scheduled_job_store,
            gsi_build_repository,
            expiration_repository,
            ttl_sweep_repository,
            stream_operations,
            queue_operations,
            pubsub_provider,
            stream_retention_repository,
            system_stream_trim_repository,
            table_reclaim_repository,
            clock: _,
            _directory,
            location,
        } = self;
        drop(operations);
        drop(scheduled_job_store);
        drop(gsi_build_repository);
        drop(expiration_repository);
        drop(ttl_sweep_repository);
        drop(stream_operations);
        drop(queue_operations);
        drop(pubsub_provider);
        drop(stream_retention_repository);
        drop(system_stream_trim_repository);
        drop(table_reclaim_repository);
        match location {
            #[cfg(feature = "sqlite")]
            DatabaseLocation::Sqlite { .. } => {}
            #[cfg(feature = "turso")]
            DatabaseLocation::Turso { .. } => {}
            #[cfg(feature = "rocksdb")]
            DatabaseLocation::RocksDb { .. } => {}
            #[cfg(feature = "postgres")]
            DatabaseLocation::Postgres { dsn, schema } => {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .unwrap();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
                    .execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"), &[])
                    .await
                    .unwrap();
            }
            #[cfg(feature = "foundationdb")]
            DatabaseLocation::FoundationDb {
                cluster_file,
                prefix,
            } => {
                let backend = kv_backend::foundationdb::FoundationDbBackend::initialize(
                    kv_backend::foundationdb::FoundationDbOptions {
                        cluster_file: Some(&cluster_file),
                        subspace_prefix: Some(prefix.as_bytes()),
                        tenant_name: None,
                        cache_read_version_ms: 0,
                        report_conflicting_keys: false,
                        transaction_byte_limit: None,
                    },
                )
                .await
                .unwrap();
                backend.delete_namespace_keys().await.unwrap();
                assert!(!backend.namespace_has_keys().await.unwrap());
            }
        }
    }

    #[cfg(feature = "sqlite")]
    pub(crate) async fn open_sqlite_file(path: &std::path::Path) -> Self {
        Self::open_location(
            DatabaseLocation::Sqlite {
                path: path.to_str().unwrap().to_owned(),
            },
            None,
            Arc::new(TestClock::new()),
            FaultProbes::default(),
        )
        .await
    }

    pub(crate) async fn reopen(mut self) -> Self {
        #[cfg(feature = "sqlite")]
        assert!(
            !matches!(&self.location, DatabaseLocation::Sqlite { path } if path == ":memory:"),
            "reopen requires a durable fixture"
        );
        let location = self.location.clone();
        let directory = self._directory.take();
        let clock = self.clock.clone();
        drop(self);
        Self::open_location(location, directory, clock, FaultProbes::default()).await
    }

    pub(crate) async fn new_durable() -> Self {
        Self::open_new(FaultProbes::default(), true, None).await
    }

    pub(crate) async fn new() -> Self {
        Self::new_with_fault_probes(FaultProbes::default()).await
    }

    async fn new_with_fault_probes(probes: FaultProbes) -> Self {
        Self::open_new(probes, false, None).await
    }

    #[cfg(feature = "postgres")]
    pub(crate) async fn new_postgres() -> Self {
        Self::open_new(FaultProbes::default(), false, Some("postgres")).await
    }

    async fn open_new(probes: FaultProbes, durable: bool, backend: Option<&str>) -> Self {
        let mut backend = backend.map(str::to_owned).unwrap_or_else(|| {
            std::env::var("STORAGE_TEST_BACKEND").unwrap_or_else(|_| {
                if cfg!(feature = "sqlite") {
                    "sqlite"
                } else if cfg!(feature = "turso") {
                    "turso"
                } else if cfg!(feature = "rocksdb") {
                    "rocksdb"
                } else if cfg!(feature = "postgres") {
                    "postgres"
                } else {
                    "foundationdb"
                }
                .to_owned()
            })
        });
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        if probes.kv.is_some() && matches!(backend.as_str(), "sqlite" | "turso" | "postgres") {
            backend = if cfg!(feature = "rocksdb") {
                "rocksdb"
            } else {
                "foundationdb"
            }
            .to_owned();
        }
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        if probes.sql.is_some() && matches!(backend.as_str(), "rocksdb" | "foundationdb") {
            backend = if cfg!(feature = "sqlite") {
                "sqlite"
            } else if cfg!(feature = "turso") {
                "turso"
            } else {
                "postgres"
            }
            .to_owned();
        }
        let (location, directory) = match backend.as_str() {
            #[cfg(feature = "sqlite")]
            "sqlite" if !durable => (
                DatabaseLocation::Sqlite {
                    path: ":memory:".to_owned(),
                },
                None,
            ),
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let directory = tempfile::tempdir().unwrap();
                let path = directory
                    .path()
                    .join("test.db")
                    .to_str()
                    .unwrap()
                    .to_owned();
                (DatabaseLocation::Sqlite { path }, Some(directory))
            }
            #[cfg(feature = "turso")]
            "turso" => {
                let directory = tempfile::tempdir().unwrap();
                let path = directory
                    .path()
                    .join("test.db")
                    .to_str()
                    .unwrap()
                    .to_owned();
                (DatabaseLocation::Turso { path }, Some(directory))
            }
            #[cfg(feature = "rocksdb")]
            "rocksdb" => {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("test.db");
                (DatabaseLocation::RocksDb { path }, Some(directory))
            }
            #[cfg(feature = "postgres")]
            "postgres" => {
                let dsn = required_env(
                    "TEST_POSTGRES_DSN",
                    "set TEST_POSTGRES_DSN for Postgres tests",
                );
                let schema = format!("rewrite_test_{}", uuid::Uuid::new_v4().simple());
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .unwrap();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
                    .execute(&format!("CREATE SCHEMA \"{schema}\""), &[])
                    .await
                    .unwrap();
                (DatabaseLocation::Postgres { dsn, schema }, None)
            }
            #[cfg(feature = "foundationdb")]
            "foundationdb" => {
                let cluster_file = required_env(
                    "FDB_CLUSTER_FILE",
                    "set FDB_CLUSTER_FILE for FoundationDB tests",
                );
                assert!(
                    std::path::Path::new(&cluster_file).is_file(),
                    "FDB_CLUSTER_FILE must exist"
                );
                let prefix = format!("rewrite-tests/{}", uuid::Uuid::new_v4().simple());
                (
                    DatabaseLocation::FoundationDb {
                        cluster_file,
                        prefix,
                    },
                    None,
                )
            }
            _ => panic!("STORAGE_TEST_BACKEND={backend:?} requires its compiled feature"),
        };
        // Durability only changes SQLite's default in-memory location.
        let _ = durable;
        Self::open_location(location, directory, Arc::new(TestClock::new()), probes).await
    }

    async fn open_location(
        location: DatabaseLocation,
        directory: Option<tempfile::TempDir>,
        clock: Arc<TestClock>,
        probes: FaultProbes,
    ) -> Self {
        match &location {
            #[cfg(feature = "sqlite")]
            DatabaseLocation::Sqlite { path } => {
                let backend = sql_backend::sqlite::SqliteBackend::initialize(path)
                    .await
                    .unwrap();
                let backend = if let Some(probe) = probes.sql.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = sql_backend::SqlStore::initialize_schema(backend)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                Self::from_store(store, jobs, clock, directory, location)
            }
            #[cfg(feature = "turso")]
            DatabaseLocation::Turso { path } => {
                let backend = sql_backend::turso::TursoBackend::initialize(path)
                    .await
                    .unwrap();
                let backend = if let Some(probe) = probes.sql.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = sql_backend::SqlStore::initialize_schema(backend)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                Self::from_store(store, jobs, clock, directory, location)
            }
            #[cfg(feature = "rocksdb")]
            DatabaseLocation::RocksDb { path } => {
                let backend =
                    kv_backend::rocksdb::RocksDbBackend::initialize(path.clone()).unwrap();
                let backend = if let Some(probe) = probes.kv.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = kv_backend::KvStore::initialize_format(backend)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                Self::from_store(store, jobs, clock, directory, location)
            }
            #[cfg(feature = "postgres")]
            DatabaseLocation::Postgres { dsn, schema } => {
                let isolated_dsn = isolated_postgres_dsn(dsn, schema);
                let backend =
                    sql_backend::postgres::PostgresBackend::initialize(&isolated_dsn, 4, false)
                        .await
                        .unwrap();
                let backend = if let Some(probe) = probes.sql.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = sql_backend::SqlStore::initialize_schema(backend)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                Self::from_store(store, jobs, clock, directory, location)
            }
            #[cfg(feature = "foundationdb")]
            DatabaseLocation::FoundationDb {
                cluster_file,
                prefix,
            } => {
                let backend = kv_backend::foundationdb::FoundationDbBackend::initialize(
                    kv_backend::foundationdb::FoundationDbOptions {
                        cluster_file: Some(cluster_file),
                        subspace_prefix: Some(prefix.as_bytes()),
                        tenant_name: None,
                        cache_read_version_ms: 0,
                        report_conflicting_keys: false,
                        transaction_byte_limit: None,
                    },
                )
                .await
                .unwrap();
                let backend = if let Some(probe) = probes.kv.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = kv_backend::KvStore::initialize_format(backend)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                Self::from_store(store, jobs, clock, directory, location)
            }
        }
    }
}
