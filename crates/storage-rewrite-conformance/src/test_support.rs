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
use storage_provider::{DynamoOperations, MaintenanceOperations, StorageStreamOperations};
use storage_types::{
    AttributeDefinition, BillingMode, Clock, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, StorageError, TableName, TimestampMillis,
    context::WrappedError as _,
};

struct TestClock {
    millis: AtomicI64,
    system_time: AtomicBool,
    retry_pause: std::sync::Mutex<Option<Arc<RetryPause>>>,
}

impl TestClock {
    fn new() -> Self {
        Self {
            millis: AtomicI64::new(TimestampMillis::now().timestamp_millis()),
            system_time: AtomicBool::new(false),
            retry_pause: std::sync::Mutex::new(None),
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
    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = storage_types::StorageResult<()>> + Send + '_>,
    > {
        let pause = self.retry_pause.lock().unwrap().take();
        Box::pin(async move {
            if let Some(pause) = pause {
                pause.arrived.notify_one();
                pause.released.notified().await;
            }
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }
}

/// Hold a retry after native rollback and before its next attempt.
#[derive(Default)]
pub(crate) struct RetryPause {
    pub(crate) arrived: tokio::sync::Notify,
    pub(crate) released: tokio::sync::Notify,
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
        format!("{dsn}{separator}options=-csearch_path%3D{schema}&application_name={schema}")
    } else {
        format!("{dsn} options='-c search_path={schema}' application_name={schema}")
    }
}

pub(crate) type BoundedQueueCounts = Arc<
    dyn Fn(
            String,
            std::num::NonZeroU32,
        ) -> futures::future::BoxFuture<
            'static,
            queue_provider::QueueResult<Option<queue_provider::QueueMessageCounts>>,
        > + Send
        + Sync,
>;

pub(crate) type CanonicalHistoryRead = Arc<
    dyn Fn(
            Vec<stream_provider::TableHistoryIndexRead>,
        ) -> futures::future::BoxFuture<
            'static,
            storage_types::StorageResult<Vec<stream_provider::TableHistoryIndexPage>>,
        > + Send
        + Sync,
>;

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
pub(crate) type NativeGsiRead = Arc<
    dyn Fn(
            TableName,
            Vec<storage_backfill::GsiProjectionKey>,
            bool,
        ) -> futures::future::BoxFuture<
            'static,
            storage_types::StorageResult<(
                Vec<storage_provider::NativeProjectionAddress>,
                Vec<storage_types::WireItem>,
            )>,
        > + Send
        + Sync,
>;

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
fn native_gsi_read<B: kv_backend::KvBackend + 'static>(
    store: kv_backend::KvStore<B>,
) -> NativeGsiRead {
    use storage_provider::{CatalogAttempt as _, ProjectionScanAttempt as _};
    let store = Arc::new(store);
    Arc::new(move |name, keys, scan| {
        let store = Arc::clone(&store);
        Box::pin(async move {
            storage_provider::DynamoPort::with_attempt(
                store.as_ref(),
                storage_types::AttemptPlan::new(
                    storage_types::AttemptMode::ReadOnly,
                    storage_types::DeadlineClass::PointRead,
                )
                .start(storage_types::AttemptDeadlineSource::attempt_deadlines(
                    store.as_ref(),
                )),
                move |attempt| {
                    Box::pin(async move {
                        let stamp = attempt.read_catalog_stamp(&name).await?.ok_or_else(|| {
                            StorageError::internal("native GSI fixture table is missing")
                        })?;
                        let table = attempt.read_table_at_stamp(&name, stamp).await?;
                        let addresses = attempt
                            .encode_gsi_projection_addresses(&table, &keys)
                            .await?;
                        for address in &addresses {
                            let index = storage_types::IndexName::new("by_g");
                            let incarnation = &table.binding.gsi(&index).unwrap().build;
                            address.require_scope(&table, &index, incarnation)?;
                            let mut other_table = table.as_ref().clone();
                            other_table.history_id = [99; 16].into();
                            assert!(
                                address
                                    .require_scope(&other_table, &index, incarnation)
                                    .is_err()
                            );
                            assert!(
                                address
                                    .require_scope(
                                        &table,
                                        &storage_types::IndexName::new("other"),
                                        incarnation
                                    )
                                    .is_err()
                            );
                            assert!(
                                address
                                    .require_scope(&table, &index, &[99; 16].into())
                                    .is_err()
                            );
                        }
                        let images = if scan {
                            let request = storage_provider::GsiProjectionScan::try_new(
                                table,
                                storage_types::IndexName::new("by_g"),
                                None,
                                Some(2),
                            )?;
                            attempt
                                .scan_gsi_projections(&request)
                                .await?
                                .items
                                .into_iter()
                                .map(|row| row.image)
                                .collect()
                        } else {
                            Vec::new()
                        };
                        Ok((addresses, images))
                    })
                },
            )
            .await
        })
    })
}

pub(crate) type CatalogGsiPhase = Arc<
    dyn Fn(
            TableName,
        ) -> futures::future::BoxFuture<
            'static,
            storage_types::StorageResult<storage_types::GsiPhase>,
        > + Send
        + Sync,
>;

/// One page of the regional markers of a table (table, after, limit) with each
/// marker key decoded to its key attributes.
pub(crate) type RegionalMarkerRead = Arc<
    dyn Fn(
            TableName,
            Option<Vec<u8>>,
            std::num::NonZeroU32,
        ) -> futures::future::BoxFuture<
            'static,
            storage_types::StorageResult<(
                storage_provider::MarkerPage,
                Vec<storage_types::KeyAttributes>,
            )>,
        > + Send
        + Sync,
>;

pub(crate) struct TestStore {
    pub(crate) catalog_gsi_phase: CatalogGsiPhase,
    pub(crate) regional_markers: RegionalMarkerRead,
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    pub(crate) native_gsi_read: Option<NativeGsiRead>,
    pub(crate) operations: Arc<dyn DynamoOperations>,
    pub(crate) canonical_history_read: CanonicalHistoryRead,
    pub(crate) independent_operations: Option<Arc<dyn DynamoOperations>>,
    pub(crate) scheduled_job_store: bg_job_store::ScheduledJobStore,
    /// Scheduled maintenance steps and maintenance commands of this store.
    pub(crate) maintenance: Arc<dyn MaintenanceOperations>,
    pub(crate) stream_operations: Arc<dyn StorageStreamOperations>,
    pub(crate) queue_operations: Arc<dyn QueueOperations>,
    /// `GetQueueAttributes` counts with an explicit region bound.
    pub(crate) bounded_queue_counts: BoundedQueueCounts,
    pub(crate) pubsub_provider: Arc<dyn pubsub_provider::PubsubProvider>,
    pub(crate) regional_bindings: Arc<dyn storage_provider::RegionalBindingOperations>,
    pub(crate) regional_receive: Arc<dyn storage_provider::RegionalReceiveOperations>,
    pub(crate) regional_send: Arc<dyn storage_provider::RegionalSendOperations>,
    pub(crate) regional_pairing: Arc<dyn storage_provider::RegionalPairingOperations>,
    pub(crate) regional_unpair: Arc<dyn storage_provider::RegionalUnpairOperations>,
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
    /// Replaces the fixture budgets, for a deadline that must pass in real
    /// time.
    attempt_deadlines: Option<storage_types::AttemptDeadlines>,
}

impl TestStore {
    pub(crate) fn pause_next_retry(&self) -> Arc<RetryPause> {
        let pause = Arc::new(RetryPause::default());
        *self.clock.retry_pause.lock().unwrap() = Some(Arc::clone(&pause));
        pause
    }

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

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
    pub(crate) async fn new_durable_sql_with_fault_probe(
        probe: Arc<dyn sql_backend::SqlFaultProbe>,
    ) -> Self {
        Self::open_new(
            FaultProbes {
                sql: Some(probe),
                ..FaultProbes::default()
            },
            true,
            None,
            0,
        )
        .await
    }

    #[cfg(feature = "foundationdb")]
    pub(crate) fn foundationdb_process_scope(&self) -> (String, String) {
        match &self.location {
            DatabaseLocation::FoundationDb {
                cluster_file,
                prefix,
            } => (cluster_file.clone(), prefix.clone()),
            #[cfg(any(
                feature = "sqlite",
                feature = "turso",
                feature = "postgres",
                feature = "rocksdb"
            ))]
            _ => panic!("process proof requires a FoundationDB fixture"),
        }
    }

    #[cfg(feature = "foundationdb")]
    pub(crate) async fn open_foundationdb_process_scope(
        cluster_file: String,
        prefix: String,
        probe: Arc<dyn kv_backend::KvFaultProbe>,
    ) -> Self {
        Self::open_location(
            DatabaseLocation::FoundationDb {
                cluster_file,
                prefix,
            },
            None,
            Arc::new(TestClock::new()),
            FaultProbes {
                kv: Some(probe),
                ..Default::default()
            },
            0,
        )
        .await
    }

    /// The SQLite database file of this store, if it is a SQLite store.
    #[cfg(feature = "sqlite")]
    pub(crate) fn sqlite_path(&self) -> Option<&str> {
        match &self.location {
            DatabaseLocation::Sqlite { path } => Some(path),
            #[cfg(any(
                feature = "turso",
                feature = "postgres",
                feature = "rocksdb",
                feature = "foundationdb"
            ))]
            _ => None,
        }
    }

    /// The Turso database file of this store, if it is a Turso store.
    #[cfg(feature = "turso")]
    pub(crate) fn turso_path(&self) -> Option<&str> {
        match &self.location {
            DatabaseLocation::Turso { path } => Some(path),
            #[cfg(any(
                feature = "sqlite",
                feature = "postgres",
                feature = "rocksdb",
                feature = "foundationdb"
            ))]
            _ => None,
        }
    }

    #[cfg(feature = "postgres")]
    /// Run `sql` on a fresh connection to this store's Postgres database.
    #[cfg(feature = "postgres")]
    pub(crate) async fn execute_postgres(&self, sql: &str) {
        let (client, connection) =
            tokio_postgres::connect(&self.postgres_dsn(), tokio_postgres::NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.batch_execute(sql).await.unwrap();
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_dsn(&self) -> String {
        match &self.location {
            #[cfg(feature = "postgres")]
            DatabaseLocation::Postgres { dsn, schema } => isolated_postgres_dsn(dsn, schema),
            #[allow(unreachable_patterns)]
            _ => panic!("Postgres test requires an isolated Postgres schema"),
        }
    }

    /// Wait for this fixture's committed history to fall below PostgreSQL's
    /// cluster-wide readable bound. The observer never allocates a transaction
    /// ID.
    pub(crate) async fn wait_until_changes_are_readable(&self) {
        #[cfg(feature = "postgres")]
        if matches!(&self.location, DatabaseLocation::Postgres { .. }) {
            let (client, connection) =
                tokio_postgres::connect(&self.postgres_dsn(), tokio_postgres::NoTls)
                    .await
                    .unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let row = client
                .query_opt(
                    "SELECT offset_order FROM sys_stream_entries ORDER BY offset_order DESC LIMIT \
                     1",
                    &[],
                )
                .await
                .unwrap();
            let Some(row) = row else {
                return;
            };
            let bytes: Vec<u8> = row.get(0);
            let expected =
                u64::from_be_bytes(bytes.try_into().expect("history position has eight bytes"));
            self.wait_for_postgres_read_bound(&client, expected, 65_536)
                .await;
        }
    }

    pub(crate) async fn wait_until_user_item_is_readable(&self, item: storage_types::StreamItemId) {
        #[cfg(feature = "postgres")]
        if matches!(&self.location, DatabaseLocation::Postgres { .. }) {
            let (client, connection) =
                tokio_postgres::connect(&self.postgres_dsn(), tokio_postgres::NoTls)
                    .await
                    .unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            assert_eq!(&item.as_bytes()[..4], b"XID1");
            let expected = u64::from_be_bytes(item.as_bytes()[4..].try_into().unwrap());
            self.wait_for_postgres_read_bound(&client, expected, 1)
                .await;
        }
        let _ = item;
    }

    #[cfg(feature = "postgres")]
    async fn wait_for_postgres_read_bound(
        &self,
        client: &tokio_postgres::Client,
        expected: u64,
        scale: u64,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            // Each statement sees a fresh snapshot and its matching persisted
            // offset.
            let row = client
                .query_one(
                    "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint + \
                     transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
                    &[],
                )
                .await
                .unwrap();
            let bound = u64::try_from(row.get::<_, i64>(0))
                .unwrap()
                .checked_mul(scale)
                .unwrap();
            if expected < bound {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "committed position {expected} did not become readable; last exclusive bound \
                 {bound}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
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
            + MaintenanceOperations
            + StorageStreamOperations
            + QueueOperations
            + storage_provider::DynamoPort
            + queue_provider::QueuePort
            + pubsub_provider::PubsubProvider
            + 'static,
        for<'a> <S as storage_provider::DynamoPort>::Attempt<'a>:
            stream_provider::TableHistoryIndexAttempt,
        for<'a> <S as storage_provider::DynamoPort>::Attempt<'a>:
            storage_provider::RegionalCatalogAttempt,
        for<'a> <S as storage_provider::DynamoPort>::Attempt<'a>:
            storage_provider::RegionalReceiveAttempt,
        for<'a> <S as storage_provider::DynamoPort>::Attempt<'a>:
            storage_provider::RegionalPairingAttempt,
    {
        let store = Arc::new(store);
        let phase_store = Arc::clone(&store);
        let catalog_gsi_phase: CatalogGsiPhase = Arc::new(move |name| {
            use storage_provider::CatalogAttempt as _;
            let store = Arc::clone(&phase_store);
            Box::pin(async move {
                storage_provider::DynamoPort::with_attempt(
                    store.as_ref(),
                    storage_types::AttemptPlan::new(
                        storage_types::AttemptMode::ReadOnly,
                        storage_types::DeadlineClass::PointRead,
                    )
                    .start(
                        storage_types::AttemptDeadlineSource::attempt_deadlines(store.as_ref()),
                    ),
                    move |attempt| {
                        Box::pin(async move {
                            let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                            let table = attempt.read_table_at_stamp(&name, stamp).await?;
                            Ok(table
                                .binding
                                .gsi(&storage_types::IndexName::new("by_g"))
                                .unwrap()
                                .phase)
                        })
                    },
                )
                .await
            })
        });
        let markers_store = Arc::clone(&store);
        let regional_markers: RegionalMarkerRead = Arc::new(move |name, after, limit| {
            use storage_provider::{CatalogAttempt as _, RegionalMarkerAttempt as _};
            let store = Arc::clone(&markers_store);
            Box::pin(async move {
                storage_provider::DynamoPort::with_attempt(
                    store.as_ref(),
                    storage_types::AttemptPlan::new(
                        storage_types::AttemptMode::ReadOnly,
                        storage_types::DeadlineClass::PointRead,
                    )
                    .start(
                        storage_types::AttemptDeadlineSource::attempt_deadlines(store.as_ref()),
                    ),
                    move |attempt| {
                        Box::pin(async move {
                            let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                            let table = attempt.read_table_at_stamp(&name, stamp).await?;
                            let page = attempt
                                .list_regional_markers(&table, after.as_deref(), limit)
                                .await?;
                            let decoded = page
                                .markers
                                .iter()
                                .map(|(suffix, _)| {
                                    storage_types::ItemKey::from_sorted_storage_suffix(
                                        &table.info,
                                        None,
                                        suffix,
                                    )?
                                    .key_attributes(&table.info)
                                })
                                .collect::<storage_types::StorageResult<Vec<_>>>()?;
                            Ok((page, decoded))
                        })
                    },
                )
                .await
            })
        });
        let counts_store = Arc::clone(&store);
        let bounded_queue_counts: BoundedQueueCounts = Arc::new(move |url, scan_limit| {
            let store = Arc::clone(&counts_store);
            Box::pin(async move {
                queue_provider::get_queue_with_bounded_counts(store.as_ref(), &url, scan_limit)
                    .await
                    .map(|found| found.map(|(_, counts)| counts))
            })
        });
        let history_store = Arc::clone(&store);
        let canonical_history_read: CanonicalHistoryRead = Arc::new(move |reads| {
            let store = Arc::clone(&history_store);
            Box::pin(async move {
                storage_provider::DynamoPort::with_attempt(
                    store.as_ref(),
                    storage_types::AttemptPlan::new(
                        storage_types::AttemptMode::ReadOnly,
                        storage_types::DeadlineClass::PointRead,
                    )
                    .start(
                        storage_types::AttemptDeadlineSource::attempt_deadlines(store.as_ref()),
                    ),
                    move |attempt| {
                        Box::pin(async move {
                            stream_provider::read_table_history_indexes(attempt, &reads).await
                        })
                    },
                )
                .await
            })
        });
        Self {
            independent_operations: None,
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            native_gsi_read: None,
            bounded_queue_counts,
            canonical_history_read,
            catalog_gsi_phase,
            regional_markers,
            operations: store.clone(),
            scheduled_job_store,
            maintenance: store.clone(),
            stream_operations: store.clone(),
            queue_operations: store.clone(),
            regional_bindings: store.clone(),
            regional_receive: store.clone(),
            regional_send: store.clone(),
            regional_pairing: store.clone(),
            regional_unpair: store.clone(),
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

    pub(crate) fn set_time(&self, time: TimestampMillis) {
        assert!(
            !self.clock.system_time.load(Ordering::SeqCst),
            "cannot set wall time"
        );
        self.clock
            .millis
            .store(time.timestamp_millis(), Ordering::SeqCst);
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
            "basic_with_gsi" => {
                let request = CreateTableRequest::try_from(serde_json::json!({
                    "TableName": table.to_string(),
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "S"},
                        {"AttributeName": "gsi_pk", "AttributeType": "S"},
                        {"AttributeName": "gsi_sk", "AttributeType": "S"}
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"}
                    ],
                    "GlobalSecondaryIndexes": [{
                        "IndexName": "gsi1",
                        "KeySchema": [
                            {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                            {"AttributeName": "gsi_sk", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    }]
                }))
                .unwrap();
                self.operations.create_table(request).await.unwrap();
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
            canonical_history_read,
            catalog_gsi_phase,
            regional_markers,
            independent_operations,
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            native_gsi_read,
            scheduled_job_store,
            maintenance,
            stream_operations,
            queue_operations,
            bounded_queue_counts,
            pubsub_provider,
            regional_bindings,
            regional_receive,
            regional_send,
            regional_pairing,
            regional_unpair,
            clock: _,
            _directory,
            location,
        } = self;
        drop(independent_operations);
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        drop(native_gsi_read);
        drop(operations);
        drop(canonical_history_read);
        drop(catalog_gsi_phase);
        drop(regional_markers);
        drop(bounded_queue_counts);
        drop(scheduled_job_store);
        drop(maintenance);
        drop(stream_operations);
        drop(queue_operations);
        drop(pubsub_provider);
        drop(regional_bindings);
        drop(regional_receive);
        drop(regional_send);
        drop(regional_pairing);
        drop(regional_unpair);
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
                eprintln!("OWNED_FIXTURE deleted postgres schema={schema}");
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
                        reply_byte_limit: storage_types::FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
                        report_conflicting_keys: false,
                        transaction_byte_limit: None,
                        attempt_deadlines: fixture_deadlines(),
                    },
                )
                .await
                .unwrap();
                backend.delete_namespace_keys().await.unwrap();
                assert!(!backend.namespace_has_keys().await.unwrap());
                eprintln!("OWNED_FIXTURE deleted foundationdb namespace={prefix}");
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
            0,
        )
        .await
    }

    /// A SQLite store whose schema was created with replication off: no
    /// regional relation exists.
    #[cfg(feature = "sqlite")]
    pub(crate) async fn open_sqlite_regional_off(path: &std::path::Path) -> Self {
        let clock = Arc::new(TestClock::new());
        let backend = sql_backend::sqlite::SqliteBackend::initialize(
            path.to_str().unwrap(),
            fixture_deadlines(),
        )
        .await
        .unwrap()
        .with_foreign_await_policy(storage_common::writer_occupancy::ForeignAwaitPolicy::Fail);
        let store = sql_backend::SqlStore::initialize_schema(
            backend,
            storage_types::InitialStreamLineages::ONE,
            false,
        )
        .await
        .unwrap()
        .with_clock(clock.clone());
        let jobs = store.scheduled_job_store(Default::default()).unwrap();
        let location = DatabaseLocation::Sqlite {
            path: path.to_str().unwrap().to_owned(),
        };
        Self::from_store(store, jobs, clock, None, location)
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
        Self::open_location(location, directory, clock, FaultProbes::default(), 0).await
    }

    pub(crate) async fn new_durable() -> Self {
        Self::open_new(FaultProbes::default(), true, None, 0).await
    }

    pub(crate) async fn new() -> Self {
        Self::new_with_fault_probes(FaultProbes::default()).await
    }

    /// One probe for whichever family `STORAGE_TEST_BACKEND` selects.
    pub(crate) async fn new_with_native_probes(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))] sql: Arc<
            dyn sql_backend::SqlFaultProbe,
        >,
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))] kv: Arc<
            dyn kv_backend::KvFaultProbe,
        >,
    ) -> Self {
        Self::new_with_native_probes_and_deadlines(
            #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
            sql,
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            kv,
            None,
        )
        .await
    }

    /// As `new_with_native_probes`; `attempt_deadlines` replaces the fixture
    /// budgets when given.
    pub(crate) async fn new_with_native_probes_and_deadlines(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))] sql: Arc<
            dyn sql_backend::SqlFaultProbe,
        >,
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))] kv: Arc<
            dyn kv_backend::KvFaultProbe,
        >,
        attempt_deadlines: Option<storage_types::AttemptDeadlines>,
    ) -> Self {
        let kv_family = matches!(
            Self::selected_backend().as_str(),
            "rocksdb" | "foundationdb"
        );
        Self::new_with_fault_probes(FaultProbes {
            #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
            sql: (!kv_family).then_some(sql),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            kv: kv_family.then_some(kv),
            attempt_deadlines,
        })
        .await
    }

    /// Opt into cached ordinary writes through the production initialize path.
    /// Each nextest test owns its process, so this is the first nonzero network
    /// selection.
    pub(crate) async fn new_with_cached_write_probes(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))] sql: Arc<
            dyn sql_backend::SqlFaultProbe,
        >,
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))] kv: Arc<
            dyn kv_backend::KvFaultProbe,
        >,
    ) -> Self {
        let kv_family = matches!(
            Self::selected_backend().as_str(),
            "rocksdb" | "foundationdb"
        );
        Self::open_new(
            FaultProbes {
                #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
                sql: (!kv_family).then_some(sql),
                #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
                kv: kv_family.then_some(kv),
                attempt_deadlines: None,
            },
            false,
            None,
            100,
        )
        .await
    }

    /// A store whose native drivers report every event to `census`. SQLite
    /// uses a file, so its durability barriers run as in production.
    pub(crate) async fn new_with_census(
        census: Arc<crate::native_census_test_support::NativeCensus>,
    ) -> Self {
        let kv_family = matches!(
            Self::selected_backend().as_str(),
            "rocksdb" | "foundationdb"
        );
        Self::open_new(
            FaultProbes {
                #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
                sql: (!kv_family)
                    .then(|| Arc::clone(&census) as Arc<dyn sql_backend::SqlFaultProbe>),
                #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
                kv: kv_family.then(|| Arc::clone(&census) as Arc<dyn kv_backend::KvFaultProbe>),
                attempt_deadlines: None,
            },
            true,
            None,
            0,
        )
        .await
    }

    /// A second store over the same database: another process of a shared
    /// database provider (Postgres, FoundationDB). Drop it before `finish`
    /// of the first store, never `finish` it.
    pub(crate) async fn open_same_database(&self) -> Self {
        Self::open_location(
            self.location.clone(),
            None,
            Arc::new(TestClock::new()),
            FaultProbes::default(),
            0,
        )
        .await
    }

    /// As `open_same_database`, with the native drivers reporting to `census`.
    pub(crate) async fn open_same_database_with_census(
        &self,
        census: Arc<crate::native_census_test_support::NativeCensus>,
    ) -> Self {
        let kv_family = matches!(
            Self::selected_backend().as_str(),
            "rocksdb" | "foundationdb"
        );
        Self::open_location(
            self.location.clone(),
            None,
            Arc::new(TestClock::new()),
            FaultProbes {
                #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
                sql: (!kv_family)
                    .then(|| Arc::clone(&census) as Arc<dyn sql_backend::SqlFaultProbe>),
                #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
                kv: kv_family.then(|| Arc::clone(&census) as Arc<dyn kv_backend::KvFaultProbe>),
                attempt_deadlines: None,
            },
            0,
        )
        .await
    }

    /// The provider that `STORAGE_TEST_BACKEND` selects, or the default.
    pub(crate) fn selected_backend() -> String {
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
    }

    async fn new_with_fault_probes(probes: FaultProbes) -> Self {
        Self::open_new(probes, false, None, 0).await
    }

    #[cfg(feature = "postgres")]
    pub(crate) async fn new_postgres() -> Self {
        Self::open_new(FaultProbes::default(), false, Some("postgres"), 0).await
    }

    async fn open_new(
        probes: FaultProbes,
        durable: bool,
        backend: Option<&str>,
        _cache_read_version_ms: u16,
    ) -> Self {
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
                eprintln!("OWNED_FIXTURE created postgres schema={schema}");
                (DatabaseLocation::Postgres { dsn, schema }, None)
            }
            #[cfg(feature = "foundationdb")]
            "foundationdb" => {
                let cluster_file = required_env(
                    "TEST_FDB_CLUSTER_FILE",
                    "set TEST_FDB_CLUSTER_FILE for FoundationDB tests",
                );
                assert!(
                    std::path::Path::new(&cluster_file).is_file(),
                    "TEST_FDB_CLUSTER_FILE must exist"
                );
                let prefix = format!("rewrite-tests/{}", uuid::Uuid::new_v4().simple());
                eprintln!("OWNED_FIXTURE created foundationdb namespace={prefix}");
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
        Self::open_location(
            location,
            directory,
            Arc::new(TestClock::new()),
            probes,
            _cache_read_version_ms,
        )
        .await
    }

    async fn open_location(
        location: DatabaseLocation,
        directory: Option<tempfile::TempDir>,
        clock: Arc<TestClock>,
        probes: FaultProbes,
        _cache_read_version_ms: u16,
    ) -> Self {
        let deadlines = probes.attempt_deadlines.unwrap_or_else(fixture_deadlines);
        match &location {
            #[cfg(feature = "sqlite")]
            DatabaseLocation::Sqlite { path } => {
                let backend = sql_backend::sqlite::SqliteBackend::initialize(path, deadlines)
                    .await
                    .unwrap()
                    .with_foreign_await_policy(
                        storage_common::writer_occupancy::ForeignAwaitPolicy::Fail,
                    );
                let backend = if let Some(probe) = probes.sql.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = sql_backend::SqlStore::initialize_schema(
                    backend,
                    storage_types::InitialStreamLineages::ONE,
                    true,
                )
                .await
                .unwrap()
                .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                let independent = store
                    .clone()
                    .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
                let mut fixture = Self::from_store(store, jobs, clock, directory, location);
                fixture.independent_operations = Some(Arc::new(independent));
                fixture
            }
            #[cfg(feature = "turso")]
            DatabaseLocation::Turso { path } => {
                let backend = sql_backend::turso::TursoBackend::initialize(path, deadlines)
                    .await
                    .unwrap();
                let backend = if let Some(probe) = probes.sql.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = sql_backend::SqlStore::initialize_schema(
                    backend,
                    storage_types::InitialStreamLineages::ONE,
                    true,
                )
                .await
                .unwrap()
                .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                let independent = store
                    .clone()
                    .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
                let mut fixture = Self::from_store(store, jobs, clock, directory, location);
                fixture.independent_operations = Some(Arc::new(independent));
                fixture
            }
            #[cfg(feature = "rocksdb")]
            DatabaseLocation::RocksDb { path } => {
                let backend =
                    kv_backend::rocksdb::RocksDbBackend::initialize(path.clone(), deadlines, 256)
                        .unwrap()
                        .with_foreign_await_policy(
                            storage_common::writer_occupancy::ForeignAwaitPolicy::Fail,
                        )
                        .unwrap();
                let backend = if let Some(probe) = probes.kv.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = kv_backend::KvStore::initialize_format(
                    backend,
                    storage_types::InitialStreamLineages::ONE,
                )
                .await
                .unwrap()
                .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                let native_gsi_read = native_gsi_read(store.clone());
                let independent = store
                    .clone()
                    .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
                let mut fixture = Self::from_store(store, jobs, clock, directory, location);
                fixture.independent_operations = Some(Arc::new(independent));
                fixture.native_gsi_read = Some(native_gsi_read);
                fixture
            }
            #[cfg(feature = "postgres")]
            DatabaseLocation::Postgres { dsn, schema } => {
                let isolated_dsn = isolated_postgres_dsn(dsn, schema);
                let backend = sql_backend::postgres::PostgresBackend::initialize(
                    &isolated_dsn,
                    4,
                    false,
                    deadlines,
                )
                .await
                .unwrap();
                let backend = if let Some(probe) = probes.sql.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = sql_backend::SqlStore::initialize_schema(
                    backend,
                    storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
                    true,
                )
                .await
                .unwrap()
                .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                let independent = store
                    .clone()
                    .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
                let mut fixture = Self::from_store(store, jobs, clock, directory, location);
                fixture.independent_operations = Some(Arc::new(independent));
                fixture
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
                        cache_read_version_ms: _cache_read_version_ms,
                        reply_byte_limit: storage_types::FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
                        report_conflicting_keys: false,
                        transaction_byte_limit: None,
                        attempt_deadlines: deadlines,
                    },
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "FoundationDB test initialization failed: {error:#?}; {:?}",
                        error.as_ref()
                    )
                });
                let backend = if let Some(probe) = probes.kv.clone() {
                    backend.with_fault_probe(probe)
                } else {
                    backend
                };
                let store = kv_backend::KvStore::initialize_format(
                    backend,
                    storage_provider::StorageBackend::FoundationDb
                        .default_initial_stream_lineages(),
                )
                .await
                .unwrap()
                .with_clock(clock.clone());
                let jobs = store.scheduled_job_store(Default::default()).unwrap();
                let native_gsi_read = native_gsi_read(store.clone());
                let independent = store
                    .clone()
                    .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
                let mut fixture = Self::from_store(store, jobs, clock, directory, location);
                fixture.independent_operations = Some(Arc::new(independent));
                fixture.native_gsi_read = Some(native_gsi_read);
                fixture
            }
        }
    }
}

/// Fixture stores never fail by elapsed time on a loaded machine. Deadline
/// tests build their own backend and move an injected clock.
fn fixture_deadlines() -> storage_types::AttemptDeadlines {
    storage_types::AttemptDeadlines::try_new(
        [std::time::Duration::from_secs(120); storage_types::DeadlineClass::COUNT],
        std::time::Duration::from_millis(storage_types::DEFAULT_COMMIT_GRACE_MS),
    )
    .expect("fixture budgets are positive")
}
