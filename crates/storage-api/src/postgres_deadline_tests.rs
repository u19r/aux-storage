//! Real HTTP requests, manager admission, and PostgreSQL settlement.

use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use sql_backend::{SqlFaultCall, SqlFaultProbe, SqlNativeEvent};
use storage::{DatabaseManager, DatabaseManagerRuntimeOptions, InitializedStore};
use storage_provider::DynamoOperations as _;
use storage_types::{AttemptDeadlines, StorageError};

#[derive(Default)]
struct NativeProbe {
    calls: AtomicU64,
    begins: AtomicU64,
    commits: AtomicU64,
    rollbacks: AtomicU64,
    writes: AtomicU64,
    cleanups: AtomicU64,
    hold_cleanup: AtomicBool,
    cleanup_entered: tokio::sync::Notify,
    released: Mutex<bool>,
    release: Condvar,
}

impl SqlFaultProbe for NativeProbe {
    fn fail(&self, _: SqlFaultCall<'_>) -> Option<StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        None
    }
    fn observe(&self, event: SqlNativeEvent) {
        let counter = match event {
            SqlNativeEvent::Begin => &self.begins,
            SqlNativeEvent::Commit => &self.commits,
            SqlNativeEvent::Rollback => &self.rollbacks,
            SqlNativeEvent::RowsWritten { rows } => {
                self.writes.fetch_add(rows, Ordering::SeqCst);
                return;
            }
            _ => return,
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }
    fn before_postgres_connection_cleanup(&self) {
        self.cleanups.fetch_add(1, Ordering::SeqCst);
        if self.hold_cleanup.load(Ordering::SeqCst) {
            self.cleanup_entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release.wait(released).unwrap();
            }
        }
    }
}

impl NativeProbe {
    fn counts(&self) -> [u64; 5] {
        [
            &self.calls,
            &self.begins,
            &self.commits,
            &self.rollbacks,
            &self.writes,
        ]
        .map(|count| count.load(Ordering::SeqCst))
    }
    fn unblock(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

struct CleanupRelease(Arc<NativeProbe>);
impl Drop for CleanupRelease {
    fn drop(&mut self) {
        self.0.unblock();
    }
}

type Store = sql_backend::SqlStore<sql_backend::postgres::PostgresBackend>;

struct HttpFixture {
    admin: tokio_postgres::Client,
    admin_task: tokio::task::JoinHandle<()>,
    schema: String,
    store: Arc<Store>,
    database: Arc<DatabaseManager>,
    probe: Arc<NativeProbe>,
    client: reqwest::Client,
    url: String,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
    proxy: Option<sql_backend::postgres::reply_loss::ReplyLossForwarder>,
}

fn create_table() -> Value {
    json!({"TableName":"deadline", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}], "BillingMode":"PAY_PER_REQUEST"})
}
fn update_counter() -> Value {
    json!({"TableName":"deadline","Key":{"pk":{"S":"counter"}},"UpdateExpression":"ADD #counter :one",
        "ExpressionAttributeNames":{"#counter":"count"},"ExpressionAttributeValues":{":one":{"N":"1"}}})
}

impl HttpFixture {
    async fn open(with_proxy: bool, queue_ms: u64) -> Self {
        let dsn = std::env::var("TEST_POSTGRES_DSN").expect("owned TEST_POSTGRES_DSN");
        let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let admin_task = tokio::spawn(async move {
            connection.await.unwrap();
        });
        let schema = format!("http_deadline_{}", uuid::Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        eprintln!("http_fixture_schema={schema} phase=created");
        let mut scoped = reqwest::Url::parse(&dsn).unwrap();
        let mut pairs: Vec<(String, String)> = scoped
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let options = pairs
            .iter()
            .find(|(key, _)| key == "options")
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        pairs.retain(|(key, _)| key != "options");
        pairs.push((
            "options".into(),
            format!("{options} -csearch_path={schema}"),
        ));
        scoped.query_pairs_mut().clear().extend_pairs(&pairs);
        // PostgreSQL startup options use URI percent decoding, not form
        // decoding.
        scoped.set_query(Some(&scoped.query().unwrap().replace('+', "%20")));
        let deadlines =
            AttemptDeadlines::try_new([Duration::from_secs(10); 7], Duration::from_secs(30))
                .unwrap();
        let backend = sql_backend::postgres::PostgresBackend::initialize(
            scoped.as_str(),
            1,
            false,
            deadlines,
        )
        .await
        .unwrap();
        let store = Arc::new(
            sql_backend::SqlStore::initialize_schema(
                backend,
                storage_types::InitialStreamLineages::ONE,
                true,
            )
            .await
            .unwrap(),
        );
        store
            .create_table(serde_json::from_value(create_table()).unwrap())
            .await
            .unwrap();
        store
            .put_item(
                serde_json::from_value(
                    json!({"TableName":"deadline","Item":{"pk":{"S":"counter"},"count":{"N":"0"}}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let proxy = if with_proxy {
            Some(
                sql_backend::postgres::reply_loss::ReplyLossForwarder::bind(
                    scoped.as_str(),
                    b"COMMIT\0",
                )
                .await,
            )
        } else {
            None
        };
        let mut manager_dsn = scoped.clone();
        manager_dsn
            .query_pairs_mut()
            .append_pair("application_name", &format!("{schema}_manager"));
        manager_dsn.set_query(Some(&manager_dsn.query().unwrap().replace('+', "%20")));
        if let Some(proxy) = &proxy {
            manager_dsn.set_host(Some("127.0.0.1")).unwrap();
            manager_dsn.set_port(Some(proxy.port)).unwrap();
        }
        let probe = Arc::new(NativeProbe::default());
        let backend = sql_backend::postgres::PostgresBackend::initialize(
            manager_dsn.as_str(),
            1,
            false,
            deadlines,
        )
        .await
        .unwrap()
        .with_fault_probe(probe.clone());
        let manager_store = Arc::new(
            sql_backend::SqlStore::initialize_schema(
                backend,
                storage_types::InitialStreamLineages::ONE,
                true,
            )
            .await
            .unwrap(),
        );
        let config = storage_provider::StorageConfig {
            backend_type: storage_provider::StorageBackend::Postgres,
            initial_stream_lineages: None,
            connection_string: None,
            file_path: None,
            sqlite: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
            postgres: Some(storage_provider::PostgresSettings {
                dsn: manager_dsn.to_string(),
                max_pool_size: 1,
                tls: false,
            }),
        };
        let database = Arc::new(
            DatabaseManager::from_initialized_store(
                config,
                InitializedStore::native(manager_store),
                DatabaseManagerRuntimeOptions::builder()
                    .enable_database_jobs(false)
                    .attempt_deadlines(deadlines)
                    .admission_config(storage::AdmissionConfig {
                        minimum_concurrency: 1,
                        maximum_concurrency: 1,
                        control_reserve_concurrency: 0,
                        queue_capacity: 1,
                        max_queue_wait_ms: queue_ms,
                        ..Default::default()
                    })
                    .build(),
            )
            .await
            .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(
            super::service_host::prepare_app_state(
                database.clone(),
                &config::RootConfig::default(),
                &address.to_string(),
            )
            .unwrap(),
        );
        state.publish_startup_readiness();
        let router = crate::router::server_router(state, false);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            admin,
            admin_task,
            schema,
            store,
            database,
            probe,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(90))
                .build()
                .unwrap(),
            url: format!("http://{address}/"),
            stop,
            server,
            proxy,
        }
    }

    fn send(&self, operation: &str, body: Value) -> tokio::task::JoinHandle<reqwest::Response> {
        let client = self.client.clone();
        let url = self.url.clone();
        let operation = operation.to_owned();
        tokio::spawn(async move {
            client
                .post(url)
                .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
                .json(&body)
                .send()
                .await
                .unwrap()
        })
    }
    async fn wait_admission(&self, in_flight: usize, queued: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = self.database.default_admission_controller().snapshot();
                if snapshot.in_flight == in_flight && snapshot.queue_depth == queued {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual manager admission state");
        eprintln!("actual_admission in_flight={in_flight} queue_depth={queued}");
    }
    async fn warm_table(&self) {
        let response = self
            .send(
                "GetItem",
                json!({"TableName":"deadline","Key":{"pk":{"S":"counter"}},"ConsistentRead":true}),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        self.wait_admission(0, 0).await;
    }
    async fn counter(&self) -> Value {
        let response = self.store.get_item(serde_json::from_value(json!({"TableName":"deadline","Key":{"pk":{"S":"counter"}},"ConsistentRead":true})).unwrap()).await.unwrap();
        serde_json::to_value(response).unwrap()["Item"]["count"]["N"].clone()
    }
    async fn wait_blocked(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let blocked: bool = self
                    .admin
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pg_backend_pid() = \
                         ANY(pg_blocking_pids(pid)))",
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if blocked {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real native statement blocked by owned admin transaction");
        eprintln!("native_lock_wait observed=true");
    }
    async fn close(self) {
        self.probe.unblock();
        self.database.shutdown_background_services().await.unwrap();
        self.stop.send(()).unwrap();
        self.server.await.unwrap();
        drop(self.database);
        drop(self.store);
        if let Some(proxy) = self.proxy {
            tokio::time::timeout(Duration::from_secs(10), proxy.close())
                .await
                .unwrap();
        }
        let schema = self.schema.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let active: i64 = self
                    .admin
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND \
                         query <> '' AND state <> 'idle' AND query LIKE $1",
                        &[&format!("%{schema}%")],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if active == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
        eprintln!("http_fixture_schema={} phase=deleted", self.schema);
        drop(self.admin);
        self.admin_task.await.unwrap();
    }
}

async fn assert_unavailable(response: reqwest::Response) {
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get("retry-after").unwrap(), "1");
    let body = response.text().await.unwrap();
    assert!(body.contains("ServiceUnavailable"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_queued_cold_postgres_http_when_admission_expires_then_503_without_native_work() {
    let fixture = HttpFixture::open(false, 1000).await;
    let held = fixture
        .database
        .acquire_admission("default", storage::AdmissionClass::Write)
        .await
        .unwrap();
    let before = fixture.probe.counts();
    let request = fixture.send(
        "PutItem",
        json!({"TableName":"deadline","Item":{"pk":{"S":"rejected"}}}),
    );
    fixture.wait_admission(1, 1).await;
    assert_unavailable(request.await.unwrap()).await;
    assert_eq!(fixture.probe.counts(), before);
    drop(held);
    fixture.wait_admission(0, 0).await;
    let response = fixture
        .store
        .get_item(
            serde_json::from_value(
                json!({"TableName":"deadline","Key":{"pk":{"S":"rejected"}},"ConsistentRead":true}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.item.is_none());
    eprintln!("queued_expiry http=503 native_delta=[0,0,0,0,0] durable_item=absent");
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_locked_postgres_http_update_when_deadline_expires_then_session_dropped_and_no_increment()
 {
    let fixture = HttpFixture::open(false, 60000).await;
    fixture.warm_table().await;
    fixture
        .admin
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.sys_system_stream_scope IN ACCESS EXCLUSIVE MODE",
            fixture.schema
        ))
        .await
        .unwrap();
    let before = fixture.probe.counts();
    let cleanups_before = fixture.probe.cleanups.load(Ordering::SeqCst);
    let request = fixture.send("UpdateItem", update_counter());
    fixture.wait_blocked().await;
    fixture.wait_admission(1, 0).await;
    let response = tokio::time::timeout(Duration::from_secs(60), request)
        .await
        .unwrap()
        .unwrap();
    fixture.admin.batch_execute("ROLLBACK").await.unwrap();
    assert_unavailable(response).await;
    fixture.wait_admission(0, 0).await;
    let after = fixture.probe.counts();
    assert_eq!(after[2], before[2]);
    // An expiry before COMMIT drops the session (no ROLLBACK is sent); the
    // server ends the transaction when the socket closes.
    assert_eq!(after[3], before[3], "no ROLLBACK is acknowledged");
    let cleanups = fixture.probe.cleanups.load(Ordering::SeqCst) - cleanups_before;
    assert_eq!(cleanups, 1, "the expired session is discarded");
    assert_eq!(fixture.counter().await, "0");
    eprintln!("locked_update http=503 commits=0 session_discarded={cleanups} durable_counter=0");
    fixture.close().await;
}

fn manager_update() -> storage::UpdateItemInput {
    storage::UpdateItemInput::builder()
        .table_name(storage_types::TableName::new("deadline"))
        .key(
            serde_json::from_value::<storage_types::KeyAttributes>(json!({"pk":{"S":"counter"}}))
                .unwrap(),
        )
        .update_expression("ADD #counter :one".to_owned())
        .expression_attribute_names(
            serde_json::from_value::<storage_types::VecMap<String, String>>(
                json!({"#counter":"count"}),
            )
            .unwrap(),
        )
        .expression_attribute_values(
            serde_json::from_value::<storage_types::AttributeMap>(json!({":one":{"N":"1"}}))
                .unwrap(),
        )
        .build()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResponseWaiter {
    Http,
    Manager,
    DroppedManager,
}

async fn check_lost_commit(waiter: ResponseWaiter) {
    let fixture = HttpFixture::open(true, 60000).await;
    fixture.warm_table().await;
    let before = fixture.probe.counts();
    let proxy = fixture.proxy.as_ref().unwrap();
    fixture.probe.hold_cleanup.store(true, Ordering::SeqCst);
    let release = CleanupRelease(fixture.probe.clone());
    proxy.arm();
    let mut http = None;
    let mut manager = None;
    if waiter == ResponseWaiter::Http {
        http = Some(fixture.send("UpdateItem", update_counter()));
    } else {
        let database = fixture.database.clone();
        manager = Some(tokio::spawn(async move {
            database.update_item(manager_update()).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while proxy.commands.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real forwarded COMMIT");
    // A manager response waiter can disappear without dropping its detached
    // native owner.
    if waiter == ResponseWaiter::DroppedManager {
        let task = manager.take().unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    tokio::time::timeout(
        Duration::from_secs(60),
        fixture.probe.cleanup_entered.notified(),
    )
    .await
    .expect("actual ConnectionTask Drop reached");
    fixture.wait_admission(1, 0).await;
    if let Some(http) = &http {
        assert!(!http.is_finished());
    }
    if let Some(manager) = &manager {
        assert!(!manager.is_finished());
    }
    // The held Drop still owns the real socket: PostgreSQL retains that
    // session.
    let old_pid: i32 = fixture
        .admin
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND query='COMMIT'",
            &[&format!("{}_manager", fixture.schema)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(fixture.counter().await, "1");
    assert_eq!(proxy.commands.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.probe.counts()[1] - before[1],
        1,
        "one native write attempt before queued read"
    );
    let second = fixture.send(
        "GetItem",
        json!({"TableName":"deadline","Key":{"pk":{"S":"counter"}},"ConsistentRead":true}),
    );
    fixture.wait_admission(1, 1).await;
    assert!(!second.is_finished());
    proxy.disarm();
    drop(release);
    if let Some(http) = http {
        let response = http.await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("InternalServerError")
        );
    }
    if let Some(manager) = manager {
        assert!(manager.await.unwrap().unwrap_err().is_commit_unknown());
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture
            .admin
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
                &[&old_pid],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observed old session closes after actual socket destruction");
    let response = second.await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    fixture.wait_admission(0, 0).await;
    assert_eq!(fixture.counter().await, "1");
    let after = fixture.probe.counts();
    assert_eq!(proxy.commands.load(Ordering::Acquire), 1);
    assert_eq!(
        after[1] - before[1],
        2,
        "one write attempt and one promoted read, without replay"
    );
    // Suppressed native replies cannot produce an acknowledged RowsWritten
    // event. The one submitted write attempt, one forwarded COMMIT and durable
    // ADD result establish that the increment committed once.
    assert_eq!(after[4] - before[4], 0);
    eprintln!(
        "lost_commit waiter={waiter:?} forwarded_commits=1 write_attempts=1 acknowledged_rows=0 \
         durable_counter=1 cleanup_held_permit=1 queued=1 released_permit=0 \
         old_session_closed=true"
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_forwarded_http_commit_when_reply_is_lost_then_500_once_after_real_cleanup() {
    check_lost_commit(ResponseWaiter::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_forwarded_manager_commit_when_reply_is_lost_then_typed_unknown_after_real_cleanup() {
    check_lost_commit(ResponseWaiter::Manager).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_dropped_manager_waiter_when_commit_reply_is_lost_then_permit_survives_real_cleanup()
{
    check_lost_commit(ResponseWaiter::DroppedManager).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_locked_postgres_batch_when_deadline_aborts_then_all_items_remain_unprocessed() {
    let fixture = HttpFixture::open(false, 60000).await;
    fixture.warm_table().await;
    fixture
        .admin
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.sys_system_stream_scope IN ACCESS EXCLUSIVE MODE",
            fixture.schema
        ))
        .await
        .unwrap();
    let writes = json!({"deadline":[{"PutRequest":{"Item":{"pk":{"S":"batch-one"}}}},{"PutRequest":{"Item":{"pk":{"S":"batch-two"}}}}]});
    let before = fixture.probe.counts();
    let request = fixture.send("BatchWriteItem", json!({"RequestItems":writes}));
    fixture.wait_blocked().await;
    let response = tokio::time::timeout(Duration::from_secs(60), request)
        .await
        .unwrap()
        .unwrap();
    fixture.admin.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    assert_eq!(
        response.json::<Value>().await.unwrap()["UnprocessedItems"],
        writes
    );
    let after = fixture.probe.counts();
    assert_eq!(after[2], before[2]);
    assert_eq!(after[4], before[4]);
    for key in ["batch-one", "batch-two"] {
        let response = fixture
            .store
            .get_item(
                serde_json::from_value(
                    json!({"TableName":"deadline","Key":{"pk":{"S":key}},"ConsistentRead":true}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.item.is_none());
    }
    fixture.wait_admission(0, 0).await;
    eprintln!("known_abort_batch http=200 unprocessed=2 durable_items=0 commits=0");
    fixture.close().await;
}
