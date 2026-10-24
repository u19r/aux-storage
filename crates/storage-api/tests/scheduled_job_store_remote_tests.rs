#[cfg(feature = "foundationdb")]
use std::ffi::OsString;
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
use std::{
    fs,
    process::{Child, Command, Stdio},
};
use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
use http_request::reqwest::Client;
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, CommitOutcome, Mutation, RangeDirection, RangeRequest,
    RangeRequestInput, StoreKey, StoreLevel, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
use serde_json::{Value, json};
use storage::{
    DatabaseManager, DatabaseManagerRuntimeOptions, RemoteScheduledJobStoreConfig,
    ScheduledJobStore, ScheduledJobStoreError,
};
use storage_api::{AppState, server_router};
use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};
use tempfile::TempDir;
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
use tokio::time::sleep;
use tokio::{net::TcpListener, sync::Barrier, task::JoinHandle};
#[cfg(feature = "postgres")]
use tokio_postgres::NoTls;
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
use uuid::Uuid;

struct RunningStorageApi {
    endpoint_url: String,
    database_path: PathBuf,
    _temp_dir: TempDir,
    server_task: JoinHandle<()>,
}

impl RunningStorageApi {
    async fn restart(&mut self) {
        self.server_task.abort();
        let _ = (&mut self.server_task).await;
        let replacement = spawn_server(&self.database_path).await;
        self.endpoint_url = replacement.0;
        self.server_task = replacement.1;
    }
}

impl Drop for RunningStorageApi {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

async fn start_server(temp_dir: TempDir, database_path: PathBuf) -> RunningStorageApi {
    let (endpoint_url, server_task) = spawn_server(&database_path).await;
    RunningStorageApi {
        endpoint_url,
        database_path,
        _temp_dir: temp_dir,
        server_task,
    }
}

async fn spawn_server(database_path: &Path) -> (String, JoinHandle<()>) {
    let database = Arc::new(
        DatabaseManager::new_with_config_and_runtime_options(
            sqlite_config(database_path),
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
        .expect("construct transactional SQLite server database"),
    );
    let app_state = Arc::new(AppState::new(database));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind storage API test server");
    let address = listener.local_addr().expect("storage API test address");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, server_router(app_state, false))
            .await
            .expect("serve storage API test router");
    });
    (format!("http://{address}"), server_task)
}

async fn new_server() -> RunningStorageApi {
    let temp_dir = tempfile::tempdir().expect("create storage API test directory");
    let database_path = temp_dir.path().join("storage.sqlite");
    start_server(temp_dir, database_path).await
}

fn sqlite_config(database_path: &Path) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(database_path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            force_file_backed_database: true,
            immediate_gsi_consistency: true,
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

async fn connect_client(endpoint_url: &str) -> ScheduledJobStore {
    let config =
        RemoteScheduledJobStoreConfig::new(vec![endpoint_url.to_string()], Duration::from_secs(10))
            .expect("HTTP-only remote scheduled-store configuration");
    storage::connect_remote_scheduled_job_store(config)
        .await
        .expect("connect remote scheduled-store client")
}

fn key(value: &[u8]) -> StoreKey {
    StoreKey::try_from(value).expect("test key is non-empty")
}

fn limits() -> TransactionLimits {
    TransactionLimits {
        max_checks: NonZeroU32::new(8).expect("non-zero check limit"),
        max_mutations: NonZeroU32::new(8).expect("non-zero mutation limit"),
        max_bytes: NonZeroUsize::new(16 * 1024).expect("non-zero byte limit"),
    }
}

fn put_transaction(id: u8, key_bytes: &[u8], value: &[u8]) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([id; 16]),
        checks: Vec::new(),
        mutations: vec![Mutation::Put(key(key_bytes), Bytes::copy_from_slice(value))],
        limits: limits(),
    })
    .expect("valid put transaction")
}

fn checked_pair_transaction(
    id: u8,
    shared_key: &[u8],
    marker_key: &[u8],
    value: &[u8],
) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([id; 16]),
        checks: vec![Check::Absent(key(shared_key))],
        mutations: vec![
            Mutation::Put(key(shared_key), Bytes::copy_from_slice(value)),
            Mutation::Put(key(marker_key), Bytes::copy_from_slice(value)),
        ],
        limits: limits(),
    })
    .expect("valid checked pair transaction")
}

fn range_request(direction: RangeDirection, continuation: Option<StoreKey>) -> RangeRequest {
    RangeRequest::try_from(RangeRequestInput {
        start: Some(key(b"range-a")),
        end: Some(key(b"range-z")),
        direction,
        item_limit: NonZeroU32::new(2).expect("non-zero item limit"),
        byte_limit: NonZeroUsize::new(16 * 1024).expect("non-zero byte limit"),
        continuation,
    })
    .expect("valid range request")
}

fn page_keys(page: &scheduled_bg_jobs::store::RangePage) -> Vec<Vec<u8>> {
    page.entries
        .iter()
        .map(|entry| entry.key.as_bytes().to_vec())
        .collect()
}

#[tokio::test]
async fn given_coherent_remote_store_when_racing_and_reopening_then_store_contract_holds() {
    let mut server = new_server().await;
    let left = connect_client(&server.endpoint_url).await;
    let right = connect_client(&server.endpoint_url).await;

    assert_eq!(left.capabilities().level(), StoreLevel::Distributed);
    assert_eq!(right.capabilities(), left.capabilities());

    for (id, key_bytes, value) in [
        (1, b"many-a".as_slice(), b"value-a".as_slice()),
        (2, b"many-b".as_slice(), b"value-b".as_slice()),
        (3, b"range-a".as_slice(), b"a".as_slice()),
        (4, b"range-b".as_slice(), b"b".as_slice()),
        (5, b"range-c".as_slice(), b"c".as_slice()),
        (6, b"range-z".as_slice(), b"z".as_slice()),
    ] {
        assert_eq!(
            left.commit(&put_transaction(id, key_bytes, value))
                .await
                .expect("seed transaction"),
            CommitOutcome::Committed
        );
    }

    let values = right
        .get_many(&[
            key(b"many-b"),
            key(b"missing"),
            key(b"many-b"),
            key(b"many-a"),
        ])
        .await
        .expect("ordered get-many");
    assert_eq!(
        values,
        vec![
            Some(Bytes::from_static(b"value-b")),
            None,
            Some(Bytes::from_static(b"value-b")),
            Some(Bytes::from_static(b"value-a")),
        ]
    );

    let first_forward = left
        .read_range(range_request(RangeDirection::Forward, None))
        .await
        .expect("first forward range page");
    assert_eq!(
        page_keys(&first_forward),
        vec![b"range-a".to_vec(), b"range-b".to_vec()]
    );
    let forward_cursor = first_forward
        .continuation
        .clone()
        .expect("forward continuation");
    let second_forward = left
        .read_range(range_request(RangeDirection::Forward, Some(forward_cursor)))
        .await
        .expect("second forward range page");
    assert_eq!(page_keys(&second_forward), vec![b"range-c".to_vec()]);
    assert!(second_forward.continuation.is_none());

    let first_reverse = right
        .read_range(range_request(RangeDirection::Reverse, None))
        .await
        .expect("first reverse range page");
    assert_eq!(
        page_keys(&first_reverse),
        vec![b"range-c".to_vec(), b"range-b".to_vec()]
    );
    let reverse_cursor = first_reverse
        .continuation
        .clone()
        .expect("reverse continuation");
    let second_reverse = right
        .read_range(range_request(RangeDirection::Reverse, Some(reverse_cursor)))
        .await
        .expect("second reverse range page");
    assert_eq!(page_keys(&second_reverse), vec![b"range-a".to_vec()]);
    assert!(second_reverse.continuation.is_none());

    let replay_transaction = put_transaction(10, b"replay", b"first");
    assert_eq!(
        left.commit(&replay_transaction)
            .await
            .expect("initial committed receipt"),
        CommitOutcome::Committed
    );
    assert_eq!(
        right
            .commit(&replay_transaction)
            .await
            .expect("exact committed replay"),
        CommitOutcome::Committed
    );
    let mismatch = right
        .commit(&put_transaction(10, b"replay", b"changed"))
        .await
        .expect_err("changed operation content must be rejected");
    assert_eq!(mismatch, ScheduledJobStoreError::OperationReuseMismatch);

    let left_race = checked_pair_transaction(11, b"race-shared", b"race-left", b"left");
    let right_race = checked_pair_transaction(12, b"race-shared", b"race-right", b"right");
    let barrier = Arc::new(Barrier::new(2));
    let left_barrier = Arc::clone(&barrier);
    let right_barrier = Arc::clone(&barrier);
    let left_call = async {
        left_barrier.wait().await;
        left.commit(&left_race).await
    };
    let right_call = async {
        right_barrier.wait().await;
        right.commit(&right_race).await
    };
    let (left_result, right_result) = tokio::join!(left_call, right_call);
    let left_outcome = left_result.expect("left conditional race result");
    let right_outcome = right_result.expect("right conditional race result");
    assert_ne!(left_outcome, right_outcome);
    assert!(
        matches!(
            (&left_outcome, &right_outcome),
            (CommitOutcome::Committed, CommitOutcome::Conflict)
                | (CommitOutcome::Conflict, CommitOutcome::Committed)
        ),
        "left={left_outcome:?}, right={right_outcome:?}"
    );

    let race_values = left
        .get_many(&[key(b"race-shared"), key(b"race-left"), key(b"race-right")])
        .await
        .expect("race effects");
    assert_eq!(
        race_values
            .iter()
            .skip(1)
            .filter(|value| value.is_some())
            .count(),
        1,
        "losing transaction must not partially apply its marker"
    );
    assert!(race_values[0].is_some());

    server.restart().await;
    let reopened = connect_client(&server.endpoint_url).await;
    assert_eq!(
        reopened
            .commit(&replay_transaction)
            .await
            .expect("committed receipt replay after restart"),
        CommitOutcome::Committed
    );
    let restart_mismatch = reopened
        .commit(&put_transaction(10, b"replay", b"changed"))
        .await
        .expect_err("mismatch remains rejected after restart");
    assert_eq!(
        restart_mismatch,
        ScheduledJobStoreError::OperationReuseMismatch
    );
    assert_eq!(
        reopened
            .commit(&left_race)
            .await
            .expect("left race outcome replay after restart"),
        left_outcome
    );
    assert_eq!(
        reopened
            .commit(&right_race)
            .await
            .expect("right race outcome replay after restart"),
        right_outcome
    );
}

#[test]
fn remote_scheduled_job_store_test_surface_is_http_only() {
    assert!(
        RemoteScheduledJobStoreConfig::new(
            vec!["https://private-storage"],
            Duration::from_secs(1),
        )
            .is_err()
    );
    assert!(
        RemoteScheduledJobStoreConfig::new(vec!["http://private-storage"], Duration::from_secs(1),)
            .is_ok()
    );
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
const PROCESS_READY_ATTEMPTS: usize = 600;
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
const PROCESS_READY_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(any(feature = "postgres", feature = "foundationdb"))]
const PROCESS_HTTP_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
struct RemoteProcessPair {
    left: RunningStorageApiProcess,
    right: RunningStorageApiProcess,
    _root: TempDir,
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
impl RemoteProcessPair {
    async fn new(backend: Value) -> Self {
        let root = tempfile::tempdir().expect("create remote process proof directory");
        let (left_port, right_port) = reserve_process_ports();
        let left_config = root.path().join("left.json");
        let right_config = root.path().join("right.json");
        write_process_config(&left_config, left_port, &backend);
        write_process_config(&right_config, right_port, &backend);

        let mut left = RunningStorageApiProcess::start(left_config, left_port);
        let mut right = RunningStorageApiProcess::start(right_config, right_port);
        tokio::join!(left.wait_for_ready(), right.wait_for_ready());

        Self {
            left,
            right,
            _root: root,
        }
    }

    async fn restart_left(&mut self) {
        self.left.restart().await;
    }
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
struct RunningStorageApiProcess {
    child: Option<Child>,
    config_path: PathBuf,
    endpoint_url: String,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    client: Client,
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
impl RunningStorageApiProcess {
    fn start(config_path: PathBuf, port: u16) -> Self {
        let stdout_path = config_path.with_extension("stdout.log");
        let stderr_path = config_path.with_extension("stderr.log");
        let stdout = fs::File::create(&stdout_path).expect("create storage-api stdout log");
        let stderr = fs::File::create(&stderr_path).expect("create storage-api stderr log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_storage-api"));
        command
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        #[cfg(feature = "foundationdb")]
        if let Some(library_path) = foundationdb_library_path() {
            command.env("DYLD_LIBRARY_PATH", library_path);
        }
        let child = command
            .spawn()
            .expect("spawn storage-api remote proof process");
        let client = Client::builder()
            .timeout(PROCESS_HTTP_TIMEOUT)
            .build()
            .expect("build storage-api readiness client");
        Self {
            child: Some(child),
            config_path,
            endpoint_url: format!("http://127.0.0.1:{port}/storage"),
            stdout_path,
            stderr_path,
            client,
        }
    }

    async fn wait_for_ready(&mut self) {
        for _ in 0..PROCESS_READY_ATTEMPTS {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("storage-api process exists")
                .try_wait()
                .expect("inspect storage-api process")
            {
                let config_path = preserve_process_log(&self.config_path, "config.json");
                let stdout_path = preserve_process_log(&self.stdout_path, "stdout");
                let stderr_path = preserve_process_log(&self.stderr_path, "stderr");
                panic!(
                    "storage-api process exited before readiness ({status}); config: {}; stdout: \
                     {}; stderr: {}",
                    config_path.display(),
                    stdout_path.display(),
                    stderr_path.display()
                );
            }
            if self
                .client
                .get(format!(
                    "{}/ready",
                    self.endpoint_url.trim_end_matches("/storage")
                ))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            sleep(PROCESS_READY_INTERVAL).await;
        }
        let config_path = preserve_process_log(&self.config_path, "config.json");
        let stdout_path = preserve_process_log(&self.stdout_path, "stdout");
        let stderr_path = preserve_process_log(&self.stderr_path, "stderr");
        panic!(
            "storage-api process did not become ready; config: {}; stdout: {}; stderr: {}",
            config_path.display(),
            stdout_path.display(),
            stderr_path.display()
        );
    }

    async fn restart(&mut self) {
        self.stop();
        let replacement = Self::start(
            self.config_path.clone(),
            port_from_endpoint(&self.endpoint_url),
        );
        *self = replacement;
        self.wait_for_ready().await;
    }

    fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[cfg(feature = "foundationdb")]
fn foundationdb_library_path() -> Option<OsString> {
    let mut paths = ["/usr/local/lib", "/opt/homebrew/lib"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    if let Some(existing) = std::env::var_os("DYLD_LIBRARY_PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    (!paths.is_empty())
        .then(|| std::env::join_paths(paths).ok())
        .flatten()
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn preserve_process_log(path: &Path, stream: &str) -> PathBuf {
    let destination = PathBuf::from(format!(
        "/tmp/remote-scheduled-job-process-{}-{stream}.log",
        std::process::id()
    ));
    let _ = fs::copy(path, &destination);
    destination
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
impl Drop for RunningStorageApiProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn reserve_process_ports() -> (u16, u16) {
    let left = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve left process port");
    let right = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve right process port");
    (
        left.local_addr().expect("read left process port").port(),
        right.local_addr().expect("read right process port").port(),
    )
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn port_from_endpoint(endpoint: &str) -> u16 {
    endpoint
        .rsplit_once(':')
        .and_then(|(_, suffix)| suffix.split('/').next())
        .and_then(|port| port.parse().ok())
        .expect("process endpoint has a port")
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn write_process_config(path: &Path, port: u16, backend: &Value) {
    let mut backends = backend.clone();
    if let Value::Object(backends) = &mut backends {
        for backend_name in [
            "sqlite",
            "turso",
            "postgres",
            "rocksdb",
            "foundationdb",
            "remote",
        ] {
            backends
                .entry(backend_name.to_string())
                .or_insert(Value::Null);
        }
    }
    let config = json!({
        "version": "1.0",
        "http": {"bind_addr": format!("127.0.0.1:{port}")},
        "features": {
            "backends": backends,
            "metrics": {"enabled": false},
            "tracing": {"log_level": "warn"},
            "runtime": {"enable_background_workers": false}
        }
    });
    fs::write(
        path,
        serde_json::to_vec_pretty(&config).expect("serialize remote process config"),
    )
    .expect("write remote process config");
}

#[cfg(feature = "postgres")]
const REMOTE_PROOF_POSTGRES_DSN: &str = "postgresql://ross@localhost/postgres";

#[cfg(feature = "postgres")]
struct PostgresJobFixture {
    schema: String,
    dsn: String,
}

#[cfg(feature = "postgres")]
impl PostgresJobFixture {
    async fn new() -> Self {
        let schema = format!("remote_job_{}", Uuid::new_v4().simple());
        let (client, connection) = tokio_postgres::connect(REMOTE_PROOF_POSTGRES_DSN, NoTls)
            .await
            .expect("connect PostgreSQL remote proof fixture");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .expect("create PostgreSQL remote proof schema");
        Self {
            dsn: format!("{REMOTE_PROOF_POSTGRES_DSN}?options=-csearch_path%3D{schema}"),
            schema,
        }
    }

    fn backend_config(&self) -> Value {
        let admission = config::StorageAdmissionConfig::default();
        json!({
            "postgres": {
                "dsn": self.dsn,
                "max_pool_size": admission.minimum_concurrency + admission.control_reserve_concurrency,
                "background_max_pool_size": 2,
                "tls": false,
                "immediate_gsi_consistency": true
            }
        })
    }

    async fn cleanup(self) {
        let (client, connection) = tokio_postgres::connect(REMOTE_PROOF_POSTGRES_DSN, NoTls)
            .await
            .expect("reconnect PostgreSQL remote proof fixture");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                self.schema
            ))
            .await
            .expect("drop PostgreSQL remote proof schema");
    }
}

#[cfg(feature = "foundationdb")]
fn foundationdb_backend_config() -> Value {
    let cluster_file = std::env::var_os("FDB_CLUSTER_FILE")
        .map(PathBuf::from)
        .or_else(|| {
            [
                "/usr/local/etc/foundationdb/fdb.cluster",
                "/opt/homebrew/etc/foundationdb/fdb.cluster",
                "/etc/foundationdb/fdb.cluster",
            ]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.is_file())
        })
        .expect("FoundationDB remote proof requires a local cluster file");
    json!({
        "foundationdb": {
            "cluster_file": cluster_file,
            "subspace_prefix": format!("scheduled-http/{}/", Uuid::now_v7()),
            "cache_read_version_ms": 0,
            "immediate_gsi_consistency": true
        }
    })
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
async fn run_two_process_remote_contract(pair: &mut RemoteProcessPair) {
    let (left, right) = tokio::join!(
        connect_client(&pair.left.endpoint_url),
        connect_client(&pair.right.endpoint_url)
    );
    assert_eq!(left.capabilities().level(), StoreLevel::Distributed);
    assert_eq!(right.capabilities(), left.capabilities());

    let replay_transaction = put_transaction(40, b"remote-replay", b"first");
    assert_eq!(
        left.commit(&replay_transaction)
            .await
            .expect("initial remote commit"),
        CommitOutcome::Committed
    );
    assert_eq!(
        right
            .commit(&replay_transaction)
            .await
            .expect("cross-process replay"),
        CommitOutcome::Committed
    );
    let mismatch = right
        .commit(&put_transaction(40, b"remote-replay", b"changed"))
        .await
        .expect_err("cross-process operation mismatch");
    assert_eq!(mismatch, ScheduledJobStoreError::OperationReuseMismatch);

    let left_race = checked_pair_transaction(41, b"remote-race", b"remote-left", b"left");
    let right_race = checked_pair_transaction(42, b"remote-race", b"remote-right", b"right");
    let barrier = Arc::new(Barrier::new(2));
    let left_barrier = Arc::clone(&barrier);
    let right_barrier = Arc::clone(&barrier);
    let (left_result, right_result) = tokio::join!(
        async {
            left_barrier.wait().await;
            left.commit(&left_race).await
        },
        async {
            right_barrier.wait().await;
            right.commit(&right_race).await
        }
    );
    let left_outcome = left_result.expect("left cross-process conditional race");
    let right_outcome = right_result.expect("right cross-process conditional race");
    assert!(
        matches!(
            (&left_outcome, &right_outcome),
            (CommitOutcome::Committed, CommitOutcome::Conflict)
                | (CommitOutcome::Conflict, CommitOutcome::Committed)
        ),
        "left={left_outcome:?}, right={right_outcome:?}"
    );

    for store in [&left, &right] {
        let values = store
            .get_many(&[
                key(b"remote-race"),
                key(b"remote-left"),
                key(b"remote-right"),
            ])
            .await
            .expect("cross-process race effects");
        assert!(values[0].is_some());
        assert_eq!(
            values
                .iter()
                .skip(1)
                .filter(|value| value.is_some())
                .count(),
            1,
            "losing cross-process transaction must not partially apply"
        );
    }

    pair.restart_left().await;
    let reopened = connect_client(&pair.left.endpoint_url).await;
    assert_eq!(
        reopened
            .commit(&replay_transaction)
            .await
            .expect("replay after API restart"),
        CommitOutcome::Committed
    );
    assert_eq!(
        right
            .commit(&left_race)
            .await
            .expect("left outcome replay through switched endpoint"),
        left_outcome
    );
    assert_eq!(
        reopened
            .commit(&right_race)
            .await
            .expect("right outcome replay after API restart"),
        right_outcome
    );
}

#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit live two-process PostgreSQL remote scheduled-store proof"]
async fn given_two_postgres_storage_api_processes_when_job_store_is_raced_then_contract_holds() {
    let fixture = PostgresJobFixture::new().await;
    let mut pair = RemoteProcessPair::new(fixture.backend_config()).await;
    run_two_process_remote_contract(&mut pair).await;
    fixture.cleanup().await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit live two-process FoundationDB remote scheduled-store proof"]
async fn given_two_foundationdb_storage_api_processes_when_job_store_is_raced_then_contract_holds()
{
    let mut pair = RemoteProcessPair::new(foundationdb_backend_config()).await;
    run_two_process_remote_contract(&mut pair).await;
}
