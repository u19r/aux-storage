#[cfg(feature = "foundationdb")]
use std::path::PathBuf;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "foundationdb")]
use queue_provider::FoundationDbSettings;
#[cfg(feature = "postgres")]
use queue_provider::PostgresSettings;
use queue_provider::{
    CreateQueueRequest, DeleteMessageRequest, QueueBackend, QueueConfig, ReceiveMessageRequest,
    SendMessageRequest,
};

use crate::{QueueManager, create_queue_provider};

const SOAK_MESSAGE_COUNT: usize = 240;
const SOAK_WORKER_COUNT: usize = 12;
const RECEIVE_BATCH_SIZE: u32 = 10;

struct BackendCase {
    name: &'static str,
    config: QueueConfig,
}

#[derive(Debug, Default)]
struct SoakReport {
    sent: usize,
    received: usize,
    duplicate_receives: usize,
    dropped_messages: usize,
    delayed_messages: usize,
    over_five_attempt_receives: usize,
}

fn unique_suffix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{label}-{nanos}-{}", std::process::id())
}

#[cfg(feature = "rocksdb")]
fn local_rocksdb_path(label: &str) -> std::path::PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    workspace_root
        .join("target")
        .join("queue-test-data")
        .join(unique_suffix(label))
}

fn empty_config(backend_type: QueueBackend) -> QueueConfig {
    QueueConfig {
        backend_type,
        connection_string: None,
        file_path: None,
        postgres: None,
        foundationdb: None,
        remote: None,
    }
}

fn sqlite_case(_label: &str) -> BackendCase {
    let mut config = empty_config(QueueBackend::SQLite);
    config.connection_string = Some(":memory:".to_string());
    BackendCase {
        name: "sqlite",
        config,
    }
}

#[cfg(feature = "turso")]
fn turso_case(_label: &str) -> BackendCase {
    let mut config = empty_config(QueueBackend::Turso);
    config.connection_string = Some(":memory:".to_string());
    BackendCase {
        name: "turso",
        config,
    }
}

#[cfg(feature = "rocksdb")]
fn rocksdb_case(label: &str) -> BackendCase {
    let mut config = empty_config(QueueBackend::RocksDB);
    config.connection_string = Some(local_rocksdb_path(label).to_string_lossy().to_string());
    BackendCase {
        name: "rocksdb",
        config,
    }
}

#[cfg(feature = "postgres")]
fn postgres_case(_label: &str) -> Option<BackendCase> {
    let dsn = std::env::var("AUX_QUEUE_POSTGRES_DSN")
        .or_else(|_| std::env::var("POSTGRES_DSN"))
        .unwrap_or_else(|_| {
            let user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());
            format!("postgres://{user}@localhost:5432/postgres")
        });
    if dsn.trim().is_empty() {
        return None;
    }
    let mut config = empty_config(QueueBackend::Postgres);
    config.connection_string = Some(dsn.clone());
    config.postgres = Some(PostgresSettings {
        dsn,
        max_pool_size: 16,
        background_max_pool_size: 4,
        tls: false,
    });
    Some(BackendCase {
        name: "postgres",
        config,
    })
}

#[cfg(feature = "foundationdb")]
fn local_fdb_cluster_file_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("FDB_CLUSTER_FILE") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }

    [
        "/usr/local/etc/foundationdb/fdb.cluster",
        "/opt/homebrew/etc/foundationdb/fdb.cluster",
        "/etc/foundationdb/fdb.cluster",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| path.is_file())
}

#[cfg(feature = "foundationdb")]
fn foundationdb_case(label: &str) -> Option<BackendCase> {
    let cluster_file = local_fdb_cluster_file_path()?;
    let mut config = empty_config(QueueBackend::FoundationDb);
    config.foundationdb = Some(FoundationDbSettings {
        cluster_file: Some(cluster_file.to_string_lossy().to_string()),
        subspace_prefix: Some(format!("tests/queue-correctness/{}/", unique_suffix(label))),
        ..FoundationDbSettings::default()
    });
    Some(BackendCase {
        name: "foundationdb",
        config,
    })
}

fn available_single_node_backend_cases(label: &str) -> Vec<BackendCase> {
    #[allow(unused_mut)]
    let mut cases = vec![sqlite_case(label)];
    cases
}

fn available_multi_node_backend_cases(_label: &str) -> Vec<BackendCase> {
    #[allow(unused_mut)]
    let mut cases = Vec::new();
    #[cfg(feature = "foundationdb")]
    if let Some(case) = foundationdb_case(_label) {
        cases.push(case);
    }
    #[cfg(feature = "postgres")]
    if let Some(case) = postgres_case(_label) {
        cases.push(case);
    }
    cases
}

async fn manager_for_case(case: &BackendCase) -> Option<Arc<QueueManager>> {
    let storage = match create_queue_provider(case.config.clone()).await {
        Ok(storage) => storage,
        Err(error) if case.name == "foundationdb" || case.name == "postgres" => {
            eprintln!("skipping {} queue correctness case: {error}", case.name);
            return None;
        }
        Err(error) => panic!("create {} queue provider: {error:?}", case.name),
    };
    if let Err(error) = storage.initialize().await {
        if case.name == "foundationdb" || case.name == "postgres" {
            eprintln!("skipping {} queue correctness case: {error}", case.name);
            return None;
        }
        panic!("initialize {} queue provider: {error:?}", case.name);
    }
    Some(Arc::new(QueueManager::new(Arc::from(storage))))
}

async fn required_manager_for_case(case: &BackendCase) -> Arc<QueueManager> {
    manager_for_case(case)
        .await
        .unwrap_or_else(|| panic!("{} provider should be available", case.name))
}

async fn create_queue(manager: &QueueManager, queue_name: &str) -> String {
    manager
        .create_queue(CreateQueueRequest {
            queue_name: queue_name.to_string(),
            attributes: None,
        })
        .await
        .expect("create queue")
        .queue_url
}

async fn send_message(manager: &QueueManager, queue_url: &str, body: String) -> String {
    manager
        .send_message(SendMessageRequest {
            queue_url: queue_url.to_string(),
            message_body: body,
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .expect("send message")
        .message_id
        .to_string()
}

async fn receive_messages(
    manager: &QueueManager,
    queue_url: &str,
    max_messages: u32,
) -> Vec<queue_provider::MessageResponse> {
    manager
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.to_string(),
            max_number_of_messages: Some(max_messages),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap_or_else(|error| panic!("receive messages from {queue_url}: {error:?}"))
        .messages
}

async fn receive_one_within_attempts(
    manager: &QueueManager,
    queue_url: &str,
    max_attempts: usize,
) -> Vec<queue_provider::MessageResponse> {
    let mut messages = Vec::new();
    for _ in 0..max_attempts {
        messages = receive_messages(manager, queue_url, 1).await;
        if !messages.is_empty() {
            break;
        }
    }
    messages
}

async fn run_twelve_worker_no_drop_duplicate(case: &BackendCase) {
    let manager = required_manager_for_case(case).await;
    let queue_url = create_queue(
        &manager,
        &format!("{}-twelve-workers", unique_suffix(case.name)),
    )
    .await;

    for index in 0..120 {
        send_message(&manager, &queue_url, format!("message-{index}")).await;
    }

    let mut unique_ids = HashSet::new();
    let mut total_received = 0usize;
    for _ in 0..4 {
        let mut workers = Vec::new();
        for _ in 0..12 {
            let manager = Arc::clone(&manager);
            let queue_url = queue_url.clone();
            workers.push(tokio::spawn(async move {
                receive_messages(&manager, &queue_url, RECEIVE_BATCH_SIZE).await
            }));
        }

        for worker in workers {
            let messages = worker.await.expect("join receive worker");
            total_received += messages.len();
            for message in messages {
                assert!(
                    unique_ids.insert(message.message_id.clone()),
                    "{} duplicate message claimed in 12-worker receive wave: {}",
                    case.name,
                    message.message_id
                );
            }
        }

        if unique_ids.len() == 120 {
            break;
        }
    }

    assert_eq!(unique_ids.len(), 120, "{} dropped messages", case.name);
    assert_eq!(total_received, 120, "{} received count mismatch", case.name);
}

async fn run_known_visible_found_quickly(case: &BackendCase) {
    let manager = required_manager_for_case(case).await;
    let queue_url = create_queue(
        &manager,
        &format!("{}-bounded-discovery", unique_suffix(case.name)),
    )
    .await;
    let sent_id = send_message(&manager, &queue_url, "known-visible".to_string()).await;

    let messages = receive_one_within_attempts(&manager, &queue_url, 4).await;
    assert_eq!(
        messages.len(),
        1,
        "{} did not find known message",
        case.name
    );
    assert_eq!(messages[0].message_id, sent_id);
}

async fn run_send_to_receive_under_500ms(case: &BackendCase) {
    let manager = required_manager_for_case(case).await;
    let queue_url = create_queue(
        &manager,
        &format!("{}-send-to-receive", unique_suffix(case.name)),
    )
    .await;
    send_message(&manager, &queue_url, "latency".to_string()).await;

    let started_at = Instant::now();
    let mut messages = Vec::new();
    while started_at.elapsed() < Duration::from_millis(500) {
        messages = receive_messages(&manager, &queue_url, 1).await;
        if !messages.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(messages.len(), 1, "{} did not receive message", case.name);
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "{} message was not receivable within 500ms after send response",
        case.name
    );
}

async fn run_stale_receipt_does_not_delete_current_claim(case: &BackendCase) {
    let manager = required_manager_for_case(case).await;
    let queue_url = create_queue(
        &manager,
        &format!("{}-stale-receipt", unique_suffix(case.name)),
    )
    .await;
    let sent_id = send_message(&manager, &queue_url, "stale-receipt".to_string()).await;

    let first = receive_one_within_attempts(&manager, &queue_url, 4).await;
    assert_eq!(first.len(), 1);
    manager
        .change_message_visibility(queue_provider::ChangeMessageVisibilityRequest {
            queue_url: queue_url.clone(),
            receipt_handle: first[0].receipt_handle.as_str().into(),
            visibility_timeout: 0,
        })
        .await
        .expect("make first claim visible");

    let second = receive_one_within_attempts(&manager, &queue_url, 16).await;
    assert_eq!(
        second.len(),
        1,
        "{} did not redeliver after visibility timeout zero",
        case.name
    );
    assert_eq!(second[0].message_id, sent_id);

    let stale_delete = manager
        .delete_message(DeleteMessageRequest {
            queue_url: queue_url.clone(),
            receipt_handle: first[0].receipt_handle.as_str().into(),
        })
        .await;
    assert!(
        stale_delete.is_err(),
        "{} stale receipt handle should not delete current claim",
        case.name
    );

    manager
        .change_message_visibility(queue_provider::ChangeMessageVisibilityRequest {
            queue_url: queue_url.clone(),
            receipt_handle: second[0].receipt_handle.as_str().into(),
            visibility_timeout: 0,
        })
        .await
        .expect("make second claim visible");
    let still_present = receive_one_within_attempts(&manager, &queue_url, 4).await;
    assert_eq!(still_present.len(), 1);
    assert_eq!(still_present[0].message_id, sent_id);
}

async fn run_node_leave_join_preserves_messages(case: &BackendCase) {
    let first_manager = required_manager_for_case(case).await;
    let queue_url = create_queue(
        &first_manager,
        &format!("{}-node-rejoin", unique_suffix(case.name)),
    )
    .await;
    let mut sent_ids = HashSet::new();
    for index in 0..20 {
        sent_ids.insert(
            send_message(&first_manager, &queue_url, format!("node-message-{index}")).await,
        );
    }
    drop(first_manager);

    let second_manager = required_manager_for_case(case).await;
    let mut received_ids = HashSet::new();
    for _ in 0..4 {
        for message in receive_messages(&second_manager, &queue_url, 10).await {
            received_ids.insert(message.message_id);
        }
        if received_ids.len() == sent_ids.len() {
            break;
        }
    }

    assert_eq!(
        received_ids, sent_ids,
        "{} node leave/join dropped or changed messages",
        case.name
    );
}

async fn run_soak(case: &BackendCase) -> SoakReport {
    let manager = required_manager_for_case(case).await;
    let queue_url = create_queue(&manager, &format!("{}-soak", unique_suffix(case.name))).await;
    let sent_at_by_id = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Instant>::new()));
    let sent_count = Arc::new(AtomicUsize::new(0));
    let received_ids = Arc::new(tokio::sync::Mutex::new(HashSet::<String>::new()));
    let duplicate_receives = Arc::new(tokio::sync::Mutex::new(0usize));
    let delayed_messages = Arc::new(tokio::sync::Mutex::new(0usize));
    let over_five_attempt_receives = Arc::new(tokio::sync::Mutex::new(0usize));
    let empty_poll_streak = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::new();
    for _ in 0..SOAK_WORKER_COUNT {
        let manager = Arc::clone(&manager);
        let queue_url = queue_url.clone();
        let sent_count = Arc::clone(&sent_count);
        let received_ids = Arc::clone(&received_ids);
        let duplicate_receives = Arc::clone(&duplicate_receives);
        let delayed_messages = Arc::clone(&delayed_messages);
        let over_five_attempt_receives = Arc::clone(&over_five_attempt_receives);
        let empty_poll_streak = Arc::clone(&empty_poll_streak);
        let sent_at_by_id = Arc::clone(&sent_at_by_id);
        workers.push(tokio::spawn(async move {
            loop {
                let received_len = received_ids.lock().await.len();
                let sent_len = sent_count.load(Ordering::Acquire);
                if sent_len >= SOAK_MESSAGE_COUNT && received_len >= SOAK_MESSAGE_COUNT {
                    break;
                }
                let messages = receive_messages(&manager, &queue_url, RECEIVE_BATCH_SIZE).await;
                if messages.is_empty() {
                    if sent_len >= SOAK_MESSAGE_COUNT && sent_len > received_len {
                        let streak = empty_poll_streak
                            .fetch_add(1, Ordering::AcqRel)
                            .saturating_add(1);
                        if streak > 5 {
                            let mut over_five = over_five_attempt_receives.lock().await;
                            *over_five = over_five.saturating_add(1);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                empty_poll_streak.store(0, Ordering::Release);
                for message in messages {
                    let sent_at = sent_at_by_id.lock().await.get(&message.message_id).copied();
                    if let Some(sent_at) = sent_at
                        && sent_at.elapsed() > Duration::from_millis(500)
                    {
                        let mut delayed = delayed_messages.lock().await;
                        *delayed = delayed.saturating_add(1);
                    }
                    let mut received = received_ids.lock().await;
                    if !received.insert(message.message_id) {
                        let mut duplicates = duplicate_receives.lock().await;
                        *duplicates = duplicates.saturating_add(1);
                    }
                }
            }
        }));
    }

    for index in 0..SOAK_MESSAGE_COUNT {
        let id = send_message(&manager, &queue_url, format!("soak-message-{index}")).await;
        sent_at_by_id.lock().await.insert(id, Instant::now());
        sent_count.fetch_add(1, Ordering::Release);
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    for worker in workers {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let _ = tokio::time::timeout(remaining, worker).await;
    }

    let received = received_ids.lock().await.len();
    SoakReport {
        sent: SOAK_MESSAGE_COUNT,
        received,
        duplicate_receives: *duplicate_receives.lock().await,
        dropped_messages: SOAK_MESSAGE_COUNT.saturating_sub(received),
        delayed_messages: *delayed_messages.lock().await,
        over_five_attempt_receives: *over_five_attempt_receives.lock().await,
    }
}

#[tokio::test]
async fn sqs_delivery_invariants_run_across_available_backends() {
    for case in available_single_node_backend_cases("delivery") {
        run_twelve_worker_no_drop_duplicate(&case).await;
        run_known_visible_found_quickly(&case).await;
        run_send_to_receive_under_500ms(&case).await;
    }
}

#[tokio::test]
async fn stale_receipts_preserve_messages_across_single_node_backends() {
    for case in available_single_node_backend_cases("failure-injection") {
        run_stale_receipt_does_not_delete_current_claim(&case).await;
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
#[ignore = "turso single-node queue correctness currently trips a turso_core arithmetic panic"]
async fn turso_single_node_sqs_delivery_invariants() {
    let case = turso_case("turso-delivery");
    run_twelve_worker_no_drop_duplicate(&case).await;
    run_known_visible_found_quickly(&case).await;
    run_send_to_receive_under_500ms(&case).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
#[ignore = "RocksDB service-level queue harness can conflict with same-process RocksDB locks; \
            provider-level RocksDB correctness runs by default"]
async fn rocksdb_single_node_sqs_delivery_invariants() {
    let case = rocksdb_case("rocksdb-delivery");
    run_twelve_worker_no_drop_duplicate(&case).await;
    run_known_visible_found_quickly(&case).await;
    run_send_to_receive_under_500ms(&case).await;
    run_stale_receipt_does_not_delete_current_claim(&case).await;
}

#[tokio::test]
#[ignore = "multi-node queue correctness requires FoundationDB or Postgres services"]
async fn multi_node_delivery_invariants_run_across_available_backends() {
    for case in available_multi_node_backend_cases("multi-node-delivery") {
        if manager_for_case(&case).await.is_none() {
            continue;
        }
        run_twelve_worker_no_drop_duplicate(&case).await;
        run_known_visible_found_quickly(&case).await;
        run_send_to_receive_under_500ms(&case).await;
    }
}

#[tokio::test]
#[ignore = "multi-node queue correctness requires FoundationDB or Postgres services"]
async fn stale_receipts_and_node_rejoin_preserve_messages_across_multi_node_backends() {
    for case in available_multi_node_backend_cases("multi-node-failure-injection") {
        if manager_for_case(&case).await.is_none() {
            continue;
        }
        run_stale_receipt_does_not_delete_current_claim(&case).await;
        run_node_leave_join_preserves_messages(&case).await;
    }
}

#[tokio::test]
#[ignore = "12-worker multi-node soak requires FoundationDB or Postgres services"]
async fn twelve_worker_multi_node_soak_records_delivery_failures() {
    for case in available_multi_node_backend_cases("multi-node-soak") {
        if manager_for_case(&case).await.is_none() {
            continue;
        }
        let report = run_soak(&case).await;
        assert_eq!(
            report.duplicate_receives, 0,
            "{} duplicate receives in soak report: {:?}",
            case.name, report
        );
        assert_eq!(
            report.dropped_messages, 0,
            "{} dropped messages in soak report: {:?}",
            case.name, report
        );
        assert_eq!(
            report.delayed_messages, 0,
            "{} delayed messages in soak report: {:?}",
            case.name, report
        );
        let over_five_attempt_receives = report.over_five_attempt_receives;
        eprintln!(
            "{} queue soak report: {:?}; over_five_attempt_receives={}",
            case.name, report, over_five_attempt_receives
        );
        assert_eq!(report.received, report.sent);
    }
}
