use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use queue_provider::{
    CreateQueueRequest, DeleteMessageRequest, QueueError, QueueValidationKind,
    ReceiveMessageRequest, SendMessageRequest,
};

use crate::{
    QueueManager,
    constants_tests::{RECEIVE_BATCH_SIZE, SOAK_MESSAGE_COUNT, SOAK_WORKER_COUNT},
    queue_test_support_tests::{
        BackendCase, required_manager_for_case, select_native_backend_name,
        selected_distributed_backend_case, selected_native_backend_case, unique_suffix,
    },
};

#[derive(Debug, Default)]
struct SoakReport {
    sent: usize,
    received: usize,
    duplicate_receives: usize,
    dropped_messages: usize,
    delayed_messages: usize,
    over_five_attempt_receives: usize,
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

async fn run_twelve_worker_no_drop_duplicate(case: &BackendCase, manager: &Arc<QueueManager>) {
    let queue_url = create_queue(
        manager,
        &format!("{}-twelve-workers", unique_suffix(case.name)),
    )
    .await;

    for index in 0..120 {
        send_message(manager, &queue_url, format!("message-{index}")).await;
    }

    let mut unique_ids = HashSet::new();
    let mut total_received = 0usize;
    for _ in 0..4 {
        let mut workers = Vec::new();
        for _ in 0..12 {
            let manager = Arc::clone(manager);
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

async fn run_known_visible_found_quickly(case: &BackendCase, manager: &Arc<QueueManager>) {
    let queue_url = create_queue(
        manager,
        &format!("{}-bounded-discovery", unique_suffix(case.name)),
    )
    .await;
    let sent_id = send_message(manager, &queue_url, "known-visible".to_string()).await;

    let messages = receive_one_within_attempts(manager, &queue_url, 4).await;
    assert_eq!(
        messages.len(),
        1,
        "{} did not find known message",
        case.name
    );
    assert_eq!(messages[0].message_id, sent_id);
}

async fn run_send_to_receive_under_500ms(case: &BackendCase, manager: &Arc<QueueManager>) {
    let queue_url = create_queue(
        manager,
        &format!("{}-send-to-receive", unique_suffix(case.name)),
    )
    .await;
    send_message(manager, &queue_url, "latency".to_string()).await;

    let started_at = Instant::now();
    let mut messages = Vec::new();
    while started_at.elapsed() < Duration::from_millis(500) {
        messages = receive_messages(manager, &queue_url, 1).await;
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

async fn run_stale_receipt_does_not_delete_current_claim(
    case: &BackendCase,
    manager: &Arc<QueueManager>,
) {
    let queue_url = create_queue(
        manager,
        &format!("{}-stale-receipt", unique_suffix(case.name)),
    )
    .await;
    let sent_id = send_message(manager, &queue_url, "stale-receipt".to_string()).await;

    let first = receive_one_within_attempts(manager, &queue_url, 4).await;
    assert_eq!(first.len(), 1);
    manager
        .change_message_visibility(queue_provider::ChangeMessageVisibilityRequest {
            queue_url: queue_url.clone(),
            receipt_handle: first[0].receipt_handle.as_str().into(),
            visibility_timeout: 0,
        })
        .await
        .expect("make first claim visible");

    let second = receive_one_within_attempts(manager, &queue_url, 16).await;
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
    // AWS permits an old, otherwise valid receipt handle to return success when
    // it no longer identifies the current claim; the operation must not remove
    // that claim. Providers that reject stale handles use this typed validation
    // error instead.
    if let Err(error) = stale_delete {
        assert!(
            matches!(
                error,
                QueueError::Validation {
                    kind: QueueValidationKind::MessageNotFoundOrAlreadyProcessed
                        | QueueValidationKind::MessageNotFound,
                    ..
                } | QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    ..
                }
            ),
            "{} stale receipt returned unrelated error: {error:?}",
            case.name
        );
    }

    manager
        .change_message_visibility(queue_provider::ChangeMessageVisibilityRequest {
            queue_url: queue_url.clone(),
            receipt_handle: second[0].receipt_handle.as_str().into(),
            visibility_timeout: 0,
        })
        .await
        .expect("make second claim visible");
    let still_present = receive_one_within_attempts(manager, &queue_url, 4).await;
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
async fn selected_single_node_backend_runs_sqs_delivery_invariants() {
    let case = selected_native_backend_case("delivery");
    let manager = required_manager_for_case(&case).await;
    run_twelve_worker_no_drop_duplicate(&case, &manager).await;
    run_known_visible_found_quickly(&case, &manager).await;
    run_send_to_receive_under_500ms(&case, &manager).await;
}

#[tokio::test]
async fn selected_single_node_backend_preserves_messages_from_stale_receipts() {
    let case = selected_native_backend_case("failure-injection");
    let manager = required_manager_for_case(&case).await;
    run_stale_receipt_does_not_delete_current_claim(&case, &manager).await;
}

#[cfg(any(feature = "foundationdb", feature = "postgres"))]
#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn selected_multi_node_delivery_invariants() {
    let case = selected_distributed_backend_case("multi-node-delivery");
    let manager = required_manager_for_case(&case).await;
    run_twelve_worker_no_drop_duplicate(&case, &manager).await;
    run_known_visible_found_quickly(&case, &manager).await;
    run_send_to_receive_under_500ms(&case, &manager).await;
}

#[cfg(any(feature = "foundationdb", feature = "postgres"))]
#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn selected_multi_node_stale_receipts_and_node_rejoin_preserve_messages() {
    let case = selected_distributed_backend_case("multi-node-failure-injection");
    let manager = required_manager_for_case(&case).await;
    run_stale_receipt_does_not_delete_current_claim(&case, &manager).await;
    drop(manager);
    run_node_leave_join_preserves_messages(&case).await;
}

#[tokio::test]
#[ignore = "12-worker multi-node soak requires FoundationDB or Postgres services"]
async fn twelve_worker_multi_node_soak_records_delivery_failures() {
    let case = selected_distributed_backend_case("multi-node-soak");
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

#[test]
fn given_backend_inventory_when_selecting_acceptance_case_then_selection_is_exact_and_nonempty() {
    assert_eq!(
        select_native_backend_name(Some(" postgres "), &["sqlite", "postgres"]),
        Ok("postgres")
    );
    assert!(select_native_backend_name(Some("foundationdb"), &["sqlite", "postgres"]).is_err());
    assert_eq!(select_native_backend_name(None, &["turso"]), Ok("turso"));
    assert!(select_native_backend_name(None, &[]).is_err());
    assert!(select_native_backend_name(None, &["turso", "rocksdb"]).is_err());
}
