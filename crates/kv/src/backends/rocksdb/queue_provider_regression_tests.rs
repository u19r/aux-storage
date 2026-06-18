use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use queue_provider::{Queue, QueueMessage, QueueProvider, ReceiptHandle};
use storage_types::{DurationSeconds, TimestampMillis};
use uuid::Uuid;

use crate::{
    RocksDbKvStore, SortedKvDbStorageProvider,
    keyspace::compact::{self, QueueStorageId},
    kv_support_tests::rocksdb_test_path,
    sorted_kv_store::SortedKvStore,
};

async fn create_test_provider() -> SortedKvDbStorageProvider<RocksDbKvStore> {
    SortedKvDbStorageProvider::new(
        RocksDbKvStore::new(rocksdb_test_path("queue-regression")).unwrap(),
    )
}

fn queue(queue_name: &str, queue_url: &str) -> Queue {
    Queue {
        queue_name: queue_name.to_string(),
        queue_url: queue_url.to_string(),
        attributes: Default::default(),
        created_at: TimestampMillis::now(),
    }
}

fn message(queue_url: &str, body: String) -> QueueMessage {
    QueueMessage {
        queue_url: queue_url.to_string(),
        body,
        created_at: TimestampMillis::now(),
        visibility_timestamp: Some(TimestampMillis::now()),
        ..Default::default()
    }
}

fn large_text_body(target_len: usize) -> String {
    let mut body = String::with_capacity(target_len);
    while body.len() < target_len {
        body.push_str(&Uuid::now_v7().to_string());
    }
    body.truncate(target_len);
    body
}

async fn queue_body_records(
    provider: &SortedKvDbStorageProvider<RocksDbKvStore>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    provider
        .kv_store
        .get_prefix(b"q", true, None, true)
        .await
        .expect("read queue partition records")
        .items
        .into_iter()
        .filter_map(|(key, value)| {
            let key = key.into_vec();
            matches!(
                compact::parse_compact_key(&key),
                Ok(compact::ParsedCompactKey::PartitionedQueueData {
                    kind: compact::QueueRecordKind::Body,
                    ..
                })
            )
            .then(|| (key, value.into_vec()))
        })
        .collect()
}

async fn legacy_queue_records(
    provider: &SortedKvDbStorageProvider<RocksDbKvStore>,
) -> Vec<Vec<u8>> {
    provider
        .kv_store
        .get_prefix(b"sys/queues/", true, None, true)
        .await
        .expect("read legacy queue records")
        .items
        .into_iter()
        .map(|(key, _)| key.into_vec())
        .collect()
}

#[tokio::test]
async fn large_message_payload_is_split_and_reassembled() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/large-message";
    provider
        .create_queue(queue("large-message", queue_url))
        .await
        .expect("create queue");
    let large_body = large_text_body(900 * 1024);

    let sent_id = provider
        .send_message(message(queue_url, large_body.clone()))
        .await
        .expect("send large message");
    let records = queue_body_records(&provider).await;
    assert!(records.len() > 1, "large payload should be chunked");
    assert!(
        records.iter().all(|(_, value)| value.len() <= 100 * 1024),
        "all stored payload records should fit under FoundationDB's 100KB value limit"
    );

    let received = provider
        .receive_messages(
            queue_url,
            1,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive large message");

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].message_id, sent_id.to_string());
    assert_eq!(received[0].body, large_body);
}

#[tokio::test]
async fn large_message_payload_chunks_are_removed_by_cleanup() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/large-message-cleanup";
    provider
        .create_queue(queue("large-message-cleanup", queue_url))
        .await
        .expect("create queue");

    provider
        .send_message(message(queue_url, large_text_body(300 * 1024)))
        .await
        .expect("send large message");
    assert!(queue_body_records(&provider).await.len() > 1);

    let received = provider
        .receive_messages(
            queue_url,
            1,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive large message");
    assert_eq!(received.len(), 1);
    provider
        .delete_message(
            queue_url,
            ReceiptHandle::from(received[0].receipt_handle.as_str()),
        )
        .await
        .expect("delete large message");

    let cleaned = provider
        .cleanup_queue_payload_orphans(128)
        .await
        .expect("cleanup payload orphans");
    assert_eq!(cleaned, 1);
    assert!(queue_body_records(&provider).await.is_empty());
}

#[tokio::test]
async fn concurrent_receives_claim_each_message_once() {
    let provider = Arc::new(create_test_provider().await);
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/concurrency";
    provider
        .create_queue(queue("concurrency", queue_url))
        .await
        .expect("create queue");

    for index in 0..24 {
        provider
            .send_message(message(queue_url, format!("message-{index}")))
            .await
            .expect("send message");
    }

    let mut handles = Vec::new();
    for _ in 0..6 {
        let provider = provider.clone();
        let queue_url = queue_url.to_string();
        handles.push(tokio::spawn(async move {
            provider
                .receive_messages(
                    &queue_url,
                    4,
                    DurationSeconds::from(30),
                    DurationSeconds::from(0),
                )
                .await
                .expect("receive messages")
        }));
    }

    let mut unique_ids = HashSet::new();
    let mut total_received = 0usize;
    for handle in handles {
        let messages = handle.await.expect("join receive task");
        total_received += messages.len();
        for message in messages {
            let message_id = message.message_id;
            assert!(
                unique_ids.insert(message_id.clone()),
                "duplicate message claimed: {}",
                message_id
            );
        }
    }

    assert_eq!(unique_ids.len(), 24);
    assert_eq!(total_received, 24);
}

#[tokio::test]
async fn twelve_workers_receive_known_messages_without_duplicates_or_drops() {
    let provider = Arc::new(create_test_provider().await);
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/twelve-workers";
    provider
        .create_queue(queue("twelve-workers", queue_url))
        .await
        .expect("create queue");

    for index in 0..120 {
        provider
            .send_message(message(queue_url, format!("message-{index}")))
            .await
            .expect("send message");
    }

    let mut handles = Vec::new();
    for _ in 0..12 {
        let provider = provider.clone();
        let queue_url = queue_url.to_string();
        handles.push(tokio::spawn(async move {
            provider
                .receive_messages(
                    &queue_url,
                    10,
                    DurationSeconds::from(30),
                    DurationSeconds::from(0),
                )
                .await
                .expect("receive messages")
        }));
    }

    let mut unique_ids = HashSet::new();
    let mut total_received = 0usize;
    for handle in handles {
        let messages = handle.await.expect("join receive task");
        total_received += messages.len();
        for message in messages {
            let message_id = message.message_id;
            assert!(
                unique_ids.insert(message_id.clone()),
                "duplicate message claimed in 12-worker receive wave: {}",
                message_id
            );
        }
    }

    assert_eq!(unique_ids.len(), 120);
    assert_eq!(total_received, 120);
}

#[tokio::test]
async fn known_visible_message_is_found_within_four_receive_attempts() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/bounded-discovery";
    provider
        .create_queue(queue("bounded-discovery", queue_url))
        .await
        .expect("create queue");
    let sent_id = provider
        .send_message(message(queue_url, "known-visible".to_string()))
        .await
        .expect("send message");

    let mut received = Vec::new();
    for _ in 0..4 {
        received = provider
            .receive_messages(
                queue_url,
                1,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive messages");
        if !received.is_empty() {
            break;
        }
    }

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].message_id, sent_id.to_string());
}

#[tokio::test]
async fn sent_message_can_be_received_within_500ms_after_send_response() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/send-to-receive-latency";
    provider
        .create_queue(queue("send-to-receive-latency", queue_url))
        .await
        .expect("create queue");
    provider
        .send_message(message(queue_url, "latency".to_string()))
        .await
        .expect("send message");

    let started_at = Instant::now();
    let mut received = Vec::new();
    while started_at.elapsed() < Duration::from_millis(500) {
        received = provider
            .receive_messages(
                queue_url,
                1,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive messages");
        if !received.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(received.len(), 1);
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "message was not receivable within 500ms after send response"
    );
}

#[tokio::test]
async fn delete_and_visibility_changes_work_across_provider_clones() {
    let provider = Arc::new(create_test_provider().await);
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/cross-node";
    provider
        .create_queue(queue("cross-node", queue_url))
        .await
        .expect("create queue");
    provider
        .send_message(message(queue_url, "first".to_string()))
        .await
        .expect("send first message");
    provider
        .send_message(message(queue_url, "second".to_string()))
        .await
        .expect("send second message");

    let worker_a = provider.clone();
    let received = worker_a
        .receive_messages(
            queue_url,
            1,
            DurationSeconds::from(5),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive first message");
    let receipt_handle = ReceiptHandle::from(received[0].receipt_handle.as_str());

    let worker_b = provider.clone();
    worker_b
        .change_message_visibility(queue_url, receipt_handle.clone(), DurationSeconds::from(0))
        .await
        .expect("change visibility with second worker");

    let redelivered = provider
        .receive_messages(
            queue_url,
            1,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive redelivered message");
    assert_eq!(redelivered.len(), 1);
    let deleted_body = redelivered[0].body.clone();

    let delete_handle = ReceiptHandle::from(redelivered[0].receipt_handle.as_str());
    let worker_c = provider.clone();
    worker_c
        .delete_message(queue_url, delete_handle)
        .await
        .expect("delete message with third worker");
    assert!(
        legacy_queue_records(&provider).await.is_empty(),
        "active queue delete and visibility paths should not write legacy sys/queues records"
    );

    let remaining = provider
        .receive_messages(
            queue_url,
            10,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive remaining messages");
    assert_eq!(remaining.len(), 1);
    assert_ne!(remaining[0].body, deleted_body);
    assert_eq!(
        HashSet::from([deleted_body, remaining[0].body.clone()]),
        HashSet::from(["first".to_string(), "second".to_string()])
    );
}

#[tokio::test]
async fn malformed_receipt_handles_do_not_use_legacy_queue_fallbacks() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/malformed-handle";
    provider
        .create_queue(queue("malformed-handle", queue_url))
        .await
        .expect("create queue");
    let receipt_handle = ReceiptHandle::from("not-a-compact-receipt-handle");

    let delete_error = provider
        .delete_message(queue_url, receipt_handle.clone())
        .await
        .expect_err("malformed delete handle should fail");
    assert!(matches!(
        delete_error,
        queue_provider::QueueError::Validation {
            kind: queue_provider::QueueValidationKind::MessageNotFound,
            ..
        }
    ));

    let visibility_error = provider
        .change_message_visibility(queue_url, receipt_handle, DurationSeconds::from(0))
        .await
        .expect_err("malformed visibility handle should fail");
    assert!(matches!(
        visibility_error,
        queue_provider::QueueError::Validation {
            kind: queue_provider::QueueValidationKind::MessageNotFound,
            ..
        }
    ));

    assert!(
        legacy_queue_records(&provider).await.is_empty(),
        "malformed handles should not trigger legacy sys/queues reads or writes"
    );
}

#[tokio::test]
async fn visibility_timeout_zero_makes_claimed_message_receivable_again() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/visibility-zero";
    provider
        .create_queue(queue("visibility-zero", queue_url))
        .await
        .expect("create queue");
    let sent_id = provider
        .send_message(message(queue_url, "visible-again".to_string()))
        .await
        .expect("send message");

    let received = provider
        .receive_messages(
            queue_url,
            1,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive message");
    assert_eq!(received.len(), 1);

    provider
        .change_message_visibility(
            queue_url,
            ReceiptHandle::from(received[0].receipt_handle.as_str()),
            DurationSeconds::from(0),
        )
        .await
        .expect("change visibility to zero");

    let visible_again = provider
        .receive_messages(
            queue_url,
            1,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive visible message again");

    assert_eq!(visible_again.len(), 1);
    assert_eq!(visible_again[0].message_id, sent_id.to_string());
}

#[tokio::test]
async fn payload_cleanup_removes_deleted_messages_without_dropping_live_messages() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/payload-cleanup";
    provider
        .create_queue(queue("payload-cleanup", queue_url))
        .await
        .expect("create queue");

    for index in 0..6 {
        provider
            .send_message(message(queue_url, format!("cleanup-message-{index}")))
            .await
            .expect("send message");
    }

    let received = provider
        .receive_messages(
            queue_url,
            6,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive messages");
    assert_eq!(received.len(), 6);

    let delete_results = provider
        .delete_messages(
            queue_url,
            received
                .iter()
                .take(3)
                .map(|message| ReceiptHandle::from(message.receipt_handle.as_str()))
                .collect(),
        )
        .await
        .expect("delete messages");
    assert!(delete_results.iter().all(Result::is_ok));

    let cleaned = provider
        .cleanup_queue_payload_orphans(128)
        .await
        .expect("cleanup payload orphans");
    assert_eq!(cleaned, 3);

    let cleaned_again = provider
        .cleanup_queue_payload_orphans(128)
        .await
        .expect("cleanup payload orphans again");
    assert_eq!(cleaned_again, 0);

    for message in received.iter().skip(3) {
        provider
            .change_message_visibility(
                queue_url,
                ReceiptHandle::from(message.receipt_handle.as_str()),
                DurationSeconds::from(0),
            )
            .await
            .expect("restore live message visibility");
    }
    let still_live = provider
        .receive_messages(
            queue_url,
            6,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .expect("receive live messages");
    assert_eq!(still_live.len(), 3);
}

#[tokio::test]
async fn payload_cleanup_discards_malformed_delete_ledger_entries() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/malformed-ledger";
    provider
        .create_queue(queue("malformed-ledger", queue_url))
        .await
        .expect("create queue");
    let ledger_key = crate::queue_provider::queue_delete_ledger_key(
        QueueStorageId::new(1).expect("first queue id"),
        0,
        0,
        "not-a-message-id",
    );
    provider
        .kv_store
        .put(&ledger_key, b"not valid ledger bytes", None)
        .await
        .expect("write malformed ledger");

    let cleaned = provider
        .cleanup_queue_payload_orphans(128)
        .await
        .expect("cleanup payload orphans");
    assert_eq!(cleaned, 0);
    assert!(
        provider
            .kv_store
            .get(&ledger_key, true)
            .await
            .expect("read ledger")
            .is_none()
    );
}

#[tokio::test]
async fn queue_lookup_by_name_supports_canonical_queue_urls() {
    let provider = create_test_provider().await;
    provider.initialize().await.expect("initialize provider");

    let queue_url = "https://queue.example.test/000000000000/name-lookup";
    provider
        .create_queue(queue("name-lookup", queue_url))
        .await
        .expect("create queue");

    let queue_id = QueueStorageId::new(1).expect("first queue id");
    let queue_id_bytes = provider
        .kv_store
        .get(&compact::queue_url_lookup_key(queue_url), true)
        .await
        .expect("read queue url lookup")
        .expect("queue url lookup should exist");
    assert_eq!(queue_id_bytes.len(), 6);
    assert!(
        provider
            .kv_store
            .get(&compact::queue_name_lookup_key("name-lookup"), true)
            .await
            .expect("read queue name lookup")
            .is_some()
    );
    assert!(
        provider
            .kv_store
            .get(&compact::queue_metadata_key(queue_id), true)
            .await
            .expect("read queue metadata")
            .is_some()
    );

    let queue = provider
        .get_queue_by_name("name-lookup")
        .await
        .expect("get queue by name")
        .expect("queue should exist");
    let queues = provider
        .list_queues(Some("name"))
        .await
        .expect("list queues");

    assert_eq!(queue.queue_url, queue_url);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].queue_url, queue_url);

    provider
        .delete_queue(queue_url)
        .await
        .expect("delete queue");
    assert!(
        provider
            .kv_store
            .get(&compact::queue_url_lookup_key(queue_url), true)
            .await
            .expect("read deleted queue url lookup")
            .is_none()
    );
    assert!(
        provider
            .kv_store
            .get(&compact::queue_name_lookup_key("name-lookup"), true)
            .await
            .expect("read deleted queue name lookup")
            .is_none()
    );
    assert!(
        provider
            .kv_store
            .get(&compact::queue_metadata_key(queue_id), true)
            .await
            .expect("read deleted queue metadata")
            .is_none()
    );
}
