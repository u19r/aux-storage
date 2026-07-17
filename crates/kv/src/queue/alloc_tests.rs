use std::sync::Arc;

use alloc_counter::{AllocationGuard, count_allocations};
use queue_provider::{Queue, QueueMessage, QueueProvider};
use storage_types::{DurationSeconds, TimestampMillis};

use crate::{RocksDbKvStore, SortedKvDbStorageProvider, kv_support_tests::rocksdb_test_path};

const QUEUE_CREATE_URL: &str = "https://queue.example.test/000000000000/alloc-profile";
const QUEUE_URL: &str = "https://queue.example.test/000000000000/000000000001/alloc-profile";
const MESSAGE_COUNT: usize = 32;
const RECEIVE_LIMIT: u32 = 8;
const ITERATIONS: usize = 4;

#[test]
fn queue_send_body_move_and_retry_bytes_allocation_tests() {
    for (label, size) in [
        ("1_kib", 1024usize),
        ("64_kib", 64 * 1024),
        ("maximum", 1024 * 1024),
    ] {
        let body = "x".repeat(size);
        let clone_guard = AllocationGuard::start(
            module_path!(),
            "queue_send_body_move_and_retry_bytes_allocation_tests",
            file!(),
            line!(),
            Some(label),
        );
        std::hint::black_box(body.clone());
        let cloned = clone_guard.finish();

        let move_guard = AllocationGuard::start(
            module_path!(),
            "queue_send_body_move_and_retry_bytes_allocation_tests",
            file!(),
            line!(),
            Some("owned_move"),
        );
        std::hint::black_box(body);
        let moved = move_guard.finish();
        alloc_counter::emit_report(&cloned);
        alloc_counter::emit_report(&moved);
        assert!(
            cloned.allocated_bytes.saturating_sub(moved.allocated_bytes) >= size as u64,
            "owned move should remove at least the full body copy"
        );
    }

    let retry_bytes: Arc<[u8]> = Arc::from(vec![b'x'; 1024 * 1024]);
    let retry_guard = AllocationGuard::start(
        module_path!(),
        "queue_send_body_move_and_retry_bytes_allocation_tests",
        file!(),
        line!(),
        Some("shared_retry_bytes"),
    );
    for _ in 0..4 {
        std::hint::black_box(Arc::clone(&retry_bytes));
    }
    let shared = retry_guard.finish();
    alloc_counter::emit_report(&shared);
    assert!(shared.allocated_bytes < 1024 * 1024);
}

fn queue() -> Queue {
    Queue {
        queue_name: "alloc-profile".to_string(),
        queue_url: QUEUE_CREATE_URL.to_string(),
        attributes: Default::default(),
        created_at: TimestampMillis::now(),
    }
}

fn message(index: usize) -> QueueMessage {
    QueueMessage {
        queue_url: QUEUE_URL.to_string(),
        body: format!("allocation-profile-message-{index:04}"),
        created_at: TimestampMillis::now(),
        visibility_timestamp: Some(TimestampMillis::now()),
        ..Default::default()
    }
}

async fn create_provider() -> SortedKvDbStorageProvider<RocksDbKvStore> {
    let provider = SortedKvDbStorageProvider::new(
        RocksDbKvStore::new(rocksdb_test_path("queue-alloc")).unwrap(),
    );
    provider
        .initialize()
        .await
        .expect("initialize queue provider");
    provider.create_queue(queue()).await.expect("create queue");
    provider
}

#[count_allocations(label = "kv_partitioned_queue_receive_hot_path")]
async fn measure_partitioned_queue_receive_hot_path_tests(
    provider: &SortedKvDbStorageProvider<RocksDbKvStore>,
) {
    for _ in 0..ITERATIONS {
        let messages = provider
            .receive_messages(
                QUEUE_URL,
                RECEIVE_LIMIT,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive messages");
        assert_eq!(messages.len(), RECEIVE_LIMIT as usize);
    }
}

#[tokio::test]
async fn kv_partitioned_queue_receive_allocation_profile_tests() {
    // Snapshot (2026-05-06, RocksDB): this profiles the receive path after the
    // queue claim implementation switched from per-candidate body/state awaits
    // to one batched multi_get per ready-key range.
    let provider = create_provider().await;
    for index in 0..MESSAGE_COUNT {
        provider
            .send_message(message(index))
            .await
            .expect("send message");
    }

    measure_partitioned_queue_receive_hot_path_tests(&provider).await;
}
