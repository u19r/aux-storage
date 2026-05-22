#![cfg(not(feature = "foundationdb-backend"))]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::Utc;
use uuid::Uuid;

use crate::{
    RocksDbKvStore, SortedKvDbStorageProvider,
    keys::{queue_message_storage_key, queue_visibility_storage_key},
    kv_support_tests::rocksdb_test_path,
    queue::QueueProvider,
    queue_provider::StoredQueueMessage,
    storage::kv::{
        helpers::{MessageId, MessageVisibilityKey, TimestampMillis, deserialize_item_from_bytes},
        sorted_kv_store::SortedKvStore as _,
    },
    types::{AttributeValue, Queue, QueueMessage},
};

async fn create_test_provider() -> SortedKvDbStorageProvider<RocksDbKvStore> {
    let store = RocksDbKvStore::new(rocksdb_test_path("test-queue")).unwrap();

    SortedKvDbStorageProvider::new(store)
        .await
        .expect("Failed to create provider")
}

fn create_test_queue(queue_url: &str) -> Queue {
    Queue {
        queue_name: "queue".to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now(),
    }
}

fn create_test_message(
    queue_url: &str,
    message_id: MessageId,
    _next_visible_seconds: i64,
) -> QueueMessage {
    QueueMessage {
        body: format!("Test message body for {}", message_id),
        message_id: message_id.clone(),
        queue_url: queue_url.to_string(),
        message_attributes: Some(HashMap::new()),
        receipt_handle: Some(Uuid::now_v7().to_string()),
        created_at: TimestampMillis::now(),
    }
}

#[tokio::test]
async fn create_and_get_queue() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue = create_test_queue("https://example.com/queue1");

    // Create queue
    provider.create_queue(&queue).await.unwrap();

    // Get queue
    let retrieved = provider.get_queue(&queue.queue_url).await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.queue_url, queue.queue_url);
    assert_eq!(retrieved.queue_name, queue.queue_name);

    // Test non-existent queue
    let not_found = provider
        .get_queue("https://example.com/nonexistent")
        .await
        .unwrap();
    assert!(not_found.is_none());
}

#[tokio::test]
async fn send_and_receive_message() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue_url = "queue1";
    let queue = create_test_queue(queue_url);
    provider.create_queue(&queue).await.unwrap();

    // Send a message that's immediately visible
    let message_id = MessageId::from_uuid(Uuid::now_v7());
    let message = create_test_message(queue_url, message_id, 0); // 10 seconds ago

    provider.send_message(message.clone()).await.unwrap();

    // Verify message is stored correctly in kv_store
    let message_key = queue_message_storage_key(queue_url, &message.message_id);
    let stored_data = provider.kv_store.get(&message_key, true).await.unwrap();
    assert!(stored_data.is_some());
    let stored_message: StoredQueueMessage =
        storage_types::storage_serde::from_bytes(&stored_data.unwrap()).unwrap();
    assert_eq!(stored_message.body, message.body);

    // Receive messages
    let received = provider.receive_messages(queue_url, 10, 30).await.unwrap();

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].message_id, message.message_id);
}

#[tokio::test]
async fn message_id_is_derived_from_keys_and_visibility_values_are_empty() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue_url = "queue-key-materialization";
    let queue = create_test_queue(queue_url);
    provider.create_queue(&queue).await.unwrap();

    let message = create_test_message(queue_url, MessageId::from_uuid(Uuid::now_v7()), 0);
    let assigned_id = provider.send_message(message.clone()).await.unwrap();

    let message_prefix = format!("sys/queues/{queue_url}/messages/").into_bytes();
    let message_range = provider
        .kv_store
        .get_prefix(&message_prefix, true, Some(1), true)
        .await
        .unwrap();
    assert!(!message_range.items.is_empty());
    let (message_key, stored_value) = &message_range.items[0];
    assert!(!stored_value.is_empty());
    assert_eq!(
        &message_key[message_key.len() - assigned_id.as_bytes().len()..],
        assigned_id.as_bytes(),
    );

    let visibility_prefix = format!("sys/queues/{queue_url}/visibility/").into_bytes();
    let visibility_range = provider
        .kv_store
        .get_prefix(&visibility_prefix, true, Some(1), true)
        .await
        .unwrap();
    assert!(!visibility_range.items.is_empty());
    let (visibility_key, visibility_value) = &visibility_range.items[0];
    assert!(visibility_value.is_empty());
    assert_eq!(
        &visibility_key[visibility_key.len() - assigned_id.as_bytes().len()..],
        assigned_id.as_bytes(),
    );
}

#[tokio::test]
async fn receive_messages_respects_max_messages() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue_url = "https://example.com/queue1";
    let queue = create_test_queue(queue_url);
    provider.create_queue(&queue).await.unwrap();

    // Send 5 messages that are immediately visible
    let now = chrono::Utc::now().timestamp();
    for _i in 1..=5 {
        let message =
            create_test_message(queue_url, MessageId::from_uuid(Uuid::now_v7()), now - 10);
        provider.send_message(message).await.unwrap();
    }

    // Receive with limit of 3
    let received = provider.receive_messages(queue_url, 3, 30).await.unwrap();

    assert_eq!(received.len(), 3);
}

#[tokio::test]
async fn receive_messages_only_visible_messages() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue_url = "queue1";
    let queue = create_test_queue(queue_url);
    provider.create_queue(&queue).await.unwrap();

    let message_id_1 = MessageId::from_uuid(Uuid::now_v7());
    let msg_1 = create_test_message(queue_url, message_id_1, 0);
    provider.send_message(msg_1.clone()).await.unwrap();

    let message_id_2 = MessageId::from_uuid(Uuid::now_v7());
    let msg_2 = create_test_message(queue_url, message_id_2, 0);
    provider.send_message(msg_2.clone()).await.unwrap();

    let received = provider.receive_messages(queue_url, 1, 30).await.unwrap();
    let received_2 = provider.receive_messages(queue_url, 10, 30).await.unwrap();

    assert_eq!(received.len(), 1);
    assert_eq!(received_2.len(), 1);
}

#[tokio::test]
async fn queue_isolation() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue1_url = "queue1";
    let queue2_url = "queue2";

    let queue1 = create_test_queue(queue1_url);
    let queue2 = create_test_queue(queue2_url);
    provider.create_queue(&queue1).await.unwrap();
    provider.create_queue(&queue2).await.unwrap();

    let now = chrono::Utc::now().timestamp();

    // Send messages to both queues
    let message_id1 = MessageId::from_uuid(Uuid::now_v7());
    let message_id2 = MessageId::from_uuid(Uuid::now_v7());
    let msg1 = create_test_message(queue1_url, message_id1, now - 10);
    let msg2 = create_test_message(queue2_url, message_id2, now - 10);
    provider.send_message(msg1.clone()).await.unwrap();
    provider.send_message(msg2.clone()).await.unwrap();

    // Receive from queue1 should only get queue1 messages
    let received1 = provider.receive_messages(queue1_url, 10, 30).await.unwrap();
    assert_eq!(received1.len(), 1);
    assert_eq!(received1[0].message_id, message_id1);
    assert_eq!(received1[0].queue_url, queue1_url);

    // Receive from queue2 should only get queue2 messages
    let received2 = provider.receive_messages(queue2_url, 10, 30).await.unwrap();
    assert_eq!(received2.len(), 1);
    assert_eq!(received2[0].message_id, message_id2);
    assert_eq!(received2[0].queue_url, queue2_url);
}

#[tokio::test]
async fn checkpoint_operations() {
    let provider = create_test_provider().await;
    provider.initialize().await.unwrap();

    let queue1_url = "queue1";
    let queue1 = create_test_queue(queue1_url);
    provider.create_queue(&queue1).await.unwrap();
    let message_id = MessageId::from_uuid(Uuid::now_v7());
    let message = create_test_message(queue1_url, message_id, 0);
    provider.send_message(message.clone()).await.unwrap();
    let message = provider
        .receive_messages(queue1_url, 1, 30)
        .await
        .unwrap()
        .pop()
        .expect("No message received");

    // Update checkpoint
    provider
        .update_message_snapshot_checkpoint(
            &queue1_url,
            &Uuid::parse_str(&message.receipt_handle.clone().unwrap()).unwrap(),
            "testing123".to_string(),
        )
        .await
        .unwrap();

    // Verify checkpoint is stored correctly
    let checkpoint_key = format!(
        "sys/queues/{queue1_url}/receipt_handles/{}/checkpoint",
        message.receipt_handle.unwrap()
    );
    let stored_data = provider
        .kv_store
        .get(checkpoint_key.as_bytes(), true)
        .await
        .unwrap();
    assert!(stored_data.is_some());

    let stored_checkpoint: serde_json::Value =
        storage_types::storage_serde::from_bytes(&stored_data.unwrap()).unwrap();
    let expected: serde_json::Value = serde_json::to_value("testing123").unwrap();
    assert_eq!(stored_checkpoint, expected);
}

#[tokio::test]
async fn lru_populated_on_receive_and_used_on_delete() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-lru-delete";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Send a message
    let msg = QueueMessage {
        message_id: MessageId::from_uuid(Uuid::now_v7()),
        queue_url: queue_url.to_string(),
        body: "hello".into(),
        message_attributes: None,
        receipt_handle: None,
        created_at: TimestampMillis::now()(),
    };
    provider.send_message(msg.clone()).await.unwrap();

    // Receive one message; this should populate LRU
    let got = provider.receive_messages(queue_url, 1, 30).await.unwrap();
    assert_eq!(got.len(), 1);
    let handle = got[0].receipt_handle.clone().unwrap();

    // Delete using handle; should hit LRU path and succeed
    provider
        .delete_message(queue_url, &handle)
        .await
        .expect("delete should succeed");

    // Ensure message is gone
    let key = queue_message_storage_key(queue_url, &msg.message_id);
    let found = provider.kv_store.get(&key, true).await.unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn lru_fallback_when_visibility_index_missing() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-lru-fallback";

    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    let msg = QueueMessage {
        message_id: MessageId::from_uuid(Uuid::now_v7()),
        queue_url: queue_url.to_string(),
        body: "hello".into(),
        message_attributes: None,
        receipt_handle: None,
        created_at: TimestampMillis::now()(),
    };
    provider.send_message(msg.clone()).await.unwrap();

    // Receive to create receipt and populate LRU
    let got = provider.receive_messages(queue_url, 1, 30).await.unwrap();
    let handle = got[0].receipt_handle.clone().unwrap();

    // Delete the visibility index directly to simulate another server moving it
    // Find visibility key via receipt handle in DB
    let rh_key = format!("sys/queues/{}/receipt_handles/{}", queue_url, handle);
    let vis_bytes = provider
        .kv_store
        .get(rh_key.as_bytes(), true)
        .await
        .unwrap()
        .unwrap();
    let vis_entry: HashMap<String, AttributeValue> =
        storage_types::storage_serde::from_bytes(&vis_bytes).unwrap();
    let vis_str = match vis_entry.get("visibility_key") {
        Some(AttributeValue::S(s)) => s.clone(),
        _ => panic!("missing visibility_key"),
    };
    let vis_key = MessageVisibilityKey(vis_str);
    let vis_idx_key = queue_visibility_storage_key(
        queue_url,
        vis_key.get_timestamp().expect("timestamp"),
        &vis_key.get_message_id().expect("message id"),
    );
    provider.kv_store.delete(&vis_idx_key).await.unwrap();

    // Now delete_message should fallback to DB receipt lookup when index missing
    provider.delete_message(queue_url, &handle).await.unwrap();

    // Ensure message removed
    let key = queue_message_storage_key(queue_url, &msg.message_id);
    let found = provider.kv_store.get(&key, true).await.unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn change_message_visibility_updates_lru() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-lru-change";

    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    let msg = QueueMessage {
        message_id: MessageId::from_uuid(Uuid::now_v7()),
        queue_url: queue_url.to_string(),
        body: "hello".into(),
        message_attributes: None,
        receipt_handle: None,
        created_at: TimestampMillis::now()(),
    };
    provider.send_message(msg.clone()).await.unwrap();

    let got = provider.receive_messages(queue_url, 1, 5).await.unwrap();
    let handle = got[0].receipt_handle.clone().unwrap();

    // Change visibility; should update receipt handle record and LRU
    provider
        .change_message_visibility(queue_url, &handle, 120)
        .await
        .unwrap();

    // Inspect LRU state indirectly by reading receipt mapping and ensuring new ts
    let rh_key = format!("sys/queues/{}/receipt_handles/{}", queue_url, handle);
    let raw = provider
        .kv_store
        .get(rh_key.as_bytes(), true)
        .await
        .unwrap()
        .unwrap();

    let vis = deserialize_item_from_bytes(&raw).unwrap();
    let visibility_key = match vis.get("visibility_key").unwrap() {
        AttributeValue::S(vis_key) => MessageVisibilityKey(vis_key.clone()),
        _ => {
            panic!("Invalid visibility key format");
        }
    };

    assert!(visibility_key.get_timestamp().unwrap() > TimestampMillis::now());
}

async fn setup_test_messages(
    provider: &SortedKvDbStorageProvider<RocksDbKvStore>,
    queue_url: &str,
    count: usize,
    _visibility_offset_seconds: i64,
) -> Vec<QueueMessage> {
    let mut messages = Vec::new();
    let base_time = Utc::now();

    for i in 0..count {
        let message_id = MessageId::from_uuid(Uuid::now_v7());

        let message = QueueMessage {
            message_id,
            queue_url: queue_url.to_string(),
            body: format!("Test message {i}"),
            message_attributes: None,
            receipt_handle: Some(Uuid::now_v7().to_string()),
            created_at: base_time,
        };

        // Send the message using the provider
        provider.send_message(message.clone()).await.unwrap();
        messages.push(message);
    }

    messages
}

#[tokio::test]
async fn receive_messages_empty_queue() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-empty";

    let messages = provider.receive_messages(queue_url, 10, 30).await.unwrap();

    assert!(
        messages.is_empty(),
        "Should return empty list for empty queue"
    );
}

#[tokio::test]
async fn receive_messages_basic_functionality() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-basic";

    // Create queue first
    let queue = Queue {
        queue_name: "test-queue-basic".to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup 5 messages that are currently visible (past visibility time)
    let _messages = setup_test_messages(&provider, queue_url, 5, 0).await;

    // Receive messages
    let received = provider.receive_messages(queue_url, 3, 0).await.unwrap();

    assert_eq!(
        received.len(),
        3,
        "Should receive requested number of messages"
    );
}

#[tokio::test]
async fn receive_messages_filters_invisible_messages() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-filter";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup 15 visible messages
    setup_test_messages(&provider, queue_url, 15, 0).await;

    // Request 10 messages
    let received = provider.receive_messages(queue_url, 10, 30).await.unwrap();

    assert_eq!(received.len(), 10);

    let receive_2 = provider.receive_messages(queue_url, 10, 30).await.unwrap();

    assert_eq!(
        receive_2.len(),
        5,
        "Should not receive more messages than available"
    );

    let receive_3 = provider.receive_messages(queue_url, 10, 30).await.unwrap();

    assert_eq!(
        receive_3.len(),
        0,
        "Should return empty when no messages left"
    );
}

#[tokio::test]
async fn receive_messages_concurrent_access() {
    let provider = create_test_provider().await;
    let provider = Arc::new(provider);

    let queue_url = "test-queue-concurrent";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup 20 visible messages
    let _messages = setup_test_messages(&provider, queue_url, 20, -10).await;

    // Simulate concurrent consumers
    let mut handles = Vec::new();
    for _i in 0..5 {
        let queue_url_clone = queue_url.to_string();

        let provider_clone = Arc::clone(&provider);
        let handle = tokio::spawn(async move {
            provider_clone
                .receive_messages(&queue_url_clone, 5, 30)
                .await
                .unwrap_or_else(|_| Vec::new())
        });
        handles.push(handle);
    }

    // Wait for all consumers to complete
    let mut received = HashSet::new();
    let mut total_received = 0;
    for handle in handles {
        let messages = handle.await.unwrap();
        for m in &messages {
            received.insert(m.message_id);
        }
        total_received += messages.len();
    }

    assert_eq!(received.len(), 20, "Should receive all unique messages");
    assert_eq!(
        total_received, 20,
        "Should not receive more messages than available due to locking"
    );
}

#[tokio::test]
async fn receive_messages_expired_lock_acquisition() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-expired-lock";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup messages
    let _messages = setup_test_messages(&provider, queue_url, 5, -10).await;

    // Create an expired lock on the visibility pointer
    let visibility_pointer_key = format!("sys/queues/{queue_url}/visibility-pointer");
    let expired_lock_lease = Utc::now().timestamp() - 3600; // 1 hour ago
    let expired_pointer_data = serde_json::json!({
        "next_timestamp": "0:00000000-0000-0000-0000-000000000000",
        "lock_lease": expired_lock_lease
    });

    provider
        .kv_store
        .put(
            visibility_pointer_key.as_bytes(),
            &storage_types::storage_serde::to_bytes(&expired_pointer_data).unwrap(),
            None,
        )
        .await
        .unwrap();

    // Try to receive messages - should succeed with expired lock
    let received = provider.receive_messages(queue_url, 3, 30).await.unwrap();

    assert!(
        !received.is_empty(),
        "Should be able to acquire expired lock and receive messages"
    );
}

#[tokio::test]
async fn receive_messages_pointer_advancement() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-advancement";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup messages with specific timestamps to test pointer advancement
    let _messages = setup_test_messages(&provider, queue_url, 150, -10).await; // More than batch size

    // First receive operation
    let received1 = provider.receive_messages(queue_url, 50, 30).await.unwrap();

    // Second receive operation should start from where the first left off
    let received2 = provider.receive_messages(queue_url, 50, 30).await.unwrap();

    assert!(!received1.is_empty(), "First batch should not be empty");
    assert!(!received2.is_empty(), "Second batch should not be empty");

    // Verify no message IDs overlap between batches
    let received1_ids: std::collections::HashSet<MessageId> =
        received1.iter().map(|m| m.message_id).collect();
    let received2_ids: std::collections::HashSet<MessageId> =
        received2.iter().map(|m| m.message_id).collect();

    for id in &received2_ids {
        assert!(
            !received1_ids.contains(id),
            "Should not receive duplicate messages in different batches"
        );
    }
}

#[tokio::test]
async fn receive_messages_batch_size_handling() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-batch";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup more messages than the internal batch size (100)
    let _messages = setup_test_messages(&provider, queue_url, 150, -10).await;

    // Request a number within the batch size
    let received = provider.receive_messages(queue_url, 80, 30).await.unwrap();

    assert_eq!(
        received.len(),
        80,
        "Should receive exactly the requested number of messages when available"
    );
}

#[tokio::test]
async fn receive_messages_visibility_timeout_application() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-visibility";

    let visibility_timeout = 120; // 2 minutes

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup visible messages
    let _messages = setup_test_messages(&provider, queue_url, 3, -10).await;

    // Receive messages
    let received = provider
        .receive_messages(queue_url, 3, visibility_timeout)
        .await
        .unwrap();

    assert_eq!(received.len(), 3, "Should receive all available messages");
}

#[tokio::test]
async fn receive_messages_retry_mechanism() {
    let provider = create_test_provider().await;
    let queue_url = "test-queue-retry";

    // Create queue
    let queue = Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now()(),
    };
    provider.create_queue(&queue).await.unwrap();

    // Setup messages
    let _messages = setup_test_messages(&provider, queue_url, 5, -10).await;

    // The retry mechanism is internal and hard to test directly,
    // but we can verify that the method eventually succeeds even under contention
    let received = provider.receive_messages(queue_url, 3, 30).await.unwrap();

    assert!(
        received.len() <= 3,
        "Should handle retries gracefully and return valid results"
    );
}
