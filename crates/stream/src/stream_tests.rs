use std::sync::{Arc, Mutex};

use storage_types::UserStreamName;
use stream_provider::{
    CursorPosition, StreamPartitioningMode, SubscriptionDestination, SubscriptionMessage,
    SubscriptionMessageSender, SubscriptionSendFuture, SubscriptionSendOutcome,
};

use crate::stream_test_support_tests::{create_test_manager, create_test_manager_with_sender};

#[derive(Default)]
struct RecordingSubscriptionSender {
    messages: Mutex<Vec<SubscriptionMessage>>,
}

impl RecordingSubscriptionSender {
    fn messages(&self) -> Vec<SubscriptionMessage> {
        self.messages
            .lock()
            .expect("recording sender mutex should not be poisoned")
            .clone()
    }
}

impl SubscriptionMessageSender for RecordingSubscriptionSender {
    fn send_subscription_message<'a>(
        &'a self,
        message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a> {
        Box::pin(async move {
            self.messages
                .lock()
                .expect("recording sender mutex should not be poisoned")
                .push(message);
            Ok(SubscriptionSendOutcome::AcceptedForDelivery)
        })
    }
}

#[tokio::test]
async fn stream_manager_create_and_get() {
    let manager = create_test_manager().await;

    let result = manager.create_stream("test-stream", Some(3600)).await;
    assert!(result.is_ok());

    let stream = manager.get_stream("test-stream").await.unwrap();
    assert!(stream.is_some());
    let stream = stream.unwrap();
    assert_eq!(stream.name.as_str(), "test-stream");
    assert_eq!(stream.ttl_seconds, Some(3600.into()));
}

#[tokio::test]
async fn stream_manager_accepts_custom_subscription_message_sender() {
    let sender = Arc::new(RecordingSubscriptionSender::default());
    let manager = create_test_manager_with_sender(Some(sender.clone())).await;

    assert!(manager.has_subscription_message_sender());

    let configured_sender = manager
        .subscription_message_sender()
        .expect("custom subscription sender should be configured");
    let outcome = configured_sender
        .send_subscription_message(SubscriptionMessage::new(
            "sub-1",
            "msg-1",
            SubscriptionDestination::new("https", "https://example.com/hook"),
            b"payload".to_vec(),
        ))
        .await
        .expect("custom sender should accept message");

    assert_eq!(outcome, SubscriptionSendOutcome::AcceptedForDelivery);
    let messages = sender.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].subscription_id, "sub-1");
    assert_eq!(messages[0].destination.endpoint, "https://example.com/hook");
    assert_eq!(messages[0].payload, b"payload");
}

#[tokio::test]
async fn stream_manager_ttl_validation() {
    let manager = create_test_manager().await;

    // TTL too small
    let result = manager.create_stream("test-stream", Some(0)).await;
    assert!(result.is_err());

    // TTL too large
    let result = manager.create_stream("test-stream", Some(31_536_001)).await;
    assert!(result.is_err());

    // Valid TTL
    let result = manager.create_stream("test-stream", Some(3600)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_manager_append_validation() {
    let manager = create_test_manager().await;

    manager.create_stream("test-stream", None).await.unwrap();

    // Empty data
    let result = manager.append_item("test-stream", b"").await;
    assert!(result.is_err());

    // Data too large
    let large_data = vec![0u8; 1_048_577];
    let result = manager.append_item("test-stream", &large_data).await;
    assert!(result.is_err());

    // Valid data
    let result = manager.append_item("test-stream", b"valid data").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_manager_partitioned_api_is_backward_compatible() {
    let manager = create_test_manager().await;

    manager
        .create_stream_with_partitioning(
            "partitioned-stream",
            None,
            StreamPartitioningMode::KeyOrdered,
        )
        .await
        .unwrap();

    let stream = manager
        .get_stream("partitioned-stream")
        .await
        .unwrap()
        .unwrap();
    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    assert_eq!(stream.partitioning_mode, StreamPartitioningMode::Single);
    #[cfg(all(
        not(feature = "sqlite"),
        not(feature = "turso"),
        not(feature = "postgres"),
        any(feature = "rocksdb", feature = "foundationdb")
    ))]
    assert_eq!(stream.partitioning_mode, StreamPartitioningMode::KeyOrdered);

    let item_id = manager
        .append_item_with_partition_key("partitioned-stream", b"payload", Some("customer-1"))
        .await
        .unwrap();

    let page = manager
        .read_forward(UserStreamName::new("partitioned-stream"), None, Some(10))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, item_id);
    assert_eq!(page.items[0].data, b"payload");
}

#[tokio::test]
async fn stream_manager_pagination_limits() {
    let manager = create_test_manager().await;

    manager.create_stream("test-stream", None).await.unwrap();

    manager.append_item("test-stream", b"item1").await.unwrap();

    // Default limit
    let page = manager
        .read_forward(UserStreamName::new("test-stream"), None, None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);

    // Limit clamping (too high)
    let page = manager
        .read_forward(UserStreamName::new("test-stream"), None, Some(2000))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);

    // Limit clamping (too low)
    let page = manager
        .read_forward(UserStreamName::new("test-stream"), None, Some(0))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);

    // Valid limit
    let page = manager
        .read_forward(UserStreamName::new("test-stream"), None, Some(10))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
}

#[tokio::test]
async fn stream_manager_cursor_operations() {
    let manager = create_test_manager().await;

    manager.create_stream("test-stream", None).await.unwrap();

    // Create cursor
    let result = manager
        .create_cursor("test-stream", "consumer1", CursorPosition::Head)
        .await;
    assert!(result.is_ok());

    // Get cursor
    let cursor = manager
        .get_cursor("test-stream", "consumer1")
        .await
        .unwrap();
    assert!(cursor.is_some());

    // Add more items
    manager.append_item("test-stream", b"item2").await.unwrap();

    // Read from cursor with default limit
    let page = manager
        .read_from_cursor("test-stream", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);

    // Read from cursor with custom limit
    let page = manager
        .read_from_cursor("test-stream", "consumer1", Some(5))
        .await
        .unwrap();
    assert!(page.items.is_empty()); // Already read all available items

    // Delete cursor
    let result = manager.delete_cursor("test-stream", "consumer1").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_manager_delete() {
    let manager = create_test_manager().await;

    manager.create_stream("test-stream", None).await.unwrap();

    manager.append_item("test-stream", b"item1").await.unwrap();

    manager
        .create_cursor("test-stream", "consumer1", CursorPosition::Head)
        .await
        .unwrap();

    // Delete stream
    let result = manager.delete_stream("test-stream").await;
    assert!(result.is_ok());

    // Verify stream is gone
    let stream = manager.get_stream("test-stream").await.unwrap();
    assert!(stream.is_none());

    // Verify cursor is gone
    let cursor = manager.get_cursor("test-stream", "consumer1").await;

    assert!(cursor.is_err());
}

#[tokio::test]
async fn stream_manager_full_workflow() {
    let manager = create_test_manager().await;

    // Create stream
    manager.create_stream("events", Some(3600)).await.unwrap();

    // Add items
    let id1 = manager.append_item("events", b"user login").await.unwrap();
    let id2 = manager.append_item("events", b"page view").await.unwrap();
    let id3 = manager.append_item("events", b"user logout").await.unwrap();

    // Create cursors
    manager
        .create_cursor("events", "analytics", CursorPosition::Head)
        .await
        .unwrap();
    manager
        .create_cursor("events", "alerts", CursorPosition::Tail)
        .await
        .unwrap();

    // Read all items forward
    let page = manager
        .read_forward(UserStreamName::new("events"), None, Some(10))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);
    assert_eq!(page.items[0].id, id1);
    assert_eq!(page.items[1].id, id2);
    assert_eq!(page.items[2].id, id3);

    // Read all items backward
    let page = manager
        .read_backward("events", None, Some(10))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);
    assert_eq!(page.items[0].id, id3);
    assert_eq!(page.items[1].id, id2);
    assert_eq!(page.items[2].id, id1);

    // Add more items after cursor creation
    let id4 = manager.append_item("events", b"new event 1").await.unwrap();
    let id5 = manager.append_item("events", b"new event 2").await.unwrap();

    // Read from alerts cursor (should get items after tail position)
    let cursor_page = manager
        .read_from_cursor("events", "alerts", Some(10))
        .await
        .unwrap();
    assert_eq!(cursor_page.items.len(), 2);
    assert_eq!(cursor_page.items[0].id, id4);
    assert_eq!(cursor_page.items[1].id, id5);

    // Read from analytics cursor (should get items after head position)
    let cursor_page = manager
        .read_from_cursor("events", "analytics", Some(10))
        .await
        .unwrap();
    assert_eq!(cursor_page.items.len(), 5);
    assert_eq!(cursor_page.items[0].id, id1);
    assert_eq!(cursor_page.items[1].id, id2);

    let stream = manager.get_stream("events").await.unwrap().unwrap();
    assert_eq!(stream.name.as_str(), "events");
    assert_eq!(stream.ttl_seconds, Some(3600.into()));
}

#[tokio::test]
async fn stream_manager_stream_name_validation() {
    let manager = create_test_manager().await;

    // Create maximum length string
    let max_length_stream_name = "a".repeat(255);

    // Valid stream names
    let valid_names = vec![
        "valid-stream",
        "stream123",
        "my_stream",
        "stream-name",
        "stream.name",
        "a",
        "A",
        "1",
        "stream_with_underscores_and-hyphens.and.periods",
        &max_length_stream_name, // Maximum length
    ];

    for name in valid_names {
        let result = manager.create_stream(name, None).await;
        assert!(result.is_ok(), "Stream name '{name}' should be valid");
    }

    // Invalid stream names - empty
    let result = manager.create_stream("", None).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be empty"));

    // Invalid stream names - too long
    let long_name = "a".repeat(256);
    let result = manager.create_stream(&long_name, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must not exceed 255 characters")
    );

    // Invalid stream names - start with non-alphanumeric
    let invalid_starts = vec!["-stream", "_stream", ".stream"];
    for name in invalid_starts {
        let result = manager.create_stream(name, None).await;
        assert!(
            result.is_err(),
            "Stream name '{name}' should be invalid (bad start)"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must start and end with alphanumeric")
        );
    }

    // Invalid stream names - end with non-alphanumeric
    let invalid_ends = vec!["stream-", "stream_", "stream."];
    for name in invalid_ends {
        let result = manager.create_stream(name, None).await;
        assert!(
            result.is_err(),
            "Stream name '{name}' should be invalid (bad end)"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must start and end with alphanumeric")
        );
    }

    // Invalid stream names - invalid middle characters
    let invalid_middle = vec!["stream@name", "stream name", "stream!name"];
    for name in invalid_middle {
        let result = manager.create_stream(name, None).await;
        assert!(
            result.is_err(),
            "Stream name '{name}' should be invalid (bad middle)"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("can only contain alphanumeric")
        );
    }
}

#[tokio::test]
async fn stream_manager_cursor_name_validation() {
    let manager = create_test_manager().await;

    // Create a stream first
    manager.create_stream("test-stream", None).await.unwrap();

    // Create maximum length string
    let max_length_cursor_name = "a".repeat(64);

    // Valid cursor names
    let valid_names = vec![
        "valid-cursor",
        "cursor123",
        "my_cursor",
        "cursor-name",
        "a",
        "A",
        "1",
        "cursor_with_underscores_and-hyphens",
        &max_length_cursor_name, // Maximum length
    ];

    for name in valid_names {
        let result = manager
            .create_cursor("test-stream", name, CursorPosition::Head)
            .await;
        assert!(result.is_ok(), "Cursor name '{name}' should be valid");
    }

    // Invalid cursor names - empty
    let result = manager
        .create_cursor("test-stream", "", CursorPosition::Head)
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be empty"));

    // Invalid cursor names - too long
    let long_name = "a".repeat(65);
    let result = manager
        .create_cursor("test-stream", &long_name, CursorPosition::Head)
        .await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must not exceed 64 characters")
    );

    // Invalid cursor names - start with non-alphanumeric
    let invalid_starts = vec!["-cursor", "_cursor"];
    for name in invalid_starts {
        let result = manager
            .create_cursor("test-stream", name, CursorPosition::Head)
            .await;
        assert!(
            result.is_err(),
            "Cursor name '{name}' should be invalid (bad start)"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must start and end with alphanumeric")
        );
    }

    // Invalid cursor names - end with non-alphanumeric
    let invalid_ends = vec!["cursor-", "cursor_"];
    for name in invalid_ends {
        let result = manager
            .create_cursor("test-stream", name, CursorPosition::Head)
            .await;
        assert!(
            result.is_err(),
            "Cursor name '{name}' should be invalid (bad end)"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must start and end with alphanumeric")
        );
    }

    // Invalid cursor names - invalid middle characters
    let invalid_middle = vec!["cursor@name", "cursor name", "cursor!name", "cursor.name"];
    for name in invalid_middle {
        let result = manager
            .create_cursor("test-stream", name, CursorPosition::Head)
            .await;
        assert!(
            result.is_err(),
            "Cursor name '{name}' should be invalid (bad middle)"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("can only contain alphanumeric")
        );
    }
}

#[tokio::test]
async fn given_private_stream_and_cursor_when_items_are_consumed_then_traces_omit_identifiers_and_payload()
 {
    use storage_observability_test_support::observation::observe;
    let manager = create_test_manager().await;
    let stream = "private-stream-log-marker";
    let cursor = "private-cursor-log-marker";
    let body = "private-stream-body-marker";
    let observed = observe(async {
        manager.create_stream(stream, None).await.unwrap();
        let item = manager.append_item(stream, body.as_bytes()).await.unwrap();
        manager
            .create_cursor(stream, cursor, CursorPosition::Head)
            .await
            .unwrap();
        let page = manager
            .read_from_cursor(stream, cursor, Some(10))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        item
    })
    .await;
    assert!(!observed.spans.is_empty());
    let traces = format!("{:?}{:?}", observed.events, observed.spans);
    let item_id = observed.result.to_string();
    for private in [stream, cursor, body, item_id.as_str()] {
        assert!(
            !traces.contains(private),
            "private stream data entered tracing"
        );
    }
}
