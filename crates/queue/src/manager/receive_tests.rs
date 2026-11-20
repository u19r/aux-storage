use std::time::{Duration, Instant};

use queue_provider::{CreateQueueRequest, ReceiveMessageRequest};

use super::QueueManager;
use crate::queue_test_support_tests::{create_selected_test_service, unique_suffix};

async fn receive_fixture() -> (QueueManager, String) {
    let manager = create_selected_test_service("receive-wait").await;
    let queue = manager
        .create_queue(CreateQueueRequest {
            queue_name: unique_suffix("receive-wait"),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    (manager, queue.queue_url)
}

#[tokio::test]
async fn given_empty_queue_when_long_poll_expires_then_empty_response_does_not_return_early() {
    let (manager, queue_url) = receive_fixture().await;
    let started = Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(120),
        manager.receive_message(ReceiveMessageRequest {
            queue_url,
            max_number_of_messages: Some(1),
            visibility_timeout: None,
            wait_time_seconds: Some(1),
            attribute_names: None,
            message_attribute_names: None,
        }),
    )
    .await
    .expect("long-poll fixture timed out")
    .unwrap();
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(response.messages.is_empty());
}
