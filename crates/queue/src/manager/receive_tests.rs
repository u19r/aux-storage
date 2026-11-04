use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use queue_provider::{CreateQueueRequest, ReceiveMessageRequest, SendMessageRequest};

use super::QueueManager;
use crate::queue_test_support_tests::{create_selected_test_service, unique_suffix};

async fn receive_fixture() -> (QueueManager, String) {
    let manager = create_selected_test_service("receive-wait").await;
    let queue = manager
        .create_queue(CreateQueueRequest {
            queue_name: unique_suffix("receive-wait"),
            attributes: None,
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

#[tokio::test]
async fn given_empty_receive_when_message_is_published_then_long_poll_returns_that_message() {
    let (mut manager, queue_url) = receive_fixture().await;
    let empty = Arc::new(tokio::sync::Notify::new());
    manager.empty_receive_signal = Some(Arc::clone(&empty));
    let manager = Arc::new(manager);
    let receiving = tokio::spawn({
        let manager = Arc::clone(&manager);
        let queue_url = queue_url.clone();
        async move {
            manager
                .receive_message(ReceiveMessageRequest {
                    queue_url,
                    max_number_of_messages: Some(1),
                    visibility_timeout: None,
                    wait_time_seconds: Some(20),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await
                .unwrap()
        }
    });
    // This is emitted only after the native provider returned an empty page.
    tokio::time::timeout(Duration::from_secs(120), empty.notified())
        .await
        .expect("receiver never observed an empty queue");
    manager
        .send_message(SendMessageRequest {
            queue_url,
            message_body: "arrived while waiting".into(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(120), receiving)
        .await
        .expect("long-poll fixture timed out")
        .unwrap();
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.messages[0].body, "arrived while waiting");
}
