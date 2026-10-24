//! Queue expiry through the replacement store with one fixture-owned clock.

use std::{collections::HashMap, time::Duration};

use queue_provider::{Queue, SendMessageRequest, queue_message_for_send};
use storage_observability_test_support::observation::observe;
use storage_types::DurationSeconds;

use super::test_support::TestStore;

#[tokio::test]
async fn given_a_message_past_retention_when_receive_runs_then_stale_message_is_not_delivered() {
    let store = TestStore::new().await;
    let name = format!("clock_{}", uuid::Uuid::new_v4().simple());
    let url = format!("http://localhost/queue/{name}");
    let queue = Queue {
        queue_name: name,
        queue_url: url.clone(),
        attributes: HashMap::from([("MessageRetentionPeriod".to_owned(), "60".to_owned())]),
        created_at: store.now(),
    };
    store
        .queue_operations
        .create_queue(queue)
        .await
        .expect("create queue");
    let request = SendMessageRequest {
        queue_url: url.clone(),
        message_body: "expires".to_owned(),
        delay_seconds: None,
        message_attributes: None,
    };
    let sent = observe(
        store
            .queue_operations
            .send_message(queue_message_for_send(request, store.now())),
    )
    .await;
    sent.assert_no_warning_or_error();
    sent.result.expect("send message");
    let before = store
        .queue_operations
        .get_queue_with_message_counts(&url)
        .await
        .expect("count before expiry")
        .expect("queue exists")
        .1;
    assert_eq!(before.visible, 1);

    store.advance_time(Duration::from_secs(61));
    let received = observe(store.queue_operations.receive_messages(
        &url,
        1,
        DurationSeconds::from(30),
        DurationSeconds::from(0),
    ))
    .await;
    received.assert_no_warning_or_error();
    assert!(received.result.expect("receive after expiry").is_empty());
    let after = store
        .queue_operations
        .get_queue_with_message_counts(&url)
        .await
        .expect("count after expiry")
        .expect("queue exists")
        .1;
    assert_eq!(after.visible, 0);
    store.finish().await;
}

#[tokio::test]
async fn given_deleted_message_when_new_message_is_sent_then_only_new_generation_is_delivered() {
    let store = TestStore::new().await;
    let name = format!("generation_{}", uuid::Uuid::new_v4().simple());
    let url = format!("http://localhost/queue/{name}");
    store
        .queue_operations
        .create_queue(Queue {
            queue_name: name,
            queue_url: url.clone(),
            attributes: HashMap::new(),
            created_at: store.now(),
        })
        .await
        .expect("create queue");
    for body in ["generation-a", "generation-b"] {
        store
            .queue_operations
            .send_message(queue_message_for_send(
                SendMessageRequest {
                    queue_url: url.clone(),
                    message_body: body.to_owned(),
                    delay_seconds: None,
                    message_attributes: None,
                },
                store.now(),
            ))
            .await
            .expect("send generation");
        let received = store
            .queue_operations
            .receive_messages(
                &url,
                10,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive current generation");
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].body, body);
        if body == "generation-a" {
            store
                .queue_operations
                .delete_message(
                    &url,
                    queue_provider::ReceiptHandle(received[0].receipt_handle.clone()),
                )
                .await
                .expect("delete first generation");
            store.advance_time(Duration::from_secs(31));
            assert!(
                store
                    .queue_operations
                    .receive_messages(
                        &url,
                        10,
                        DurationSeconds::from(30),
                        DurationSeconds::from(0),
                    )
                    .await
                    .expect("deleted generation stays absent after visibility expiry")
                    .is_empty()
            );
        }
    }
    store.finish().await;
}
