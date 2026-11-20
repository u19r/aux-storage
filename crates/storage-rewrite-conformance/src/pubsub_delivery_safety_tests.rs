//! W3 test 1 (finding R5): a queue store that rejects the SNS delivery for a
//! while does not drop the notification. Runs through `PubsubManager` on the
//! SQL provider that `STORAGE_TEST_BACKEND` selects; the probe rejects every
//! insert into the queue message table while it is armed. Before W3 the
//! record became `Failed` after five attempts and the store saw only five
//! sends: the test failed at "the store rejected seven sends".

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use pubsub_provider::{PublishRequest, SubscribeRequest, SubscriptionProtocol};

use super::test_support::TestStore;

/// Rejects queue message inserts with a capacity rejection while armed.
#[derive(Default)]
struct QueueInsertRejection {
    armed: AtomicBool,
    rejected: AtomicUsize,
}

impl sql_backend::SqlFaultProbe for QueueInsertRejection {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        let is_queue_insert = matches!(
            call.operation,
            sql_backend::SqlFaultOperation::Execute | sql_backend::SqlFaultOperation::Query
        ) && call
            .sql
            .strip_prefix("WITH changed AS (")
            .unwrap_or(call.sql)
            .starts_with("INSERT INTO")
            && call.sql.contains("sys_queue_messages");
        if !is_queue_insert || !self.armed.load(Ordering::SeqCst) {
            return None;
        }
        self.rejected.fetch_add(1, Ordering::SeqCst);
        Some(storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::CAPACITY).into())
    }
}

#[tokio::test]
async fn given_queue_store_rejects_sends_when_it_recovers_then_the_notification_arrives_once() {
    let probe = Arc::new(QueueInsertRejection::default());
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let queue_manager = Arc::new(queue::QueueManager::new(store.queue_operations.clone()));
    let manager = pubsub::PubsubManager::builder()
        .provider(store.pubsub_provider.clone())
        .queue_wake(queue_manager.wake_handle())
        .build()
        .unwrap();
    let name = format!("r5_{}", uuid::Uuid::new_v4().simple());
    let queue_url = queue_manager
        .create_queue(queue_provider::CreateQueueRequest {
            queue_name: name.clone(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let topic = manager.create_topic(name.clone()).await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: super::pubsub_contract_tests::queue_arn(&name),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    probe.armed.store(true, Ordering::SeqCst);
    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn.clone(),
            message: "kept".to_owned(),
            subject: None,
            message_attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let mut passes = 0;
    while probe.rejected.load(Ordering::SeqCst) < 7 && passes < 40 {
        let _ = manager.process_due_deliveries(10).await;
        store.advance_time(Duration::from_secs(30));
        tokio::time::sleep(Duration::from_millis(20)).await;
        passes += 1;
    }
    assert!(
        probe.rejected.load(Ordering::SeqCst) >= 7,
        "the store rejected seven sends"
    );
    probe.armed.store(false, Ordering::SeqCst);
    for _ in 0..3 {
        let _ = manager.process_due_deliveries(10).await;
        store.advance_time(Duration::from_secs(30));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let received = queue_manager
        .receive_message(queue_provider::ReceiveMessageRequest {
            queue_url,
            max_number_of_messages: Some(10),
            visibility_timeout: Some(300),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap()
        .messages;
    assert_eq!(received.len(), 1, "the notification arrives exactly once");
    store.finish().await;
}

/// A dead-letter insert that the store refuses for its size settles the
/// record as `DeadLetterRejected` (a counted drop); it does not fail the
/// settle, which would attempt the endpoint again at every lease.
#[tokio::test]
async fn given_dead_letter_insert_rejected_for_capacity_when_settled_then_the_record_is_dropped() {
    use pubsub_provider::{DeliveryAttemptResult, DeliveryFailureCode, DropReason, SettleOutcome};

    use super::{
        pubsub_contract_tests::{create_topic, queue_arn},
        pubsub_delivery_contract_tests::{
            claim, https_subscription, materialize, publish, queue, rendered, settle, unique,
        },
    };

    let probe = Arc::new(QueueInsertRejection::default());
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let dlq = unique("capacity-dlq");
    queue(&store, &dlq, &[]).await;
    let topic = create_topic(&store, &unique("capacity"), HashMap::new())
        .await
        .unwrap();
    let policy = format!("{{\"deadLetterTargetArn\":\"{}\"}}", queue_arn(&dlq));
    https_subscription(
        &store,
        &topic.topic_arn,
        &[("RedrivePolicy", policy.as_str())],
    )
    .await;
    publish(&store, &topic.topic_arn, "x", HashMap::new()).await;
    materialize(&store).await;
    probe.armed.store(true, Ordering::SeqCst);
    let claimed = claim(&store, "worker").await;
    assert_eq!(claimed.len(), 1);
    let permanent = DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400));
    let report = settle(&store, &claimed[0], permanent, rendered("x")).await;
    assert_eq!(
        report.outcome,
        SettleOutcome::Dropped(DropReason::DeadLetterRejected)
    );
    assert!(
        probe.rejected.load(Ordering::SeqCst) >= 1,
        "the insert was refused"
    );
    store.finish().await;
}
