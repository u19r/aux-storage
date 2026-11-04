use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use pubsub_provider::{
    ConfirmSubscriptionRequest, DeliveryRecordId, DeliveryStatus, PublishRequest, PubsubProvider,
    SubscribeRequest, SubscriptionProtocol,
};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use stream_provider::{
    SubscriptionMessage, SubscriptionMessageSender, SubscriptionSendFuture, SubscriptionSendOutcome,
};

use crate::manager::{PubsubDeliveryConfig, PubsubManager};

const DELIVERY_COUNT: usize = 24;
struct GatedSender {
    active: AtomicUsize,
    max_active: AtomicUsize,
    armed: AtomicBool,
    barrier: tokio::sync::Barrier,
}

impl GatedSender {
    fn new(concurrency: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
            barrier: tokio::sync::Barrier::new(concurrency),
        }
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }
}

impl SubscriptionMessageSender for GatedSender {
    fn send_subscription_message<'a>(
        &'a self,
        _message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            if self.armed.load(Ordering::SeqCst) {
                self.barrier.wait().await;
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(SubscriptionSendOutcome::Delivered)
        })
    }
}

async fn run_delivery_batch(concurrency: usize) -> usize {
    let driver = SqliteBackend::initialize(":memory:").await.expect("sqlite");
    let provider = Arc::new(
        SqlStore::initialize_schema(driver)
            .await
            .expect("sqlite schema"),
    );
    PubsubProvider::initialize(provider.as_ref())
        .await
        .expect("pubsub schema");
    let sender = Arc::new(GatedSender::new(concurrency));
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .subscription_message_sender(sender.clone())
        .delivery_config(PubsubDeliveryConfig {
            delivery_concurrency: concurrency,
            ..PubsubDeliveryConfig::default()
        })
        .build()
        .expect("manager builds");
    let topic = manager
        .create_topic("parallel-orders")
        .await
        .expect("topic");
    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .expect("subscription");
    let token = subscription
        .confirmation
        .token()
        .expect("pending confirmation")
        .to_string();
    provider
        .confirm_subscription(ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token,
        })
        .await
        .expect("confirmation");
    // Subscription acceptance queues a confirmation delivery. Settle it before
    // measuring the notification batch so it cannot consume a batch slot.
    assert_eq!(manager.process_due_deliveries(1).await.unwrap(), 1);

    sender.armed.store(true, Ordering::SeqCst);
    let mut record_ids = Vec::with_capacity(DELIVERY_COUNT);
    for index in 0..DELIVERY_COUNT {
        let response = manager
            .publish(PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: format!("message-{index}-{}", "x".repeat(1024)),
                subject: None,
                message_attributes: HashMap::new(),
            })
            .await
            .expect("publish");
        record_ids.push(DeliveryRecordId(format!(
            "{}:{}",
            subscription.subscription_arn, response.message_id
        )));
    }

    let processed = tokio::time::timeout(
        Duration::from_secs(120),
        manager.process_due_deliveries(DELIVERY_COUNT),
    )
    .await
    .expect("delivery-window fixture made no progress")
    .expect("deliveries process");
    assert_eq!(processed, DELIVERY_COUNT);
    for record_id in record_ids {
        let record = provider
            .get_delivery_record(&record_id)
            .await
            .expect("record lookup")
            .expect("record exists");
        assert_eq!(record.status, DeliveryStatus::Delivered);
        assert_eq!(record.attempts, 1);
        assert!(record.lease_owner.is_none());
    }
    assert_eq!(
        manager
            .process_due_deliveries(DELIVERY_COUNT)
            .await
            .unwrap(),
        0
    );

    sender.max_active()
}

#[tokio::test(flavor = "current_thread")]
async fn given_bounded_delivery_window_when_sends_overlap_then_each_claim_settles_once() {
    let serial = run_delivery_batch(1).await;
    let parallel = run_delivery_batch(8).await;

    assert_eq!(serial, 1);
    assert_eq!(parallel, 8);
}
