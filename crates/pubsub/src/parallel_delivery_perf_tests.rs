use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloc_counter::{AllocationGuard, AllocationReport};
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
// Keep external-send latency above native fixture overhead so this measures
// delivery concurrency rather than SQLite setup and settlement noise.
const DELIVERY_DELAY: Duration = Duration::from_millis(50);

struct SlowSender {
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl SlowSender {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        }
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }
}

impl SubscriptionMessageSender for SlowSender {
    fn send_subscription_message<'a>(
        &'a self,
        _message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(DELIVERY_DELAY).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(SubscriptionSendOutcome::Delivered)
        })
    }
}

struct DeliveryRun {
    elapsed: Duration,
    max_active: usize,
    allocations: AllocationReport<'static>,
}

async fn run_delivery_batch(concurrency: usize, label: &'static str) -> DeliveryRun {
    let driver = SqliteBackend::initialize(":memory:").await.expect("sqlite");
    let provider = Arc::new(
        SqlStore::initialize_schema(driver)
            .await
            .expect("sqlite schema"),
    );
    PubsubProvider::initialize(provider.as_ref())
        .await
        .expect("pubsub schema");
    let sender = Arc::new(SlowSender::new());
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

    let guard = AllocationGuard::start(
        module_path!(),
        "parallel_delivery_window",
        file!(),
        line!(),
        Some(label),
    );
    let started = Instant::now();
    let processed = manager
        .process_due_deliveries(DELIVERY_COUNT)
        .await
        .expect("deliveries process");
    let elapsed = started.elapsed();
    let allocations = guard.finish();
    let _ = alloc_counter::emit_report(&allocations);

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

    DeliveryRun {
        elapsed,
        max_active: sender.max_active(),
        allocations,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn delivery_window_reduces_wall_time_without_losing_claim_updates() {
    let serial = run_delivery_batch(1, "serial").await;
    let parallel = run_delivery_batch(8, "parallel").await;

    eprintln!(
        "delivery_window serial_ns={} parallel_ns={} serial_max_active={} parallel_max_active={} \
         serial_allocations={} serial_bytes={} parallel_allocations={} parallel_bytes={}",
        serial.elapsed.as_nanos(),
        parallel.elapsed.as_nanos(),
        serial.max_active,
        parallel.max_active,
        serial.allocations.allocation_count,
        serial.allocations.allocated_bytes,
        parallel.allocations.allocation_count,
        parallel.allocations.allocated_bytes,
    );
    assert_eq!(serial.max_active, 1);
    assert!(parallel.max_active >= 8);
    assert!(parallel.elapsed < serial.elapsed / 2);
}
