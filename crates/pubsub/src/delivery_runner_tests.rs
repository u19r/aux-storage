//! W3 runner tests 18, 20, 21 and 25 on one SQLite store with a fake
//! subscription transport (design 9.1).

use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use pubsub_provider::{
    ConfirmSubscriptionRequest, PublishRequest, PubsubProvider, SubscribeRequest, Subscription,
    SubscriptionProtocol, TopicArn,
};
use queue::{CreateQueueRequest, QueueManager, ReceiveMessageRequest};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::MessagingIdentity;
use stream_provider::{
    SubscriptionMessage, SubscriptionMessageSender, SubscriptionSendFuture, SubscriptionSendOutcome,
};
use tokio::sync::Notify;

use crate::{
    delivery_config::DeliveryRunnerConfig, delivery_runner::DeliveryRunner, manager::PubsubManager,
};

type Store = SqlStore<SqliteBackend>;

async fn store() -> Arc<Store> {
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    let store =
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap();
    PubsubProvider::initialize(&store).await.unwrap();
    Arc::new(store)
}

fn config(max_in_flight: usize, per_destination: usize) -> DeliveryRunnerConfig {
    DeliveryRunnerConfig {
        max_in_flight: NonZeroUsize::new(max_in_flight).unwrap(),
        per_destination: NonZeroUsize::new(per_destination).unwrap(),
        claim_batch: NonZeroU32::new(32).unwrap(),
        // Only a wake or a due time can start work quickly.
        idle_probe: Duration::from_secs(3_600),
        error_backoff: Duration::from_millis(50),
        // The sweep finds the confirmations that the store wrote.
        sweep_interval: Duration::from_millis(50),
        ..DeliveryRunnerConfig::default()
    }
}

/// Records the sends of each host. Sends to a held host wait for `release`.
struct HostSender {
    held: Mutex<Vec<String>>,
    release: Notify,
    active: Mutex<HashMap<String, usize>>,
    peak: Mutex<HashMap<String, usize>>,
    sent: Mutex<Vec<String>>,
    started: AtomicUsize,
}

impl HostSender {
    fn new(held: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::new(held.iter().map(|host| (*host).to_owned()).collect()),
            release: Notify::new(),
            active: Mutex::new(HashMap::new()),
            peak: Mutex::new(HashMap::new()),
            sent: Mutex::new(Vec::new()),
            started: AtomicUsize::new(0),
        })
    }

    fn sent_to(&self, host: &str) -> usize {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|sent| sent.as_str() == host)
            .count()
    }

    fn peak(&self, host: &str) -> usize {
        self.peak.lock().unwrap().get(host).copied().unwrap_or(0)
    }

    fn release_all(&self) {
        self.held.lock().unwrap().clear();
        self.release.notify_waiters();
    }
}

impl SubscriptionMessageSender for HostSender {
    fn send_subscription_message<'a>(
        &'a self,
        message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a> {
        Box::pin(async move {
            let host = url::Url::parse(&message.destination.endpoint)
                .ok()
                .and_then(|url| url.host_str().map(str::to_owned))
                .unwrap_or_default();
            self.started.fetch_add(1, Ordering::SeqCst);
            {
                let mut active = self.active.lock().unwrap();
                let count = active.entry(host.clone()).or_default();
                *count += 1;
                let mut peak = self.peak.lock().unwrap();
                let top = peak.entry(host.clone()).or_default();
                *top = (*top).max(*count);
            }
            loop {
                let released = self.release.notified();
                if !self.held.lock().unwrap().contains(&host) {
                    break;
                }
                released.await;
            }
            *self.active.lock().unwrap().get_mut(&host).unwrap() -= 1;
            self.sent.lock().unwrap().push(host);
            Ok(SubscriptionSendOutcome::Delivered)
        })
    }
}

/// A confirmed HTTPS subscription; its confirmation record settles without
/// a request because the subscription is already confirmed.
async fn confirmed_https(store: &Store, topic_arn: &TopicArn, host: &str) {
    let subscription = store
        .create_subscription(
            Subscription::try_new(
                SubscribeRequest {
                    topic_arn: topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: format!("https://{host}/hook"),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
                &MessagingIdentity::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .confirm_subscription(ConfirmSubscriptionRequest {
            topic_arn: topic_arn.clone(),
            token: subscription.confirmation.token().unwrap().to_owned(),
        })
        .await
        .unwrap();
}

fn publish(topic_arn: &TopicArn, index: usize) -> PublishRequest {
    PublishRequest {
        topic_arn: topic_arn.clone(),
        message: format!("message-{index}"),
        subject: None,
        message_attributes: HashMap::new(),
    }
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Test 20: a publish starts delivery on this node at once, although the
/// runner probes only once an hour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_idle_runner_when_published_then_wake_delivers_without_waiting_for_a_probe() {
    let store = store().await;
    let queues = Arc::new(QueueManager::new(store.clone()));
    let queue_url = queues
        .create_queue(CreateQueueRequest {
            queue_name: "woken".to_owned(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let manager = Arc::new(
        PubsubManager::builder()
            .provider(store.clone())
            .queue_wake(queues.wake_handle())
            .build()
            .unwrap(),
    );
    let topic = manager.create_topic("woken").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "arn:aws:sqs:us-east-1:000000000000:woken".to_owned(),
            attributes: HashMap::from([("RawMessageDelivery".to_owned(), "true".to_owned())]),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let runner = DeliveryRunner::start(manager.clone(), config(8, 8)).unwrap();
    let stats = runner.stats();
    eventually("the first sweep", || {
        stats.sweeps.load(Ordering::SeqCst) >= 1
    })
    .await;

    manager.publish(publish(&topic.topic_arn, 1)).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = queues
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.clone(),
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(30),
                    wait_time_seconds: Some(1),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await
                .unwrap();
            if let Some(message) = response.messages.into_iter().next() {
                return message.body;
            }
        }
    })
    .await
    .expect("the wake delivered the message before the next probe");
    assert_eq!(received, "message-1");
    runner.shutdown().await.unwrap();
    assert_eq!(stats.delivered.load(Ordering::SeqCst), 1);
}

/// Tests 18 and 21: destination A blocks, B answers at once. B completes
/// while A is blocked; A never has more than `per_destination` requests;
/// the runner never leases more than `max_in_flight` records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_blocked_destination_when_runner_delivers_then_other_destination_completes() {
    let store = store().await;
    let sender = HostSender::new(&["a.example"]);
    let manager = Arc::new(
        PubsubManager::builder()
            .provider(store.clone())
            .subscription_message_sender(sender.clone())
            .build()
            .unwrap(),
    );
    let topic = manager.create_topic("split").await.unwrap();
    confirmed_https(&store, &topic.topic_arn, "a.example").await;
    confirmed_https(&store, &topic.topic_arn, "b.example").await;
    let runner = DeliveryRunner::start(manager.clone(), config(8, 3)).unwrap();
    let stats = runner.stats();
    for index in 0..6 {
        manager
            .publish(publish(&topic.topic_arn, index))
            .await
            .unwrap();
    }

    eventually("six deliveries to B", || sender.sent_to("b.example") == 6).await;
    assert_eq!(sender.sent_to("a.example"), 0, "A is still blocked");
    assert_eq!(sender.peak("a.example"), 3, "per-destination limit");
    // W3S: A's other three records stay in the store until A has a free
    // slot; nothing is written for them (no waiting list, no deferral).
    assert_eq!(stats.retried.load(Ordering::SeqCst), 0);

    sender.release_all();
    eventually("six deliveries to A", || sender.sent_to("a.example") == 6).await;
    eventually("both confirmations", || {
        stats.delivered.load(Ordering::SeqCst) == 14
    })
    .await;
    runner.shutdown().await.unwrap();
    assert!(stats.peak_in_flight.load(Ordering::SeqCst) <= 8);
    assert_eq!(sender.peak("a.example"), 3);
    assert_eq!(
        stats.delivered.load(Ordering::SeqCst),
        14,
        "12 notifications and 2 confirmations, each settled once"
    );
}
