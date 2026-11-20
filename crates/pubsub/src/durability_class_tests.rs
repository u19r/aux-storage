//! W3 test 24 at the attempt boundary: the durability class of each pubsub
//! attempt (design section 6). Publish is `Durable`; the chunk claim, the
//! chunk commit, the delivery claim and the local delivery are `Relaxed`.
//! The SQLite and RocksDB drivers skip the barrier wait of a `Relaxed`
//! attempt (`storage-common` durability tests); this test proves that each
//! pubsub attempt asks for the class.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use pubsub_provider::{
    AskMode, ClaimTerms, DeliveryClaimRequest, DestinationAsk, DestinationKey, LocalDelivery,
    LocalDeliveryItem, PublishRequest, PubsubMessageId, PubsubPort, PubsubProvider,
    RenderedQueueMessage, SettleOutcome, SubscribeRequest, Subscription, SubscriptionProtocol,
    SweepCursor,
};
use queue_provider::{QueueArn, QueueOperations};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::{
    AttemptDurability, AttemptMode, AttemptRequest, Clock, MessagingIdentity, StorageResult,
    TimestampMillis,
};

type Store = SqlStore<SqliteBackend>;

/// Records the mode and class of each pubsub attempt.
struct Recording {
    store: Arc<Store>,
    classes: Mutex<Vec<(AttemptMode, AttemptDurability)>>,
}

impl Recording {
    fn take(&self) -> Vec<AttemptDurability> {
        std::mem::take(&mut *self.classes.lock().unwrap())
            .into_iter()
            .map(|(_, durability)| durability)
            .collect()
    }
}

impl Clock for Recording {
    fn now(&self) -> TimestampMillis {
        Clock::now(self.store.as_ref())
    }
}

impl storage_types::AttemptDeadlineSource for Recording {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        storage_types::AttemptDeadlines::default()
    }
}

impl PubsubPort for Recording {
    type Attempt<'native>
        = <Store as PubsubPort>::Attempt<'native>
    where Self: 'native;

    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        step: F,
    ) -> impl std::future::Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.classes
            .lock()
            .unwrap()
            .push((request.mode, request.durability));
        PubsubPort::with_attempt(self.store.as_ref(), request, step)
    }
}

#[tokio::test]
async fn given_sns_path_when_each_attempt_runs_then_only_publish_is_durable() {
    use AttemptDurability::{Durable, Relaxed};
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    let store = Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap(),
    );
    let port = Recording {
        store: store.clone(),
        classes: Mutex::new(Vec::new()),
    };
    QueueOperations::create_queue(
        store.as_ref(),
        queue_provider::Queue {
            queue_url: "http://localhost/000000000000/classes".to_owned(),
            queue_name: "classes".to_owned(),
            attributes: HashMap::new(),
            created_at: TimestampMillis::now(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let identity = MessagingIdentity::default();
    let topic = port
        .create_topic(
            pubsub_provider::Topic::try_new(
                pubsub_provider::CreateTopicRequest {
                    name: pubsub_provider::TopicName::new("classes").unwrap(),
                    attributes: HashMap::new(),
                    tags: std::collections::BTreeMap::new(),
                },
                &identity,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let endpoint = "arn:aws:sqs:us-east-1:000000000000:classes";
    port.create_subscription(
        Subscription::try_new(
            SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: endpoint.to_owned(),
                attributes: HashMap::new(),
                extra_json: serde_json::Value::Null,
            },
            &identity,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(
        port.take().iter().all(|class| *class == Durable),
        "catalog changes"
    );

    port.accept_publish(
        PublishRequest {
            topic_arn: topic.topic_arn,
            message: "x".to_owned(),
            subject: None,
            message_attributes: HashMap::new(),
        },
        PubsubMessageId::new(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(port.take(), [Durable], "publish");
    assert_eq!(
        port.materialize_publish_intents(10).await.unwrap().records,
        1
    );
    assert_eq!(
        port.take(),
        [Relaxed, Relaxed],
        "chunk claim and chunk commit"
    );
    let now = Clock::now(&port);
    let claimed = port
        .claim_deliveries(DeliveryClaimRequest {
            terms: ClaimTerms {
                owner: "classes".to_owned(),
                now,
                lease_until: now + 30_000,
            },
            asks: vec![DestinationAsk {
                destination: DestinationKey::of_endpoint(SubscriptionProtocol::Queue, endpoint),
                take: std::num::NonZeroU32::MIN,
                mode: AskMode::Due,
            }],
        })
        .await
        .unwrap()
        .remove(0)
        .claimed
        .remove(0);
    assert_eq!(port.take(), [Relaxed], "delivery claim");
    let report = port
        .deliver_to_local_queue(LocalDelivery {
            queue: QueueArn::parse(endpoint, &identity).unwrap(),
            items: vec![LocalDeliveryItem {
                claimed: claimed.state,
                fence: claimed.fence,
                message_id: claimed.payload.message_id,
                message: RenderedQueueMessage {
                    body: "x".to_owned(),
                    attributes: None,
                },
            }],
            jitter: 0,
            identity: storage_types::MessagingIdentity::default(),
        })
        .await
        .unwrap()
        .settled()
        .unwrap()
        .remove(0);
    assert_eq!(report.outcome, SettleOutcome::Delivered);
    assert_eq!(port.take(), [Relaxed], "local delivery");
    let page = port
        .sweep_destination_heads(SweepCursor::from_start(None), std::num::NonZeroU32::MIN)
        .await
        .unwrap();
    assert!(page.heads.is_empty());
    assert_eq!(port.take(), [Relaxed], "the sweep is a hint");
}

/// W3 test 23 (F11): an idle runner probes with read-only attempts and opens
/// no write attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_idle_runner_when_ten_probe_intervals_pass_then_no_write_attempt_opens() {
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    let store = Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap(),
    );
    let port = Arc::new(Recording {
        store,
        classes: Mutex::new(Vec::new()),
    });
    let manager = Arc::new(
        crate::PubsubManager::builder()
            .provider(port.clone())
            .build()
            .unwrap(),
    );
    let config = crate::DeliveryRunnerConfig {
        idle_probe: std::time::Duration::from_millis(20),
        sweep_interval: std::time::Duration::from_millis(20),
        ..crate::DeliveryRunnerConfig::default()
    };
    let runner = crate::DeliveryRunner::start(manager, config).unwrap();
    let stats = runner.stats();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    runner.shutdown().await.unwrap();
    assert!(
        stats.sweeps.load(std::sync::atomic::Ordering::SeqCst) >= 10,
        "ten sweep intervals"
    );
    let modes: Vec<AttemptMode> = std::mem::take(&mut *port.classes.lock().unwrap())
        .into_iter()
        .map(|(mode, _)| mode)
        .collect();
    assert!(!modes.is_empty());
    assert!(
        modes.iter().all(|mode| *mode == AttemptMode::ReadOnly),
        "only read-only probes: {modes:?}"
    );
    assert_eq!(stats.claims.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// W3 test 26 (store part): ten entries with one empty message give nine
/// successful members, one failed member, and one Durable attempt.
#[tokio::test]
async fn given_batch_with_one_invalid_entry_when_published_then_one_durable_attempt_writes_nine() {
    use AttemptDurability::Durable;
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    let store = Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap(),
    );
    let port = Arc::new(Recording {
        store,
        classes: Mutex::new(Vec::new()),
    });
    let manager = crate::PubsubManager::builder()
        .provider(port.clone())
        .build()
        .unwrap();
    let topic = manager.create_topic("batch").await.unwrap();
    port.take();
    let entries = (0..10)
        .map(|index| pubsub_provider::PublishBatchEntry {
            id: format!("e{index}"),
            request: PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: if index == 4 {
                    String::new()
                } else {
                    format!("m{index}")
                },
                subject: None,
                message_attributes: HashMap::new(),
            },
            message_structure: None,
        })
        .collect();
    let results = manager
        .publish_batch(topic.topic_arn.clone(), entries)
        .await
        .unwrap();
    assert_eq!(port.take(), [Durable], "one durability barrier");
    let failed: Vec<_> = results
        .iter()
        .filter_map(|result| match result {
            pubsub_provider::PublishBatchResult::Failed { id, error } => {
                Some((id.clone(), error.aws_query_message()))
            }
            pubsub_provider::PublishBatchResult::Successful { .. } => None,
        })
        .collect();
    assert_eq!(
        failed,
        [(
            "e4".to_owned(),
            "Invalid parameter: Empty message".to_owned()
        )]
    );
    assert_eq!(results.len() - failed.len(), 9);
    assert!(
        port.next_publish_due().await.unwrap().is_some(),
        "nine intents wait for materialization"
    );
    let missing =
        pubsub_provider::TopicArn::new("arn:aws:sns:us-east-1:000000000000:none").unwrap();
    assert!(matches!(
        manager.publish_batch(missing, Vec::new()).await,
        Err(pubsub_provider::PubsubError::ResourceNotFound { .. })
    ));
}
