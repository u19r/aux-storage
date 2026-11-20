//! Review D4 and W3S (iv) on RocksDB: an undecodable state value is set aside
//! one hour with its bytes unchanged, the valid record behind it is claimed
//! in the same claim, and a node that decodes the value later claims it.

use std::collections::{BTreeMap, HashMap};

use pubsub_provider::{
    AskMode, ClaimTerms, ClaimedDelivery, CreateTopicRequest, DeliveryClaimRequest,
    DeliveryLocator, DeliveryRecordId, DestinationAsk, DestinationKey, PublishChunkClaimLimit,
    PublishChunkClaimRequest, PublishRequest, PubsubMessageId, PubsubPort, PubsubProvider,
    SubscribeRequest, Subscription, SubscriptionProtocol, Topic, TopicName,
};
use storage_types::{AttemptMode, AttemptPlan, Clock, DeadlineClass, MessagingIdentity};

use crate::{KvAttempt, KvMutation, KvStore, rocksdb::RocksDbBackend};

async fn poison_store() -> (
    tempfile::TempDir,
    KvStore<RocksDbBackend>,
    Topic,
    Subscription,
) {
    let root = tempfile::tempdir().unwrap();
    let backend = RocksDbBackend::initialize(
        root.path().join("store"),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    PubsubProvider::initialize(&store).await.unwrap();
    let identity = MessagingIdentity::default();
    let topic = store
        .create_topic(
            Topic::try_new(
                CreateTopicRequest {
                    name: TopicName::new("poison").unwrap(),
                    attributes: HashMap::new(),
                    tags: BTreeMap::new(),
                },
                &identity,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let subscription = store
        .create_subscription(
            Subscription::try_new(
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: "arn:aws:sqs:us-east-1:000000000000:poison".to_owned(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
                &identity,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    (root, store, topic, subscription)
}

#[tokio::test]
async fn given_undecodable_state_when_claimed_then_key_moves_and_later_decodes() {
    let (_root, store, topic, subscription) = poison_store().await;
    let mut ids = Vec::new();
    for message in ["first", "second"] {
        let id = PubsubMessageId::new();
        store
            .accept_publish(
                PublishRequest {
                    topic_arn: topic.topic_arn.clone(),
                    message: message.to_owned(),
                    subject: None,
                    message_attributes: HashMap::new(),
                },
                id.clone(),
                false,
            )
            .await
            .unwrap();
        ids.push(format!("{}:{id}", subscription.subscription_arn));
    }
    assert_eq!(
        store.materialize_publish_intents(32).await.unwrap().records,
        2
    );
    let poison = DeliveryRecordId(ids[0].clone());
    let (state, _) = store.get_delivery(&poison).await.unwrap().unwrap();
    let old_key = crate::pubsub_schedule::state_key(&state.locator());
    let good = state.encode_value().unwrap();
    put(&store, old_key.clone(), vec![0]).await;
    // W3S review finding 2: the undecodable head still names its
    // destination in the sweep (from the record's payload).
    let swept = store
        .sweep_destination_heads(
            pubsub_provider::SweepCursor(vec![pubsub_provider::SweepLane::After(None)]),
            std::num::NonZeroU32::new(16).unwrap(),
        )
        .await
        .unwrap()
        .heads;
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].destination, state.destination);

    let now = Clock::now(&store).max(state.due_at);
    let claimed = claim(&store, &state.destination, now).await;
    assert_eq!(
        claimed.len(),
        1,
        "an undecodable state does not stop the claim"
    );
    assert_eq!(claimed[0].state.id.0, ids[1], "the valid record is claimed");
    let moved = DeliveryLocator {
        due_at: now + 3_600_000,
        ..state.locator()
    };
    let new_key = crate::pubsub_schedule::state_key(&moved);
    assert_eq!(get(&store, old_key).await, None, "the old key is gone");
    assert_eq!(
        get(&store, new_key.clone()).await,
        Some(vec![0]),
        "same bytes, T + 1 h"
    );

    // A node that decodes the value claims it after T + 1 h, once.
    put(&store, new_key, good).await;
    // The valid record's lease ended long ago; only the moved one waits.
    let early = claim(&store, &state.destination, now + 3_599_999).await;
    assert!(early.iter().all(|claimed| claimed.state.id != poison));
    let later = claim(&store, &state.destination, now + 3_600_000).await;
    assert_eq!(later.len(), 1);
    assert_eq!(later[0].state.id, poison);
    assert_eq!(later[0].state.attempts, 0);

    // W3S review finding 6: with its payload missing the row is deleted
    // once its retry deadline (23 days for a queue) has passed.
    delete(&store, crate::pubsub_schedule::payload_key(&poison)).await;
    let past_deadline = now + 24 * 86_400_000;
    let last = claim(&store, &state.destination, past_deadline).await;
    assert!(last.iter().all(|claimed| claimed.state.id != poison));
    assert!(store.get_delivery(&poison).await.unwrap().is_none());
}

async fn delete(store: &KvStore<RocksDbBackend>, key: Vec<u8>) {
    PubsubPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move { attempt.attempt.stage(vec![KvMutation::Delete { key }]) })
        },
    )
    .await
    .unwrap();
}

/// W3S review finding 1: an intent moved as poison that decodes later is
/// claimed from its moved due key and materialized, not deleted as an orphan.
#[tokio::test]
async fn given_undecodable_intent_when_moved_and_decodable_later_then_it_is_materialized() {
    let (_root, store, topic, _subscription) = poison_store().await;
    let id = PubsubMessageId::new();
    store
        .accept_publish(
            PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: "late".to_owned(),
                subject: None,
                message_attributes: HashMap::new(),
            },
            id.clone(),
            false,
        )
        .await
        .unwrap();
    let key = crate::pubsub_publication::intent_key(&id);
    let good = get(&store, key.clone()).await.unwrap();
    put(&store, key.clone(), vec![0]).await;

    let now = Clock::now(&store);
    let materialize = |at| {
        store.materialize_publish_chunks(PublishChunkClaimRequest {
            limit: PublishChunkClaimLimit::try_from(32_usize).unwrap(),
            now: at,
        })
    };
    assert_eq!(materialize(now).await.unwrap().records, 0, "set aside");
    let moved = now + 3_600_000;
    assert_eq!(
        PubsubProvider::next_publish_due(&store).await.unwrap(),
        Some(moved)
    );

    put(&store, key, good).await;
    assert_eq!(materialize(moved - 1).await.unwrap().records, 0);
    assert_eq!(
        materialize(moved).await.unwrap().records,
        1,
        "the moved intent decodes and is materialized"
    );
    assert_eq!(
        PubsubProvider::next_publish_due(&store).await.unwrap(),
        None,
        "no due key is left"
    );
}

async fn claim(
    store: &KvStore<RocksDbBackend>,
    destination: &DestinationKey,
    now: storage_types::TimestampMillis,
) -> Vec<ClaimedDelivery> {
    store
        .claim_deliveries(DeliveryClaimRequest {
            terms: ClaimTerms {
                owner: "poison-test".to_owned(),
                now,
                lease_until: now + 30_000,
            },
            asks: vec![DestinationAsk {
                destination: destination.clone(),
                take: std::num::NonZeroU32::new(2).unwrap(),
                mode: AskMode::Due,
            }],
        })
        .await
        .unwrap()
        .remove(0)
        .claimed
}

async fn put(store: &KvStore<RocksDbBackend>, key: Vec<u8>, value: Vec<u8>) {
    PubsubPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move { attempt.attempt.stage(vec![KvMutation::Put { key, value }]) })
        },
    )
    .await
    .unwrap();
}

async fn get(store: &KvStore<RocksDbBackend>, key: Vec<u8>) -> Option<Vec<u8>> {
    PubsubPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                attempt
                    .attempt
                    .get(&key, storage_types::ReadProtection::Snapshot)
                    .await
            })
        },
    )
    .await
    .unwrap()
}

/// Records the work of each acquisition; its permits do nothing.
#[derive(Default)]
struct RecordingAdmission {
    works: std::sync::Mutex<Vec<&'static str>>,
}

struct NoPermit;

impl storage_common::native_admission::NativeAdmissionPermit for NoPermit {
    fn complete(
        self: Box<Self>,
        _outcome: storage_common::native_admission::NativeAdmissionOutcome<'_>,
        _elapsed: std::time::Duration,
    ) {
    }
}

impl storage_common::native_admission::NativeAdmission for RecordingAdmission {
    fn connection_id(&self) -> &str {
        "test"
    }

    fn require_reserved_attempts(
        &self,
        _required: std::num::NonZeroUsize,
    ) -> storage_types::StorageResult<()> {
        Ok(())
    }

    fn acquire(
        &self,
        work: storage_types::NativeWork,
    ) -> storage_common::native_admission::NativeAdmissionFuture<'_> {
        use storage_types::NativeWork;
        self.works.lock().unwrap().push(match work {
            NativeWork::Recovery => "recovery",
            NativeWork::PointRead => "point_read",
            NativeWork::RangeRead => "range_read",
            NativeWork::Write => "write",
            NativeWork::Background => "background",
        });
        Box::pin(async {
            Ok(Box::new(NoPermit)
                as Box<
                    dyn storage_common::native_admission::NativeAdmissionPermit,
                >)
        })
    }

    fn drain_native(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = storage_types::StorageResult<()>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

fn lane_binding(
    admission: &std::sync::Arc<RecordingAdmission>,
) -> std::sync::Arc<storage_common::native_admission::NativeAdmissionBinding> {
    std::sync::Arc::new(
        storage_common::native_admission::NativeAdmissionBinding::bound(admission.clone()),
    )
}

#[tokio::test]
async fn given_admitted_store_when_pubsub_operations_run_then_each_attempt_takes_the_lane_work() {
    use storage_common::native_admission::{AdmittedStore, AttemptAdmission};

    let root = tempfile::tempdir().unwrap();
    let backend = RocksDbBackend::initialize(
        root.path().join("store"),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    PubsubProvider::initialize(&store).await.unwrap();
    let topic = Topic::try_new(
        CreateTopicRequest {
            name: TopicName::new("lanes").unwrap(),
            attributes: HashMap::new(),
            tags: BTreeMap::new(),
        },
        &MessagingIdentity::default(),
    )
    .unwrap();
    let background_admission = std::sync::Arc::new(RecordingAdmission::default());
    let foreground_admission = std::sync::Arc::new(RecordingAdmission::default());
    let background = store.admitted(AttemptAdmission::Background(lane_binding(
        &background_admission,
    )));
    let foreground = store.admitted(AttemptAdmission::Foreground(lane_binding(
        &foreground_admission,
    )));

    let created = background.create_topic(topic.clone()).await.unwrap();
    let read = foreground.get_topic(&created.topic_arn).await.unwrap();
    foreground
        .list_topics(pubsub_provider::ListTopicsRequest { next_token: None })
        .await
        .unwrap();
    foreground.create_topic(topic).await.unwrap();

    assert_eq!(*background_admission.works.lock().unwrap(), ["background"]);
    assert_eq!(
        *foreground_admission.works.lock().unwrap(),
        ["point_read", "range_read", "write"]
    );
    assert_eq!(read, Some(created));
}
