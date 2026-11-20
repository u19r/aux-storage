//! Review D4 and W3S (iv) on SQLite: a state row or a payload that does not
//! decode is set aside one hour in the claim with its bytes unchanged, and a
//! node that decodes it later claims it once.

use std::collections::{BTreeMap, HashMap};

use pubsub_provider::{
    AskMode, ClaimTerms, ClaimedDelivery, CreateTopicRequest, DeliveryClaimRequest,
    DeliveryRecordId, DestinationAsk, DestinationKey, PublishRequest, PubsubMessageId, PubsubPort,
    PubsubProvider, SubscribeRequest, Subscription, SubscriptionProtocol, Topic, TopicName,
};
use storage_types::{AttemptMode, AttemptPlan, Clock, DeadlineClass, MessagingIdentity};

use crate::{SqlAttempt, SqlLayout, SqlStatement, SqlValue};

#[tokio::test]
async fn given_undecodable_state_and_payload_when_claimed_then_due_moves_and_later_delivers() {
    let backend = crate::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store = crate::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
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
    // Finding 2: an intent whose `due_at` column disagrees with its blob is
    // decoded (the column is the only due source), not set aside.
    let message = ids[0].rsplit(':').next().unwrap().to_owned();
    execute(
        &store,
        "UPDATE sys_pubsub_publish_intents SET due_at = due_at - 1000 WHERE message_id = {1}",
        &message,
        None,
    )
    .await;
    assert_eq!(
        store.materialize_publish_intents(32).await.unwrap().records,
        2
    );
    // (iv): the state of the first record does not decode (its failure
    // column is unknown to this node); the payload of the second does not.
    let (first, _) = store
        .get_delivery(&DeliveryRecordId(ids[0].clone()))
        .await
        .unwrap()
        .unwrap();
    let (second, _) = store
        .get_delivery(&DeliveryRecordId(ids[1].clone()))
        .await
        .unwrap()
        .unwrap();
    // W3S review finding 2: a head whose key text does not parse still
    // names its destination in the sweep.
    execute(
        &store,
        "UPDATE {state} SET destination_key = 'bad' || destination_key WHERE record_id = {1}",
        &ids[0],
        None,
    )
    .await;
    assert_eq!(
        sweep_destinations(&store).await,
        vec![first.destination.clone()]
    );
    execute(
        &store,
        "UPDATE {state} SET destination_key = substr(destination_key, 4) WHERE record_id = {1}",
        &ids[0],
        None,
    )
    .await;
    let bad_payload = vec![0_u8];
    let good_payload = execute(
        &store,
        "SELECT row_blob FROM {payload} WHERE record_id = {1}",
        &ids[1],
        None,
    )
    .await;
    execute(
        &store,
        "UPDATE {state} SET last_failure = 'future_code' WHERE record_id = {1}",
        &ids[0],
        None,
    )
    .await;
    execute(
        &store,
        "UPDATE {payload} SET row_blob = {2} WHERE record_id = {1}",
        &ids[1],
        Some(bad_payload),
    )
    .await;

    let now = Clock::now(&store).max(first.due_at);
    assert!(
        claim(&store, &first.destination, now).await.is_empty(),
        "both are set aside"
    );
    for id in &ids {
        let due = execute(
            &store,
            "SELECT due_at FROM {state} WHERE record_id = {1}",
            id,
            None,
        )
        .await;
        let Some(SqlValue::Integer(due)) = due else {
            panic!("due")
        };
        assert_eq!(
            due,
            now.timestamp_millis() + 3_600_000,
            "set aside one hour"
        );
    }
    // Later, both decode: each is claimed once after T + 1 h, attempts 0.
    execute(
        &store,
        "UPDATE {state} SET last_failure = NULL WHERE record_id = {1}",
        &ids[0],
        None,
    )
    .await;
    let Some(SqlValue::Blob(good)) = good_payload else {
        panic!("payload")
    };
    execute(
        &store,
        "UPDATE {payload} SET row_blob = {2} WHERE record_id = {1}",
        &ids[1],
        Some(good),
    )
    .await;
    assert!(
        claim(&store, &first.destination, now + 3_599_999)
            .await
            .is_empty()
    );
    let later = claim(&store, &second.destination, now + 3_600_000).await;
    assert_eq!(later.len(), 2);
    assert!(later.iter().all(|claimed| claimed.state.attempts == 0));

    // W3S review finding 6: a missing payload is not moved forever; after
    // the retry deadline (23 days for a queue) the state row is deleted.
    execute(
        &store,
        "DELETE FROM {payload} WHERE record_id = {1}",
        &ids[0],
        None,
    )
    .await;
    let past_deadline = now + 24 * 86_400_000;
    let last = claim(&store, &first.destination, past_deadline).await;
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].state.id.0, ids[1]);
    assert!(
        store
            .get_delivery(&DeliveryRecordId(ids[0].clone()))
            .await
            .unwrap()
            .is_none(),
        "the undeliverable row is dropped"
    );
}

async fn sweep_destinations(
    store: &crate::SqlStore<crate::sqlite::SqliteBackend>,
) -> Vec<DestinationKey> {
    store
        .sweep_destination_heads(
            pubsub_provider::SweepCursor(vec![pubsub_provider::SweepLane::After(None)]),
            std::num::NonZeroU32::new(16).unwrap(),
        )
        .await
        .unwrap()
        .heads
        .into_iter()
        .map(|head| head.destination)
        .collect()
}

async fn claim(
    store: &crate::SqlStore<crate::sqlite::SqliteBackend>,
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

/// One raw statement: `{state}`, `{payload}`, `{1}` (the id) and `{2}` (a
/// blob) are replaced; a query returns its first value.
async fn execute(
    store: &crate::SqlStore<crate::sqlite::SqliteBackend>,
    template: &'static str,
    id: &str,
    blob: Option<Vec<u8>>,
) -> Option<SqlValue> {
    let id = id.to_owned();
    PubsubPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let sql = template
                    .replace(
                        "{state}",
                        &attempt.engine.relation_name("sys_pubsub_delivery_state"),
                    )
                    .replace(
                        "{payload}",
                        &attempt.engine.relation_name("sys_pubsub_delivery_payload"),
                    )
                    .replace("{1}", &attempt.engine.engine().placeholder(1))
                    .replace("{2}", &attempt.engine.engine().placeholder(2));
                let mut binds = vec![SqlValue::Text(id)];
                binds.extend(blob.map(SqlValue::Blob));
                if !sql.starts_with("SELECT") {
                    let changed = attempt
                        .attempt
                        .execute(SqlStatement::new(sql, binds))
                        .await?;
                    assert_eq!(changed, 1);
                    return Ok(None);
                }
                let page = attempt
                    .attempt
                    .query(
                        SqlStatement::new(sql, binds),
                        storage_types::ReadProtection::Snapshot,
                        crate::constants::PUBSUB_CATALOG_READ_BYTES,
                    )
                    .await?;
                Ok(page
                    .rows
                    .into_iter()
                    .next()
                    .and_then(|row| row.into_values().into_iter().next()))
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

    let backend = crate::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store = crate::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
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
