//! Part a of ARCH-2-i: the attempt count and request facts of each
//! `PubsubProvider` method, recorded at the port boundary.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use pubsub_provider::{
    ConfirmSubscriptionRequest, CreateTopicRequest, GetSubscriptionAttributesRequest,
    GetTopicAttributesRequest, ListSubscriptionsRequest, ListTopicsRequest, PublishRequest,
    PubsubError, PubsubMessageId, PubsubPort, PubsubProvider, PubsubValidationKind,
    SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionArn,
    SubscriptionAttributes, SubscriptionProtocol, Topic, TopicName,
};
use queue_provider::QueueOperations;
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::{
    AttemptDurability, AttemptMode, AttemptRequest, Clock, DeadlineClass, MessagingIdentity,
    NativeWork, StorageResult, TimestampMillis,
};

type Store = SqlStore<SqliteBackend>;
type Facts = (AttemptMode, DeadlineClass, NativeWork, AttemptDurability);

struct Recording {
    store: Arc<Store>,
    requests: Mutex<Vec<Facts>>,
}

impl Recording {
    fn take(&self) -> Vec<Facts> {
        std::mem::take(&mut *self.requests.lock().unwrap())
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
        self.requests.lock().unwrap().push((
            request.mode,
            request.class(),
            request.work,
            request.durability,
        ));
        PubsubPort::with_attempt(self.store.as_ref(), request, step)
    }
}

// The request constants of `pubsub-provider/src/constants.rs`, as recorded.
const CATALOG_WRITE: Facts = (
    AttemptMode::ReadWrite,
    DeadlineClass::SchemaChange,
    NativeWork::Write,
    AttemptDurability::Durable,
);
const CATALOG_POINT_READ: Facts = (
    AttemptMode::ReadOnly,
    DeadlineClass::PointRead,
    NativeWork::PointRead,
    AttemptDurability::Durable,
);
const TOPIC_ATTRIBUTES_READ: Facts = (
    AttemptMode::ReadOnly,
    DeadlineClass::PointRead,
    NativeWork::RangeRead,
    AttemptDurability::Durable,
);
const CATALOG_RANGE_READ: Facts = (
    AttemptMode::ReadOnly,
    DeadlineClass::RangeRead,
    NativeWork::RangeRead,
    AttemptDurability::Durable,
);
const PUBLISH_WRITE: Facts = (
    AttemptMode::ReadWrite,
    DeadlineClass::Write,
    NativeWork::Write,
    AttemptDurability::Durable,
);
const CHUNK_ITEMS: Facts = (
    AttemptMode::ReadWriteItems,
    DeadlineClass::BackgroundPage,
    NativeWork::Write,
    AttemptDurability::Relaxed,
);
const PUBLISH_DUE_PROBE: Facts = (
    AttemptMode::ReadOnly,
    DeadlineClass::LongPollProbe,
    NativeWork::PointRead,
    AttemptDurability::Relaxed,
);

#[tokio::test]
async fn given_each_provider_method_when_called_then_attempts_and_request_classes_are_unchanged() {
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
        requests: Mutex::new(Vec::new()),
    };
    QueueOperations::create_queue(
        store.as_ref(),
        queue_provider::Queue {
            queue_url: "http://localhost/000000000000/attempts".to_owned(),
            queue_name: "attempts".to_owned(),
            attributes: HashMap::new(),
            created_at: TimestampMillis::now(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let identity = MessagingIdentity::default();
    let endpoint = "arn:aws:sqs:us-east-1:000000000000:attempts";

    // 1
    port.initialize().await.unwrap();
    assert_eq!(port.take(), vec![], "initialize");

    // 2
    let topic = port
        .create_topic(
            Topic::try_new(
                CreateTopicRequest {
                    name: TopicName::new("attempts").unwrap(),
                    attributes: HashMap::new(),
                    tags: BTreeMap::new(),
                },
                &identity,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(port.take(), vec![CATALOG_WRITE], "create_topic");

    // 3
    assert!(port.get_topic(&topic.topic_arn).await.unwrap().is_some());
    assert_eq!(port.take(), vec![CATALOG_POINT_READ], "get_topic");

    // 4
    let updated = port
        .set_topic_attributes(SetTopicAttributesRequest {
            topic_arn: topic.topic_arn.clone(),
            attributes: HashMap::from([("DisplayName".into(), "shown".into())]),
        })
        .await
        .unwrap();
    assert_eq!(updated.display_name, Some("shown".into()));
    assert_eq!(port.take(), vec![CATALOG_WRITE], "set_topic_attributes");

    // 5
    port.get_topic_attributes(GetTopicAttributesRequest {
        topic_arn: topic.topic_arn.clone(),
    })
    .await
    .unwrap();
    assert_eq!(
        port.take(),
        vec![TOPIC_ATTRIBUTES_READ],
        "get_topic_attributes"
    );

    // 6
    let listed = port
        .list_topics(ListTopicsRequest { next_token: None })
        .await
        .unwrap();
    assert_eq!(listed.topics.len(), 1);
    assert_eq!(port.take(), vec![CATALOG_RANGE_READ], "list_topics");

    // 7
    let invalid = port
        .list_topics(ListTopicsRequest {
            next_token: Some("!".into()),
        })
        .await;
    assert!(
        matches!(
            invalid,
            Err(PubsubError::Validation {
                kind: PubsubValidationKind::InvalidNextToken,
                ..
            })
        ),
        "{invalid:?}"
    );
    assert_eq!(port.take(), vec![], "list_topics invalid token");

    // 8
    port.change_topic_tags(
        &topic.topic_arn,
        BTreeMap::from([("k".into(), "v".into())]),
        Vec::new(),
    )
    .await
    .unwrap();
    assert_eq!(port.take(), vec![CATALOG_WRITE], "change_topic_tags");

    // 9
    let subscription = port
        .create_subscription(
            Subscription::try_new(
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: endpoint.into(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
                &identity,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(port.take(), vec![CATALOG_WRITE], "create_subscription");

    // 10
    assert!(
        port.get_subscription(&subscription.subscription_arn)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(port.take(), vec![CATALOG_POINT_READ], "get_subscription");

    // 11
    port.get_subscription_attributes(GetSubscriptionAttributesRequest {
        subscription_arn: subscription.subscription_arn.clone(),
    })
    .await
    .unwrap();
    assert_eq!(
        port.take(),
        vec![CATALOG_POINT_READ],
        "get_subscription_attributes"
    );

    // 12
    let missing = port
        .get_subscription_attributes(GetSubscriptionAttributesRequest {
            subscription_arn: SubscriptionArn::new(format!("{}:missing", topic.topic_arn)).unwrap(),
        })
        .await;
    assert!(
        matches!(
            missing,
            Err(PubsubError::ResourceNotFound {
                resource_type: "subscription",
                ..
            })
        ),
        "{missing:?}"
    );
    assert_eq!(
        port.take(),
        vec![CATALOG_POINT_READ],
        "get_subscription_attributes missing"
    );

    // 13
    let attributes = port
        .set_subscription_attributes(
            &subscription.subscription_arn,
            SubscriptionAttributes::parse(
                &HashMap::from([("RawMessageDelivery".into(), "true".into())]),
                &identity,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(attributes.raw_message_delivery);
    assert_eq!(
        port.take(),
        vec![CATALOG_WRITE],
        "set_subscription_attributes"
    );

    // 14
    let listed = port
        .list_subscriptions(ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn.clone()),
            next_token: None,
        })
        .await
        .unwrap();
    assert_eq!(listed.subscriptions.len(), 1);
    assert_eq!(port.take(), vec![CATALOG_RANGE_READ], "list_subscriptions");

    // 15
    let confirm = port
        .confirm_subscription(ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token: "bad-token".into(),
        })
        .await;
    assert!(
        matches!(
            confirm,
            Err(PubsubError::Validation {
                kind: PubsubValidationKind::InvalidToken,
                ..
            })
        ),
        "{confirm:?}"
    );
    assert_eq!(port.take(), vec![CATALOG_WRITE], "confirm_subscription");

    // 16
    let publish = |message: &str| PublishRequest {
        topic_arn: topic.topic_arn.clone(),
        message: message.into(),
        subject: None,
        message_attributes: HashMap::new(),
    };
    let empty = port
        .accept_publish(publish(""), PubsubMessageId::new(), false)
        .await;
    assert!(
        matches!(
            empty,
            Err(PubsubError::Validation {
                kind: PubsubValidationKind::EmptyMessage,
                ..
            })
        ),
        "{empty:?}"
    );
    assert_eq!(port.take(), vec![], "accept_publish invalid");

    // 17
    port.accept_publish(publish("x"), PubsubMessageId::new(), false)
        .await
        .unwrap();
    assert_eq!(port.take(), vec![PUBLISH_WRITE], "accept_publish");

    // 18
    assert!(port.next_publish_due().await.unwrap().is_some());
    assert_eq!(port.take(), vec![PUBLISH_DUE_PROBE], "next_publish_due");

    // 19
    let report = port.materialize_publish_intents(10).await.unwrap();
    assert_eq!(report.records, 1);
    assert_eq!(report.chunks, 1);
    assert_eq!(
        port.take(),
        vec![CHUNK_ITEMS, CHUNK_ITEMS],
        "materialize_publish_intents"
    );

    // 20
    port.delete_subscription(&subscription.subscription_arn)
        .await
        .unwrap();
    assert_eq!(port.take(), vec![CATALOG_WRITE], "delete_subscription");

    // 21
    port.delete_topic(&topic.topic_arn).await.unwrap();
    assert_eq!(port.take(), vec![CATALOG_WRITE], "delete_topic");
}
