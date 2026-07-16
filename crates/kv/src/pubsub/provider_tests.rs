use std::collections::{HashMap, HashSet};

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, GetTopicAttributesRequest, PubsubMessageId,
    PubsubProvider, PublishRequest, SetSubscriptionAttributesRequest, SubscribeRequest, SubscriptionArn,
    SubscriptionProtocol, TopicName,
};
use storage_types::TimestampMillis;
use uuid::Uuid;

use crate::{RocksDbKvStore, SortedKvDbStorageProvider, kv_support_tests::rocksdb_test_path};

async fn create_test_provider() -> SortedKvDbStorageProvider<RocksDbKvStore> {
    let store = RocksDbKvStore::new(rocksdb_test_path("pubsub-kv")).expect("open rocksdb");
    SortedKvDbStorageProvider::new(store)
}

#[tokio::test]
async fn sorted_kv_publish_intent_uses_immutable_chunked_subscription_snapshot() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("snapshot-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let mut subscriptions = Vec::new();
    for index in 0..12 {
        subscriptions.push(
            provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: format!("queue-{index}"),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
                .await
                .unwrap(),
        );
    }
    let message_id = PubsubMessageId::new_from_string(format!("message-{}", Uuid::now_v7()))
        .unwrap();
    provider
        .accept_publish(
            PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: "snapshot-body".to_string(),
                subject: None,
                message_attributes: HashMap::new(),
            },
            message_id.clone(),
            false,
        )
        .await
        .unwrap();

    provider
        .set_subscription_attributes(SetSubscriptionAttributesRequest {
            subscription_arn: subscriptions[0].subscription_arn.clone(),
            attributes: HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
        })
        .await
        .unwrap();
    provider.delete_topic(&topic.topic_arn).await.unwrap();

    let (first, competing) = tokio::join!(
        provider.materialize_publish_intents(10),
        provider.materialize_publish_intents(10),
    );
    assert_eq!(first.unwrap() + competing.unwrap(), 10);
    assert_eq!(provider.materialize_publish_intents(10).await.unwrap(), 2);
    assert_eq!(provider.materialize_publish_intents(10).await.unwrap(), 0);

    for (index, subscription) in subscriptions.iter().enumerate() {
        let record = provider
            .get_delivery_record(&DeliveryRecordId(format!(
                "{}:{}",
                subscription.subscription_arn, message_id
            )))
            .await
            .unwrap()
            .expect("accepted subscription delivery must be materialized");
        let snapshot = record
            .subscription
            .clone()
            .expect("delivery owns subscription snapshot");
        assert_eq!(snapshot.subscription_arn, subscription.subscription_arn);
        assert_eq!(snapshot.endpoint, format!("queue-{index}"));
        assert!(!snapshot.raw_message_delivery);
        if index == 1 {
            let mut delivered = record;
            delivered.status = DeliveryStatus::Delivered;
            provider.update_delivery_record(delivered).await.unwrap();
        }
    }
}

#[tokio::test]
async fn sorted_kv_pubsub_provider_persists_topic_subscription_and_attributes() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();

    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("orders-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::from([("DisplayName".to_string(), "Orders".to_string())]),
        })
        .await
        .unwrap();
    let repeated = provider
        .create_topic(CreateTopicRequest {
            name: topic.name.clone(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(repeated.topic_arn, topic.topic_arn);

    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.test/orders".to_string(),
            attributes: HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
            extra_json: serde_json::json!({"secret_ref":"vault://orders"}),
        })
        .await
        .unwrap();
    provider
        .set_subscription_attributes(SetSubscriptionAttributesRequest {
            subscription_arn: subscription.subscription_arn.clone(),
            attributes: HashMap::from([("RawMessageDelivery".to_string(), "false".to_string())]),
        })
        .await
        .unwrap();

    let attributes = provider
        .get_topic_attributes(GetTopicAttributesRequest {
            topic_arn: topic.topic_arn.clone(),
        })
        .await
        .unwrap()
        .attributes;
    assert_eq!(attributes.get("DisplayName"), Some(&"Orders".to_string()));
    assert_eq!(
        attributes.get("SubscriptionsPending"),
        Some(&"1".to_string())
    );

    let subscriptions = provider
        .list_subscriptions(pubsub_provider::ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn.clone()),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions;
    assert_eq!(subscriptions.len(), 1);
    assert!(!subscriptions[0].raw_message_delivery);
    assert!(subscriptions[0].confirmation.pending_confirmation());
    assert_eq!(
        subscriptions[0].extra_json,
        serde_json::json!({"secret_ref":"vault://orders"})
    );
}

#[tokio::test]
async fn sorted_kv_pubsub_provider_claims_due_delivery_records_once() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();
    let now = TimestampMillis::from(1_000);
    let record = delivery_record("record-1", now, Some(now));

    provider.put_delivery_record(record.clone()).await.unwrap();
    let first_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now,
            lease_expires_at: TimestampMillis::from(2_000),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(first_claim.records.len(), 1);
    assert_eq!(
        first_claim.records[0].lease_owner.as_deref(),
        Some("worker-a")
    );

    let second_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now: TimestampMillis::from(1_100),
            lease_expires_at: TimestampMillis::from(2_100),
            limit: 10,
        })
        .await
        .unwrap();
    assert!(second_claim.records.is_empty());
}

#[tokio::test]
async fn sorted_kv_pubsub_provider_competing_workers_never_double_claim() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();
    let now = TimestampMillis::from(1_000);
    provider
        .put_delivery_records(
            (0..10)
                .map(|index| delivery_record(&format!("competing-{index}"), now, Some(now)))
                .collect(),
        )
        .await
        .unwrap();

    let (first, second) = tokio::join!(
        provider.claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now,
            lease_expires_at: TimestampMillis::from(2_000),
            limit: 10,
        }),
        provider.claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now,
            lease_expires_at: TimestampMillis::from(2_000),
            limit: 10,
        }),
    );
    let claimed = first
        .unwrap()
        .records
        .into_iter()
        .chain(second.unwrap().records)
        .collect::<Vec<_>>();
    let unique_ids = claimed
        .iter()
        .map(|record| record.id.0.clone())
        .collect::<HashSet<_>>();

    assert_eq!(claimed.len(), 10);
    assert_eq!(unique_ids.len(), claimed.len());
}

#[tokio::test]
async fn sorted_kv_pubsub_provider_recovers_expired_delivery_leases() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();
    let mut record = delivery_record("record-2", TimestampMillis::from(1_000), None);
    record.lease_owner = Some("worker-a".to_string());
    record.lease_expires_at = Some(TimestampMillis::from(1_500));
    provider.put_delivery_record(record).await.unwrap();

    let claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now: TimestampMillis::from(1_600),
            lease_expires_at: TimestampMillis::from(2_600),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(claim.records.len(), 1);
    assert_eq!(claim.records[0].lease_owner.as_deref(), Some("worker-b"));
}

#[tokio::test]
async fn sorted_kv_pubsub_provider_retains_accepted_deliveries_after_topic_delete() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();

    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("delete-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.test/delete".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let mut record = delivery_record("record-3", TimestampMillis::from(1_000), None);
    record.subscription_arn = subscription.subscription_arn.clone();
    provider.put_delivery_record(record.clone()).await.unwrap();

    provider.delete_topic(&topic.topic_arn).await.unwrap();

    assert!(
        provider
            .get_subscription(&subscription.subscription_arn)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        provider
            .get_delivery_record(&record.id)
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
async fn sorted_kv_pubsub_provider_topic_subscription_scans_do_not_overlap_by_prefix() {
    let provider = create_test_provider().await;
    PubsubProvider::initialize(&provider).await.unwrap();

    let short_topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("prefix-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let long_topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("{}-long", short_topic.name)).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();

    provider
        .create_subscription(SubscribeRequest {
            topic_arn: short_topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.test/short".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    provider
        .create_subscription(SubscribeRequest {
            topic_arn: long_topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.test/long".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    let short_subscriptions = provider
        .list_subscriptions(pubsub_provider::ListSubscriptionsRequest {
            topic_arn: Some(short_topic.topic_arn),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions;
    assert_eq!(short_subscriptions.len(), 1);
    assert_eq!(
        short_subscriptions[0].endpoint,
        "https://example.test/short"
    );
}

fn delivery_record(
    id: &str,
    created_at: TimestampMillis,
    next_attempt_at: Option<TimestampMillis>,
) -> DeliveryRecord {
    DeliveryRecord {
        id: DeliveryRecordId(id.to_string()),
        kind: DeliveryRecordKind::Notification,
        message_id: PubsubMessageId::new(),
        subscription_arn: SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:orders:sub")
            .unwrap(),
        subscription: None,
        message_body: None,
        subject: None,
        message_attributes: Default::default(),
        target: DeliveryTarget::BuiltIn,
        status: DeliveryStatus::Pending,
        attempts: 0,
        next_attempt_at,
        lease_owner: None,
        lease_expires_at: None,
        last_error: None,
        created_at,
        updated_at: created_at,
    }
}
