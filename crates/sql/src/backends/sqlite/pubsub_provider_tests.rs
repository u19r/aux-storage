use std::collections::HashMap;

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, GetTopicAttributesRequest, PubsubMessageId,
    PubsubProvider, SetSubscriptionAttributesRequest, SubscribeRequest, SubscriptionArn,
    SubscriptionProtocol, TopicName,
};
use storage_types::TimestampMillis;

use super::SQLiteStorageProvider;

#[tokio::test]
async fn sqlite_pubsub_provider_persists_topic_subscription_and_attributes() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    PubsubProvider::initialize(&provider).await.unwrap();

    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new("orders").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "http://localhost/000000000000/orders".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::json!({"token_ref":"secret"}),
        })
        .await
        .unwrap();

    provider
        .set_subscription_attributes(SetSubscriptionAttributesRequest {
            subscription_arn: subscription.subscription_arn.clone(),
            attributes: HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
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
    let subscriptions = provider
        .list_subscriptions(pubsub_provider::ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions;

    assert_eq!(
        attributes.get("SubscriptionsConfirmed"),
        Some(&"1".to_string())
    );
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(
        subscriptions[0].subscription_arn,
        subscription.subscription_arn
    );
    assert!(subscriptions[0].raw_message_delivery);
    assert_eq!(subscriptions[0].extra_json["token_ref"], "secret");
}

#[tokio::test]
async fn sqlite_pubsub_provider_claims_due_delivery_records_once() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    PubsubProvider::initialize(&provider).await.unwrap();
    let now = TimestampMillis::from_timestamp(1_000);
    provider
        .put_delivery_record(delivery_record(
            "due",
            DeliveryStatus::Pending,
            Some(now),
            None,
        ))
        .await
        .unwrap();
    provider
        .put_delivery_record(delivery_record(
            "future",
            DeliveryStatus::Pending,
            Some(TimestampMillis::from_timestamp(3_000)),
            None,
        ))
        .await
        .unwrap();

    let first_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now,
            lease_expires_at: TimestampMillis::from_timestamp(2_000),
            limit: 10,
        })
        .await
        .unwrap()
        .records;
    let second_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now,
            lease_expires_at: TimestampMillis::from_timestamp(2_000),
            limit: 10,
        })
        .await
        .unwrap()
        .records;

    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].id.0, "due");
    assert_eq!(first_claim[0].lease_owner.as_deref(), Some("worker-a"));
    assert!(second_claim.is_empty());
}

#[tokio::test]
async fn sqlite_pubsub_provider_recovers_expired_delivery_leases() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    PubsubProvider::initialize(&provider).await.unwrap();
    provider
        .put_delivery_record(delivery_record(
            "expired",
            DeliveryStatus::RetryScheduled,
            Some(TimestampMillis::from_timestamp(1_000)),
            Some(TimestampMillis::from_timestamp(1_500)),
        ))
        .await
        .unwrap();

    let records = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now: TimestampMillis::from_timestamp(2_000),
            lease_expires_at: TimestampMillis::from_timestamp(3_000),
            limit: 10,
        })
        .await
        .unwrap()
        .records;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].lease_owner.as_deref(), Some("worker-b"));
}

fn delivery_record(
    id: &str,
    status: DeliveryStatus,
    next_attempt_at: Option<TimestampMillis>,
    lease_expires_at: Option<TimestampMillis>,
) -> DeliveryRecord {
    let now = TimestampMillis::from_timestamp(1_000);
    DeliveryRecord {
        id: DeliveryRecordId(id.to_string()),
        kind: DeliveryRecordKind::Notification,
        message_id: PubsubMessageId::new(),
        subscription_arn: SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:orders:sub")
            .unwrap(),
        message_body: None,
        subject: None,
        message_attributes: Default::default(),
        target: DeliveryTarget::BuiltIn,
        status,
        attempts: 1,
        next_attempt_at,
        lease_owner: lease_expires_at.map(|_| "worker-a".to_string()),
        lease_expires_at,
        last_error: None,
        created_at: now,
        updated_at: now,
    }
}
