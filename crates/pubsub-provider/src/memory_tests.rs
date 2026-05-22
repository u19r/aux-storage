use std::collections::HashMap;

use storage_types::TimestampMillis;

use crate::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, GetSubscriptionAttributesRequest,
    GetTopicAttributesRequest, InMemoryPubsubProvider, ListSubscriptionsRequest, PubsubMessageId,
    PubsubProvider, SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscriptionArn,
    SubscriptionProtocol, TopicName,
};

#[tokio::test]
async fn create_topic_is_idempotent_for_same_name() {
    let provider = InMemoryPubsubProvider::default();
    let name = TopicName::new("orders").unwrap();

    let first = provider
        .create_topic(CreateTopicRequest {
            name: name.clone(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let second = provider
        .create_topic(CreateTopicRequest {
            name,
            attributes: HashMap::new(),
        })
        .await
        .unwrap();

    assert_eq!(first.topic_arn, second.topic_arn);
}

#[tokio::test]
async fn subscription_persists_extra_json_for_custom_senders() {
    let provider = InMemoryPubsubProvider::default();
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new("orders").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();

    provider
        .create_subscription(crate::SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::json!({"token_ref":"secret/oauth/orders"}),
        })
        .await
        .unwrap();

    let subscriptions = provider
        .list_subscriptions(ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions;

    assert_eq!(subscriptions.len(), 1);
    assert_eq!(
        subscriptions[0].extra_json["token_ref"],
        "secret/oauth/orders"
    );

    let attributes = provider
        .get_topic_attributes(GetTopicAttributesRequest {
            topic_arn: subscriptions[0].topic_arn.clone(),
        })
        .await
        .unwrap()
        .attributes;

    assert_eq!(
        attributes.get("SubscriptionsPending"),
        Some(&"1".to_string())
    );
}

#[tokio::test]
async fn set_subscription_attributes_preserves_subscription_identity() {
    let provider = InMemoryPubsubProvider::default();
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new("orders").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let subscription = provider
        .create_subscription(crate::SubscribeRequest {
            topic_arn: topic.topic_arn,
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    let updated = provider
        .set_subscription_attributes(SetSubscriptionAttributesRequest {
            subscription_arn: subscription.subscription_arn.clone(),
            attributes: HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
        })
        .await
        .unwrap();

    assert_eq!(updated.subscription_arn, subscription.subscription_arn);
    assert!(updated.raw_message_delivery);

    let attributes = provider
        .get_subscription_attributes(GetSubscriptionAttributesRequest {
            subscription_arn: subscription.subscription_arn,
        })
        .await
        .unwrap()
        .attributes;

    assert_eq!(
        attributes.get("RawMessageDelivery"),
        Some(&"true".to_string())
    );
}

#[tokio::test]
async fn set_topic_attributes_updates_display_name() {
    let provider = InMemoryPubsubProvider::default();
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new("orders").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();

    provider
        .set_topic_attributes(SetTopicAttributesRequest {
            topic_arn: topic.topic_arn.clone(),
            attributes: HashMap::from([("DisplayName".to_string(), "Orders".to_string())]),
        })
        .await
        .unwrap();

    let attributes = provider
        .get_topic_attributes(GetTopicAttributesRequest {
            topic_arn: topic.topic_arn,
        })
        .await
        .unwrap()
        .attributes;

    assert_eq!(attributes.get("DisplayName"), Some(&"Orders".to_string()));
}

#[tokio::test]
async fn get_topic_attributes_includes_empty_display_name_by_default() {
    let provider = InMemoryPubsubProvider::default();
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new("orders").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();

    let attributes = provider
        .get_topic_attributes(GetTopicAttributesRequest {
            topic_arn: topic.topic_arn,
        })
        .await
        .unwrap()
        .attributes;

    assert_eq!(attributes.get("DisplayName"), Some(&String::new()));
}

#[tokio::test]
async fn claim_delivery_records_leases_due_records_only_once() {
    let provider = InMemoryPubsubProvider::default();
    let now = TimestampMillis::from_timestamp(1_000);
    let lease_expires_at = TimestampMillis::from_timestamp(2_000);
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
    provider
        .put_delivery_record(delivery_record(
            "delivered",
            DeliveryStatus::Delivered,
            None,
            None,
        ))
        .await
        .unwrap();

    let first_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now,
            lease_expires_at,
            limit: 10,
        })
        .await
        .unwrap()
        .records;
    let second_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now,
            lease_expires_at,
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
async fn claim_delivery_records_recovers_expired_leases() {
    let provider = InMemoryPubsubProvider::default();
    let now = TimestampMillis::from_timestamp(2_000);
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
            now,
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
