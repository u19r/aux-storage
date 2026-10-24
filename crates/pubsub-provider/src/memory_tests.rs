use std::collections::HashMap;

use storage_types::TimestampMillis;

use crate::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryRecordUpdate, DeliveryStatus, DeliveryTarget,
    GetSubscriptionAttributesRequest, GetTopicAttributesRequest, InMemoryPubsubProvider,
    ListSubscriptionsRequest, PublishAcceptanceInput, PublishRequest, PubsubError, PubsubMessageId,
    PubsubProvider, PubsubPublicationRepository, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscriptionArn, SubscriptionProtocol, SubscriptionSnapshot,
    SubscriptionSnapshotGeneration, TopicName,
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
async fn snapshot_generation_cannot_name_different_subscription_chunks() {
    let provider = InMemoryPubsubProvider::default();
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new("snapshot-generation").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let first = provider
        .create_subscription(crate::SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "first-queue".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let second = provider
        .create_subscription(crate::SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "second-queue".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let generation = SubscriptionSnapshotGeneration::new();
    let first_message = PubsubMessageId::new_from_string("first-message").unwrap();
    let second_message = PubsubMessageId::new_from_string("second-message").unwrap();

    provider
        .persist_publish(
            SubscriptionSnapshot::with_generation(
                topic.topic_arn.clone(),
                vec![first.clone()],
                1,
                generation,
            )
            .unwrap(),
            publish_acceptance(topic.topic_arn.clone(), first_message.clone(), generation),
        )
        .await
        .unwrap();
    assert!(
        provider
            .persist_publish(
                SubscriptionSnapshot::with_generation(
                    topic.topic_arn.clone(),
                    vec![second.clone()],
                    1,
                    generation,
                )
                .unwrap(),
                publish_acceptance(topic.topic_arn, second_message, generation),
            )
            .await
            .is_err()
    );
    assert_eq!(provider.materialize_publish_intents(10).await.unwrap(), 1);
    assert!(
        provider
            .get_delivery_record(&DeliveryRecordId(format!(
                "{}:{first_message}",
                first.subscription_arn
            )))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        provider
            .get_delivery_record(&DeliveryRecordId(format!(
                "{}:{first_message}",
                second.subscription_arn
            )))
            .await
            .unwrap()
            .is_none()
    );
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
            limit: crate::DeliveryClaimLimit::try_from(10usize).unwrap(),
        })
        .await
        .unwrap()
        .records;
    let second_claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now,
            lease_expires_at,
            limit: crate::DeliveryClaimLimit::try_from(10usize).unwrap(),
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
            limit: crate::DeliveryClaimLimit::try_from(10usize).unwrap(),
        })
        .await
        .unwrap()
        .records;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].lease_owner.as_deref(), Some("worker-b"));
}

#[tokio::test]
async fn update_delivery_record_rejects_stale_claim_without_resurrection() {
    let provider = InMemoryPubsubProvider::default();
    let mut record = delivery_record(
        "stale-claim",
        DeliveryStatus::Pending,
        Some(TimestampMillis::from_timestamp(1_000)),
        None,
    );
    record.message_body = Some("immutable payload".to_string());
    provider.put_delivery_record(record.clone()).await.unwrap();

    let first = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now: TimestampMillis::from_timestamp(1_000),
            lease_expires_at: TimestampMillis::from_timestamp(2_000),
            limit: crate::DeliveryClaimLimit::try_from(1usize).unwrap(),
        })
        .await
        .unwrap()
        .records
        .into_iter()
        .next()
        .expect("first delivery claim");
    let second = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now: TimestampMillis::from_timestamp(2_000),
            lease_expires_at: TimestampMillis::from_timestamp(3_000),
            limit: crate::DeliveryClaimLimit::try_from(1usize).unwrap(),
        })
        .await
        .unwrap()
        .records
        .into_iter()
        .next()
        .expect("second delivery claim");

    assert_eq!(first.claim_generation, 1);
    assert_eq!(second.claim_generation, 2);
    let mut repeated_creation = record;
    repeated_creation.status = DeliveryStatus::Failed;
    provider
        .put_delivery_record(repeated_creation)
        .await
        .unwrap();
    assert!(matches!(
        provider
            .update_delivery_record(delivery_update(
                &first,
                DeliveryStatus::RetryScheduled,
                TimestampMillis::from_timestamp(2_001),
            ))
            .await,
        Err(PubsubError::DeliveryClaimLost { ref delivery_id }) if delivery_id == "stale-claim"
    ));

    let current = provider
        .get_delivery_record(&DeliveryRecordId("stale-claim".to_string()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, DeliveryStatus::Pending);
    assert_eq!(current.claim_generation, 2);
    assert_eq!(current.lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(current.message_body.as_deref(), Some("immutable payload"));
}

#[tokio::test]
async fn update_delivery_record_rejects_same_worker_aba_claim() {
    let provider = InMemoryPubsubProvider::default();
    provider
        .put_delivery_record(delivery_record(
            "same-worker-aba",
            DeliveryStatus::Pending,
            Some(TimestampMillis::from_timestamp(1_000)),
            None,
        ))
        .await
        .unwrap();

    let first = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now: TimestampMillis::from_timestamp(1_000),
            lease_expires_at: TimestampMillis::from_timestamp(2_000),
            limit: crate::DeliveryClaimLimit::try_from(1usize).unwrap(),
        })
        .await
        .unwrap()
        .records
        .into_iter()
        .next()
        .expect("first delivery claim");
    let current = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now: TimestampMillis::from_timestamp(2_000),
            lease_expires_at: TimestampMillis::from_timestamp(3_000),
            limit: crate::DeliveryClaimLimit::try_from(1usize).unwrap(),
        })
        .await
        .unwrap()
        .records
        .into_iter()
        .next()
        .expect("current delivery claim");

    assert_ne!(first.claim_generation, current.claim_generation);
    assert!(matches!(
        provider
            .update_delivery_record(delivery_update(
                &first,
                DeliveryStatus::RetryScheduled,
                TimestampMillis::from_timestamp(2_001),
            ))
            .await,
        Err(PubsubError::DeliveryClaimLost { .. })
    ));
    provider
        .update_delivery_record(delivery_update(
            &current,
            DeliveryStatus::Delivered,
            TimestampMillis::from_timestamp(2_002),
        ))
        .await
        .unwrap();

    let final_record = provider
        .get_delivery_record(&DeliveryRecordId("same-worker-aba".to_string()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_record.status, DeliveryStatus::Delivered);
    assert_eq!(final_record.claim_generation, current.claim_generation);
}

fn publish_acceptance(
    topic_arn: crate::TopicArn,
    message_id: PubsubMessageId,
    snapshot_generation: SubscriptionSnapshotGeneration,
) -> PublishAcceptanceInput {
    PublishAcceptanceInput {
        request: PublishRequest {
            topic_arn,
            message: "body".to_string(),
            subject: None,
            message_attributes: HashMap::new(),
        },
        message_id,
        accepted_at: TimestampMillis::from_timestamp(1_000),
        custom_sender_enabled: false,
        snapshot_generation,
    }
}

fn delivery_update(
    record: &DeliveryRecord,
    status: DeliveryStatus,
    updated_at: TimestampMillis,
) -> DeliveryRecordUpdate {
    DeliveryRecordUpdate {
        record_id: record.id.clone(),
        expected_claim_generation: record.claim_generation,
        expected_lease_owner: record.lease_owner.clone().expect("claimed delivery owner"),
        status,
        attempts: record.attempts.saturating_add(1),
        next_attempt_at: None,
        last_error: None,
        updated_at,
    }
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
        subscription: None,
        message_body: None,
        subject: None,
        message_attributes: Default::default(),
        target: DeliveryTarget::BuiltIn,
        status,
        attempts: 1,
        claim_generation: 0,
        next_attempt_at,
        lease_owner: lease_expires_at.map(|_| "worker-a".to_string()),
        lease_expires_at,
        last_error: None,
        created_at: now,
        updated_at: now,
    }
}
