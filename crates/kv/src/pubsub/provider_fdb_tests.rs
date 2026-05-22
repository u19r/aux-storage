use std::collections::HashMap;

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, PubsubMessageId, PubsubProvider,
    SubscribeRequest, SubscriptionProtocol, TopicName,
};
use storage_types::TimestampMillis;
use uuid::Uuid;

use crate::{SortedKvDbStorageProvider, backends::fdb::fdb_support_tests::connect_fdb_store};

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_pubsub_provider_persists_and_claims_delivery_records() {
    let Some(store) = connect_fdb_store("fdb-pubsub").await else {
        eprintln!("Skipping FoundationDB pubsub test: unable to connect to local cluster");
        return;
    };
    let provider = SortedKvDbStorageProvider::new(store);
    PubsubProvider::initialize(&provider).await.unwrap();

    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("fdb-pubsub-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn,
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.test/fdb".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::json!({"token_ref":"vault://fdb"}),
        })
        .await
        .unwrap();
    let record = DeliveryRecord {
        id: DeliveryRecordId(format!(
            "{}:{}",
            subscription.subscription_arn,
            Uuid::now_v7()
        )),
        kind: DeliveryRecordKind::Notification,
        message_id: PubsubMessageId::new(),
        subscription_arn: subscription.subscription_arn,
        message_body: None,
        subject: None,
        message_attributes: Default::default(),
        target: DeliveryTarget::BuiltIn,
        status: DeliveryStatus::Pending,
        attempts: 0,
        next_attempt_at: None,
        lease_owner: None,
        lease_expires_at: None,
        last_error: None,
        created_at: TimestampMillis::from(1_000),
        updated_at: TimestampMillis::from(1_000),
    };
    provider.put_delivery_record(record.clone()).await.unwrap();

    let claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "fdb-worker".to_string(),
            now: TimestampMillis::from(2_000),
            lease_expires_at: TimestampMillis::from(3_000),
            limit: 10,
        })
        .await
        .unwrap();

    assert_eq!(claim.records.len(), 1);
    assert_eq!(claim.records[0].id, record.id);
    assert_eq!(claim.records[0].lease_owner.as_deref(), Some("fdb-worker"));
}
