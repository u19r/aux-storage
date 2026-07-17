use std::{collections::HashMap, time::Instant};

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
    let records = (0..32)
        .map(|_| DeliveryRecord {
            id: DeliveryRecordId(format!(
                "{}:{}",
                subscription.subscription_arn,
                Uuid::now_v7()
            )),
            kind: DeliveryRecordKind::Notification,
            message_id: PubsubMessageId::new(),
            subscription_arn: subscription.subscription_arn.clone(),
            subscription: None,
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
        })
        .collect::<Vec<_>>();
    for record in &records {
        provider.put_delivery_record(record.clone()).await.unwrap();
    }

    let started = Instant::now();
    let claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "fdb-worker".to_string(),
            now: TimestampMillis::from(2_000),
            lease_expires_at: TimestampMillis::from(3_000),
            limit: records.len(),
        })
        .await
        .unwrap();
    let elapsed = started.elapsed();
    eprintln!(
        "claimed {} FoundationDB records in {elapsed:?}",
        claim.records.len()
    );

    let mut expected_ids = records
        .into_iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();
    expected_ids.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(claim.records.len(), expected_ids.len());
    assert_eq!(
        claim
            .records
            .iter()
            .map(|record| &record.id)
            .collect::<Vec<_>>(),
        expected_ids.iter().collect::<Vec<_>>()
    );
    assert!(
        claim
            .records
            .iter()
            .all(|record| record.lease_owner.as_deref() == Some("fdb-worker"))
    );
}
