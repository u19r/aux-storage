use std::{collections::HashMap, sync::Arc};

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, DeliveryRecord, DeliveryRecordId, DeliveryRecordKind,
    DeliveryStatus, DeliveryTarget, PubsubMessageId, PubsubProvider, SubscriptionArn,
};
use storage_types::TimestampMillis;
use uuid::Uuid;

use super::{CLAIM_DELIVERY_RECORDS_SQL, PostgresStorageProvider};

#[test]
fn delivery_claim_sql_locks_and_updates_the_same_candidate_set() {
    assert!(CLAIM_DELIVERY_RECORDS_SQL.contains("FOR UPDATE SKIP LOCKED"));
    assert!(CLAIM_DELIVERY_RECORDS_SQL.contains("UPDATE sys_pubsub_kv AS delivery"));
    assert!(CLAIM_DELIVERY_RECORDS_SQL.contains("RETURNING delivery.key, delivery.value"));
}

#[tokio::test]
async fn concurrent_postgres_claims_assign_a_delivery_once() {
    let Some(dsn) = std::env::var("TEST_POSTGRES_DSN")
        .ok()
        .or_else(|| std::env::var("CUCUMBER_POSTGRES_DSN").ok())
    else {
        return;
    };
    let provider = Arc::new(
        PostgresStorageProvider::new(&dsn, 8)
            .await
            .expect("create Postgres provider"),
    );
    provider
        .initialize()
        .await
        .expect("initialize Postgres pubsub");

    let now = TimestampMillis::from_timestamp(1_000);
    provider
        .put_delivery_record(DeliveryRecord {
            id: DeliveryRecordId(format!("concurrent-{}", Uuid::new_v4())),
            kind: DeliveryRecordKind::Notification,
            message_id: PubsubMessageId::new(),
            subscription_arn: SubscriptionArn::new(
                "arn:aws:sns:us-east-1:000000000000:orders:subscription",
            )
            .expect("subscription ARN"),
            subscription: None,
            message_body: None,
            subject: None,
            message_attributes: HashMap::new(),
            target: DeliveryTarget::BuiltIn,
            status: DeliveryStatus::Pending,
            attempts: 0,
            next_attempt_at: Some(now),
            lease_owner: None,
            lease_expires_at: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .expect("store delivery");

    let first = Arc::clone(&provider);
    let second = Arc::clone(&provider);
    let (first, second) = tokio::join!(
        first.claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-a".to_string(),
            now,
            lease_expires_at: TimestampMillis::from_timestamp(2_000),
            limit: 1,
        }),
        second.claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "worker-b".to_string(),
            now,
            lease_expires_at: TimestampMillis::from_timestamp(2_000),
            limit: 1,
        }),
    );

    assert_eq!(
        first.expect("first claim").records.len() + second.expect("second claim").records.len(),
        1,
        "exactly one worker claims the delivery"
    );
}
