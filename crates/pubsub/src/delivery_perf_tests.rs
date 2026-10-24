use std::{collections::HashMap, sync::Arc, time::Instant};

use alloc_counter::AllocationGuard;
use pubsub_provider::{
    DeliveryRecordId, DeliveryStatus, PublishRequest, PubsubProvider, SubscribeRequest,
    SubscriptionProtocol,
};
use queue::{CreateQueueRequest, QueueManager, ReceiveMessageRequest};
use queue_provider::QueueProvider;
use serde_json::Value;
use sql_backend::{SqlStore, sqlite::SqliteBackend};

use crate::{
    manager::PubsubManager,
    notification::{PubsubNotificationSignRequest, PubsubNotificationSigner},
};

struct FixedSigner;

impl PubsubNotificationSigner for FixedSigner {
    fn sign(&self, request: PubsubNotificationSignRequest<'_>) -> crate::PubsubResult<String> {
        std::hint::black_box(request.string_to_sign);
        Ok("signature".to_string())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn given_sqlite_deliveries_when_job_runs_then_payloads_claims_and_allocations_match() {
    for (label, size, signed, raw) in [
        ("sqlite_unsigned_1k", 1024, false, false),
        ("sqlite_signed_1k", 1024, true, false),
        ("sqlite_raw_1k", 1024, false, true),
        ("sqlite_unsigned_64k", 65536, false, false),
        ("sqlite_signed_64k", 65536, true, false),
        ("sqlite_raw_64k", 65536, false, true),
    ] {
        let driver = SqliteBackend::initialize(":memory:").await.unwrap();
        let provider = Arc::new(SqlStore::initialize_schema(driver).await.unwrap());
        PubsubProvider::initialize(provider.as_ref()).await.unwrap();
        QueueProvider::initialize(provider.as_ref()).await.unwrap();
        let queue = Arc::new(QueueManager::new(provider.clone()));
        queue
            .create_queue(CreateQueueRequest {
                queue_name: "orders".to_string(),
                attributes: None,
            })
            .await
            .unwrap();
        let mut builder = PubsubManager::builder()
            .provider(provider.clone())
            .queue_manager(queue.clone());
        if signed {
            builder = builder.notification_signer(Arc::new(FixedSigner));
        }
        let manager = builder.build().unwrap();
        let topic = manager.create_topic("orders").await.unwrap();
        let subscription = manager
            .subscribe(SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: "orders".to_string(),
                attributes: HashMap::from([("RawMessageDelivery".to_string(), raw.to_string())]),
                extra_json: Value::Null,
            })
            .await
            .unwrap();
        let body = "x".repeat(size);
        let mut ids = Vec::new();
        for _ in 0..10 {
            let published = manager
                .publish(PublishRequest {
                    topic_arn: topic.topic_arn.clone(),
                    message: body.clone(),
                    subject: Some("subject雪".to_string()),
                    message_attributes: HashMap::from([(
                        "event".to_string(),
                        "created".to_string(),
                    )]),
                })
                .await
                .unwrap();
            ids.push(DeliveryRecordId(format!(
                "{}:{}",
                subscription.subscription_arn, published.message_id
            )));
        }
        let started = Instant::now();
        let guard = AllocationGuard::start(
            module_path!(),
            "sqlite_delivery_job",
            file!(),
            line!(),
            Some(label),
        );
        let processed = manager.process_due_deliveries(10).await.unwrap();
        let report = guard.finish();
        let elapsed = started.elapsed();
        let _ = alloc_counter::emit_report(&report);
        eprintln!(
            "delivery_time label={label} operations={processed} elapsed_ns={}",
            elapsed.as_nanos()
        );
        assert_eq!(processed, 10);
        assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 0);
        for id in ids {
            let record = provider.get_delivery_record(&id).await.unwrap().unwrap();
            assert_eq!(record.status, DeliveryStatus::Delivered);
            assert_eq!(record.attempts, 1);
            assert_eq!(record.claim_generation, 1);
            assert!(record.lease_owner.is_none());
            assert_eq!(record.message_body.as_deref(), Some(body.as_str()));
        }
        let messages = queue
            .receive_message(ReceiveMessageRequest {
                queue_url: "orders".to_string(),
                max_number_of_messages: Some(10),
                visibility_timeout: None,
                wait_time_seconds: None,
                attribute_names: None,
                message_attribute_names: Some(vec!["All".to_string()]),
            })
            .await
            .unwrap()
            .messages;
        assert_eq!(messages.len(), 10);
        for message in messages {
            if raw {
                assert_eq!(message.body, body);
                assert!(message.message_attributes.is_some());
            } else {
                let value: Value = serde_json::from_str(&message.body).unwrap();
                assert_eq!(value["Message"], body);
                assert_eq!(value["Subject"], "subject雪");
                assert_eq!(value["SignatureVersion"], "1");
                assert_eq!(value["Signature"], if signed { "signature" } else { "" });
                assert_eq!(value["MessageAttributes"]["event"]["Value"], "created");
                assert!(message.message_attributes.is_none());
            }
        }
    }
}
