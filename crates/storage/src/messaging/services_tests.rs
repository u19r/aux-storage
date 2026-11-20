use storage_types::MessagingIdentity;

use super::{services::*, settings::*};
use crate::DatabaseManager;

fn identity() -> MessagingIdentity {
    MessagingIdentity {
        partition: "aws".into(),
        region: "eu-west-2".into(),
        account_id: "123456789012".into(),
    }
}

fn pubsub_settings() -> PubsubServiceConfig {
    PubsubServiceConfig {
        delivery: pubsub::PubsubDeliveryConfig::default(),
        runner: pubsub::DeliveryRunnerConfig::default(),
    }
}

fn sqs_subscription(topic_arn: pubsub_provider::TopicArn) -> pubsub::SubscribeRequest {
    pubsub::SubscribeRequest {
        topic_arn,
        protocol: pubsub::SubscriptionProtocol::Queue,
        endpoint: "arn:aws:sqs:eu-west-2:123456789012:q1".into(),
        attributes: Default::default(),
        extra_json: serde_json::Value::Null,
    }
}

#[tokio::test]
async fn given_default_config_when_built_then_no_service_runs() {
    let database = DatabaseManager::new_for_test().await.unwrap();
    let services = MessagingServices::build(&database, MessagingConfig::default()).unwrap();
    assert!(services.queue.is_none());
    assert!(services.pubsub.is_none());
    services.start_delivery(&database).await.unwrap();
    database.check_background_services().unwrap();
}

#[tokio::test]
async fn given_queue_and_pubsub_config_when_built_then_both_use_one_identity() {
    let database = DatabaseManager::new_for_test().await.unwrap();
    let services = MessagingServices::build(
        &database,
        MessagingConfig {
            queue: Some(queue::QueueEndpoint {
                public_base_url: "http://localhost/queue".into(),
            }),
            pubsub: Some(pubsub_settings()),
            identity: identity(),
        },
    )
    .unwrap();
    assert_eq!(services.identity(), &identity());

    let created = services
        .queue
        .as_ref()
        .unwrap()
        .create_queue(queue::CreateQueueRequest {
            queue_name: "q1".into(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(created.queue_url, "http://localhost/queue/123456789012/q1");

    let pubsub = services.pubsub.as_ref().unwrap();
    let topic = pubsub.create_topic("t1").await.unwrap();
    assert!(
        topic
            .topic_arn
            .to_string()
            .starts_with("arn:aws:sns:eu-west-2:123456789012:")
    );
    // The route manager has the queue wake, so an SQS endpoint is accepted.
    pubsub
        .subscribe(sqs_subscription(topic.topic_arn))
        .await
        .unwrap();
}

#[tokio::test]
async fn given_pubsub_without_queue_when_sqs_subscribe_then_not_implemented() {
    let database = DatabaseManager::new_for_test().await.unwrap();
    let services = MessagingServices::build(
        &database,
        MessagingConfig {
            pubsub: Some(pubsub_settings()),
            identity: identity(),
            ..Default::default()
        },
    )
    .unwrap();
    let pubsub = services.pubsub.as_ref().unwrap();
    let topic = pubsub.create_topic("t1").await.unwrap();
    let error = pubsub
        .subscribe(sqs_subscription(topic.topic_arn))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        pubsub_provider::ProtocolRejection::NotImplemented
            .error("sqs")
            .to_string()
    );
}
