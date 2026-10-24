use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use pubsub_provider::{
    ListSubscriptionsRequest, PublishRequest, SubscribeRequest, SubscriptionProtocol,
};
use stream_provider::{
    SubscriptionMessage, SubscriptionMessageSender, SubscriptionSendFuture, SubscriptionSendOutcome,
};

use super::test_support::TestStore;

#[derive(Default)]
struct RecordingSender {
    messages: Mutex<Vec<SubscriptionMessage>>,
}

impl RecordingSender {
    fn messages(&self) -> Vec<SubscriptionMessage> {
        self.messages.lock().unwrap().clone()
    }
}

impl SubscriptionMessageSender for RecordingSender {
    fn send_subscription_message<'a>(
        &'a self,
        message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a> {
        Box::pin(async move {
            self.messages.lock().unwrap().push(message);
            Ok(SubscriptionSendOutcome::AcceptedForDelivery)
        })
    }
}

#[tokio::test]
async fn given_manager_when_create_send_receive_delete_then_roundtrip() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture.use_system_time();
    let outcome = async {
        let provider = fixture.queue_operations.clone();
        let manager = queue::QueueManager::new(provider);
        let queue_url = manager
            .create_queue(queue_provider::CreateQueueRequest {
                queue_name: "mgr-roundtrip".to_string(),
                attributes: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager create failed: {err:?}"))?
            .queue_url;
        manager
            .send_message(queue_provider::SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "mgr-body".to_string(),
                delay_seconds: None,
                message_attributes: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager send failed: {err:?}"))?;
        let received = manager
            .receive_message(queue_provider::ReceiveMessageRequest {
                queue_url: queue_url.clone(),
                max_number_of_messages: Some(1),
                visibility_timeout: Some(30),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager receive failed: {err:?}"))?
            .messages;
        anyhow::ensure!(received.len() == 1, "manager receive empty");
        anyhow::ensure!(received[0].body == "mgr-body", "manager body changed");
        manager
            .delete_message(queue_provider::DeleteMessageRequest {
                queue_url: queue_url.clone(),
                receipt_handle: received[0].receipt_handle.as_str().into(),
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager delete failed: {err:?}"))?;
        let drained = manager
            .receive_message(queue_provider::ReceiveMessageRequest {
                queue_url,
                max_number_of_messages: Some(10),
                visibility_timeout: Some(30),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager drain failed: {err:?}"))?
            .messages;
        anyhow::ensure!(drained.is_empty(), "manager delete left messages");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_manager_when_stale_receipt_then_current_preserved() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture.use_system_time();
    let outcome = async {
        let provider = fixture.queue_operations.clone();
        let manager = queue::QueueManager::new(provider);
        let queue_url = manager
            .create_queue(queue_provider::CreateQueueRequest {
                queue_name: "mgr-stale".to_string(),
                attributes: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager create failed: {err:?}"))?
            .queue_url;
        let sent_id = manager
            .send_message(queue_provider::SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "stale".to_string(),
                delay_seconds: None,
                message_attributes: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager send failed: {err:?}"))?
            .message_id
            .to_string();
        let receive_one = |queue_url: &str| {
            let manager = &manager;
            let queue_url = queue_url.to_string();
            async move {
                let mut messages = Vec::new();
                for _ in 0..8 {
                    messages = manager
                        .receive_message(queue_provider::ReceiveMessageRequest {
                            queue_url: queue_url.clone(),
                            max_number_of_messages: Some(1),
                            visibility_timeout: Some(30),
                            wait_time_seconds: Some(0),
                            attribute_names: None,
                            message_attribute_names: None,
                        })
                        .await
                        .map_err(|err| anyhow::anyhow!("receive failed: {err:?}"))?
                        .messages;
                    if !messages.is_empty() {
                        break;
                    }
                }
                Ok::<_, anyhow::Error>(messages)
            }
        };
        let first = receive_one(&queue_url).await?;
        anyhow::ensure!(first.len() == 1, "expected first manager claim");
        manager
            .change_message_visibility(queue_provider::ChangeMessageVisibilityRequest {
                queue_url: queue_url.clone(),
                receipt_handle: first[0].receipt_handle.as_str().into(),
                visibility_timeout: 0,
            })
            .await
            .map_err(|err| anyhow::anyhow!("manager visibility failed: {err:?}"))?;
        let second = receive_one(&queue_url).await?;
        anyhow::ensure!(second.len() == 1, "expected manager redelivery");
        anyhow::ensure!(
            second[0].message_id == sent_id,
            "manager redelivery changed identity"
        );
        match manager
            .delete_message(queue_provider::DeleteMessageRequest {
                queue_url: queue_url.clone(),
                receipt_handle: first[0].receipt_handle.as_str().into(),
            })
            .await
        {
            Ok(()) => {}
            Err(err) => anyhow::ensure!(
                matches!(
                    err,
                    queue_provider::QueueError::Validation {
                        kind: queue_provider::QueueValidationKind::MessageNotFoundOrAlreadyProcessed
                            | queue_provider::QueueValidationKind::MessageNotFound,
                        ..
                    } | queue_provider::QueueError::ResourceNotFound {
                        resource_type: "receipt_handle",
                        ..
                    }
                ),
                "manager stale delete unrelated error: {err:?}"
            ),
        }
        manager
            .change_message_visibility(queue_provider::ChangeMessageVisibilityRequest {
                queue_url: queue_url.clone(),
                receipt_handle: second[0].receipt_handle.as_str().into(),
                visibility_timeout: 0,
            })
            .await
            .map_err(|err| anyhow::anyhow!("restore current failed: {err:?}"))?;
        let still = receive_one(&queue_url).await?;
        anyhow::ensure!(
            still.len() == 1 && still[0].message_id == sent_id,
            "manager stale delete removed current claim"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_manager_when_create_topic_idempotent_then_same_arn() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture.use_system_time();
    let outcome = async {
        let provider = fixture.pubsub_provider.clone();
        let manager = pubsub::PubsubManager::new(provider);
        let first = manager
            .create_topic("mgr-orders")
            .await
            .map_err(|err| anyhow::anyhow!("manager create failed: {err:?}"))?;
        let second = manager
            .create_topic("mgr-orders")
            .await
            .map_err(|err| anyhow::anyhow!("manager idempotent failed: {err:?}"))?;
        anyhow::ensure!(
            first.topic_arn == second.topic_arn,
            "manager idempotent create changed ARN"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_manager_when_custom_sender_then_extra_json_preserved() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture.use_system_time();
    let outcome = async {
        let provider = fixture.pubsub_provider.clone();
        let sender = Arc::new(RecordingSender::default());
        let manager = pubsub::PubsubManager::builder()
            .provider(provider.clone())
            .subscription_message_sender(sender.clone())
            .build()
            .map_err(|err| anyhow::anyhow!("manager build failed: {err:?}"))?;
        let topic = manager
            .create_topic("mgr-custom")
            .await
            .map_err(|err| anyhow::anyhow!("create topic failed: {err:?}"))?;
        manager
            .subscribe(SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Https,
                endpoint: "https://example.com/hook".to_string(),
                attributes: HashMap::new(),
                extra_json: serde_json::json!({"token_ref":"secret/oauth/orders"}),
            })
            .await
            .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
        // Confirm the pending HTTPS subscription.
        let subscription = provider
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(topic.topic_arn.clone()),
                next_token: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("list failed: {err:?}"))?
            .subscriptions
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("subscription missing"))?;
        let token = subscription
            .confirmation
            .token()
            .ok_or_else(|| anyhow::anyhow!("subscription lacks token"))?
            .to_string();
        manager
            .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
                topic_arn: topic.topic_arn.clone(),
                token,
            })
            .await
            .map_err(|err| anyhow::anyhow!("confirm failed: {err:?}"))?;
        manager
            .publish(PublishRequest {
                topic_arn: topic.topic_arn,
                message: "created".to_string(),
                subject: Some("order".to_string()),
                message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
            })
            .await
            .map_err(|err| anyhow::anyhow!("publish failed: {err:?}"))?;
        anyhow::ensure!(
            sender.messages().is_empty(),
            "manager delivered before processing due deliveries"
        );
        let processed = manager
            .process_due_deliveries(10)
            .await
            .map_err(|err| anyhow::anyhow!("process failed: {err:?}"))?;
        anyhow::ensure!(processed >= 1, "manager processed nothing");
        let messages = sender.messages();
        anyhow::ensure!(messages.len() == 1, "expected 1 custom delivery");
        anyhow::ensure!(messages[0].payload == b"created", "custom payload changed");
        anyhow::ensure!(
            messages[0].destination.extra_json["token_ref"] == "secret/oauth/orders",
            "custom extra_json lost"
        );
        anyhow::ensure!(
            messages[0].attributes.get("event") == Some(&"created".to_string()),
            "custom attributes lost"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_manager_when_queue_delivery_then_notification_persisted() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture.use_system_time();
    let outcome = async {
        let queue_provider = fixture.queue_operations.clone();
        let pubsub_provider = fixture.pubsub_provider.clone();
        let queue_manager = Arc::new(queue::QueueManager::new(queue_provider));
        let manager = pubsub::PubsubManager::builder()
            .provider(pubsub_provider)
            .queue_manager(queue_manager.clone())
            .build()
            .map_err(|err| anyhow::anyhow!("manager build failed: {err:?}"))?;
        let queue_url = queue_manager
            .create_queue(queue_provider::CreateQueueRequest {
                queue_name: "mgr-notify".to_string(),
                attributes: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("create queue failed: {err:?}"))?
            .queue_url;
        let topic = manager
            .create_topic("mgr-notify-topic")
            .await
            .map_err(|err| anyhow::anyhow!("create topic failed: {err:?}"))?;
        manager
            .subscribe(SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: queue_url.clone(),
                attributes: HashMap::new(),
                extra_json: serde_json::Value::Null,
            })
            .await
            .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
        manager
            .publish(PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: "created".to_string(),
                subject: Some("order".to_string()),
                message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
            })
            .await
            .map_err(|err| anyhow::anyhow!("publish failed: {err:?}"))?;
        let processed = manager
            .process_due_deliveries(10)
            .await
            .map_err(|err| anyhow::anyhow!("process failed: {err:?}"))?;
        anyhow::ensure!(processed >= 1, "manager processed nothing");
        let received = queue_manager
            .receive_message(queue_provider::ReceiveMessageRequest {
                queue_url,
                max_number_of_messages: Some(1),
                visibility_timeout: None,
                wait_time_seconds: None,
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .map_err(|err| anyhow::anyhow!("queue receive failed: {err:?}"))?
            .messages;
        anyhow::ensure!(received.len() == 1, "queue notification missing");
        let body: serde_json::Value = serde_json::from_str(&received[0].body)
            .map_err(|err| anyhow::anyhow!("notification not JSON: {err:?}"))?;
        anyhow::ensure!(
            body["Message"] == "created",
            "notification Message changed: {}",
            received[0].body
        );
        anyhow::ensure!(
            body["TopicArn"] == topic.topic_arn.as_str(),
            "notification TopicArn changed"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
