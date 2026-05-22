use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use http_request::{
    HttpClient, HttpResponse, StatusCode, Transport, TransportFuture,
    reqwest::{Request, header::HeaderMap},
};
use pubsub_provider::{
    DeliveryRecordId, DeliveryStatus, InMemoryPubsubProvider, ListSubscriptionsRequest,
    PublishRequest, PubsubProvider, SubscribeRequest, Subscription, SubscriptionProtocol, TopicArn,
    TopicName,
};
use queue::{
    CreateQueueRequest, QueueBackend, QueueConfig, QueueManager as TestQueueManager,
    ReceiveMessageRequest, create_queue_provider,
};
use serde_json::Value;
use stream_provider::{
    StreamError, SubscriptionMessage, SubscriptionMessageSender, SubscriptionSendFuture,
    SubscriptionSendOutcome,
};

use crate::{
    PubsubAction, PubsubDeliveryConfig, PubsubManager, PubsubSuccess,
    notification::{
        NotificationRenderContext, PubsubNotificationSignRequest, PubsubNotificationSigner,
        notification_body, notification_headers, notification_string_to_sign,
    },
};

#[derive(Default)]
struct RecordingSender {
    messages: Mutex<Vec<SubscriptionMessage>>,
}

struct FailingSender;
struct FixedSigner;

#[derive(Debug, Clone)]
struct RecordingHttpTransport {
    inner: Arc<RecordingHttpTransportInner>,
}

#[derive(Debug)]
struct RecordingHttpTransportInner {
    statuses: Mutex<Vec<StatusCode>>,
    payloads: Mutex<Vec<Vec<u8>>>,
    headers: Mutex<Vec<HeaderMap>>,
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

impl SubscriptionMessageSender for FailingSender {
    fn send_subscription_message<'a>(
        &'a self,
        _message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a> {
        Box::pin(async move { Err(StreamError::validation("delivery unavailable")) })
    }
}

impl PubsubNotificationSigner for FixedSigner {
    fn sign(
        &self,
        request: PubsubNotificationSignRequest<'_>,
    ) -> pubsub_provider::PubsubResult<String> {
        assert_eq!(request.signature_version, "1");
        assert!(
            request
                .string_to_sign
                .starts_with("Message\ncreated\nMessageId\nmessage-id\n")
        );
        assert!(
            request.string_to_sign.ends_with(
                "TopicArn\narn:aws:sns:us-east-1:000000000000:orders\nType\nNotification"
            )
        );
        Ok("signed".to_string())
    }
}

impl RecordingHttpTransport {
    fn new(statuses: Vec<StatusCode>) -> Self {
        Self {
            inner: Arc::new(RecordingHttpTransportInner {
                statuses: Mutex::new(statuses),
                payloads: Mutex::new(Vec::new()),
                headers: Mutex::new(Vec::new()),
            }),
        }
    }

    fn payloads(&self) -> Vec<Vec<u8>> {
        self.inner.payloads.lock().unwrap().clone()
    }

    fn headers(&self) -> Vec<HeaderMap> {
        self.inner.headers.lock().unwrap().clone()
    }
}

impl Transport for RecordingHttpTransport {
    fn send(&self, request: Request) -> TransportFuture {
        let status = self.inner.statuses.lock().unwrap().remove(0);
        let payload = request
            .body()
            .and_then(|body| body.as_bytes())
            .unwrap_or_default()
            .to_vec();
        self.inner
            .headers
            .lock()
            .unwrap()
            .push(request.headers().clone());
        self.inner.payloads.lock().unwrap().push(payload);
        let url = request.url().clone();
        Box::pin(async move {
            Ok(HttpResponse::from_mock(
                status,
                HeaderMap::new(),
                Vec::new(),
                url,
            ))
        })
    }
}

async fn create_test_queue_manager() -> TestQueueManager {
    let config = QueueConfig {
        backend_type: QueueBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        postgres: None,
        foundationdb: None,
        remote: None,
    };
    let provider = create_queue_provider(config).await.unwrap();
    provider.initialize().await.unwrap();
    TestQueueManager::new(Arc::from(provider))
}

async fn first_subscription(
    provider: &InMemoryPubsubProvider,
    topic_arn: TopicArn,
) -> Subscription {
    provider
        .list_subscriptions(ListSubscriptionsRequest {
            topic_arn: Some(topic_arn),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions
        .into_iter()
        .next()
        .unwrap()
}

async fn confirm_first_subscription(
    manager: &PubsubManager,
    provider: &InMemoryPubsubProvider,
    topic_arn: TopicArn,
) -> Subscription {
    let subscription = first_subscription(provider, topic_arn.clone()).await;
    let token = subscription.confirmation.token().unwrap().to_string();
    manager
        .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest { topic_arn, token })
        .await
        .unwrap();
    provider
        .get_subscription(&subscription.subscription_arn)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn create_topic_is_idempotent_through_manager() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let manager = PubsubManager::new(provider);

    let first = manager.create_topic("orders").await.unwrap();
    let second = manager.create_topic("orders").await.unwrap();

    assert_eq!(first.topic_arn, second.topic_arn);
}

#[tokio::test]
async fn publish_routes_http_subscription_to_custom_sender_with_extra_json() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let sender = Arc::new(RecordingSender::default());
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .subscription_message_sender(sender.clone())
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();

    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::json!({"token_ref":"secret/oauth/orders"}),
        })
        .await
        .unwrap();
    confirm_first_subscription(&manager, &provider, topic.topic_arn.clone()).await;

    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "created".to_string(),
            subject: Some("order".to_string()),
            message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
        })
        .await
        .unwrap();

    let messages = sender.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload, b"created");
    assert_eq!(
        messages[0].destination.extra_json["token_ref"],
        "secret/oauth/orders"
    );
    assert_eq!(
        messages[0].attributes.get("event"),
        Some(&"created".to_string())
    );
}

#[tokio::test]
async fn subscribe_stores_subscription_for_topic_listing() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let queue_manager = Arc::new(create_test_queue_manager().await);
    queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "orders".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_manager(queue_manager)
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();

    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "orders".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
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
    assert_eq!(subscriptions[0].protocol, SubscriptionProtocol::Queue);
}

#[tokio::test]
async fn execute_query_action_uses_manager_control_plane() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let manager = PubsubManager::new(provider);

    let success = manager
        .execute_query_action(PubsubAction::CreateTopic {
            name: TopicName::new("orders").unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();

    let PubsubSuccess::CreateTopic { topic_arn } = success else {
        panic!("unexpected query action result");
    };
    assert_eq!(
        topic_arn.as_str(),
        "arn:aws:sns:us-east-1:000000000000:orders"
    );
}

#[tokio::test]
async fn publish_records_custom_sender_failure_without_rejecting_publish() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .subscription_message_sender(Arc::new(FailingSender))
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();

    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    confirm_first_subscription(&manager, &provider, topic.topic_arn.clone()).await;

    let response = manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "created".to_string(),
            subject: None,
            message_attributes: HashMap::new(),
        })
        .await;

    assert!(response.is_ok());
}

#[tokio::test]
async fn process_due_deliveries_sends_pending_builtin_http_delivery() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let transport =
        RecordingHttpTransport::new(vec![StatusCode::NO_CONTENT, StatusCode::NO_CONTENT]);
    let http_client = HttpClient::builder()
        .with_transport(transport.clone())
        .build()
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .http_client(http_client)
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();
    let subscribe = manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(subscribe.subscription_arn, "pending confirmation");
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    let subscription =
        confirm_first_subscription(&manager, &provider, topic.topic_arn.clone()).await;

    let publish = manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "created".to_string(),
            subject: Some("order".to_string()),
            message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
        })
        .await
        .unwrap();
    let record_id = DeliveryRecordId(format!(
        "{}:{}",
        subscription.subscription_arn, publish.message_id
    ));
    let pending = provider
        .get_delivery_record(&record_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, DeliveryStatus::Pending);
    assert_eq!(pending.attempts, 0);
    assert_eq!(pending.message_body.as_deref(), Some("created"));

    let processed = manager.process_due_deliveries(10).await.unwrap();

    assert_eq!(processed, 1);
    let delivered = provider
        .get_delivery_record(&record_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.status, DeliveryStatus::Delivered);
    assert_eq!(delivered.attempts, 1);
    let payloads = transport.payloads();
    assert_eq!(payloads.len(), 2);
    let body = String::from_utf8(payloads[1].clone()).unwrap();
    let fixture: Value =
        serde_json::from_str(include_str!("../tests/fixtures/aws/notification-http.json")).unwrap();
    assert_eq!(
        normalize_notification_body(&body),
        fixture["body"].as_str().unwrap()
    );

    let headers = transport.headers();
    assert_eq!(headers.len(), 2);
    assert_eq!(
        normalize_notification_headers(&headers[1]),
        fixture["headers"]
    );
}

#[tokio::test]
async fn publish_sends_queue_notification_body_matching_aws_fixture() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let queue_manager = Arc::new(create_test_queue_manager().await);
    queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "orders-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider)
        .queue_manager(queue_manager.clone())
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "orders-queue".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "created".to_string(),
            subject: Some("order".to_string()),
            message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
        })
        .await
        .unwrap();

    let messages = queue_manager
        .receive_message(ReceiveMessageRequest {
            queue_url: "orders-queue".to_string(),
            max_number_of_messages: Some(1),
            visibility_timeout: None,
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: Some(vec!["All".to_string()]),
        })
        .await
        .unwrap()
        .messages;
    assert_eq!(messages.len(), 1);
    let fixture: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/aws/notification-queue.json"
    ))
    .unwrap();
    assert_eq!(
        normalize_notification_body(&messages[0].body),
        fixture["body"].as_str().unwrap()
    );
    assert!(messages[0].message_attributes.is_none());
}

#[tokio::test]
async fn publish_sends_raw_queue_body_and_attributes_matching_aws_fixture() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let queue_manager = Arc::new(create_test_queue_manager().await);
    queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "orders-raw-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider)
        .queue_manager(queue_manager.clone())
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "orders-raw-queue".to_string(),
            attributes: HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "raw-created".to_string(),
            subject: Some("order".to_string()),
            message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
        })
        .await
        .unwrap();

    let messages = queue_manager
        .receive_message(ReceiveMessageRequest {
            queue_url: "orders-raw-queue".to_string(),
            max_number_of_messages: Some(1),
            visibility_timeout: None,
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: Some(vec!["All".to_string()]),
        })
        .await
        .unwrap()
        .messages;
    assert_eq!(messages.len(), 1);
    let fixture: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/aws/notification-queue-raw.json"
    ))
    .unwrap();
    assert_eq!(messages[0].body, fixture["body"].as_str().unwrap());
    assert_eq!(
        serde_json::to_value(messages[0].message_attributes.as_ref().unwrap()).unwrap(),
        fixture["message_attributes"]
    );
}

#[tokio::test]
async fn notification_body_and_headers_match_normalized_aws_http_fixture() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let manager = PubsubManager::new(provider.clone());
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let subscription = first_subscription(&provider, topic.topic_arn.clone()).await;
    let request = PublishRequest {
        topic_arn: topic.topic_arn,
        message: "created".to_string(),
        subject: Some("order".to_string()),
        message_attributes: HashMap::from([("event".to_string(), "created".to_string())]),
    };
    let message_id = pubsub_provider::PubsubMessageId::new();
    let body = notification_body(NotificationRenderContext {
        request: &request,
        message_id: &message_id,
        subscription: &subscription,
        delivery_config: &PubsubDeliveryConfig::default(),
        signer: None,
    })
    .unwrap();
    let headers = notification_headers(&message_id, &subscription, &request).unwrap();
    let fixture: Value =
        serde_json::from_str(include_str!("../tests/fixtures/aws/notification-http.json")).unwrap();

    assert_eq!(
        normalize_notification_body(&body),
        fixture["body"].as_str().unwrap()
    );
    assert_eq!(normalize_notification_headers(&headers), fixture["headers"]);
}

#[tokio::test]
async fn subscribe_sends_confirmation_body_and_headers_matching_aws_fixture() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let transport = RecordingHttpTransport::new(vec![StatusCode::NO_CONTENT]);
    let http_client = HttpClient::builder()
        .with_transport(transport.clone())
        .build()
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .http_client(http_client)
        .delivery_config(PubsubDeliveryConfig {
            subscribe_url_base: Some("https://sns.us-east-1.amazonaws.com/".to_string()),
            ..PubsubDeliveryConfig::default()
        })
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();

    let response = manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    assert_eq!(response.subscription_arn, "pending confirmation");
    let attributes = manager
        .get_topic_attributes(pubsub_provider::GetTopicAttributesRequest {
            topic_arn: topic.topic_arn.clone(),
        })
        .await
        .unwrap()
        .attributes;
    assert_eq!(
        attributes.get("SubscriptionsPending"),
        Some(&"1".to_string())
    );
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);

    let fixture: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/aws/subscription-confirmation-http.json"
    ))
    .unwrap();
    let payloads = transport.payloads();
    assert_eq!(payloads.len(), 1);
    let body = String::from_utf8(payloads[0].clone()).unwrap();
    assert_eq!(
        normalize_confirmation_body(&body),
        fixture["body"].as_str().unwrap()
    );
    let headers = transport.headers();
    assert_eq!(headers.len(), 1);
    assert_eq!(
        normalize_confirmation_headers(&headers[0]),
        fixture["headers"]
    );

    let subscription = first_subscription(&provider, topic.topic_arn.clone()).await;
    let token = subscription.confirmation.token().unwrap().to_string();
    let confirmed = manager
        .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token,
        })
        .await
        .unwrap();
    assert_eq!(confirmed.subscription_arn, subscription.subscription_arn);
    let attributes = manager
        .get_topic_attributes(pubsub_provider::GetTopicAttributesRequest {
            topic_arn: topic.topic_arn,
        })
        .await
        .unwrap()
        .attributes;
    assert_eq!(
        attributes.get("SubscriptionsPending"),
        Some(&"0".to_string())
    );
    assert_eq!(
        attributes.get("SubscriptionsConfirmed"),
        Some(&"1".to_string())
    );
}

#[tokio::test]
async fn process_due_deliveries_retries_then_fails_builtin_http_delivery() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let transport = RecordingHttpTransport::new(vec![
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::INTERNAL_SERVER_ERROR,
    ]);
    let http_client = HttpClient::builder()
        .with_transport(transport)
        .build()
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .http_client(http_client)
        .delivery_config(PubsubDeliveryConfig {
            worker_id: "test-worker".to_string(),
            lease_duration: Duration::from_secs(30),
            max_attempts: 2,
            retry_base_delay: Duration::ZERO,
            request_timeout: Duration::from_secs(1),
            subscribe_url_base: None,
            unsubscribe_url_base: None,
            signing_cert_url: None,
        })
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let subscription =
        confirm_first_subscription(&manager, &provider, topic.topic_arn.clone()).await;
    assert_eq!(manager.process_due_deliveries(1).await.unwrap(), 1);
    let publish = manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "created".to_string(),
            subject: None,
            message_attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let record_id = DeliveryRecordId(format!(
        "{}:{}",
        subscription.subscription_arn, publish.message_id
    ));

    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    let retry = provider
        .get_delivery_record(&record_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.status, DeliveryStatus::RetryScheduled);
    assert_eq!(retry.attempts, 1);

    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    let failed = provider
        .get_delivery_record(&record_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, DeliveryStatus::Failed);
    assert_eq!(failed.attempts, 2);
}

#[tokio::test]
async fn notification_body_uses_configured_unsubscribe_url_and_signer() {
    let provider = Arc::new(InMemoryPubsubProvider::default());
    let manager = PubsubManager::new(provider.clone());
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.com/hook".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let subscription = first_subscription(&provider, topic.topic_arn.clone()).await;
    let message_id = pubsub_provider::PubsubMessageId::new_from_string("message-id").unwrap();
    let request = PublishRequest {
        topic_arn: topic.topic_arn,
        message: "created".to_string(),
        subject: Some("order".to_string()),
        message_attributes: HashMap::new(),
    };
    let config = PubsubDeliveryConfig {
        unsubscribe_url_base: Some("https://pubsub.example.test/".to_string()),
        signing_cert_url: Some("https://pubsub.example.test/signing.pem".to_string()),
        ..PubsubDeliveryConfig::default()
    };

    let body = notification_body(NotificationRenderContext {
        request: &request,
        message_id: &message_id,
        subscription: &subscription,
        delivery_config: &config,
        signer: Some(&FixedSigner),
    })
    .unwrap();
    let value: Value = serde_json::from_str(&body).unwrap();

    assert_eq!(value["Signature"], "signed");
    assert_eq!(
        value["SigningCertURL"],
        "https://pubsub.example.test/signing.pem"
    );
    assert_eq!(
        value["UnsubscribeURL"],
        format!(
            "https://pubsub.example.test/?Action=Unsubscribe&SubscriptionArn={}",
            subscription.subscription_arn
        )
    );
}

#[test]
fn notification_string_to_sign_uses_aws_notification_field_order() {
    let topic_arn = TopicName::new("orders")
        .map(|name| pubsub_provider::TopicArn::compose("aws", "us-east-1", "000000000000", &name))
        .unwrap();
    let message_id = pubsub_provider::PubsubMessageId::new_from_string("message-id").unwrap();
    let request = PublishRequest {
        topic_arn,
        message: "created".to_string(),
        subject: Some("order".to_string()),
        message_attributes: HashMap::new(),
    };

    assert_eq!(
        notification_string_to_sign(&request, &message_id, "2026-05-05T18:19:42.137Z"),
        "Message\ncreated\nMessageId\nmessage-id\nSubject\norder\nTimestamp\n2026-05-05T18:19:42.\
         137Z\nTopicArn\narn:aws:sns:us-east-1:000000000000:orders\nType\nNotification"
    );
}

fn normalize_notification_body(body: &str) -> String {
    let value: Value = serde_json::from_str(body).unwrap();
    let mut normalized = body.to_string();
    for (field, placeholder) in [
        ("MessageId", "<MESSAGE_ID>"),
        ("TopicArn", "<TOPIC_ARN>"),
        ("Timestamp", "<TIMESTAMP>"),
    ] {
        let actual = value[field].as_str().unwrap();
        normalized = normalized.replace(
            &format!("\"{field}\" : \"{actual}\""),
            &format!("\"{field}\" : \"{placeholder}\""),
        );
    }
    normalized = normalized.replace("\"Signature\" : \"\"", "\"Signature\" : \"<SIGNATURE>\"");
    normalized = normalized.replace(
        "\"SigningCertURL\" : \"\"",
        "\"SigningCertURL\" : \"<SIGNING_CERT_URL>\"",
    );
    normalized = normalized.replace(
        "\"UnsubscribeURL\" : \"\"",
        "\"UnsubscribeURL\" : \"<UNSUBSCRIBE_URL>\"",
    );
    normalized
}

fn normalize_notification_headers(headers: &HeaderMap) -> Value {
    serde_json::json!({
        "content-type": header_value(headers, "content-type"),
        "user-agent": header_value(headers, "user-agent"),
        "x-amz-sns-message-type": header_value(headers, "x-amz-sns-message-type"),
        "x-amz-sns-message-id": "<MESSAGE_ID>",
        "x-amz-sns-topic-arn": "<TOPIC_ARN>",
        "x-amz-sns-subscription-arn": "<SUBSCRIPTION_ARN>",
    })
}

fn normalize_confirmation_body(body: &str) -> String {
    let value: Value = serde_json::from_str(body).unwrap();
    let mut normalized = body.to_string();
    let topic_arn = value["TopicArn"].as_str().unwrap();
    let token = value["Token"].as_str().unwrap();
    let message = value["Message"].as_str().unwrap();
    let subscribe_url = value["SubscribeURL"].as_str().unwrap();
    for (actual, placeholder) in [
        (subscribe_url, "<SUBSCRIBE_URL>"),
        (topic_arn, "<TOPIC_ARN>"),
        (token, "<TOKEN>"),
        (
            message,
            "You have chosen to subscribe to the topic <TOPIC_ARN>.\\nTo confirm the \
             subscription, visit the SubscribeURL included in this message.",
        ),
        (value["MessageId"].as_str().unwrap(), "<MESSAGE_ID>"),
        (value["Timestamp"].as_str().unwrap(), "<TIMESTAMP>"),
    ] {
        normalized = normalized.replace(actual, placeholder);
    }
    normalized = normalized.replace("\"Signature\" : \"\"", "\"Signature\" : \"<SIGNATURE>\"");
    normalized = normalized.replace(
        "\"SigningCertURL\" : \"\"",
        "\"SigningCertURL\" : \"<SIGNING_CERT_URL>\"",
    );
    normalized
}

fn normalize_confirmation_headers(headers: &HeaderMap) -> Value {
    serde_json::json!({
        "content-type": header_value(headers, "content-type"),
        "user-agent": header_value(headers, "user-agent"),
        "x-amz-sns-message-type": header_value(headers, "x-amz-sns-message-type"),
        "x-amz-sns-message-id": "<MESSAGE_ID>",
        "x-amz-sns-topic-arn": "<TOPIC_ARN>",
    })
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers.get(name).unwrap().to_str().unwrap().to_string()
}
