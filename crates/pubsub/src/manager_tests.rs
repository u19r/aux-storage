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
    DeliveryRecordId, ListSubscriptionsRequest, PublishRequest, PubsubProvider, SubscribeRequest,
    Subscription, SubscriptionProtocol, TopicArn, TopicName,
};
use queue::{CreateQueueRequest, QueueManager as TestQueueManager, ReceiveMessageRequest};
use serde_json::Value;
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use stream_provider::{
    StreamError, SubscriptionMessage, SubscriptionMessageSender, SubscriptionSendFuture,
    SubscriptionSendOutcome,
};

use crate::{
    delivery_config::{DeliveryTiming, PubsubDeliveryConfig},
    manager::PubsubManager,
    notification::{
        NotificationRenderContext, PubsubNotificationSignRequest, PubsubNotificationSigner,
        notification_body, notification_headers, notification_string_to_sign,
    },
    protocol::{PubsubAction, PubsubSuccess},
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

/// One SQLite store holds the topics, the deliveries, and the queues, as in
/// a deployment: an SNS delivery to SQS is one transaction on it.
type TestProvider = SqlStore<SqliteBackend>;

async fn sqlite_provider() -> Arc<TestProvider> {
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .expect("sqlite");
    Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .expect("sqlite schema"),
    )
}

fn queue_arn(name: &str) -> String {
    format!("arn:aws:sqs:us-east-1:000000000000:{name}")
}

async fn first_subscription(provider: &TestProvider, topic_arn: TopicArn) -> Subscription {
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
    provider: &TestProvider,
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
    let provider = sqlite_provider().await;
    let manager = PubsubManager::new(provider);

    let first = manager.create_topic("orders").await.unwrap();
    let second = manager.create_topic("orders").await.unwrap();

    assert_eq!(first.topic_arn, second.topic_arn);
}

#[tokio::test]
async fn publish_routes_http_subscription_to_custom_sender_with_extra_json() {
    let provider = sqlite_provider().await;
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
            message_attributes: HashMap::from([(
                "event".to_string(),
                "created".to_string().into(),
            )]),
        })
        .await
        .unwrap();

    assert!(sender.messages().is_empty());
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 2);
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
    let provider = sqlite_provider().await;
    let queue_manager = Arc::new(TestQueueManager::new(provider.clone()));
    queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "orders".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queue_manager.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();

    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("orders"),
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
    let provider = sqlite_provider().await;
    let manager = PubsubManager::new(provider);

    let success = manager
        .execute_query_action(PubsubAction::CreateTopic(
            pubsub_provider::CreateTopicRequest {
                name: TopicName::new("orders").unwrap(),
                attributes: HashMap::new(),
                tags: std::collections::BTreeMap::new(),
            },
        ))
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
    let provider = sqlite_provider().await;
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
    let provider = sqlite_provider().await;
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
            message_attributes: HashMap::from([(
                "event".to_string(),
                "created".to_string().into(),
            )]),
        })
        .await
        .unwrap();
    assert_eq!(
        provider
            .materialize_publish_intents(10)
            .await
            .unwrap()
            .records,
        1
    );
    let record_id = DeliveryRecordId(format!(
        "{}:{}",
        subscription.subscription_arn, publish.message_id
    ));
    let (pending, payload) = provider.get_delivery(&record_id).await.unwrap().unwrap();
    assert!(pending.lease.is_none());
    assert_eq!(pending.attempts, 0);
    assert_eq!(payload.message_body.as_deref(), Some("created"));

    let processed = manager.process_due_deliveries(10).await.unwrap();

    assert_eq!(processed, 1);
    assert_eq!(
        provider.get_delivery(&record_id).await.unwrap(),
        None,
        "a delivered record is deleted (W3 design 1.5)"
    );
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
    let provider = sqlite_provider().await;
    let queue_manager = Arc::new(TestQueueManager::new(provider.clone()));
    queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "orders-queue".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider)
        .queue_wake(queue_manager.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("orders-queue"),
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
            message_attributes: HashMap::from([(
                "event".to_string(),
                "created".to_string().into(),
            )]),
        })
        .await
        .unwrap();

    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
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
    let provider = sqlite_provider().await;
    let queue_manager = Arc::new(TestQueueManager::new(provider.clone()));
    queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "orders-raw-queue".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider)
        .queue_wake(queue_manager.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("orders").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("orders-raw-queue"),
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
            message_attributes: HashMap::from([(
                "event".to_string(),
                "created".to_string().into(),
            )]),
        })
        .await
        .unwrap();

    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
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
    let provider = sqlite_provider().await;
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
        message_attributes: HashMap::from([("event".to_string(), "created".to_string().into())]),
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
    let provider = sqlite_provider().await;
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

/// The stale sweep deletes a pending subscription while its confirmation
/// record is still queued: the record is settled without a request.
#[tokio::test]
async fn given_pending_subscription_deleted_when_its_confirmation_is_due_then_it_is_dropped_without_a_request()
 {
    let provider = sqlite_provider().await;
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

    provider
        .delete_subscription(&subscription.subscription_arn)
        .await
        .unwrap();

    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    assert_eq!(transport.payloads().len(), 0, "no request was sent");
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        0,
        "the record is gone, not retried"
    );
}

/// R5: a transient HTTP failure schedules a retry; no attempt counter ends
/// the delivery before its one-hour deadline.
#[tokio::test]
async fn process_due_deliveries_schedules_retry_for_transient_http_failure() {
    let provider = sqlite_provider().await;
    let transport = RecordingHttpTransport::new(vec![
        StatusCode::NO_CONTENT,
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
            timing: DeliveryTiming::from(&config::PubsubRunnerConfig {
                request_timeout_seconds: 1,
                ..config::PubsubRunnerConfig::default()
            }),
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
    assert_eq!(manager.process_due_deliveries(1).await.unwrap(), 1);
    let subscription =
        confirm_first_subscription(&manager, &provider, topic.topic_arn.clone()).await;
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
    let (retry, _) = provider.get_delivery(&record_id).await.unwrap().unwrap();
    assert!(retry.lease.is_none());
    assert_eq!(retry.attempts, 1);
    assert_eq!(
        retry.last_failure,
        Some(pubsub_provider::DeliveryFailureCode::HttpStatus(500))
    );
    assert!(retry.due_at > provider.now());
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        0,
        "the retry is not due before its back-off"
    );
}

#[tokio::test]
async fn notification_body_uses_configured_unsubscribe_url_and_signer() {
    let provider = sqlite_provider().await;
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

#[tokio::test]
async fn given_typed_attributes_when_delivered_then_raw_queue_retains_types_and_large_envelope_succeeds()
 {
    let provider = sqlite_provider().await;
    let queues = Arc::new(TestQueueManager::new(provider.clone()));
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queues.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("typed-delivery").await.unwrap();
    let mut urls = Vec::new();
    for raw in [false, true] {
        let url = queues
            .create_queue(CreateQueueRequest {
                queue_name: format!("typed-delivery-{raw}"),
                attributes: None,
                ..Default::default()
            })
            .await
            .unwrap()
            .queue_url;
        manager
            .subscribe(SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: queue_arn(&format!("typed-delivery-{raw}")),
                attributes: HashMap::from([("RawMessageDelivery".to_owned(), raw.to_string())]),
                extra_json: Value::Null,
            })
            .await
            .unwrap();
        urls.push(url);
    }
    let attributes = HashMap::from([
        (
            "array".to_owned(),
            pubsub_provider::MessageAttribute::StringArray("[1,true,null]".to_owned()),
        ),
        (
            "count".to_owned(),
            pubsub_provider::MessageAttribute::Number("42".to_owned()),
        ),
        (
            "blob".to_owned(),
            pubsub_provider::MessageAttribute::Binary(vec![0, 1, 255]),
        ),
    ]);
    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn.clone(),
            message: "typed".to_owned(),
            subject: None,
            message_attributes: attributes,
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 2);
    for (raw, url) in [false, true].into_iter().zip(&urls) {
        let response = queues
            .receive_message(ReceiveMessageRequest {
                queue_url: url.clone(),
                max_number_of_messages: Some(1),
                visibility_timeout: Some(30),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: Some(vec!["All".into()]),
            })
            .await
            .unwrap();
        assert_eq!(response.messages.len(), 1);
        let message = &response.messages[0];
        if raw {
            let attributes = message.message_attributes.as_ref().unwrap();
            assert_eq!(attributes["array"].data_type, "String.Array");
            assert_eq!(
                attributes["array"].string_value.as_deref(),
                Some("[1,true,null]")
            );
            assert_eq!(attributes["count"].data_type, "Number");
            assert_eq!(attributes["count"].string_value.as_deref(), Some("42"));
            assert_eq!(attributes["blob"].data_type, "Binary");
            assert_eq!(attributes["blob"].binary_value.as_deref(), Some("AAH/"));
        } else {
            let body: Value = serde_json::from_str(&message.body).unwrap();
            assert_eq!(
                body["MessageAttributes"]["blob"],
                serde_json::json!({"Type": "Binary", "Value": "AAH/"})
            );
        }
    }
    let published = manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn.clone(),
            message: "large-attribute-set".to_owned(),
            subject: None,
            message_attributes: (0..11)
                .map(|index| (format!("attribute{index}"), "value".to_owned().into()))
                .collect(),
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 2);
    let subscriptions = provider
        .list_subscriptions(ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions;
    for subscription in subscriptions {
        let record = provider
            .get_delivery(&DeliveryRecordId(format!(
                "{}:{}",
                subscription.subscription_arn, published.message_id
            )))
            .await
            .unwrap();
        assert_eq!(
            record, None,
            "the envelope is delivered; the raw delivery with 11 attributes is a permanent \
             failure that is dropped (no dead-letter queue)"
        );
    }
    let (envelope_url, raw_url) = (&urls[0], &urls[1]);
    for (url, expected) in [(envelope_url, 1), (raw_url, 0)] {
        let received = queues
            .receive_message(ReceiveMessageRequest {
                queue_url: url.clone(),
                max_number_of_messages: Some(10),
                visibility_timeout: Some(30),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .unwrap();
        assert_eq!(received.messages.len(), expected, "{url}");
    }
}

/// W3 step 6: the delivery commit wakes a long poll on the destination queue
/// in this process, as a send does.
#[tokio::test]
async fn given_long_poll_on_queue_when_sns_delivers_then_the_receive_wakes_at_once() {
    let provider = sqlite_provider().await;
    let queue_manager = Arc::new(TestQueueManager::new(provider.clone()));
    let queue_url = queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "woken".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queue_manager.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("woken").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("woken"),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let (received, ()) = tokio::join!(
        queue_manager.receive_message(ReceiveMessageRequest {
            queue_url,
            max_number_of_messages: Some(1),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(10),
            attribute_names: None,
            message_attribute_names: None,
        }),
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            manager
                .publish(PublishRequest {
                    topic_arn: topic.topic_arn.clone(),
                    message: "wake".to_string(),
                    subject: None,
                    message_attributes: HashMap::new(),
                })
                .await
                .unwrap();
            assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
        }
    );
    assert_eq!(received.unwrap().messages.len(), 1);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the delivery woke the long poll before its probe interval"
    );
}

/// With a 60 s probe interval only the wake handle can deliver the message
/// to the waiting receive.
#[tokio::test]
async fn given_long_probe_interval_when_sns_delivers_then_the_wake_handle_wakes_the_receive() {
    let provider = sqlite_provider().await;
    let queue_manager = Arc::new(
        TestQueueManager::new(provider.clone())
            .with_receive_probe_interval(Duration::from_secs(60)),
    );
    let queue_url = queue_manager
        .create_queue(CreateQueueRequest {
            queue_name: "woken".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queue_manager.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("woken").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("woken"),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let (received, ()) = tokio::join!(
        queue_manager.receive_message(ReceiveMessageRequest {
            queue_url,
            max_number_of_messages: Some(1),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(10),
            attribute_names: None,
            message_attribute_names: None,
        }),
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            manager
                .publish(PublishRequest {
                    topic_arn: topic.topic_arn.clone(),
                    message: "wake".to_string(),
                    subject: None,
                    message_attributes: HashMap::new(),
                })
                .await
                .unwrap();
            assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
        }
    );
    assert_eq!(received.unwrap().messages.len(), 1);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "only the wake handle can beat a 60 s probe interval"
    );
}

fn query(action: &str) -> crate::protocol::PubsubAction {
    crate::protocol::decode_query_request(action.as_bytes()).unwrap()
}

fn next_token(xml: &str) -> Option<String> {
    let (_, rest) = xml.split_once("<NextToken>")?;
    let (token, _) = rest.split_once("</NextToken>")?;
    Some(token.to_owned())
}

/// W3 test 32: 250 topics and 250 subscriptions are listed in three pages
/// each, every item once; a bad token and a missing topic are errors.
#[tokio::test]
async fn given_250_topics_when_listed_then_next_token_pages_return_each_once() {
    let provider = sqlite_provider().await;
    let queues = Arc::new(TestQueueManager::new(provider.clone()));
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queues.wake_handle())
        .build()
        .unwrap();
    let mut topics = Vec::new();
    for index in 0..250 {
        let topic = manager
            .create_topic(format!("paged-{index:03}"))
            .await
            .unwrap();
        manager
            .subscribe(SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: queue_arn("paged"),
                attributes: HashMap::new(),
                extra_json: Value::Null,
            })
            .await
            .unwrap();
        topics.push(topic.topic_arn);
    }
    for (action, member) in [
        ("ListTopics", "<TopicArn>"),
        ("ListSubscriptions", "<SubscriptionArn>"),
    ] {
        let mut seen = std::collections::HashSet::new();
        let mut token: Option<String> = None;
        let mut pages = 0;
        loop {
            let request = match &token {
                Some(token) => format!("Action={action}&NextToken={token}"),
                None => format!("Action={action}"),
            };
            let success = manager.execute_query_action(query(&request)).await.unwrap();
            let xml = crate::protocol::render_query_success(&success, "id");
            pages += 1;
            for item in xml.split(member).skip(1) {
                let arn = item.split('<').next().unwrap().to_owned();
                assert!(seen.insert(arn), "{action}: an item appeared twice");
            }
            token = next_token(&xml);
            if token.is_none() {
                break;
            }
        }
        assert_eq!(pages, 3, "{action}");
        assert_eq!(seen.len(), 250, "{action}");
    }

    let error = manager
        .execute_query_action(query("Action=ListTopics&NextToken=%25%25"))
        .await
        .unwrap_err();
    assert_eq!(error.aws_query_error_type(), "InvalidParameter");
    assert_eq!(error.aws_query_message(), "Invalid parameter: NextToken");
    let error = manager
        .execute_query_action(query(
            "Action=ListSubscriptionsByTopic&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%\
             3Amissing",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.aws_query_error_type(), "NotFound");
    assert_eq!(error.aws_query_message(), "Topic does not exist");
}

struct BrokenSigner;

impl PubsubNotificationSigner for BrokenSigner {
    fn sign(
        &self,
        _request: PubsubNotificationSignRequest<'_>,
    ) -> pubsub_provider::PubsubResult<String> {
        Err(pubsub_provider::PubsubError::storage(
            "signing key is unavailable",
        ))
    }
}

/// Review D5: an error before the attempt (here the signer) is settled as a
/// transient internal failure under the fence, so the record reaches its
/// deadline instead of being claimed again every lease.
#[tokio::test]
async fn given_signer_error_when_delivered_then_record_is_settled_as_transient() {
    let provider = sqlite_provider().await;
    let queues = Arc::new(TestQueueManager::new(provider.clone()));
    queues
        .create_queue(CreateQueueRequest {
            queue_name: "signed".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queues.wake_handle())
        .notification_signer(Arc::new(BrokenSigner))
        .build()
        .unwrap();
    let topic = manager.create_topic("signed").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("signed"),
            attributes: HashMap::new(),
            extra_json: Value::Null,
        })
        .await
        .unwrap();
    let subscription = first_subscription(&provider, topic.topic_arn.clone()).await;
    let publish = manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "unsigned".to_string(),
            subject: None,
            message_attributes: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    let (record, _) = provider
        .get_delivery(&DeliveryRecordId(format!(
            "{}:{}",
            subscription.subscription_arn, publish.message_id
        )))
        .await
        .unwrap()
        .expect("the record waits for its retry");
    assert!(record.lease.is_none());
    assert_eq!(record.attempts, 1);
    assert_eq!(
        record.last_failure,
        Some(pubsub_provider::DeliveryFailureCode::Internal)
    );
}

/// Review D9: an `sqs` endpoint in another region is not this store's queue,
/// also when a local queue has the same name; the delivery is a permanent
/// `DestinationMissing` and the local queue stays empty.
#[tokio::test]
async fn given_queue_arn_in_another_region_when_delivered_then_destination_is_missing() {
    let provider = sqlite_provider().await;
    let queues = Arc::new(TestQueueManager::new(provider.clone()));
    let url = queues
        .create_queue(CreateQueueRequest {
            queue_name: "elsewhere".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queues.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("regions").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "arn:aws:sqs:eu-west-1:000000000000:elsewhere".to_owned(),
            attributes: HashMap::new(),
            extra_json: Value::Null,
        })
        .await
        .expect("Subscribe accepts another region, as AWS does");
    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "far".to_string(),
            subject: None,
            message_attributes: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    let received = queues
        .receive_message(ReceiveMessageRequest {
            queue_url: url,
            max_number_of_messages: Some(10),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert!(
        received.messages.is_empty(),
        "no delivery to the local queue"
    );
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        0,
        "dropped"
    );
}

/// Review D9: a subscription dead-letter target in another region is
/// missing at the move, although a local queue has its name.
#[tokio::test]
async fn given_dead_letter_target_in_another_region_when_moved_then_it_is_missing() {
    let provider = sqlite_provider().await;
    let queues = Arc::new(TestQueueManager::new(provider.clone()));
    let dlq = queues
        .create_queue(CreateQueueRequest {
            queue_name: "far-dlq".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let manager = PubsubManager::builder()
        .provider(provider.clone())
        .queue_wake(queues.wake_handle())
        .build()
        .unwrap();
    let topic = manager.create_topic("far-dlq").await.unwrap();
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn("absent"),
            attributes: HashMap::from([(
                "RedrivePolicy".to_owned(),
                r#"{"deadLetterTargetArn":"arn:aws:sqs:eu-west-1:000000000000:far-dlq"}"#
                    .to_owned(),
            )]),
            extra_json: Value::Null,
        })
        .await
        .unwrap();
    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn,
            message: "lost".to_string(),
            subject: None,
            message_attributes: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    let received = queues
        .receive_message(ReceiveMessageRequest {
            queue_url: dlq,
            max_number_of_messages: Some(10),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert!(received.messages.is_empty(), "no move to the other region");
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        0,
        "dropped"
    );
}
