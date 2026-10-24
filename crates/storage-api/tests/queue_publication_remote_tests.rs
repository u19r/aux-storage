use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use axum::response::IntoResponse;
use queue::{
    CreateQueueRequest, DeleteMessageRequest, MessageId, PublishMessageRequest, QueueManager,
    ReceiveMessageRequest, SendMessageRequest,
};
use queue_provider::{RemoteCredentialStrategy, RemoteQueueSettings, RemoteSigv4Settings};
use queue_remote::RemoteQueueProvider;
use storage::DatabaseManager;
use storage_api::{AppState, server_router};
use tokio::{net::TcpListener, task::JoinHandle};

struct RemotePublicationFixture {
    manager: QueueManager,
    queue_url: String,
    id: MessageId,
    requests: Arc<AtomicUsize>,
    lose_reply: Arc<AtomicBool>,
    lost_replies: Arc<AtomicUsize>,
    server: JoinHandle<()>,
}
impl Drop for RemotePublicationFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl RemotePublicationFixture {
    async fn new() -> Self {
        let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/queue", listener.local_addr().unwrap());
        let native = Arc::new(QueueManager::new(
            database.initialization_queue_provider().unwrap(),
        ));
        let state = AppState::new(database).with_messaging(
            Some(native),
            endpoint.clone(),
            "000000000000".into(),
            #[cfg(feature = "pubsub")]
            None,
        );
        let requests = Arc::new(AtomicUsize::new(0));
        let lose_reply = Arc::new(AtomicBool::new(false));
        let lost_replies = Arc::new(AtomicUsize::new(0));
        let (count, loss, lost) = (requests.clone(), lose_reply.clone(), lost_replies.clone());
        let app = server_router(Arc::new(state), false).layer(axum::middleware::from_fn(move |request: axum::extract::Request, next: axum::middleware::Next| {
            let (count, loss, lost) = (count.clone(), loss.clone(), lost.clone());
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                let publication = request.headers().get("x-amz-target").is_some_and(|value| value == "AmazonSQS.PublishMessage");
                let response = next.run(request).await;
                if publication && response.status().is_success() && loss.swap(false, Ordering::Relaxed) {
                    lost.fetch_add(1, Ordering::Relaxed);
                    return (axum::http::StatusCode::SERVICE_UNAVAILABLE, axum::Json(serde_json::json!({"__type":"ServiceUnavailable","message":"publication reply unavailable"}))).into_response();
                }
                response
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let provider = Arc::new(
            RemoteQueueProvider::new(RemoteQueueSettings {
                endpoint_urls: vec![endpoint],
                region: None,
                tls: false,
                credentials: RemoteCredentialStrategy::DefaultChain,
                timeouts: None,
                sigv4: RemoteSigv4Settings::default(),
            })
            .await
            .unwrap(),
        );
        let manager = QueueManager::new(provider);
        let queue_url = manager
            .create_queue(CreateQueueRequest {
                queue_name: "stable-remote-publication".into(),
                attributes: None,
            })
            .await
            .unwrap()
            .queue_url;
        Self {
            manager,
            queue_url,
            id: MessageId::from_bytes([9; queue::MESSAGE_ID_VERSIONSTAMP_LEN]),
            requests,
            lose_reply,
            lost_replies,
            server,
        }
    }
    fn publish(&self, body: &str) -> PublishMessageRequest {
        PublishMessageRequest {
            message_id: self.id,
            message: SendMessageRequest {
                queue_url: self.queue_url.clone(),
                message_body: body.into(),
                delay_seconds: None,
                message_attributes: None,
            },
        }
    }
    async fn receive(&self) -> Vec<queue::MessageResponse> {
        self.manager
            .receive_message(ReceiveMessageRequest {
                queue_url: self.queue_url.clone(),
                max_number_of_messages: Some(10),
                visibility_timeout: Some(60),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .unwrap()
            .messages
    }
}

#[tokio::test]
async fn given_native_remote_publication_when_replayed_then_one_http_operation_preserves_stable_pending_identity_and_visibility()
 {
    let fixture = RemotePublicationFixture::new().await;
    fixture.requests.store(0, Ordering::Relaxed);
    for _ in 0..8 {
        assert_eq!(
            fixture
                .manager
                .publish_message(fixture.publish("stable work"))
                .await
                .unwrap()
                .message_id,
            fixture.id
        );
    }
    assert_eq!(
        fixture.requests.load(Ordering::Relaxed),
        8,
        "publication was emulated through extra client requests"
    );
    let messages = fixture.receive().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, fixture.id.to_string());
    fixture
        .manager
        .publish_message(fixture.publish("stable work"))
        .await
        .unwrap();
    assert!(fixture.receive().await.is_empty());
    let error = fixture
        .manager
        .publish_message(fixture.publish("changed work"))
        .await
        .unwrap_err();
    assert_eq!(
        error.aws_query_error_type(),
        queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    );
    fixture
        .manager
        .delete_message(DeleteMessageRequest {
            queue_url: fixture.queue_url.clone(),
            receipt_handle: messages[0].receipt_handle.as_str().into(),
        })
        .await
        .unwrap();
    fixture
        .manager
        .publish_message(fixture.publish("stable work"))
        .await
        .unwrap();
    assert_eq!(fixture.receive().await.len(), 1);
}

#[tokio::test]
async fn given_native_publication_committed_before_reply_failure_when_replayed_then_original_message_is_recovered_without_duplicate_delivery()
 {
    let fixture = RemotePublicationFixture::new().await;
    fixture.lose_reply.store(true, Ordering::Relaxed);
    // The HTTP response is replaced only after the real native publication
    // commits.
    let _ = fixture
        .manager
        .publish_message(fixture.publish("recoverable work"))
        .await;
    assert_eq!(fixture.lost_replies.load(Ordering::Relaxed), 1);
    assert_eq!(
        fixture
            .manager
            .publish_message(fixture.publish("recoverable work"))
            .await
            .unwrap()
            .message_id,
        fixture.id
    );
    let messages = fixture.receive().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, fixture.id.to_string());
    assert_eq!(messages[0].body, "recoverable work");
}
