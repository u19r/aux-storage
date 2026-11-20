//! W2 D8 (C3): the batch payload limit through both SQS protocols.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use storage::DatabaseManager;
use tower::ServiceExt;

use crate::{AppState, router::server_router};

async fn app() -> Router {
    let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let manager = Arc::new(
        queue::QueueManager::new(
            database
                .messaging_store(storage::messaging::MessagingLane::Customer)
                .map(|store| store.queue)
                .unwrap(),
        )
        .with_endpoint(queue::QueueEndpoint {
            public_base_url: "http://localhost/queue".into(),
        }),
    );
    let state = Arc::new(AppState::new(database).with_messaging(Some(manager), None));
    server_router(state, false)
}

async fn post(app: &Router, request: Request<Body>) -> (StatusCode, String) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn json_batch(queue_url: &str, second: usize) -> Request<Body> {
    let body = json!({
        "QueueUrl": queue_url,
        "Entries": [
            {"Id": "a", "MessageBody": "a".repeat(524_288)},
            {"Id": "b", "MessageBody": "b".repeat(second)}
        ]
    });
    Request::builder()
        .method("POST")
        .uri("/queue")
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", "AmazonSQS.SendMessageBatch")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn query_batch(queue_url: &str, second: usize) -> Request<Body> {
    let body = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("Action", "SendMessageBatch")
        .append_pair("QueueUrl", queue_url)
        .append_pair("SendMessageBatchRequestEntry.1.Id", "a")
        .append_pair(
            "SendMessageBatchRequestEntry.1.MessageBody",
            &"a".repeat(524_288),
        )
        .append_pair("SendMessageBatchRequestEntry.2.Id", "b")
        .append_pair(
            "SendMessageBatchRequestEntry.2.MessageBody",
            &"b".repeat(second),
        )
        .finish();
    Request::builder()
        .method("POST")
        .uri("/queue")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .unwrap()
}

const TOO_LONG: &str =
    "Batch requests cannot be longer than 1048576 bytes. You have sent 1048577 bytes.";

#[tokio::test]
async fn given_batch_of_1048577_bytes_when_sent_by_either_protocol_then_batch_request_too_long() {
    let app = app().await;
    let created = Request::builder()
        .method("POST")
        .uri("/queue")
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", "AmazonSQS.CreateQueue")
        .body(Body::from(json!({"QueueName": "batch-limit"}).to_string()))
        .unwrap();
    let (status, body) = post(&app, created).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let queue_url = serde_json::from_str::<serde_json::Value>(&body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .to_owned();

    let (status, body) = post(&app, json_batch(&queue_url, 524_288)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = post(&app, json_batch(&queue_url, 524_289)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["__type"], "com.amazonaws.sqs#BatchRequestTooLong");
    assert_eq!(body["message"], TOO_LONG);

    let (status, body) = post(&app, query_batch(&queue_url, 524_288)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = post(&app, query_batch(&queue_url, 524_289)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("<Code>AWS.SimpleQueueService.BatchRequestTooLong</Code>"),
        "{body}"
    );
    assert!(body.contains(TOO_LONG), "{body}");
}
