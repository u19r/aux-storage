//! SNS route admission, error mapping, disconnect and runner delivery.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use serde_json::{Value, json};
use storage::{DatabaseManager, admission::AdmissionController};
use tower::ServiceExt;

use crate::{AppState, router::server_router};

/// Bound of every wait in this file.
const WAIT_LIMIT: Duration = Duration::from_secs(10);

async fn sns(app: &Router, body: &str) -> (StatusCode, HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/pubsub")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

async fn sns_get(app: &Router, path_and_query: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path_and_query)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn sqs(app: &Router, action: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queue")
                .header("content-type", "application/x-amz-json-1.0")
                .header("x-amz-target", format!("AmazonSQS.{action}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The text between `<name>` and `</name>`.
fn tag(xml: &str, name: &str) -> String {
    let (_, rest) = xml
        .split_once(&format!("<{name}>"))
        .unwrap_or_else(|| panic!("no <{name}> in {xml}"));
    let (value, _) = rest
        .split_once(&format!("</{name}>"))
        .unwrap_or_else(|| panic!("no </{name}> in {xml}"));
    value.to_owned()
}

/// Hold every admission permit, also those that finishing attempts release,
/// so that the next storage attempt queues.
async fn saturate(controller: &AdmissionController) -> Vec<storage::AdmissionPermit> {
    let mut permits = Vec::new();
    tokio::time::timeout(WAIT_LIMIT, async {
        loop {
            while let Ok(permit) = controller.try_acquire(storage::AdmissionClass::Write) {
                permits.push(permit);
            }
            if controller.snapshot().in_flight == permits.len() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("other attempts kept admission permits");
    assert!(
        !permits.is_empty(),
        "admission is not enabled for the fixture"
    );
    permits
}

async fn until_queued(controller: &AdmissionController) {
    tokio::time::timeout(WAIT_LIMIT, async {
        while controller.snapshot().queue_depth == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("no attempt queued for admission");
}

/// Eight fixed foreground permits; a queued attempt waits `max_queue_wait_ms`.
async fn fixture(max_queue_wait_ms: u64) -> (Router, Arc<AppState>, AdmissionController) {
    let database = Arc::new(
        DatabaseManager::new_for_test_with_runtime_options(
            storage::DatabaseManagerRuntimeOptions::builder()
                .admission_config(storage::AdmissionConfig {
                    minimum_concurrency: 8,
                    maximum_concurrency: 12,
                    max_queue_wait_ms,
                    ..Default::default()
                })
                .build(),
        )
        .await
        .unwrap(),
    );
    let mut root = config::RootConfig::default();
    root.queue.enabled = true;
    root.pubsub.enabled = true;
    let state = Arc::new(
        crate::service_host::prepare_app_state(Arc::clone(&database), &root, "127.0.0.1:3000")
            .unwrap(),
    );
    state.publish_startup_readiness();
    let app = server_router(Arc::clone(&state), false);
    let controller = database.default_admission_controller();
    (app, state, controller)
}

async fn topic(app: &Router, name: &str) -> String {
    let (status, _, body) = sns(app, &format!("Action=CreateTopic&Name={name}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    tag(&body, "TopicArn")
}

async fn queue_with_arn(app: &Router, name: &str) -> (String, String) {
    let (status, created) = sqs(app, "CreateQueue", json!({"QueueName": name})).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let url = created["QueueUrl"].as_str().unwrap().to_owned();
    let (status, attributes) = sqs(
        app,
        "GetQueueAttributes",
        json!({"QueueUrl": url, "AttributeNames": ["QueueArn"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{attributes}");
    let arn = attributes["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_owned();
    (url, arn)
}

async fn subscribe_queue(app: &Router, topic_arn: &str, queue_arn: &str) {
    let (status, _, body) = sns(
        app,
        &format!("Action=Subscribe&TopicArn={topic_arn}&Protocol=sqs&Endpoint={queue_arn}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_saturated_admission_when_sns_publishes_then_its_attempt_queues_and_the_route_holds_no_permit()
 {
    let (app, _state, controller) = fixture(10_000).await;
    let arn = topic(&app, "held").await;
    let permits = saturate(&controller).await;
    let publish = {
        let app = app.clone();
        tokio::spawn(async move {
            sns(
                &app,
                &format!("Action=Publish&TopicArn={arn}&Message=queued"),
            )
            .await
        })
    };
    until_queued(&controller).await;
    assert_eq!(
        controller.snapshot().in_flight,
        permits.len(),
        "the route took a permit of its own"
    );
    drop(permits);
    let (status, _, body) = tokio::time::timeout(WAIT_LIMIT, publish)
        .await
        .expect("publish did not finish after admission resumed")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<MessageId>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_refused_attempt_when_sns_publishes_then_503_service_unavailable_with_retry_after() {
    let (app, _state, controller) = fixture(1).await;
    let arn = topic(&app, "refused").await;
    let permits = saturate(&controller).await;
    let (status, headers, body) = sns(
        &app,
        &format!("Action=Publish&TopicArn={arn}&Message=refused"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body.contains("<Code>ServiceUnavailable</Code>"), "{body}");
    assert!(!body.contains("dynamodb"), "{body}");
    let retry_after: u64 = headers
        .get("retry-after")
        .expect("retry-after header")
        .to_str()
        .unwrap()
        .parse()
        .expect("retry-after is whole seconds");
    assert!(retry_after >= 1);
    drop(permits);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_client_disconnects_during_publish_when_admission_resumes_then_publish_commits() {
    let (app, state, controller) = fixture(10_000).await;
    let (queue_url, queue_arn) = queue_with_arn(&app, "sink").await;
    let arn = topic(&app, "gone").await;
    subscribe_queue(&app, &arn, &queue_arn).await;
    let permits = saturate(&controller).await;
    let client = {
        let app = app.clone();
        tokio::spawn(async move {
            sns(
                &app,
                &format!("Action=Publish&TopicArn={arn}&Message=after-disconnect"),
            )
            .await
        })
    };
    until_queued(&controller).await;
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    drop(permits);

    let manager = state.pubsub_manager.as_ref().unwrap();
    let delivered = tokio::time::timeout(WAIT_LIMIT, async {
        let mut total = 0;
        while total < 1 {
            total += manager.process_due_deliveries(10).await.unwrap();
            if total < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        total
    })
    .await
    .expect("publish did not commit after its client disconnected");
    assert!(delivered >= 1);
    let (status, received) = sqs(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl": queue_url, "WaitTimeSeconds": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{received}");
    assert!(
        received["Messages"][0]["Body"]
            .as_str()
            .unwrap()
            .contains("after-disconnect"),
        "{received}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_queue_and_pubsub_when_delivery_started_then_a_waiting_receive_gets_the_published_message()
 {
    let (app, state, _controller) = fixture(10_000).await;
    state
        .messaging
        .as_ref()
        .unwrap()
        .start_delivery(&state.db_manager)
        .await
        .unwrap();
    let (queue_url, queue_arn) = queue_with_arn(&app, "live").await;
    let arn = topic(&app, "live").await;
    subscribe_queue(&app, &arn, &queue_arn).await;
    let receive = {
        let app = app.clone();
        tokio::spawn(async move {
            sqs(
                &app,
                "ReceiveMessage",
                json!({"QueueUrl": queue_url, "WaitTimeSeconds": 10}),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, _, body) = sns(
        &app,
        &format!("Action=Publish&TopicArn={arn}&Message=through-runner"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, received) = tokio::time::timeout(WAIT_LIMIT, receive)
        .await
        .expect("the waiting receive got no message from the delivery runner")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{received}");
    assert!(
        received["Messages"][0]["Body"]
            .as_str()
            .unwrap()
            .contains("through-runner"),
        "{received}"
    );
    state
        .db_manager
        .shutdown_background_services()
        .await
        .unwrap();
}

async fn topic_subscriptions(state: &AppState, arn: &str) -> Vec<pubsub::Subscription> {
    state
        .pubsub_manager
        .as_ref()
        .unwrap()
        .list_subscriptions(pubsub::ListSubscriptionsRequest {
            topic_arn: Some(pubsub::TopicArn::new(arn.to_owned()).unwrap()),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_pending_http_subscription_when_subscribe_url_is_fetched_with_get_then_it_is_confirmed()
 {
    let (app, state, _controller) = fixture(1_000).await;
    let arn = topic(&app, "get-confirm").await;
    let (status, _, body) = sns(
        &app,
        &format!("Action=Subscribe&TopicArn={arn}&Protocol=http&Endpoint=http://127.0.0.1:9/hook"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let subscriptions = topic_subscriptions(&state, &arn).await;
    assert_eq!(subscriptions.len(), 1);
    assert!(subscriptions[0].confirmation.pending_confirmation());
    let token = subscriptions[0].confirmation.token().unwrap().to_owned();
    let subscription_arn = subscriptions[0].subscription_arn.as_str().to_owned();

    let (status, body) = sns_get(
        &app,
        &format!("/pubsub?Action=ConfirmSubscription&TopicArn={arn}&Token={token}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<ConfirmSubscriptionResponse"), "{body}");
    assert_eq!(tag(&body, "SubscriptionArn"), subscription_arn);
    let subscriptions = topic_subscriptions(&state, &arn).await;
    assert_eq!(subscriptions.len(), 1);
    assert!(!subscriptions[0].confirmation.pending_confirmation());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_get_on_trailing_slash_route_when_listing_topics_then_it_answers_like_post() {
    let (app, _state, _controller) = fixture(1_000).await;
    let arn = topic(&app, "get-list").await;

    let (status, body) = sns_get(&app, "/pubsub/?Action=ListTopics").await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("<ListTopicsResponse"), "{body}");
    assert!(body.contains(&arn), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_get_with_unknown_token_or_no_query_then_errors_match_post() {
    let (app, _state, _controller) = fixture(1_000).await;
    let arn = topic(&app, "get-errors").await;
    let pairs = format!(
        "Action=ConfirmSubscription&TopicArn={arn}&Token=00000000-0000-7000-8000-000000000000"
    );

    let (get_status, get_body) = sns_get(&app, &format!("/pubsub?{pairs}")).await;
    let (post_status, _, post_body) = sns(&app, &pairs).await;
    assert_eq!(get_status, StatusCode::BAD_REQUEST, "{get_body}");
    assert_eq!(get_status, post_status, "{get_body} / {post_body}");
    assert!(
        get_body.contains("<Code>InvalidParameter</Code>"),
        "{get_body}"
    );
    assert!(
        post_body.contains("<Code>InvalidParameter</Code>"),
        "{post_body}"
    );

    let (get_status, get_body) = sns_get(&app, "/pubsub").await;
    let (post_status, _, post_body) = sns(&app, "").await;
    assert_eq!(get_status, post_status, "{get_body} / {post_body}");
    assert_eq!(tag(&get_body, "Code"), tag(&post_body, "Code"));
}
