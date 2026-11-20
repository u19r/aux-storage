//! Waiting receives hold no admission permit and no ingress slot.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use storage::DatabaseManager;
use tower::ServiceExt;

use crate::{AppState, router::server_router};

async fn call(app: &Router, target: &str, content_type: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(if target.starts_with("AmazonSQS.") {
                    "/queue"
                } else {
                    "/"
                })
                .header("content-type", content_type)
                .header("x-amz-target", target)
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

async fn sqs(app: &Router, action: &str, body: Value) -> (StatusCode, Value) {
    call(
        app,
        &format!("AmazonSQS.{action}"),
        "application/x-amz-json-1.0",
        body,
    )
    .await
}

async fn dynamodb(app: &Router, action: &str, body: Value) -> (StatusCode, Value) {
    call(
        app,
        &format!("DynamoDB_20120810.{action}"),
        "application/x-amz-json-1.0",
        body,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_waiting_receives_when_dynamodb_writes_then_no_permit_or_ingress_slot_is_held() {
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
    let state = Arc::new(
        AppState::new(Arc::clone(&database)).with_messaging(Some(Arc::clone(&manager)), None),
    );
    let app = server_router(Arc::clone(&state), false);
    let controller = database.default_admission_controller();
    let ingress_limit = database.fixed_ingress_limit();

    let (status, created) = sqs(&app, "CreateQueue", json!({"QueueName": "idle"})).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let queue_url = created["QueueUrl"].as_str().unwrap().to_owned();
    let (status, table) = dynamodb(
        &app,
        "CreateTable",
        json!({
            "TableName": "writes",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{table}");

    let waiting = 16;
    let receives = (0..waiting)
        .map(|_| {
            let app = app.clone();
            let queue_url = queue_url.clone();
            tokio::spawn(async move {
                sqs(
                    &app,
                    "ReceiveMessage",
                    json!({"QueueUrl": queue_url, "WaitTimeSeconds": 5}),
                )
                .await
            })
        })
        .collect::<Vec<_>>();

    // Every receive has passed its probe and claim phase and now waits.
    tokio::time::timeout(Duration::from_secs(5), async {
        while manager.waiting_receives() < waiting {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("receives never started to wait");
    for index in 0..10 {
        assert_eq!(manager.waiting_receives(), waiting);
        // A probe may hold one point-read permit for its own attempt.
        let admission = controller.snapshot();
        assert!(admission.in_flight <= 1, "{admission:?}");
        assert_eq!(state.ingress_semaphore.available_permits(), ingress_limit);
        let (status, put) = dynamodb(
            &app,
            "PutItem",
            json!({"TableName": "writes", "Item": {"pk": {"S": format!("item-{index}")}}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{put}");
    }

    for receive in receives {
        let (status, response) = receive.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{response}");
        assert!(
            response["Messages"]
                .as_array()
                .is_none_or(|messages| messages.is_empty())
        );
    }
    let admission = controller.snapshot();
    assert_eq!(admission.in_flight, 0);
    assert_eq!(admission.rejection_count, 0);
}

/// Hold every admission permit, also those that finishing attempts release,
/// so that the next storage attempt queues.
async fn saturate(
    controller: &storage::admission::AdmissionController,
) -> Vec<storage::AdmissionPermit> {
    let mut permits = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
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

async fn until_queued(controller: &storage::admission::AdmissionController) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while controller.snapshot().queue_depth == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("no attempt queued for admission");
}

struct Fixture {
    app: Router,
    state: Arc<AppState>,
    manager: Arc<queue::QueueManager>,
    controller: storage::admission::AdmissionController,
    ingress_limit: usize,
}

/// Eight fixed foreground permits (twelve less the control reserve); a queued
/// attempt waits up to 10 s, so a test controls when it resumes.
async fn fixture(probe_interval: Duration) -> Fixture {
    let database = Arc::new(
        DatabaseManager::new_for_test_with_runtime_options(
            storage::DatabaseManagerRuntimeOptions::builder()
                .admission_config(storage::AdmissionConfig {
                    // A fixed limit: the controller does not grow while a
                    // test holds every permit.
                    minimum_concurrency: 8,
                    maximum_concurrency: 12,
                    max_queue_wait_ms: 10_000,
                    ..Default::default()
                })
                .build(),
        )
        .await
        .unwrap(),
    );
    let manager = Arc::new(
        queue::QueueManager::new(
            database
                .messaging_store(storage::messaging::MessagingLane::Customer)
                .map(|store| store.queue)
                .unwrap(),
        )
        .with_receive_probe_interval(probe_interval)
        .with_endpoint(queue::QueueEndpoint {
            public_base_url: "http://localhost/queue".into(),
        }),
    );
    let state = Arc::new(
        AppState::new(Arc::clone(&database)).with_messaging(Some(Arc::clone(&manager)), None),
    );
    Fixture {
        app: server_router(Arc::clone(&state), false),
        state,
        manager,
        controller: database.default_admission_controller(),
        ingress_limit: database.fixed_ingress_limit(),
    }
}

/// W1 follow-up: a short poll never waits, so it keeps its ingress slot
/// until its response, also while its storage attempt queues for admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_short_poll_when_its_attempt_queues_for_admission_then_it_holds_its_ingress_slot() {
    let fixture = fixture(Duration::from_millis(200)).await;
    let (status, created) = sqs(&fixture.app, "CreateQueue", json!({"QueueName": "short"})).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let queue_url = created["QueueUrl"].as_str().unwrap().to_owned();

    let permits = saturate(&fixture.controller).await;
    let poll = {
        let app = fixture.app.clone();
        tokio::spawn(async move {
            sqs(
                &app,
                "ReceiveMessage",
                json!({"QueueUrl": queue_url, "WaitTimeSeconds": 0}),
            )
            .await
        })
    };
    until_queued(&fixture.controller).await;
    assert_eq!(
        fixture.state.ingress_semaphore.available_permits(),
        fixture.ingress_limit - 1,
        "the short poll released its ingress slot before its response"
    );
    drop(permits);
    let (status, response) = poll.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(
        fixture.state.ingress_semaphore.available_permits(),
        fixture.ingress_limit
    );
    assert_eq!(fixture.manager.waiting_receives(), 0);
}

/// W1 follow-up: a send whose client disconnects while its attempt waits
/// for admission still commits and wakes a waiting receive. The probe
/// interval is longer than the test, so only the local signal can deliver.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_client_disconnects_during_send_when_admission_resumes_then_send_commits_and_signals()
{
    let fixture = fixture(Duration::from_secs(60)).await;
    let (status, created) = sqs(&fixture.app, "CreateQueue", json!({"QueueName": "gone"})).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let queue_url = created["QueueUrl"].as_str().unwrap().to_owned();
    let waiter = {
        let app = fixture.app.clone();
        let queue_url = queue_url.clone();
        tokio::spawn(async move {
            sqs(
                &app,
                "ReceiveMessage",
                json!({"QueueUrl": queue_url, "WaitTimeSeconds": 10}),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.manager.waiting_receives() < 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("receive never started to wait");

    let permits = saturate(&fixture.controller).await;
    let client = {
        let app = fixture.app.clone();
        tokio::spawn(async move {
            sqs(
                &app,
                "SendMessage",
                json!({"QueueUrl": queue_url, "MessageBody": "after disconnect"}),
            )
            .await
        })
    };
    until_queued(&fixture.controller).await;
    // The client disconnects: its request future is dropped.
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    drop(permits);

    let started = std::time::Instant::now();
    let (status, response) = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the waiting receive was not woken")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["Messages"][0]["Body"], "after disconnect");
    assert!(started.elapsed() < Duration::from_secs(5));
}
