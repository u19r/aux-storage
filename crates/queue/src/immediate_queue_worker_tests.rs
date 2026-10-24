#![cfg(feature = "sqlite")]

use std::{sync::Arc, time::Duration};

use bg_jobs_tokio::QueueOutcome;
use tokio::sync::Notify;

use crate::{
    CreateQueueRequest, DeleteMessageRequest, GetQueueAttributesRequest, ImmediateJobHandler,
    ImmediateJobQueueError, ImmediateJobQueueRuntime, ImmediateJobRunnerConfig,
    ReceiveMessageRequest, SendMessageRequest, queue_test_support_tests::create_sqlite_service,
};

struct HeldHandler {
    started: Notify,
    release: Notify,
    finished: Notify,
}

#[async_trait::async_trait]
impl ImmediateJobHandler for HeldHandler {
    async fn handle(&self, _: &str) -> Result<QueueOutcome, ImmediateJobQueueError> {
        self.started.notify_one();
        self.release.notified().await;
        self.finished.notify_one();
        Ok(QueueOutcome::Complete)
    }
}

#[tokio::test]
async fn given_a_handler_longer_than_visibility_when_the_queue_runner_executes_then_the_delivery_remains_exclusive()
 {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(create_sqlite_service(directory.path().join("queue.sqlite")).await);
    let queue_url = manager
        .create_queue(CreateQueueRequest {
            queue_name: "leased-jobs".into(),
            attributes: None,
        })
        .await
        .unwrap()
        .queue_url;
    manager
        .send_message(SendMessageRequest {
            queue_url: queue_url.clone(),
            message_body:
                r#"{"dispatch_id":"test","job_name":"test","table_name":"test","key":{}}"#.into(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    manager
        .send_message(SendMessageRequest {
            queue_url: queue_url.clone(),
            message_body: "waiting-delivery".into(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    let handler = Arc::new(HeldHandler {
        started: Notify::new(),
        release: Notify::new(),
        finished: Notify::new(),
    });
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let runner = ImmediateJobQueueRuntime {
        manager: manager.clone(),
        queue_url: queue_url.clone(),
    }
    .prepare_runner(
        handler.clone(),
        ImmediateJobRunnerConfig {
            maximum_job_workers: 0,
            visibility_timeout_secs: 3,
            ..ImmediateJobRunnerConfig::default()
        },
    )
    .await
    .unwrap();
    let running = tokio::spawn(runner.run_until_shutdown(receiver));
    tokio::time::timeout(Duration::from_secs(10), handler.started.notified())
        .await
        .unwrap();
    let backlog = manager
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.clone(),
            max_number_of_messages: Some(10),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    for message in &backlog.messages {
        manager
            .delete_message(DeleteMessageRequest {
                queue_url: queue_url.clone(),
                receipt_handle: message.receipt_handle.as_str().into(),
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    let competing = manager
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.clone(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(3),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    for message in &competing.messages {
        manager
            .delete_message(DeleteMessageRequest {
                queue_url: queue_url.clone(),
                receipt_handle: message.receipt_handle.as_str().into(),
            })
            .await
            .unwrap();
    }
    shutdown.send(true).unwrap();
    handler.release.notify_one();
    let cleared = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let attributes = manager
                .get_queue_attributes(GetQueueAttributesRequest {
                    queue_url: queue_url.clone(),
                    attribute_names: None,
                })
                .await
                .unwrap()
                .attributes;
            if attributes["ApproximateNumberOfMessages"] == "0"
                && attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("native queue runner stops after draining the handler")
        .unwrap()
        .unwrap();
    assert_eq!(
        backlog.messages.len(),
        1,
        "a busy worker leased waiting work without renewing it"
    );
    assert_eq!(backlog.messages[0].body, "waiting-delivery");
    assert!(
        competing.messages.is_empty(),
        "active delivery became visible to a competing receiver"
    );
    cleared.expect("completed delivery is acknowledged through the native queue");
}

struct FailingHandler(std::sync::atomic::AtomicUsize);

#[tokio::test]
async fn given_a_queue_backlog_when_failure_diagnostics_load_then_counts_include_visible_in_flight_and_delayed_work()
 {
    let directory = tempfile::tempdir().unwrap();
    let (manager, queue_url, dead_letter_url) =
        prepare_retry_queues(&directory.path().join("diagnostics.sqlite")).await;
    for delay in [None, Some(60)] {
        manager
            .send_message(SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "diagnostic fixture".into(),
                delay_seconds: delay,
                message_attributes: None,
            })
            .await
            .unwrap();
    }
    manager
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.clone(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(60),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    let worker = super::immediate_queue_worker::ImmediateQueueWorker::new(
        ImmediateJobQueueRuntime {
            manager: manager.clone(),
            queue_url: queue_url.clone(),
        },
        Arc::new(FailingHandler(Default::default())),
        ImmediateJobRunnerConfig::default(),
        dead_letter_url,
    );
    let counts = worker.load_failure_backlog().await.unwrap();
    assert_eq!(
        (counts.visible, counts.not_visible, counts.delayed),
        (1, 1, 1)
    );
    manager
        .delete_queue(crate::DeleteQueueRequest { queue_url })
        .await
        .unwrap();
    assert!(worker.load_failure_backlog().await.is_none());
}

#[async_trait::async_trait]
impl ImmediateJobHandler for FailingHandler {
    async fn handle(&self, _: &str) -> Result<QueueOutcome, ImmediateJobQueueError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(ImmediateJobQueueError::message("retryable"))
    }
}

async fn prepare_retry_queues(
    path: &std::path::Path,
) -> (Arc<crate::QueueManager>, String, String) {
    let manager = Arc::new(create_sqlite_service(path).await);
    let mut urls = Vec::new();
    for name in ["retry-source", "retry-dlq"] {
        urls.push(
            manager
                .create_queue(CreateQueueRequest {
                    queue_name: name.into(),
                    attributes: None,
                })
                .await
                .unwrap()
                .queue_url,
        );
    }
    manager
        .send_message(SendMessageRequest {
            queue_url: urls[0].clone(),
            message_body: "retained-work".into(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    (manager, urls.remove(0), urls.remove(0))
}

async fn release_delivery(
    manager: &crate::QueueManager,
    source: &str,
    message: &crate::MessageResponse,
) {
    manager
        .change_message_visibility(crate::ChangeMessageVisibilityRequest {
            queue_url: source.into(),
            receipt_handle: message.receipt_handle.as_str().into(),
            visibility_timeout: 0,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn given_repeated_failures_across_worker_restarts_when_attempts_exhaust_then_dlq_accepts_before_source_deletion()
 {
    use bg_jobs_tokio::QueueWorker;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retry.sqlite");
    let (manager, source, dlq) = prepare_retry_queues(&path).await;
    let handler = Arc::new(FailingHandler(Default::default()));
    let mut delays = Vec::new();
    let mut original_sent = None;
    for attempt in 1..=3 {
        let manager = Arc::new(create_sqlite_service(&path).await);
        let worker = crate::immediate_queue_worker::ImmediateQueueWorker::new(
            ImmediateJobQueueRuntime {
                manager: manager.clone(),
                queue_url: source.clone(),
            },
            handler.clone(),
            ImmediateJobRunnerConfig {
                maximum_attempts: std::num::NonZeroU32::new(3).unwrap(),
                ..Default::default()
            },
            dlq.clone(),
        );
        let message = worker.receive().await.unwrap().unwrap();
        assert_eq!(
            message.attributes.as_ref().unwrap()["ApproximateReceiveCount"],
            attempt.to_string()
        );
        let sent = message.attributes.as_ref().unwrap()["SentTimestamp"].clone();
        if attempt == 1 {
            original_sent = Some(sent.clone());
        }
        assert_eq!(Some(&sent), original_sent.as_ref());
        let outcome = worker.execute(&message).await.unwrap();
        if attempt < 3 {
            let QueueOutcome::RetryAfter(delay) = outcome else {
                panic!("must retry")
            };
            delays.push(delay);
        } else {
            assert_eq!(outcome, QueueOutcome::Complete);
        }
        worker.settle(&message, outcome).await;
        if attempt < 3 {
            release_delivery(&manager, &source, &message).await;
        }
    }
    assert!(delays[1] >= delays[0]);
    assert_queue_empty(&manager, &source).await;
    assert_eq!(handler.0.load(std::sync::atomic::Ordering::Relaxed), 3);
    let received = manager
        .receive_message(ReceiveMessageRequest {
            queue_url: dlq,
            wait_time_seconds: Some(0),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(30),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert_eq!(received.messages.len(), 1);
    let envelope: serde_json::Value = serde_json::from_str(&received.messages[0].body).unwrap();
    assert_eq!(envelope["body"], "retained-work");
    assert_eq!(envelope["receive_count"], 3);
    assert_eq!(envelope["attempt_limit"], 3);
    assert_eq!(envelope["reason"], "attempts_exhausted");
    assert_eq!(
        envelope["original_attributes"]["SentTimestamp"].as_str(),
        original_sent.as_deref()
    );
    assert!(
        manager
            .receive_message(ReceiveMessageRequest {
                queue_url: source,
                wait_time_seconds: Some(0),
                max_number_of_messages: Some(1),
                visibility_timeout: Some(30),
                attribute_names: None,
                message_attribute_names: None
            })
            .await
            .unwrap()
            .messages
            .is_empty()
    );
}

#[tokio::test]
async fn given_failed_dlq_publication_when_redelivered_then_source_survives_and_business_attempts_do_not_repeat()
 {
    use bg_jobs_tokio::QueueWorker;
    let directory = tempfile::tempdir().unwrap();
    let (manager, source, dlq) =
        prepare_retry_queues(&directory.path().join("failed-dlq.sqlite")).await;
    let handler = Arc::new(FailingHandler(Default::default()));
    for target in ["unavailable-dlq".to_owned(), dlq.clone()] {
        let worker = crate::immediate_queue_worker::ImmediateQueueWorker::new(
            ImmediateJobQueueRuntime {
                manager: manager.clone(),
                queue_url: source.clone(),
            },
            handler.clone(),
            ImmediateJobRunnerConfig {
                maximum_attempts: std::num::NonZeroU32::MIN,
                ..Default::default()
            },
            target.clone(),
        );
        let message = worker.receive().await.unwrap().unwrap();
        let outcome = worker.execute(&message).await.unwrap();
        if target == dlq {
            assert_eq!(outcome, QueueOutcome::Complete);
        } else {
            assert!(matches!(outcome, QueueOutcome::RetryAfter(_)));
        }
        worker.settle(&message, outcome).await;
        if target != dlq {
            release_delivery(&manager, &source, &message).await;
        }
    }
    assert_eq!(handler.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        manager
            .receive_message(ReceiveMessageRequest {
                queue_url: dlq,
                wait_time_seconds: Some(0),
                max_number_of_messages: Some(1),
                visibility_timeout: Some(30),
                attribute_names: None,
                message_attribute_names: None
            })
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
}

#[tokio::test]
async fn given_dlq_acceptance_without_source_ack_when_the_delivery_reappears_then_only_dlq_handoff_replays()
 {
    use bg_jobs_tokio::QueueWorker;
    let directory = tempfile::tempdir().unwrap();
    let (manager, source, dlq) =
        prepare_retry_queues(&directory.path().join("unacked.sqlite")).await;
    let handler = Arc::new(FailingHandler(Default::default()));
    let worker = crate::immediate_queue_worker::ImmediateQueueWorker::new(
        ImmediateJobQueueRuntime {
            manager: manager.clone(),
            queue_url: source.clone(),
        },
        handler.clone(),
        ImmediateJobRunnerConfig {
            maximum_attempts: std::num::NonZeroU32::MIN,
            ..Default::default()
        },
        dlq.clone(),
    );
    let first = worker.receive().await.unwrap().unwrap();
    assert_eq!(worker.execute(&first).await, Some(QueueOutcome::Complete));
    // A crash after publication leaves the original delivery unacknowledged.
    release_delivery(&manager, &source, &first).await;
    let replay = worker.receive().await.unwrap().unwrap();
    assert_eq!(replay.message_id, first.message_id);
    let outcome = worker.execute(&replay).await.unwrap();
    assert_eq!(outcome, QueueOutcome::Complete);
    worker.settle(&replay, outcome).await;
    assert_queue_empty(&manager, &source).await;
    assert_eq!(handler.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    let copies = manager
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
    assert_eq!(copies.messages.len(), 2);
    for copy in copies.messages {
        let envelope: serde_json::Value = serde_json::from_str(&copy.body).unwrap();
        assert_eq!(envelope["message_id"], first.message_id);
        assert_eq!(envelope["body"], "retained-work");
    }
}

struct SuccessfulHandler;

#[async_trait::async_trait]
impl ImmediateJobHandler for SuccessfulHandler {
    async fn handle(&self, _: &str) -> Result<QueueOutcome, ImmediateJobQueueError> {
        Ok(QueueOutcome::Complete)
    }
}

#[tokio::test]
async fn given_success_on_the_last_allowed_attempt_when_settling_then_work_completes_without_dlq_publication()
 {
    use bg_jobs_tokio::QueueWorker;
    let directory = tempfile::tempdir().unwrap();
    let (manager, source, dlq) =
        prepare_retry_queues(&directory.path().join("success.sqlite")).await;
    let worker = crate::immediate_queue_worker::ImmediateQueueWorker::new(
        ImmediateJobQueueRuntime {
            manager: manager.clone(),
            queue_url: source.clone(),
        },
        Arc::new(SuccessfulHandler),
        ImmediateJobRunnerConfig {
            maximum_attempts: std::num::NonZeroU32::MIN,
            ..Default::default()
        },
        dlq.clone(),
    );
    let message = worker.receive().await.unwrap().unwrap();
    let outcome = worker.execute(&message).await.unwrap();
    assert_eq!(outcome, QueueOutcome::Complete);
    worker.settle(&message, outcome).await;
    assert_queue_empty(&manager, &source).await;
    let received = manager
        .receive_message(ReceiveMessageRequest {
            queue_url: dlq,
            max_number_of_messages: Some(1),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert!(received.messages.is_empty());
}

async fn assert_queue_empty(manager: &crate::QueueManager, queue_url: &str) {
    let attributes = manager
        .get_queue_attributes(GetQueueAttributesRequest {
            queue_url: queue_url.into(),
            attribute_names: Some(vec!["All".into()]),
        })
        .await
        .unwrap()
        .attributes;
    for key in [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ] {
        assert_eq!(attributes[key], "0", "{key}");
    }
}
