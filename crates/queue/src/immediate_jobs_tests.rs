use std::{collections::HashMap, time::Duration};

use crate::{
    QueueError, QueueInternalKind, QueueValidationKind,
    constants::{MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS, QUEUE_ATTRIBUTE_SENT_TIMESTAMP},
    immediate_jobs::ImmediateJobRunnerConfig,
    immediate_queue_worker::{
        immediate_job_receive_request, immediate_job_receive_retry_delay,
        queue_message_delay_ms_at, retry_visibility_timeout_secs,
    },
};

#[test]
fn immediate_job_runner_config_uses_safe_defaults() {
    let config = ImmediateJobRunnerConfig::default();

    assert!(config.maximum_job_workers >= 1);
    assert!(config.visibility_timeout_secs > 0);
    assert_eq!(config.maximum_attempts.get(), 180);
}

#[test]
fn immediate_job_receive_request_polls_job_queue_with_sent_timestamp_attribute() {
    let request = immediate_job_receive_request("queue-url".to_string(), 45);

    assert_eq!(request.queue_url, "queue-url");
    assert_eq!(request.max_number_of_messages, Some(1));
    assert_eq!(request.visibility_timeout, Some(45));
    assert_eq!(request.wait_time_seconds, Some(1));
    assert_eq!(
        request.attribute_names,
        Some(vec![
            QUEUE_ATTRIBUTE_SENT_TIMESTAMP.to_string(),
            crate::constants::QUEUE_ATTRIBUTE_RECEIVE_COUNT.to_string()
        ])
    );
    assert_eq!(request.message_attribute_names, None);
}

#[tokio::test]
async fn immediate_job_receive_retry_distinguishes_sender_and_backend_faults() {
    assert_eq!(
        immediate_job_receive_retry_delay(&QueueError::validation(
            QueueValidationKind::InvalidQueueUrlFormat
        )),
        Duration::from_secs(30)
    );
    assert_eq!(
        immediate_job_receive_retry_delay(&QueueError::internal(
            QueueInternalKind::ReceiveCoalescerClosed
        )),
        Duration::from_secs(1)
    );

    let error = QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat);
    let (sender, mut receiver) = tokio::sync::watch::channel(false);
    sender.send(true).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            crate::wait_for_immediate_job_receive_retry(&error, &mut receiver),
        )
        .await
        .expect("shutdown interrupts the 30-second sender-fault backoff")
    );
    let (sender, mut receiver) = tokio::sync::watch::channel(false);
    drop(sender);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            crate::wait_for_immediate_job_receive_retry(&error, &mut receiver),
        )
        .await
        .expect("a closed shutdown channel interrupts the retry backoff")
    );
}

#[test]
fn immediate_job_retry_visibility_timeout_is_capped_for_large_delays() {
    assert_eq!(retry_visibility_timeout_secs(Duration::from_secs(17)), 17);
    assert_eq!(
        retry_visibility_timeout_secs(Duration::from_secs(u64::MAX)),
        MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS
    );
}

#[test]
fn immediate_job_message_delay_uses_saturating_sent_timestamp_delta() {
    let mut attributes = HashMap::new();
    attributes.insert(
        QUEUE_ATTRIBUTE_SENT_TIMESTAMP.to_string(),
        "1000".to_string(),
    );

    assert_eq!(queue_message_delay_ms_at(&attributes, 1250), Some(250));
    assert_eq!(queue_message_delay_ms_at(&attributes, 750), Some(-250));

    attributes.insert(
        QUEUE_ATTRIBUTE_SENT_TIMESTAMP.to_string(),
        "bad".to_string(),
    );
    assert_eq!(queue_message_delay_ms_at(&attributes, 1250), None);
}

#[test]
fn given_old_work_when_retrying_then_backoff_grows_with_bounded_jitter() {
    let message: crate::MessageResponse = serde_json::from_value(serde_json::json!({
        "MessageId": "stable-job", "ReceiptHandle": "receipt", "MD5OfBody": "digest", "Body": "work"
    }))
    .unwrap();
    for (attempt, cap) in [(1, 1), (2, 2), (3, 4), (9, 256), (10, 300), (180, 300)] {
        let delay = crate::immediate_queue_worker::delivery_backoff(&message, attempt);
        assert!(
            (Duration::from_secs((cap * 80 / 100).max(1))..=Duration::from_secs(cap))
                .contains(&delay)
        );
    }
    for (streak, cap) in [
        (1, 1000),
        (2, 2000),
        (4, 8000),
        (100, 30_000),
        (u32::MAX, 30_000),
    ] {
        assert!(
            (cap * 80 / 100..=cap)
                .contains(&crate::immediate_queue_worker::empty_poll_delay_ms(streak))
        );
    }
}
