use std::{collections::HashMap, time::Duration};

use crate::{
    QueueError, QueueInternalKind, QueueValidationKind,
    constants::{
        JOB_POLL_BATCH_SIZE, MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS, MINIMUM_JOB_WORKERS,
        QUEUE_ATTRIBUTE_SENT_TIMESTAMP, SCALE_DOWN_STREAK_SECONDS,
    },
    immediate_jobs::{
        ImmediateJobRunnerConfig, immediate_job_receive_request, immediate_job_receive_retry_delay,
        normalized_max_job_workers, queue_message_delay_ms_at, retry_visibility_timeout_secs,
        scale_down_threshold, should_count_underutilized_second, should_scale_down,
        should_scale_up, wait_for_immediate_job_receive_retry,
    },
};

#[test]
fn immediate_job_runner_config_uses_safe_defaults() {
    let config = ImmediateJobRunnerConfig::default();

    assert!(config.maximum_job_workers >= MINIMUM_JOB_WORKERS);
    assert!(config.visibility_timeout_secs > 0);
}

#[test]
fn immediate_job_runner_never_starts_below_minimum_workers() {
    assert_eq!(normalized_max_job_workers(0), MINIMUM_JOB_WORKERS);
    assert_eq!(
        normalized_max_job_workers(MINIMUM_JOB_WORKERS + 3),
        MINIMUM_JOB_WORKERS + 3
    );
}

#[test]
fn immediate_job_receive_request_polls_job_queue_with_sent_timestamp_attribute() {
    let request = immediate_job_receive_request("queue-url".to_string(), 45);

    assert_eq!(request.queue_url, "queue-url");
    assert_eq!(request.max_number_of_messages, Some(JOB_POLL_BATCH_SIZE));
    assert_eq!(request.visibility_timeout, Some(45));
    assert_eq!(request.wait_time_seconds, Some(1));
    assert_eq!(
        request.attribute_names,
        Some(vec![QUEUE_ATTRIBUTE_SENT_TIMESTAMP.to_string()])
    );
    assert_eq!(request.message_attribute_names, None);
}

#[test]
fn immediate_job_receive_retry_distinguishes_sender_and_backend_faults() {
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
}

#[tokio::test]
async fn immediate_job_receive_retry_stops_without_waiting_for_delay() {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    stop_tx
        .send(true)
        .expect("worker stop receiver remains open");

    assert!(
        wait_for_immediate_job_receive_retry(
            &QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat),
            &mut stop_rx
        )
        .await
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
fn immediate_job_scaling_spawns_only_when_worker_saw_full_batch_and_capacity_remains() {
    assert!(should_scale_up(
        true,
        MINIMUM_JOB_WORKERS,
        MINIMUM_JOB_WORKERS + 1
    ));
    assert!(!should_scale_up(
        false,
        MINIMUM_JOB_WORKERS,
        MINIMUM_JOB_WORKERS + 1
    ));
    assert!(!should_scale_up(
        true,
        MINIMUM_JOB_WORKERS,
        MINIMUM_JOB_WORKERS
    ));
}

#[test]
fn immediate_job_scaling_counts_underutilized_seconds_before_scale_down() {
    let worker_count = MINIMUM_JOB_WORKERS + 2;
    let threshold = scale_down_threshold(worker_count);

    assert!(should_count_underutilized_second(
        worker_count,
        threshold.saturating_sub(1)
    ));
    assert!(!should_count_underutilized_second(worker_count, threshold));
    assert!(!should_count_underutilized_second(MINIMUM_JOB_WORKERS, 0));
    assert!(should_scale_down(SCALE_DOWN_STREAK_SECONDS, worker_count));
    assert!(!should_scale_down(
        SCALE_DOWN_STREAK_SECONDS - 1,
        worker_count
    ));
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
