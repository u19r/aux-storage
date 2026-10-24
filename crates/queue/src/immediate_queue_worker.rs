use std::{collections::HashMap, sync::Arc, time::Duration};

use bg_jobs_tokio::{QueueOutcome, QueueWorker};
use metrics_facade::{counter, histogram};

use crate::{
    ChangeMessageVisibilityRequest, DeleteMessageRequest, ImmediateJobHandler,
    ImmediateJobQueueRuntime, MessageResponse, QueueError, ReceiveMessageRequest,
    constants::{
        MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS, METRIC_QUEUE_EMPTY_RECEIVES_TOTAL,
        METRIC_QUEUE_MESSAGE_DELAY_MS, QUEUE_ATTRIBUTE_SENT_TIMESTAMP,
        SENDER_FAULT_RECEIVE_RETRY_DELAY, TRANSIENT_RECEIVE_RETRY_DELAY,
    },
};

pub(super) struct ImmediateQueueWorker {
    runtime: ImmediateJobQueueRuntime,
    handler: Arc<dyn ImmediateJobHandler>,
    config: crate::ImmediateJobRunnerConfig,
    dead_letter_queue_url: String,
    empty_polls: std::sync::atomic::AtomicU32,
}

impl ImmediateQueueWorker {
    pub(super) fn new(
        runtime: ImmediateJobQueueRuntime,
        handler: Arc<dyn ImmediateJobHandler>,
        config: crate::ImmediateJobRunnerConfig,
        dead_letter_queue_url: String,
    ) -> Self {
        Self {
            runtime,
            handler,
            config,
            dead_letter_queue_url,
            empty_polls: Default::default(),
        }
    }

    async fn change_visibility(&self, message: &MessageResponse, seconds: u32) -> bool {
        match self
            .runtime
            .manager
            .change_message_visibility(ChangeMessageVisibilityRequest {
                queue_url: self.runtime.queue_url.clone(),
                receipt_handle: message.receipt_handle.as_str().into(),
                visibility_timeout: seconds,
            })
            .await
        {
            Ok(_) => true,
            Err(_) => {
                tracing::warn!("immediate job visibility update failed");
                false
            }
        }
    }

    fn defer_empty_poll(&self) -> Duration {
        counter!(METRIC_QUEUE_EMPTY_RECEIVES_TOTAL, "runner" => "local").increment(1);
        let streak = self
            .empty_polls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        Duration::from_millis(empty_poll_delay_ms(streak))
    }

    async fn execute_delivery(&self, message: &MessageResponse) -> Option<QueueOutcome> {
        let Some(attempt) = delivery_attempt(message) else {
            tracing::warn!("immediate job receive count missing or invalid");
            return Some(QueueOutcome::RetryAfter(SENDER_FAULT_RECEIVE_RETRY_DELAY));
        };
        let retry = delivery_backoff(message, attempt);
        if attempt > self.config.maximum_attempts.get() {
            return Some(self.dead_letter_delivery(message, attempt, retry).await);
        }
        let outcome = match self.handler.handle(&message.body).await {
            Ok(QueueOutcome::Complete) => return Some(QueueOutcome::Complete),
            Ok(QueueOutcome::RetryAfter(delay)) => QueueOutcome::RetryAfter(delay.max(retry)),
            Err(error) => {
                self.log_handler_failure(attempt, &error).await;
                QueueOutcome::RetryAfter(retry)
            }
        };
        if attempt == self.config.maximum_attempts.get() {
            return Some(self.dead_letter_delivery(message, attempt, retry).await);
        }
        Some(outcome)
    }

    async fn log_handler_failure(&self, attempt: u32, error: &crate::ImmediateJobQueueError) {
        let counts = self.load_failure_backlog().await;
        tracing::warn!(
            attempt,
            error = %error,
            queue_counts_available = counts.is_some(),
            queue_visible = counts.as_ref().map(|counts| counts.visible),
            queue_in_flight = counts.as_ref().map(|counts| counts.not_visible),
            queue_delayed = counts.as_ref().map(|counts| counts.delayed),
            "immediate job handler failed"
        );
    }

    // Failure-only diagnostics must not indefinitely delay settlement or
    // retries.
    pub(super) async fn load_failure_backlog(&self) -> Option<crate::QueueMessageCounts> {
        let response = tokio::time::timeout(
            Duration::from_millis(250),
            self.runtime
                .manager
                .get_queue_attributes(crate::GetQueueAttributesRequest {
                    queue_url: self.runtime.queue_url.clone(),
                    attribute_names: Some(vec![
                        "ApproximateNumberOfMessages".into(),
                        "ApproximateNumberOfMessagesNotVisible".into(),
                        "ApproximateNumberOfMessagesDelayed".into(),
                    ]),
                }),
        )
        .await
        .ok()?
        .ok()?;
        let parse_count = |name| response.attributes.get(name)?.parse().ok();
        Some(crate::QueueMessageCounts {
            visible: parse_count("ApproximateNumberOfMessages")?,
            not_visible: parse_count("ApproximateNumberOfMessagesNotVisible")?,
            delayed: parse_count("ApproximateNumberOfMessagesDelayed")?,
        })
    }

    async fn dead_letter_delivery(
        &self,
        message: &MessageResponse,
        attempt: u32,
        retry: Duration,
    ) -> QueueOutcome {
        let body = serde_json::json!({
            "source_queue_url": self.runtime.queue_url,
            "message_id": message.message_id,
            "receive_count": attempt,
            "attempt_limit": self.config.maximum_attempts.get(),
            "reason": "attempts_exhausted",
            "original_attributes": message.attributes,
            "message_attributes": message.message_attributes,
            "body": message.body,
        });
        let published = self
            .runtime
            .manager
            .send_message(crate::SendMessageRequest {
                queue_url: self.dead_letter_queue_url.clone(),
                message_body: body.to_string(),
                delay_seconds: None,
                message_attributes: None,
            })
            .await;
        match published {
            Ok(_) => {
                tracing::warn!(attempt, "immediate job sent to dead letter queue");
                QueueOutcome::Complete
            }
            Err(_) => {
                tracing::warn!("immediate job dead letter publication failed; source retained");
                QueueOutcome::RetryAfter(retry)
            }
        }
    }
}

impl QueueWorker for ImmediateQueueWorker {
    type Message = MessageResponse;

    async fn receive(&self) -> Result<Option<MessageResponse>, Duration> {
        match self
            .runtime
            .manager
            .receive_message(immediate_job_receive_request(
                self.runtime.queue_url.clone(),
                self.config.visibility_timeout_secs,
            ))
            .await
        {
            Ok(response) => {
                let Some(message) = response.messages.into_iter().next() else {
                    return Err(self.defer_empty_poll());
                };
                self.empty_polls
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                Ok(Some(message))
            }
            Err(error) => {
                tracing::warn!("immediate job queue receive failed");
                Err(immediate_job_receive_retry_delay(&error))
            }
        }
    }

    async fn execute(&self, message: &MessageResponse) -> Option<QueueOutcome> {
        if let Some(delay) = message.attributes.as_ref().and_then(|attributes| {
            queue_message_delay_ms_at(
                attributes,
                storage_types::TimestampMillis::now().timestamp_millis(),
            )
        }) {
            histogram!(METRIC_QUEUE_MESSAGE_DELAY_MS, "runner" => "local").record(delay as f64);
        }
        self.execute_delivery(message).await
    }

    async fn renew(&self, message: &MessageResponse) -> bool {
        self.change_visibility(message, self.config.visibility_timeout_secs)
            .await
    }

    async fn settle(&self, message: &MessageResponse, outcome: QueueOutcome) {
        match outcome {
            QueueOutcome::Complete => {
                if self
                    .runtime
                    .manager
                    .delete_message(DeleteMessageRequest {
                        queue_url: self.runtime.queue_url.clone(),
                        receipt_handle: message.receipt_handle.as_str().into(),
                    })
                    .await
                    .is_err()
                {
                    tracing::warn!("immediate job queue delete failed");
                    if let Some(attempt) = delivery_attempt(message) {
                        self.change_visibility(
                            message,
                            retry_visibility_timeout_secs(delivery_backoff(message, attempt)),
                        )
                        .await;
                    }
                }
            }
            QueueOutcome::RetryAfter(delay) => {
                self.change_visibility(message, retry_visibility_timeout_secs(delay))
                    .await;
            }
        }
    }
}

pub(super) fn immediate_job_receive_retry_delay(error: &QueueError) -> Duration {
    if error.is_sender_fault() {
        SENDER_FAULT_RECEIVE_RETRY_DELAY
    } else {
        TRANSIENT_RECEIVE_RETRY_DELAY
    }
}

/// Keeps the HTTP adaptive queue worker on the shared receive retry policy.
/// Returns whether its shutdown channel requests stopping the worker.
pub async fn wait_for_immediate_job_receive_retry(
    error: &QueueError,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let retry_delay = immediate_job_receive_retry_delay(error);
    tracing::warn!(
        retry_delay_ms = retry_delay.as_millis(),
        "immediate job queue receive failed"
    );
    tokio::select! {
        () = tokio::time::sleep(retry_delay) => false,
        changed = stop_rx.changed() => changed.is_err() || *stop_rx.borrow(),
    }
}

pub(super) fn immediate_job_receive_request(
    queue_url: String,
    visibility_timeout_secs: u32,
) -> ReceiveMessageRequest {
    ReceiveMessageRequest {
        queue_url,
        max_number_of_messages: Some(1),
        visibility_timeout: Some(visibility_timeout_secs),
        wait_time_seconds: Some(1),
        attribute_names: Some(vec![
            QUEUE_ATTRIBUTE_SENT_TIMESTAMP.to_string(),
            crate::constants::QUEUE_ATTRIBUTE_RECEIVE_COUNT.to_string(),
        ]),
        message_attribute_names: None,
    }
}

pub(super) fn retry_visibility_timeout_secs(delay: Duration) -> u32 {
    u32::try_from(delay.as_secs())
        .unwrap_or(u32::MAX)
        .min(MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS)
}

pub(super) fn queue_message_delay_ms_at(
    attributes: &HashMap<String, String>,
    now_ms: i64,
) -> Option<i64> {
    let sent = attributes
        .get(QUEUE_ATTRIBUTE_SENT_TIMESTAMP)?
        .parse::<i64>()
        .ok()?;
    Some(now_ms.saturating_sub(sent))
}

fn delivery_attempt(message: &MessageResponse) -> Option<u32> {
    message
        .attributes
        .as_ref()?
        .get(crate::constants::QUEUE_ATTRIBUTE_RECEIVE_COUNT)?
        .parse::<std::num::NonZeroU32>()
        .ok()
        .map(std::num::NonZeroU32::get)
}

pub(super) fn delivery_backoff(message: &MessageResponse, attempt: u32) -> Duration {
    let cap = (1u64 << attempt.saturating_sub(1).min(32))
        .min(crate::constants::MAX_IMMEDIATE_JOB_BACKOFF_SECS);
    let seed = message
        .message_id
        .bytes()
        .chain(attempt.to_be_bytes())
        .fold(0u64, |seed, byte| {
            seed.wrapping_mul(31).wrapping_add(u64::from(byte))
        });
    Duration::from_secs((cap * (80 + seed % 21) / 100).max(1))
}

pub(super) fn empty_poll_delay_ms(streak: u32) -> u64 {
    let cap = (1000u64 << streak.saturating_sub(1).min(32))
        .min(crate::constants::MAX_EMPTY_JOB_POLL_DELAY_MS);
    let entropy = storage_types::TimestampMillis::now().timestamp_millis() as u64;
    cap * (80 + entropy % 21) / 100
}
