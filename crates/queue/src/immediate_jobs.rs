use std::{sync::Arc, time::Duration};

use bg_jobs::{
    DEFAULT_MAXIMUM_JOB_WORKERS, ImmediateJobEnqueuer, ImmediateJobHandler, ImmediateJobMessage,
    ImmediateJobProcessResult, ImmediateJobQueueError,
};
use metrics_facade::{counter, histogram};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::interval,
};

use crate::{
    ChangeMessageVisibilityRequest, DeleteMessageRequest, QueueError, QueueManager,
    ReceiveMessageRequest, SendMessageRequest,
    constants::{
        DEFAULT_JOB_VISIBILITY_TIMEOUT_SECS, JOB_POLL_BATCH_SIZE,
        MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS, METRIC_QUEUE_EMPTY_RECEIVES_TOTAL,
        METRIC_QUEUE_MESSAGE_DELAY_MS, MINIMUM_JOB_WORKERS, QUEUE_ATTRIBUTE_SENT_TIMESTAMP,
        SCALE_DOWN_STREAK_SECONDS, SCALE_DOWN_UTILIZATION_DENOMINATOR,
        SCALE_DOWN_UTILIZATION_NUMERATOR,
    },
};

#[derive(Debug, Clone)]
pub struct ImmediateJobQueueRuntime {
    pub manager: Arc<QueueManager>,
    pub queue_url: String,
}

#[derive(Debug, Clone)]
pub struct ImmediateJobQueueClient {
    runtime: ImmediateJobQueueRuntime,
}

impl ImmediateJobQueueClient {
    #[must_use]
    pub fn new(runtime: ImmediateJobQueueRuntime) -> Self {
        Self { runtime }
    }

    #[must_use]
    pub fn runtime(&self) -> &ImmediateJobQueueRuntime {
        &self.runtime
    }
}

#[async_trait::async_trait]
impl ImmediateJobEnqueuer for ImmediateJobQueueClient {
    async fn enqueue(&self, message: ImmediateJobMessage) -> Result<(), ImmediateJobQueueError> {
        let body = serde_json::to_string(&message)
            .map_err(|err| ImmediateJobQueueError::message(err.to_string()))?;
        self.runtime
            .manager
            .send_message(SendMessageRequest {
                queue_url: self.runtime.queue_url.clone(),
                message_body: body,
                delay_seconds: None,
                message_attributes: None,
            })
            .await
            .map_err(|err| ImmediateJobQueueError::message(err.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ImmediateJobRunnerConfig {
    pub maximum_job_workers: usize,
    pub visibility_timeout_secs: u32,
}

impl Default for ImmediateJobRunnerConfig {
    fn default() -> Self {
        Self {
            maximum_job_workers: usize::try_from(DEFAULT_MAXIMUM_JOB_WORKERS).unwrap_or(4),
            visibility_timeout_secs: DEFAULT_JOB_VISIBILITY_TIMEOUT_SECS,
        }
    }
}

#[must_use]
pub fn spawn_immediate_job_runner(
    runtime: ImmediateJobQueueRuntime,
    handler: Arc<dyn ImmediateJobHandler>,
    config: ImmediateJobRunnerConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let max_workers = normalized_max_job_workers(config.maximum_job_workers);
        let (stats_tx, mut stats_rx) = mpsc::unbounded_channel::<usize>();
        let mut workers = Vec::with_capacity(max_workers);
        spawn_worker(
            &mut workers,
            runtime.clone(),
            Arc::clone(&handler),
            stats_tx.clone(),
            config.visibility_timeout_secs,
        );

        let mut ticker = interval(Duration::from_secs(1));
        let mut underutilized_streak = 0usize;

        loop {
            ticker.tick().await;

            let mut total_messages = 0usize;
            let mut saw_full_batch = false;
            while let Ok(message_count) = stats_rx.try_recv() {
                total_messages = total_messages.saturating_add(message_count);
                if message_count >= usize::try_from(JOB_POLL_BATCH_SIZE).unwrap_or(10) {
                    saw_full_batch = true;
                }
            }

            if should_scale_up(saw_full_batch, workers.len(), max_workers) {
                spawn_worker(
                    &mut workers,
                    runtime.clone(),
                    Arc::clone(&handler),
                    stats_tx.clone(),
                    config.visibility_timeout_secs,
                );
                underutilized_streak = 0;
                continue;
            }

            if should_count_underutilized_second(workers.len(), total_messages) {
                underutilized_streak = underutilized_streak.saturating_add(1);
            } else {
                underutilized_streak = 0;
            }

            if should_scale_down(underutilized_streak, workers.len())
                && let Some(worker) = workers.pop()
            {
                let _ = worker.stop.send(true);
                underutilized_streak = 0;
            }
        }
    })
}

#[must_use]
struct WorkerHandle {
    stop: watch::Sender<bool>,
    _task: JoinHandle<()>,
}

fn spawn_worker(
    workers: &mut Vec<WorkerHandle>,
    runtime: ImmediateJobQueueRuntime,
    handler: Arc<dyn ImmediateJobHandler>,
    stats_tx: mpsc::UnboundedSender<usize>,
    visibility_timeout_secs: u32,
) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let task = tokio::spawn(run_worker(
        runtime,
        handler,
        stats_tx,
        visibility_timeout_secs,
        stop_rx,
    ));
    workers.push(WorkerHandle {
        stop: stop_tx,
        _task: task,
    });
}

async fn run_worker(
    runtime: ImmediateJobQueueRuntime,
    handler: Arc<dyn ImmediateJobHandler>,
    stats_tx: mpsc::UnboundedSender<usize>,
    visibility_timeout_secs: u32,
    mut stop_rx: watch::Receiver<bool>,
) {
    loop {
        if *stop_rx.borrow() {
            return;
        }

        let response = match runtime
            .manager
            .receive_message(immediate_job_receive_request(
                runtime.queue_url.clone(),
                visibility_timeout_secs,
            ))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                if wait_for_immediate_job_receive_retry(&err, &mut stop_rx).await {
                    return;
                }
                continue;
            }
        };
        if response.messages.is_empty() {
            counter!(METRIC_QUEUE_EMPTY_RECEIVES_TOTAL, "runner" => "local").increment(1);
        }
        let _ = stats_tx.send(response.messages.len());

        for message in response.messages {
            record_queue_message_delay_ms(message.attributes.as_ref(), "local");
            let receipt_handle = message.receipt_handle.as_str().to_string();
            let payload: Result<ImmediateJobMessage, _> = serde_json::from_str(&message.body);
            let Ok(payload) = payload else {
                if let Err(err) = runtime
                    .manager
                    .delete_message(DeleteMessageRequest {
                        queue_url: runtime.queue_url.clone(),
                        receipt_handle: receipt_handle.as_str().into(),
                    })
                    .await
                {
                    tracing::warn!(error = %err, "failed to delete malformed immediate job");
                }
                continue;
            };

            let outcome = match handler.handle(&payload).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        job_name = %payload.job_name,
                        "immediate job handler failed"
                    );
                    continue;
                }
            };

            match outcome {
                ImmediateJobProcessResult::DeleteMessage => {
                    if let Err(err) = runtime
                        .manager
                        .delete_message(DeleteMessageRequest {
                            queue_url: runtime.queue_url.clone(),
                            receipt_handle: receipt_handle.as_str().into(),
                        })
                        .await
                    {
                        tracing::warn!(error = %err, "failed to delete immediate job message");
                    }
                }
                ImmediateJobProcessResult::RetryAfter(delay) => {
                    let visibility_timeout = retry_visibility_timeout_secs(delay);
                    if let Err(err) = runtime
                        .manager
                        .change_message_visibility(ChangeMessageVisibilityRequest {
                            queue_url: runtime.queue_url.clone(),
                            receipt_handle: receipt_handle.as_str().into(),
                            visibility_timeout,
                        })
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            "failed to change immediate job message visibility"
                        );
                    }
                }
            }
        }
    }
}

const SENDER_FAULT_RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(30);
const TRANSIENT_RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Returns the shared retry delay for immediate-job receive failures.
///
/// Invalid requests cannot recover through immediate retries, while backend
/// failures should recover promptly without becoming a CPU or log hot loop.
#[must_use]
pub(super) fn immediate_job_receive_retry_delay(error: &QueueError) -> Duration {
    if error.is_sender_fault() {
        SENDER_FAULT_RECEIVE_RETRY_DELAY
    } else {
        TRANSIENT_RECEIVE_RETRY_DELAY
    }
}

/// Waits for the receive retry delay and reports whether the worker should
/// stop.
pub async fn wait_for_immediate_job_receive_retry(
    error: &QueueError,
    stop_rx: &mut watch::Receiver<bool>,
) -> bool {
    let retry_delay = immediate_job_receive_retry_delay(error);
    tracing::warn!(
        error = %error,
        retry_delay_ms = retry_delay.as_millis(),
        "immediate job runner failed to receive messages"
    );
    tokio::select! {
        () = tokio::time::sleep(retry_delay) => false,
        changed = stop_rx.changed() => changed.is_err() || *stop_rx.borrow(),
    }
}

fn record_queue_message_delay_ms(
    attributes: Option<&std::collections::HashMap<String, String>>,
    runner: &'static str,
) {
    let Some(delay_ms) = attributes.and_then(queue_message_delay_ms) else {
        return;
    };
    histogram!(METRIC_QUEUE_MESSAGE_DELAY_MS, "runner" => runner).record(delay_ms as f64);
}

fn queue_message_delay_ms(attributes: &std::collections::HashMap<String, String>) -> Option<i64> {
    queue_message_delay_ms_at(
        attributes,
        storage_types::TimestampMillis::now().timestamp_millis(),
    )
}

pub(super) fn queue_message_delay_ms_at(
    attributes: &std::collections::HashMap<String, String>,
    now_ms: i64,
) -> Option<i64> {
    let raw_timestamp = attributes.get(QUEUE_ATTRIBUTE_SENT_TIMESTAMP)?;
    let sent_timestamp_ms = raw_timestamp.parse::<i64>().ok()?;
    Some(now_ms.saturating_sub(sent_timestamp_ms))
}

pub(super) fn normalized_max_job_workers(maximum_job_workers: usize) -> usize {
    maximum_job_workers.max(MINIMUM_JOB_WORKERS)
}

pub(super) fn immediate_job_receive_request(
    queue_url: String,
    visibility_timeout_secs: u32,
) -> ReceiveMessageRequest {
    ReceiveMessageRequest {
        queue_url,
        max_number_of_messages: Some(JOB_POLL_BATCH_SIZE),
        visibility_timeout: Some(visibility_timeout_secs),
        wait_time_seconds: Some(1),
        attribute_names: Some(vec![QUEUE_ATTRIBUTE_SENT_TIMESTAMP.to_string()]),
        message_attribute_names: None,
    }
}

pub(super) fn retry_visibility_timeout_secs(delay: Duration) -> u32 {
    u32::try_from(
        delay
            .as_secs()
            .min(u64::from(MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS)),
    )
    .unwrap_or(MAX_IMMEDIATE_JOB_RETRY_VISIBILITY_SECS)
}

pub(super) fn should_scale_up(
    saw_full_batch: bool,
    worker_count: usize,
    max_workers: usize,
) -> bool {
    saw_full_batch && worker_count < max_workers
}

pub(super) fn scale_down_threshold(worker_count: usize) -> usize {
    worker_count
        .max(MINIMUM_JOB_WORKERS)
        .saturating_mul(usize::try_from(JOB_POLL_BATCH_SIZE).unwrap_or(10))
        .saturating_mul(SCALE_DOWN_UTILIZATION_NUMERATOR)
        / SCALE_DOWN_UTILIZATION_DENOMINATOR
}

pub(super) fn should_count_underutilized_second(
    worker_count: usize,
    total_messages: usize,
) -> bool {
    worker_count > MINIMUM_JOB_WORKERS && total_messages < scale_down_threshold(worker_count)
}

pub(super) fn should_scale_down(underutilized_streak: usize, worker_count: usize) -> bool {
    underutilized_streak >= SCALE_DOWN_STREAK_SECONDS && worker_count > MINIMUM_JOB_WORKERS
}
