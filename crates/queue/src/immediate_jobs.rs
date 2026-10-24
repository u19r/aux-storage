use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use async_trait::async_trait;
use bg_jobs_tokio::{QueueConsumer, QueueOutcome};
use tokio::{sync::watch, task::JoinError};

use crate::{
    QueueManager,
    constants::{DEFAULT_JOB_VISIBILITY_TIMEOUT_SECS, DEFAULT_MAXIMUM_JOB_WORKERS},
    immediate_queue_worker::ImmediateQueueWorker,
};

#[derive(Debug, thiserror::Error)]
pub enum ImmediateJobQueueError {
    #[error("{message}")]
    Message { message: String },
}

impl ImmediateJobQueueError {
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message {
            message: message.into(),
        }
    }
}

/// Business decoding remains with the consumer of the queue facade.
#[async_trait]
pub trait ImmediateJobHandler: Send + Sync {
    async fn handle(&self, body: &str) -> Result<QueueOutcome, ImmediateJobQueueError>;
}

#[derive(Debug, Clone)]
pub struct ImmediateJobQueueRuntime {
    pub manager: Arc<QueueManager>,
    pub queue_url: String,
}

#[derive(Debug, Clone)]
pub struct ImmediateJobRunnerConfig {
    pub maximum_job_workers: usize,
    pub visibility_timeout_secs: u32,
    pub maximum_attempts: NonZeroU32,
}

impl Default for ImmediateJobRunnerConfig {
    fn default() -> Self {
        Self {
            maximum_job_workers: usize::try_from(DEFAULT_MAXIMUM_JOB_WORKERS).unwrap_or(4),
            visibility_timeout_secs: DEFAULT_JOB_VISIBILITY_TIMEOUT_SECS,
            maximum_attempts: NonZeroU32::new(crate::constants::DEFAULT_MAXIMUM_JOB_ATTEMPTS)
                .unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// A runner whose source and dead-letter queues are ready before workers start.
pub struct ImmediateJobRunner {
    consumer: QueueConsumer<ImmediateQueueWorker>,
}

impl ImmediateJobQueueRuntime {
    pub async fn prepare_runner(
        self,
        handler: Arc<dyn ImmediateJobHandler>,
        config: ImmediateJobRunnerConfig,
    ) -> Result<ImmediateJobRunner, crate::QueueError> {
        let dead_letter_queue_url = self
            .manager
            .create_queue(crate::CreateQueueRequest {
                queue_name: format!(
                    "background-jobs-dlq-{:x}",
                    md5::compute(self.queue_url.as_bytes())
                ),
                attributes: Some(std::collections::HashMap::from([(
                    "MessageRetentionPeriod".into(),
                    crate::MAX_MESSAGE_RETENTION_SECONDS.to_string(),
                )])),
            })
            .await?
            .queue_url;
        let concurrency =
            NonZeroUsize::new(config.maximum_job_workers).unwrap_or(NonZeroUsize::MIN);
        let heartbeat =
            NonZeroU32::new(config.visibility_timeout_secs / 3).unwrap_or(NonZeroU32::MIN);
        let worker = Arc::new(ImmediateQueueWorker::new(
            self,
            handler,
            config,
            dead_letter_queue_url,
        ));
        Ok(ImmediateJobRunner {
            consumer: QueueConsumer::new(worker, concurrency, heartbeat),
        })
    }
}

impl ImmediateJobRunner {
    pub async fn run_until_shutdown(
        self,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), JoinError> {
        self.consumer.run_until_shutdown(shutdown).await
    }
}
