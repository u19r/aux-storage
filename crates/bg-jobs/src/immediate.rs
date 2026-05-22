use std::time::Duration;

use async_trait::async_trait;
use rand::random;
use serde::{Deserialize, Serialize};
use storage_types::{KeyAttributes, TableName, TimestampMillis};

use crate::BackgroundJobName;

pub const DEFAULT_MAXIMUM_JOB_WORKERS: u32 = 4;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImmediateJobMessage {
    pub dispatch_id: String,
    pub job_name: BackgroundJobName,
    pub table_name: String,
    pub key: KeyAttributes,
}

impl ImmediateJobMessage {
    #[must_use]
    pub fn new(
        job_name: BackgroundJobName,
        table_name: TableName,
        key: impl Into<KeyAttributes>,
    ) -> Self {
        Self::with_dispatch_id(new_dispatch_id(), job_name, table_name, key)
    }

    #[must_use]
    pub fn with_dispatch_id(
        dispatch_id: impl Into<String>,
        job_name: BackgroundJobName,
        table_name: TableName,
        key: impl Into<KeyAttributes>,
    ) -> Self {
        Self {
            dispatch_id: dispatch_id.into(),
            job_name,
            table_name: table_name.to_string(),
            key: key.into(),
        }
    }

    #[must_use]
    pub fn table_name(&self) -> TableName {
        TableName::new(&self.table_name)
    }
}

fn new_dispatch_id() -> String {
    let now = u64::try_from(TimestampMillis::now().timestamp_millis()).unwrap_or_default();
    let random_suffix = random::<u64>();
    format!("imjob_{now:016x}_{random_suffix:016x}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImmediateJobProcessResult {
    DeleteMessage,
    RetryAfter(Duration),
}

#[async_trait]
pub trait ImmediateJobEnqueuer: Send + Sync {
    async fn enqueue(&self, message: ImmediateJobMessage) -> Result<(), ImmediateJobQueueError>;
}

#[async_trait]
pub trait ImmediateJobHandler: Send + Sync {
    async fn handle(
        &self,
        message: &ImmediateJobMessage,
    ) -> Result<ImmediateJobProcessResult, ImmediateJobQueueError>;
}
