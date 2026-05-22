use thiserror::Error;

use crate::BackgroundJobName;

#[derive(Error, Debug)]
pub enum JobError {
    #[error("Job execution failed: {message}")]
    ExecutionFailed { message: String },

    #[error("Job with ID '{job_id}' not found")]
    JobNotFound { job_id: BackgroundJobName },

    #[error("Job is already running")]
    JobAlreadyRunning,

    #[error("Job is not running")]
    JobNotRunning,

    #[error("Failed to spawn job task: {message}")]
    SpawnFailed { message: String },
}

pub type JobResult<T> = Result<T, JobError>;

#[derive(Debug, Error)]
pub enum JobLockError {
    #[error("{message}")]
    Contention { message: String },
    #[error("{message}")]
    Store { message: String },
}

impl JobLockError {
    pub fn contention(message: impl Into<String>) -> Self {
        Self::Contention {
            message: message.into(),
        }
    }

    pub fn store(message: impl Into<String>) -> Self {
        Self::Store {
            message: message.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum JobStartupError {
    #[error(
        "invalid {env_key} value '{value}', expected '{expected_all}' or '{expected_metrics_only}'"
    )]
    InvalidJobsMode {
        env_key: &'static str,
        value: String,
        expected_all: &'static str,
        expected_metrics_only: &'static str,
    },
}

#[derive(Debug, Error)]
#[error("{kind}")]
pub struct WorkerError {
    kind: WorkerErrorKind,
}

impl WorkerError {
    #[must_use]
    pub fn store(message: impl Into<String>) -> Self {
        Self {
            kind: WorkerErrorKind::Store(message.into()),
        }
    }

    #[must_use]
    pub fn processing(message: impl Into<String>) -> Self {
        Self {
            kind: WorkerErrorKind::Processing(message.into()),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &WorkerErrorKind {
        &self.kind
    }
}

#[derive(Debug)]
pub enum WorkerErrorKind {
    Store(String),
    Processing(String),
}

impl std::fmt::Display for WorkerErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(message) => write!(f, "store error: {message}"),
            Self::Processing(message) => write!(f, "processing error: {message}"),
        }
    }
}
