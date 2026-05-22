//! Background job infrastructure for distributed, multi-node deployments.
//!
//! ## Overview
//!
//! This crate provides a lease-based distributed worker system for safe
//! background job processing across multiple nodes. All workers use `DynamoDB`
//! conditional writes to ensure work items are only processed once.
//!
//! ## Key Components
//!
//! - [`WorkerConfig`]: Configuration for distributed workers
//! - [`DistributedWorker`]: Main worker that polls and processes work items
//! - [`WorkItemStore`]: Trait for data access (query, lease, complete, fail)
//! - [`WorkItemProcessor`]: Trait for processing individual work items
//!
//! ## Usage
//!
//! ```rust,ignore
//! use bg_jobs::{WorkerConfig, DistributedWorker, WorkItemStore, WorkItemProcessor};
//!
//! // Implement WorkItemStore for your entity type
//! // Implement WorkItemProcessor for your business logic
//!
//! let config = WorkerConfig::new("my-worker")
//!     .with_lease_duration(Duration::from_secs(60));
//!
//! let worker = DistributedWorker::new(config, store, processor);
//!
//! // Run with graceful shutdown
//! let (tx, rx) = tokio::sync::watch::channel(false);
//! tokio::spawn(async move { worker.run(rx).await });
//!
//! // To shutdown:
//! tx.send(true).ok();
//! ```

// New distributed worker system
mod constants;
pub mod errors;
mod immediate;
mod jitter;
mod job_lock;
mod job_manager;
mod job_name;
pub mod startup;
mod worker;

#[cfg(test)]
mod job_manager_lock_tests;
#[cfg(test)]
mod job_manager_tests;
#[cfg(test)]
mod job_name_tests;
#[cfg(test)]
mod worker_alloc_tests;
#[cfg(test)]
mod worker_tests;

pub use errors::{JobLockError, WorkerError};
pub use immediate::{
    DEFAULT_MAXIMUM_JOB_WORKERS, ImmediateJobEnqueuer, ImmediateJobHandler, ImmediateJobMessage,
    ImmediateJobProcessResult, ImmediateJobQueueError,
};
pub use jitter::jittered;
pub use job_lock::{JobLockAttempt, JobLockResult, JobLockStore};
pub use job_manager::{BackgroundJob, JobConfig, JobHandle, JobManager};
pub use job_name::{
    BackgroundJobGroup, BackgroundJobName, BackgroundJobNameParseError, DatabaseJobKind,
    ImmediateJobKind, PeriodicJobKind,
};
pub use worker::{
    DistributedWorker, LeaseAttrValue, LeaseResult, LeaseUpdateBuilder, LeaseUpdateStatement,
    WorkItemProcessor, WorkItemStore, WorkerConfig, default_worker_id,
    is_conditional_check_failure,
};

#[cfg(test)]
mod startup_tests;
