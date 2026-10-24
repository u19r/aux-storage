use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use scheduled_bg_jobs::store::{
    CommitOutcome, RangePage, RangeRequest, StoreCapabilities, StoreKey, StoreTransaction,
};
use storage_types::StorageError;

/// Errors at the provider-owned ordered-store boundary.
///
/// Backend details stay below this boundary.  In particular, timeout is
/// retained as a typed outcome so the job-store adapter can expose the
/// upstream fixed error category without formatting backend diagnostics.
#[doc(hidden)]
pub enum RawOrderedJobStoreError {
    Backend(StorageError),
    /// Persisted bytes violated the provider's advertised key/value shape.
    Corrupt(&'static str),
    /// A commit identity was reused for different checks or mutations.
    OperationReuseMismatch,
    Timeout,
}

impl fmt::Debug for RawOrderedJobStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(_) => formatter.write_str("Backend(<redacted>)"),
            Self::Corrupt(_) => formatter.write_str("Corrupt(<redacted>)"),
            Self::OperationReuseMismatch => formatter.write_str("OperationReuseMismatch"),
            Self::Timeout => formatter.write_str("Timeout"),
        }
    }
}

impl fmt::Display for RawOrderedJobStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(_) => formatter.write_str("ordered store backend failure"),
            Self::Corrupt(_) => formatter.write_str("ordered store corrupt data"),
            Self::OperationReuseMismatch => {
                formatter.write_str("commit operation ID was reused with different content")
            }
            Self::Timeout => formatter.write_str("ordered store operation timed out"),
        }
    }
}

impl std::error::Error for RawOrderedJobStoreError {}

impl From<StorageError> for RawOrderedJobStoreError {
    fn from(error: StorageError) -> Self {
        Self::Backend(error)
    }
}

/// Provider-owned boundary for an ordered, byte-oriented background-job store.
///
/// The boundary deliberately exposes the versioned scheduled-job contract
/// values without leaking any backend implementation.  Concrete providers own
/// persistence, deadlines, and atomicity; `bg-job-store` owns the one adapter
/// which turns this boundary into the upstream `JobStore` trait.
#[doc(hidden)]
#[async_trait]
pub trait RawOrderedJobStore: Send + Sync {
    fn capabilities(&self) -> StoreCapabilities;

    fn bind_native_admission(
        &self,
        admission: std::sync::Arc<dyn crate::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<()>;

    async fn bootstrap(&self) -> Result<(), RawOrderedJobStoreError>;

    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, RawOrderedJobStoreError>;

    async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError>;

    async fn read_range(
        &self,
        request: &RangeRequest,
    ) -> Result<RangePage, RawOrderedJobStoreError>;

    async fn commit(
        &self,
        transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, RawOrderedJobStoreError>;
}
