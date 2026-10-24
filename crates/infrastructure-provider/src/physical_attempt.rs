//! Backend lowering on a caller-owned native attempt; no workflow decisions.

use storage_types::{ReadProtection, StorageResult};

use crate::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    page::{RecordPage, RecordPageRequest},
    record::{ConditionalWriteResult, DurableRecord, DurableRecordKey},
};

// Opaque records have composite byte addresses and CAS revisions; Dynamo item
// ports cannot represent them without an extra encoding/conversion layer.
pub trait PhysicalRecordAttempt: Send {
    /// The outer operation's native mode determines its baseline protection.
    /// Read-only attempts provide a coherent snapshot; write attempts protect
    /// dependent observations even when the engine uses statement snapshots.
    fn read_protection(&self) -> ReadProtection;

    /// Reject the complete native attempt after a workflow failure. Earlier
    /// effects cannot settle even if an outer caller catches that failure.
    fn reject_settlement(&mut self);

    /// Preserve a returned native cause even when the workflow caller catches
    /// it.
    fn reject_settlement_error(&mut self, error: &storage_types::StorageError);

    /// Observe namespace occupancy without decoding payloads. Conflict
    /// protection covers the empty namespace observation within this
    /// caller-owned attempt.
    fn has_records_in_namespace(
        &mut self,
        namespace: &str,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<bool>> + Send;

    /// Shared protects stable existing rows; Conflict protects an update
    /// decision. Observing a missing row does not establish a writer
    /// takeover barrier.
    /// Return one observation per requested key in request order. The batch
    /// allowance includes every requested logical key and each returned
    /// payload.
    fn load_records(
        &mut self,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
        limits: InfrastructureLimits,
    ) -> impl Future<Output = StorageResult<Vec<Option<DurableRecord>>>> + Send;

    fn scan_records(
        &mut self,
        request: RecordPageRequest,
        limits: InfrastructureLimits,
    ) -> impl Future<Output = StorageResult<RecordPage>> + Send;

    fn store_records(
        &mut self,
        batch: &RecordBatch<'_>,
    ) -> impl Future<Output = StorageResult<ConditionalWriteResult>> + Send;
}

/// Error owners expose native causes without consuming or rewriting their
/// error.
pub trait PhysicalRecordFailure {
    fn native_error(&self) -> Option<&storage_types::StorageError>;
}

impl PhysicalRecordFailure for storage_types::StorageError {
    fn native_error(&self) -> Option<&storage_types::StorageError> {
        Some(self)
    }
}

/// Own nonacceptance until a complete protected-record operation returns
/// success. Native effects and external I/O remain inside the caller's existing
/// attempt.
pub struct PhysicalRecordOperation<'attempt, A: PhysicalRecordAttempt + ?Sized> {
    attempt: &'attempt mut A,
    completed: bool,
}

impl<'attempt, A: PhysicalRecordAttempt + ?Sized> PhysicalRecordOperation<'attempt, A> {
    pub fn new(attempt: &'attempt mut A) -> Self {
        Self {
            attempt,
            completed: false,
        }
    }

    pub fn complete<R, E: PhysicalRecordFailure>(mut self, result: Result<R, E>) -> Result<R, E> {
        if let Err(error) = &result {
            match error.native_error() {
                Some(error) => self.attempt.reject_settlement_error(error),
                None => self.attempt.reject_settlement(),
            }
        }
        self.completed = true;
        result
    }
}

impl<A: PhysicalRecordAttempt + ?Sized> AsMut<A> for PhysicalRecordOperation<'_, A> {
    fn as_mut(&mut self) -> &mut A {
        self.attempt
    }
}

impl<A: PhysicalRecordAttempt + ?Sized> Drop for PhysicalRecordOperation<'_, A> {
    fn drop(&mut self) {
        if !self.completed {
            self.attempt.reject_settlement();
        }
    }
}
