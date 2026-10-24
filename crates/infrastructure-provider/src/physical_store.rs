use std::num::NonZeroUsize;

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

use crate::limits::InfrastructureLimits;

/// Opt-in physical record layout initialization on an existing native store.
/// This port owns driver/layout qualification, never regional workflow policy.
pub trait PhysicalRecordStore: Send + Sync {
    /// Qualify record lowering without I/O using this initialized store's
    /// layout. This component report does not qualify other effects in the
    /// same attempt.
    fn record_capacity(
        &self,
        limits: InfrastructureLimits,
    ) -> StorageResult<PhysicalRecordCapacity>;

    fn initialize_records(
        &self,
        limits: InfrastructureLimits,
    ) -> impl Future<Output = StorageResult<[u8; 16]>> + Send;
}

/// The initialized provider distinguishes missing evidence from no fixed byte
/// ceiling.
#[derive(Clone, Copy, Debug)]
pub enum NativeTransactionByteLimit {
    Unknown,
    Fixed(NonZeroUsize),
    /// Positive provider knowledge; memory, storage and time are still finite.
    NoFixedCeiling,
}

/// Provider-owned serialized-data evidence for protected records, separate from
/// native aggregate limits. Bounded admission can accept a serialized envelope
/// larger than a native transaction; the attempt must then fail safely.
/// Unknown aggregate accounting stays Unknown. Neither term certifies complete
/// memory, storage, time or unrelated transaction effects.
#[derive(Clone, Copy, Debug)]
pub struct PhysicalRecordCapacity {
    transaction_bytes: NativeTransactionByteLimit,
    protected_batch_bytes: Option<NonZeroUsize>,
}

impl PhysicalRecordCapacity {
    pub fn new(
        transaction_bytes: NativeTransactionByteLimit,
        protected_batch_bytes: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            transaction_bytes,
            protected_batch_bytes,
        }
    }

    /// Require initialized serialized lowering evidence. An opaque aggregate
    /// ceiling stays Unknown; a known ceiling remains enforced by its driver.
    pub fn require_bounded_admission(self) -> StorageResult<NativeTransactionByteLimit> {
        self.protected_batch_bytes
            .ok_or_else(unavailable_capacity)?;
        Ok(self.transaction_bytes)
    }
}

fn unavailable_capacity() -> storage_types::StorageError {
    InfrastructureResultKind::Unavailable {
        context: InfrastructureContext::NativeCapacity,
    }
    .into()
}
