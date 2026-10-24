//! Initialized driver limits, separate from logical values and regional policy.

use std::num::NonZeroUsize;

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

/// Unknown native limits are distinct from a positive report of no fixed byte
/// ceiling.
#[derive(Clone, Copy)]
pub struct KvNativeLayout {
    pub key_bytes: Option<NonZeroUsize>,
    pub value_bytes: Option<NonZeroUsize>,
    pub transaction_bytes: infrastructure_provider::physical_store::NativeTransactionByteLimit,
    pub prefix_bytes: usize,
    pub batch_metadata_bytes: usize,
    /// Native size accounting includes per-entry metadata as well as payloads.
    pub mutation_metadata_bytes: usize,
    pub conflict_ranges: NativeConflictRangeLayout,
}

/// Positive native conflict mechanics; unknown evidence never means zero bytes.
#[derive(Clone, Copy)]
pub enum NativeConflictRangeLayout {
    Unknown,
    NotCharged,
    CoalescedReadYourWrites { metadata_bytes: NonZeroUsize },
}

impl NativeConflictRangeLayout {
    pub(crate) fn require_metadata(self) -> StorageResult<Option<NonZeroUsize>> {
        match self {
            Self::Unknown => Err(InfrastructureResultKind::Unavailable {
                context: InfrastructureContext::NativeCapacity,
            }
            .into()),
            Self::NotCharged => Ok(None),
            Self::CoalescedReadYourWrites { metadata_bytes } => Ok(Some(metadata_bytes)),
        }
    }
}

impl KvNativeLayout {
    pub fn maximum_logical_key_bytes(self) -> usize {
        self.key_bytes.map_or(usize::MAX, |bytes| {
            bytes
                .get()
                .saturating_sub(self.prefix_bytes)
                .saturating_sub(crate::constants::KV_CHUNK_KEY_OVERHEAD)
        })
    }

    pub fn maximum_logical_value_bytes(self) -> usize {
        crate::constants::KV_MAX_VALUE_BYTES
    }

    pub(crate) fn require_record_layout(
        self,
        key_bytes: usize,
        value_bytes: usize,
    ) -> StorageResult<()> {
        if key_bytes > self.maximum_logical_key_bytes()
            || value_bytes > self.maximum_logical_value_bytes()
        {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::Bounds,
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn record_capacity(
        self,
        key_bytes: usize,
        limits: infrastructure_provider::limits::InfrastructureLimits,
        page_bytes: NonZeroUsize,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity> {
        let invalid = || InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::Bounds,
        };
        let bytes = crate::record_footprint::bound_protected_record_bytes(
            key_bytes, limits, page_bytes, self,
        )?;
        let bytes = NonZeroUsize::new(bytes).ok_or_else(invalid)?;
        Ok(
            infrastructure_provider::physical_store::PhysicalRecordCapacity::new(
                self.transaction_bytes,
                Some(bytes),
            ),
        )
    }
}
