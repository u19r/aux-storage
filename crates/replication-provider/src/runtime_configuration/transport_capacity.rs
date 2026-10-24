use std::num::NonZeroU32;

use super::{
    error::ConfigurationError,
    limits::{NativeBudgets, WorkLimits},
};
use crate::constants::{
    TRANSPORT_CLASS_SOURCE_MULTIPLIER, TRANSPORT_CLASS_WIRE_MULTIPLIER,
    TRANSPORT_RESERVATION_CLASSES, TRANSPORT_SEMANTIC_FRAME_MULTIPLIER,
};

/// Aggregate transport reservations qualified before native runtime assembly.
#[derive(Clone, Copy, Debug)]
pub struct TransportCapacity {
    permits: NonZeroU32,
    semantic_bytes: NonZeroU32,
}

impl TransportCapacity {
    pub(super) fn try_new(
        limits: &WorkLimits,
        native: NativeBudgets,
    ) -> Result<Self, ConfigurationError> {
        let invalid =
            || ConfigurationError::Invalid("transport capacity cannot honor configured work");
        let semantic = limits
            .frame_bytes
            .get()
            .checked_mul(TRANSPORT_SEMANTIC_FRAME_MULTIPLIER)
            .and_then(|bytes| u32::try_from(bytes).ok())
            .and_then(NonZeroU32::new)
            .ok_or_else(invalid)?;
        let source = native
            .maximum_original_position_bytes
            .get()
            .checked_mul(TRANSPORT_CLASS_SOURCE_MULTIPLIER)
            .ok_or_else(invalid)?;
        let reservation = limits
            .frame_bytes
            .get()
            .checked_mul(TRANSPORT_CLASS_WIRE_MULTIPLIER)
            .and_then(|bytes| bytes.checked_add(source))
            .and_then(|bytes| bytes.checked_mul(TRANSPORT_RESERVATION_CLASSES))
            .ok_or_else(invalid)?;
        let permits = (limits.queue_bytes.get() / reservation).min(limits.apply_concurrency.get());
        let permits = u32::try_from(permits)
            .ok()
            .filter(|permits| *permits >= 2)
            .and_then(NonZeroU32::new)
            .ok_or_else(invalid)?;
        Ok(Self {
            permits,
            semantic_bytes: semantic,
        })
    }

    pub fn permits(self) -> NonZeroU32 {
        self.permits
    }

    pub fn semantic_bytes(self) -> NonZeroU32 {
        self.semantic_bytes
    }
}
