use std::num::{NonZeroU64, NonZeroUsize};

use serde::{Deserialize, Serialize};

use super::error::ConfigurationError;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkLimits {
    pub frame_bytes: NonZeroUsize,
    pub frame_events: NonZeroUsize,
    /// Decompressed canonical originals; all native effects also consume
    /// attempt admission.
    pub import_bytes: NonZeroUsize,
    pub import_events: NonZeroUsize,
    pub apply_window: NonZeroUsize,
    pub apply_concurrency: NonZeroUsize,
    /// Total transport reservation budget across inbound, outbound and source
    /// classes.
    pub queue_bytes: NonZeroUsize,
}
/// Operator-selected admission. Every term names an enforced owner; this is
/// not a native capability report or a promise that a legal request fits.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBudgets {
    pub attempt: storage_types::NativeWorkLimits,
    pub reserved_attempts: NonZeroUsize,
    pub maximum_canonical_event_bytes: NonZeroUsize,
    pub maximum_original_position_bytes: NonZeroUsize,
    pub maximum_acceptance_receipt_bytes: NonZeroUsize,
    pub maximum_origin_receipt_bytes: NonZeroUsize,
    pub maximum_history_declaration_bytes: NonZeroUsize,
}

impl NativeBudgets {
    fn require_consistent_bounds(self) -> Result<(), ConfigurationError> {
        let source_bytes = self
            .maximum_canonical_event_bytes
            .get()
            .checked_add(self.maximum_history_declaration_bytes.get())
            .ok_or(ConfigurationError::Invalid(
                "complete source position allowance overflowed",
            ))?;
        if self.maximum_acceptance_receipt_bytes > self.maximum_history_declaration_bytes
            || self.maximum_origin_receipt_bytes > self.attempt.data_bytes
            || source_bytes > self.maximum_original_position_bytes.get()
        {
            return Err(ConfigurationError::Invalid(
                "inconsistent native admission budgets",
            ));
        }
        Ok(())
    }
}

impl WorkLimits {
    pub fn qualify(
        &self,
        native: NativeBudgets,
    ) -> Result<super::transport_capacity::TransportCapacity, ConfigurationError> {
        validate_work_limits(self)?;
        native.require_consistent_bounds()?;
        if self.apply_concurrency > native.reserved_attempts
            || native.maximum_canonical_event_bytes > self.import_bytes
            || native.maximum_history_declaration_bytes > self.frame_bytes
        {
            return Err(ConfigurationError::Invalid(
                "native admission cannot honor configured work",
            ));
        }
        super::transport_capacity::TransportCapacity::try_new(self, native)
    }
}
fn validate_work_limits(limits: &WorkLimits) -> Result<(), ConfigurationError> {
    if limits.import_bytes > limits.frame_bytes
        || limits.import_events > limits.frame_events
        || limits.apply_concurrency > limits.apply_window
        || limits.queue_bytes < limits.frame_bytes
    {
        return Err(ConfigurationError::Invalid("inconsistent work bounds"));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacklogBudgets {
    pub hot_history_bytes: NonZeroU64,
    /// Local safe-storage acceptance budget; remote lag never imposes quorum
    /// acknowledgement.
    pub acceptance_bytes: NonZeroU64,
    pub recovery_reserve_bytes: NonZeroU64,
    pub transfer_bytes_per_second: NonZeroU64,
    pub concurrent_joins: NonZeroUsize,
}
impl BacklogBudgets {
    pub fn qualify(&self, native: NativeBudgets) -> Result<(), ConfigurationError> {
        if self.acceptance_bytes > self.hot_history_bytes
            || self.recovery_reserve_bytes >= self.acceptance_bytes
            || self
                .acceptance_bytes
                .get()
                .checked_sub(self.recovery_reserve_bytes.get())
                .is_none_or(|bytes| u128::from(bytes) < native.attempt.data_bytes.get() as u128)
            || u128::from(self.recovery_reserve_bytes.get())
                < native.attempt.data_bytes.get() as u128
        {
            return Err(ConfigurationError::Invalid(
                "history budgets must preserve a complete customer allowance and native recovery \
                 reserve",
            ));
        }
        Ok(())
    }
}
