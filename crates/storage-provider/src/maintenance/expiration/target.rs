use storage_types::StorageResult;

use super::{ExpectedExpirationPolicy, ExpirationPolicy};

impl ExpectedExpirationPolicy {
    pub(super) fn validate_current(&self, current: &ExpirationPolicy) -> StorageResult<()> {
        if current.policy_generation != self.policy_generation
            || current.ttl_attribute != self.ttl_attribute
            || current.cursor != self.cursor
            || !matches!(
                current.status,
                storage_types::TimeToLiveStatus::Enabled
                    | storage_types::TimeToLiveStatus::Enabling
            )
        {
            return Err(storage_types::StorageEnum::ConditionalCheckFailed.into());
        }
        Ok(())
    }
}
