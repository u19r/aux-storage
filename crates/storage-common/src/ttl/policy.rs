//! TTL lifecycle decisions, including policy identity across disabled periods.

use storage_types::{
    SemanticDecision, StorageError, StorageResult, TableName, TimeToLiveDescription,
    TimeToLiveSpecification, TimeToLiveStatus, TimestampMillis,
};

use super::{TtlConfigRecord, TtlMaintenanceCheckpoint};

/// Native adapters apply only the physical work selected by this transition.
pub enum TtlPolicyChange {
    Unchanged,
    Enable(TtlConfigRecord),
    Disable(TtlConfigRecord),
}

impl TtlPolicyChange {
    pub fn try_new(
        table: &TableName,
        specification: &TimeToLiveSpecification,
        existing: Option<&TtlConfigRecord>,
        now: TimestampMillis,
    ) -> StorageResult<SemanticDecision<Self>> {
        if !specification.enabled {
            return Self::disable_policy(existing, now).map(SemanticDecision::Accepted);
        }
        Self::enable_policy(table, &specification.attribute_name, existing, now)
    }

    fn enable_policy(
        table: &TableName,
        attribute: &str,
        existing: Option<&TtlConfigRecord>,
        now: TimestampMillis,
    ) -> StorageResult<SemanticDecision<Self>> {
        let rejection = if attribute.trim().is_empty() {
            Some("Time to live attribute name must not be empty")
        } else {
            match existing.map(|config| (&config.status, config.attribute_name.as_str())) {
                Some((TimeToLiveStatus::Enabling | TimeToLiveStatus::Disabling, _)) => {
                    Some("Time to live configuration update in progress; retry later")
                }
                Some((TimeToLiveStatus::Enabled, current)) if current == attribute => {
                    return Ok(SemanticDecision::Accepted(Self::Unchanged));
                }
                Some((TimeToLiveStatus::Enabled, _)) => {
                    Some("Disable time to live before changing attribute name")
                }
                _ => None,
            }
        };
        if let Some(message) = rejection {
            return Ok(SemanticDecision::Rejected(StorageError::validation(
                message,
            )));
        }
        let mut config = TtlConfigRecord::try_new_at(
            attribute.to_owned(),
            table,
            TimeToLiveStatus::Enabling,
            now,
        )?;
        if let Some(previous) = existing {
            config.policy_generation = previous.policy_generation.next()?;
        }
        Ok(SemanticDecision::Accepted(Self::Enable(config)))
    }

    fn disable_policy(
        existing: Option<&TtlConfigRecord>,
        now: TimestampMillis,
    ) -> StorageResult<Self> {
        let Some(existing) = existing else {
            return Ok(Self::Unchanged);
        };
        if existing.status == TimeToLiveStatus::Disabled {
            return Ok(Self::Unchanged);
        }
        let mut config = existing.clone();
        config.status = TimeToLiveStatus::Disabled;
        config.checkpoint = TtlMaintenanceCheckpoint::default();
        config.touch_at(now);
        config.validate()?;
        Ok(Self::Disable(config))
    }
}

impl TtlConfigRecord {
    /// Disabled records retain identity internally without advertising an
    /// active attribute.
    pub fn describe(config: Option<Self>) -> TimeToLiveDescription {
        match config.filter(|config| config.status != TimeToLiveStatus::Disabled) {
            Some(config) => TimeToLiveDescription {
                attribute_name: Some(config.attribute_name),
                time_to_live_status: config.status,
            },
            None => TimeToLiveDescription {
                attribute_name: None,
                time_to_live_status: TimeToLiveStatus::Disabled,
            },
        }
    }
}
