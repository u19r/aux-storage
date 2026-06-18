use std::{ops::Deref, str::FromStr};

use queue_provider::{MessageId, QueueError, QueueInternalKind, QueueResult};
use serde::{Deserialize, Serialize};
use storage_types::TimestampMillis;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageVisibilityKey(pub String);

impl MessageVisibilityKey {
    /// # Errors
    ///
    /// Returns an error if the message ID cannot be extracted
    pub fn get_message_id(&self) -> QueueResult<MessageId> {
        self.0
            .split_once(':')
            .and_then(|(_, s)| MessageId::from_str(s).ok())
            .ok_or_else(|| {
                QueueError::internal_with_detail(
                    QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                    format!("value={}", self.0),
                )
            })
    }

    /// # Errors
    ///
    /// Returns an error if the timestamp cannot be extracted
    pub fn get_timestamp(&self) -> QueueResult<TimestampMillis> {
        self.0
            .split(':')
            .next()
            .and_then(|s| s.parse::<i64>().ok())
            .map(TimestampMillis::from)
            .ok_or_else(|| {
                QueueError::internal_with_detail(
                    QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                    format!("value={}", self.0),
                )
            })
    }

    #[must_use]
    pub fn min() -> Self {
        MessageVisibilityKey(format!("0:{}", MessageId::default()))
    }
}

impl std::fmt::Display for MessageVisibilityKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for MessageVisibilityKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
