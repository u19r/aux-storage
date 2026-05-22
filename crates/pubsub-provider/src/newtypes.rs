use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{PubsubError, PubsubValidationKind};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicName(String);

impl TopicName {
    pub fn new(value: impl Into<String>) -> Result<Self, PubsubError> {
        let value = value.into();
        validate_topic_name(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TopicName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicArn(String);

impl TopicArn {
    pub fn new(value: impl Into<String>) -> Result<Self, PubsubError> {
        let value = value.into();
        if !value.starts_with("arn:") || !value.contains(":sns:") {
            return Err(PubsubError::validation(
                PubsubValidationKind::InvalidTopicArn,
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn compose(
        partition: &str,
        region: &str,
        account_id: &str,
        topic_name: &TopicName,
    ) -> Self {
        Self(format!(
            "arn:{partition}:sns:{region}:{account_id}:{}",
            topic_name.as_str()
        ))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TopicArn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionArn(String);

impl SubscriptionArn {
    pub fn new(value: impl Into<String>) -> Result<Self, PubsubError> {
        let value = value.into();
        if !value.starts_with("arn:") || !value.contains(":sns:") {
            return Err(PubsubError::validation(
                PubsubValidationKind::InvalidSubscriptionArn,
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn compose(topic_arn: &TopicArn) -> Self {
        Self(format!("{}:{}", topic_arn.as_str(), Uuid::now_v7()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SubscriptionArn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PubsubMessageId(String);

impl PubsubMessageId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub fn new_from_string(value: impl Into<String>) -> Result<Self, PubsubError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PubsubError::validation(PubsubValidationKind::EmptyMessage));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for PubsubMessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PubsubMessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_topic_name(value: &str) -> Result<(), PubsubError> {
    if value.is_empty()
        || value.len() > 256
        || value.ends_with(".fifo")
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(PubsubError::validation(
            PubsubValidationKind::InvalidTopicName,
        ));
    }
    Ok(())
}
