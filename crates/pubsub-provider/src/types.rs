use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use storage_types::TimestampMillis;
use uuid::Uuid;

use crate::{
    PubsubError, PubsubMessageId, PubsubResult, PubsubValidationKind, SubscriptionArn, TopicArn,
    TopicName,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PubsubArnParts {
    pub partition: String,
    pub region: String,
    pub account_id: String,
}

impl Default for PubsubArnParts {
    fn default() -> Self {
        Self {
            partition: "aws".to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topic {
    pub topic_arn: TopicArn,
    pub name: TopicName,
    pub display_name: Option<String>,
    pub created_at: TimestampMillis,
}

impl Topic {
    pub fn attributes(
        &self,
        confirmed_subscriptions: usize,
        pending_subscriptions: usize,
    ) -> HashMap<String, String> {
        HashMap::from([
            ("TopicArn".to_string(), self.topic_arn.to_string()),
            ("Owner".to_string(), "000000000000".to_string()),
            (
                "SubscriptionsPending".to_string(),
                pending_subscriptions.to_string(),
            ),
            (
                "SubscriptionsConfirmed".to_string(),
                confirmed_subscriptions.to_string(),
            ),
            ("SubscriptionsDeleted".to_string(), "0".to_string()),
            (
                "DisplayName".to_string(),
                self.display_name.clone().unwrap_or_default(),
            ),
        ])
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionProtocol {
    Queue,
    Http,
    Https,
}

impl SubscriptionProtocol {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "sqs" => Some(Self::Queue),
            "http" => Some(Self::Http),
            "https" => Some(Self::Https),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "sqs",
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    pub fn validate_endpoint(self, endpoint: &str) -> PubsubResult<()> {
        if endpoint.is_empty() {
            return Err(PubsubError::validation(
                PubsubValidationKind::InvalidEndpoint,
            ));
        }
        match self {
            Self::Http if !endpoint.starts_with("http://") => Err(PubsubError::validation(
                PubsubValidationKind::InvalidEndpoint,
            )),
            Self::Https if !endpoint.starts_with("https://") => Err(PubsubError::validation(
                PubsubValidationKind::InvalidEndpoint,
            )),
            _ => Ok(()),
        }
    }

    pub fn subscription_confirmation(self) -> SubscriptionConfirmation {
        match self {
            Self::Queue => SubscriptionConfirmation::Confirmed,
            Self::Http | Self::Https => SubscriptionConfirmation::Pending {
                token: Uuid::now_v7().to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Subscription {
    pub subscription_arn: SubscriptionArn,
    pub topic_arn: TopicArn,
    pub protocol: SubscriptionProtocol,
    pub endpoint: String,
    pub raw_message_delivery: bool,
    #[serde(default)]
    pub confirmation: SubscriptionConfirmation,
    #[serde(default)]
    pub extra_json: serde_json::Value,
    pub created_at: TimestampMillis,
}

impl Subscription {
    pub fn attributes(&self) -> HashMap<String, String> {
        HashMap::from([
            ("TopicArn".to_string(), self.topic_arn.to_string()),
            (
                "SubscriptionArn".to_string(),
                self.subscription_arn.to_string(),
            ),
            ("Protocol".to_string(), self.protocol.as_str().to_string()),
            ("Endpoint".to_string(), self.endpoint.clone()),
            (
                "PendingConfirmation".to_string(),
                self.confirmation.pending_confirmation().to_string(),
            ),
            (
                "RawMessageDelivery".to_string(),
                self.raw_message_delivery.to_string(),
            ),
            ("Owner".to_string(), "000000000000".to_string()),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionConfirmation {
    #[default]
    Confirmed,
    Pending {
        token: String,
    },
}

impl SubscriptionConfirmation {
    #[must_use]
    pub fn pending_confirmation(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    #[must_use]
    pub fn token(&self) -> Option<&str> {
        match self {
            Self::Confirmed => None,
            Self::Pending { token } => Some(token),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRecordId(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryTarget {
    BuiltIn,
    CustomSender,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    Delivered,
    AcceptedByCustomSender,
    RetryScheduled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRecord {
    pub id: DeliveryRecordId,
    #[serde(default)]
    pub kind: DeliveryRecordKind,
    pub message_id: PubsubMessageId,
    pub subscription_arn: SubscriptionArn,
    #[serde(default)]
    pub message_body: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub message_attributes: HashMap<String, String>,
    pub target: DeliveryTarget,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub next_attempt_at: Option<TimestampMillis>,
    #[serde(default)]
    pub lease_owner: Option<String>,
    #[serde(default)]
    pub lease_expires_at: Option<TimestampMillis>,
    pub last_error: Option<String>,
    pub created_at: TimestampMillis,
    pub updated_at: TimestampMillis,
}

impl DeliveryRecord {
    pub fn is_claimable(&self, now: TimestampMillis) -> bool {
        matches!(
            self.status,
            DeliveryStatus::Pending | DeliveryStatus::RetryScheduled
        ) && self
            .next_attempt_at
            .is_none_or(|next_attempt| next_attempt <= now)
            && self
                .lease_expires_at
                .is_none_or(|lease_expires_at| lease_expires_at <= now)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeliveryRecordKind {
    #[default]
    Notification,
    SubscriptionConfirmation {
        token: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimDeliveryRecordsRequest {
    pub owner: String,
    pub now: TimestampMillis,
    pub lease_expires_at: TimestampMillis,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimDeliveryRecordsResponse {
    pub records: Vec<DeliveryRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTopicRequest {
    pub name: TopicName,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTopicResponse {
    pub topic_arn: TopicArn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ListTopicsRequest {
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListTopicsResponse {
    pub topics: Vec<Topic>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetTopicAttributesRequest {
    pub topic_arn: TopicArn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetTopicAttributesResponse {
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetTopicAttributesRequest {
    pub topic_arn: TopicArn,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub topic_arn: TopicArn,
    pub protocol: SubscriptionProtocol,
    pub endpoint: String,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub extra_json: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscribeResponse {
    pub subscription_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmSubscriptionRequest {
    pub topic_arn: TopicArn,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmSubscriptionResponse {
    pub subscription_arn: SubscriptionArn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetSubscriptionAttributesRequest {
    pub subscription_arn: SubscriptionArn,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetSubscriptionAttributesRequest {
    pub subscription_arn: SubscriptionArn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetSubscriptionAttributesResponse {
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ListSubscriptionsRequest {
    pub topic_arn: Option<TopicArn>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListSubscriptionsResponse {
    pub subscriptions: Vec<Subscription>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishRequest {
    pub topic_arn: TopicArn,
    pub message: String,
    pub subject: Option<String>,
    #[serde(default)]
    pub message_attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishResponse {
    pub message_id: PubsubMessageId,
}
