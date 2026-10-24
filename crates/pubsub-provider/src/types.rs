use std::{collections::HashMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use storage_types::TimestampMillis;
use uuid::Uuid;

use crate::{
    PubsubError, PubsubMessageId, PubsubResult, PubsubValidationKind, SubscriptionArn, TopicArn,
    TopicName,
};

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

impl From<CreateTopicRequest> for Topic {
    fn from(request: CreateTopicRequest) -> Self {
        Self {
            topic_arn: TopicArn::compose("aws", "us-east-1", "000000000000", &request.name),
            name: request.name,
            display_name: request.attributes.get("DisplayName").cloned(),
            created_at: TimestampMillis::now(),
        }
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

impl From<SubscribeRequest> for Subscription {
    fn from(request: SubscribeRequest) -> Self {
        let subscription_arn = SubscriptionArn::compose(&request.topic_arn);
        let confirmation = request.protocol.subscription_confirmation();
        Self {
            subscription_arn,
            topic_arn: request.topic_arn,
            protocol: request.protocol,
            endpoint: request.endpoint,
            raw_message_delivery: request
                .attributes
                .get("RawMessageDelivery")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            confirmation,
            extra_json: request.extra_json,
            created_at: TimestampMillis::now(),
        }
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

impl From<DeliveryTarget> for &'static str {
    fn from(target: DeliveryTarget) -> Self {
        match target {
            DeliveryTarget::BuiltIn => "built_in",
            DeliveryTarget::CustomSender => "custom_sender",
        }
    }
}

impl fmt::Display for DeliveryTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str((*self).into())
    }
}

impl FromStr for DeliveryTarget {
    type Err = PubsubError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "built_in" => Ok(Self::BuiltIn),
            "custom_sender" => Ok(Self::CustomSender),
            _ => Err(PubsubError::storage("invalid delivery target")),
        }
    }
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

impl From<DeliveryStatus> for &'static str {
    fn from(status: DeliveryStatus) -> Self {
        match status {
            DeliveryStatus::Pending => "pending",
            DeliveryStatus::Delivered => "delivered",
            DeliveryStatus::AcceptedByCustomSender => "accepted_by_custom_sender",
            DeliveryStatus::RetryScheduled => "retry_scheduled",
            DeliveryStatus::Failed => "failed",
        }
    }
}

impl fmt::Display for DeliveryStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str((*self).into())
    }
}

impl FromStr for DeliveryStatus {
    type Err = PubsubError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "delivered" => Ok(Self::Delivered),
            "accepted_by_custom_sender" => Ok(Self::AcceptedByCustomSender),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "failed" => Ok(Self::Failed),
            _ => Err(PubsubError::storage("invalid delivery status")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRecord {
    pub id: DeliveryRecordId,
    #[serde(default)]
    pub kind: DeliveryRecordKind,
    pub message_id: PubsubMessageId,
    pub subscription_arn: SubscriptionArn,
    #[serde(default)]
    pub subscription: Option<Subscription>,
    #[serde(default)]
    pub message_body: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub message_attributes: HashMap<String, String>,
    pub target: DeliveryTarget,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub claim_generation: u64,
    pub next_attempt_at: Option<TimestampMillis>,
    #[serde(default)]
    pub lease_owner: Option<String>,
    #[serde(default)]
    pub lease_expires_at: Option<TimestampMillis>,
    pub last_error: Option<String>,
    pub created_at: TimestampMillis,
    pub updated_at: TimestampMillis,
}

/// Mutable delivery outcome guarded by the claim that produced it.
///
/// The delivery payload and subscription snapshot are intentionally absent:
/// once a record is accepted, those values are immutable. Providers must
/// apply this outcome only when both the generation and owner still match the
/// active claim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRecordUpdate {
    pub record_id: DeliveryRecordId,
    pub expected_claim_generation: u64,
    pub expected_lease_owner: String,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub next_attempt_at: Option<TimestampMillis>,
    pub last_error: Option<String>,
    pub updated_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingNotificationContext {
    pub accepted_at: TimestampMillis,
    pub custom_sender_enabled: bool,
}

impl PendingNotificationContext {
    #[must_use]
    pub const fn new(accepted_at: TimestampMillis, custom_sender_enabled: bool) -> Self {
        Self {
            accepted_at,
            custom_sender_enabled,
        }
    }
}

impl DeliveryRecord {
    /// The physical delivery stores use signed 64-bit claim generations.
    /// Reject an unrepresentable record before any batch member is written.
    pub fn validate_storage_range(&self) -> PubsubResult<()> {
        i64::try_from(self.claim_generation)
            .map(|_| ())
            .map_err(|_| PubsubError::storage("delivery claim generation exceeds storage range"))
    }

    #[must_use]
    pub fn pending_notification(
        request: &PublishRequest,
        message_id: &PubsubMessageId,
        subscription: &Subscription,
        custom_sender_enabled: bool,
    ) -> Self {
        Self::pending_notification_at(
            request,
            message_id,
            subscription,
            PendingNotificationContext::new(TimestampMillis::now(), custom_sender_enabled),
        )
    }

    #[must_use]
    pub fn pending_notification_at(
        request: &PublishRequest,
        message_id: &PubsubMessageId,
        subscription: &Subscription,
        context: PendingNotificationContext,
    ) -> Self {
        let target = if context.custom_sender_enabled
            && matches!(
                subscription.protocol,
                SubscriptionProtocol::Http | SubscriptionProtocol::Https
            ) {
            DeliveryTarget::CustomSender
        } else {
            DeliveryTarget::BuiltIn
        };
        Self {
            id: DeliveryRecordId(format!("{}:{}", subscription.subscription_arn, message_id)),
            kind: DeliveryRecordKind::Notification,
            message_id: message_id.clone(),
            subscription_arn: subscription.subscription_arn.clone(),
            subscription: Some(subscription.clone()),
            message_body: Some(request.message.clone()),
            subject: request.subject.clone(),
            message_attributes: request.message_attributes.clone(),
            target,
            status: DeliveryStatus::Pending,
            attempts: 0,
            claim_generation: 0,
            next_attempt_at: None,
            lease_owner: None,
            lease_expires_at: None,
            last_error: None,
            created_at: context.accepted_at,
            updated_at: context.accepted_at,
        }
    }

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

    /// Physical due indexes use this bound; claim policy still checks the row.
    pub fn due_at(&self) -> Option<TimestampMillis> {
        if !matches!(
            self.status,
            DeliveryStatus::Pending | DeliveryStatus::RetryScheduled
        ) {
            return None;
        }
        let mut due = self.created_at;
        if let Some(next) = self.next_attempt_at {
            due = due.max(next);
        }
        if let Some(lease) = self.lease_expires_at {
            due = due.max(lease);
        }
        Some(due)
    }

    /// Build the mutable outcome for this record's active claim.
    pub fn update_for_claim(
        &self,
        status: DeliveryStatus,
        attempts: u32,
        next_attempt_at: Option<TimestampMillis>,
        last_error: Option<String>,
        updated_at: TimestampMillis,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        let Some(expected_lease_owner) = self.lease_owner.clone() else {
            return Err(PubsubError::delivery_claim_lost(self.id.0.clone()));
        };
        if self.claim_generation == 0 {
            return Err(PubsubError::delivery_claim_lost(self.id.0.clone()));
        }
        Ok(DeliveryRecordUpdate {
            record_id: self.id.clone(),
            expected_claim_generation: self.claim_generation,
            expected_lease_owner,
            status,
            attempts,
            next_attempt_at,
            last_error,
            updated_at,
        })
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

impl<'a> From<&'a DeliveryRecordKind> for (&'static str, Option<&'a str>) {
    fn from(kind: &'a DeliveryRecordKind) -> Self {
        match kind {
            DeliveryRecordKind::Notification => ("notification", None),
            DeliveryRecordKind::SubscriptionConfirmation { token } => {
                ("subscription_confirmation", Some(token))
            }
        }
    }
}

impl fmt::Display for DeliveryRecordKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, _): (&str, Option<&str>) = self.into();
        formatter.write_str(kind)
    }
}

impl TryFrom<(&str, Option<String>)> for DeliveryRecordKind {
    type Error = PubsubError;

    fn try_from((kind, token): (&str, Option<String>)) -> Result<Self, Self::Error> {
        match kind {
            "notification" => Ok(Self::Notification),
            "subscription_confirmation" => Ok(Self::SubscriptionConfirmation {
                token: token.unwrap_or_default(),
            }),
            _ => Err(PubsubError::storage("invalid delivery kind")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimDeliveryRecordsRequest {
    pub owner: String,
    pub now: TimestampMillis,
    pub lease_expires_at: TimestampMillis,
    pub limit: crate::DeliveryClaimLimit,
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
