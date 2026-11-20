use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use storage_types::{MessagingIdentity, TimestampMillis};

use crate::{
    FilterPolicyScope, PubsubError, PubsubMessageId, PubsubResult, PubsubValidationKind,
    SubscriptionArn, SubscriptionAttributes, SubscriptionRedrivePolicy, TopicArn, TopicName,
    topic_attributes::account_of,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topic {
    pub topic_arn: TopicArn,
    pub name: TopicName,
    pub display_name: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
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
            ("Owner".to_string(), self.owner().to_owned()),
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

/// Why a `Protocol` value is rejected (W3 design, section 8.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolRejection {
    /// Not an AWS protocol string.
    Unknown,
    /// An AWS protocol that this service does not implement.
    NotImplemented,
}

impl ProtocolRejection {
    #[must_use]
    pub fn error(self, value: &str) -> PubsubError {
        let kind = match self {
            Self::Unknown => PubsubValidationKind::UnsupportedProtocol,
            Self::NotImplemented => PubsubValidationKind::ProtocolNotImplemented,
        };
        PubsubError::validation_with_detail(kind, value)
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
    pub fn parse(value: &str) -> Result<Self, ProtocolRejection> {
        match value {
            "sqs" => Ok(Self::Queue),
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            "email" | "email-json" | "sms" | "lambda" | "application" | "firehose" => {
                Err(ProtocolRejection::NotImplemented)
            }
            _ => Err(ProtocolRejection::Unknown),
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

    /// An `sqs` endpoint is the ARN of a standard queue in this deployment's
    /// partition and account (AWS rejects a queue URL, section 13). The queue
    /// does not need to exist.
    pub fn validate_endpoint(
        self,
        endpoint: &str,
        identity: &MessagingIdentity,
    ) -> PubsubResult<()> {
        let is_valid = match self {
            Self::Queue => {
                queue_provider::QueueArn::parse(endpoint, identity).is_ok_and(|arn| !arn.is_fifo())
            }
            Self::Http => endpoint.starts_with("http://"),
            Self::Https => endpoint.starts_with("https://"),
        };
        match (is_valid, self) {
            (true, _) => Ok(()),
            (false, Self::Queue) => Err(PubsubError::validation(
                PubsubValidationKind::InvalidSqsEndpoint,
            )),
            (false, _) => Err(PubsubError::validation(
                PubsubValidationKind::InvalidEndpoint,
            )),
        }
    }

    pub fn subscription_confirmation(self) -> SubscriptionConfirmation {
        match self {
            Self::Queue => SubscriptionConfirmation::Confirmed,
            Self::Http | Self::Https => SubscriptionConfirmation::Pending {
                token: storage_types::ambient::uuid_now_v7().to_string(),
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
    pub filter_policy: Option<String>,
    #[serde(default)]
    pub filter_policy_scope: FilterPolicyScope,
    #[serde(default)]
    pub redrive_policy: Option<SubscriptionRedrivePolicy>,
    #[serde(default)]
    pub delivery_policy: Option<String>,
    #[serde(default)]
    pub confirmation: SubscriptionConfirmation,
    #[serde(default)]
    pub extra_json: serde_json::Value,
    pub created_at: TimestampMillis,
}

impl Subscription {
    /// Validate the endpoint and every attribute of a `Subscribe` request.
    pub fn try_new(request: SubscribeRequest, identity: &MessagingIdentity) -> PubsubResult<Self> {
        request
            .protocol
            .validate_endpoint(&request.endpoint, identity)?;
        let attributes =
            SubscriptionAttributes::parse_for_subscribe(&request.attributes, identity)?;
        let mut subscription = Self {
            subscription_arn: SubscriptionArn::compose(&request.topic_arn),
            confirmation: request.protocol.subscription_confirmation(),
            topic_arn: request.topic_arn,
            protocol: request.protocol,
            endpoint: request.endpoint,
            raw_message_delivery: false,
            filter_policy: None,
            filter_policy_scope: FilterPolicyScope::default(),
            redrive_policy: None,
            delivery_policy: None,
            extra_json: request.extra_json,
            created_at: TimestampMillis::now(),
        };
        attributes.apply(&mut subscription);
        Ok(subscription)
    }

    /// The account of the topic ARN; it is the `Owner` attribute.
    #[must_use]
    pub fn owner(&self) -> &str {
        account_of(self.topic_arn.as_str())
    }

    pub fn attributes(&self) -> HashMap<String, String> {
        let mut attributes = HashMap::from([
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
            ("Owner".to_string(), self.owner().to_owned()),
        ]);
        attributes.extend(
            self.stored_attributes()
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value)),
        );
        attributes
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
pub struct CreateTopicRequest {
    pub name: TopicName,
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct PublishRequest {
    pub topic_arn: TopicArn,
    pub message: String,
    pub subject: Option<String>,
    #[serde(default, serialize_with = "serialize_sorted_attributes")]
    pub message_attributes: HashMap<String, crate::MessageAttribute>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishResponse {
    pub message_id: PubsubMessageId,
}

/// Stored in name order, so that the bytes of one payload are the same after
/// each encode.
pub(crate) fn serialize_sorted_attributes<S: serde::Serializer>(
    attributes: &HashMap<String, crate::MessageAttribute>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_map(attributes.iter().collect::<BTreeMap<_, _>>())
}
