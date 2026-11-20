//! One attribute rule for `Subscribe` and `SetSubscriptionAttributes`
//! (W3 design, sections 3.6 and 8.3, with the AWS results of section 14).
//! Every supported attribute is validated and stored; any other name is
//! rejected. Nothing is accepted and dropped.

use std::collections::HashMap;

use queue_provider::QueueArn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage_types::MessagingIdentity;

use crate::{
    FilterPolicy, FilterPolicyScope, PubsubError, PubsubResult, PubsubValidationKind, Subscription,
};

/// A subscription dead-letter queue. A target that does not exist is
/// accepted; the failure is seen at the move (AWS, section 14).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRedrivePolicy {
    pub target: QueueArn,
}

impl SubscriptionRedrivePolicy {
    /// `Ok(None)` for `{}`, which removes the policy (accepted until a probe
    /// shows the removal form). An empty string and unknown keys are rejected.
    fn parse(raw: &str, identity: &MessagingIdentity) -> PubsubResult<Option<Self>> {
        if raw.is_empty() {
            return Err(invalid("RedrivePolicy: missing required value"));
        }
        let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(raw) else {
            return Err(invalid("RedrivePolicy: failed to parse JSON."));
        };
        if fields.is_empty() {
            return Ok(None);
        }
        if let Some(field) = fields.keys().find(|field| *field != "deadLetterTargetArn") {
            return Err(invalid(&format!(
                "RedrivePolicy: Unrecognized field {field}"
            )));
        }
        let target = fields
            .get("deadLetterTargetArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("RedrivePolicy: deadLetterTargetArn is an invalid arn"))?;
        let target = queue_provider::QueueArn::parse(target, identity).map_err(|_| {
            // A well-formed ARN of another service is not an `invalid arn`
            // (suite script
            // `subscription_redrive_policy_non_sqs_target_rejected`).
            let others = target.split(':').collect::<Vec<_>>();
            if others.len() >= 6 && others[0] == "arn" && others[2] != "sqs" {
                invalid("RedrivePolicy: deadLetterTargetArn must be a SQS queue")
            } else {
                invalid("RedrivePolicy: deadLetterTargetArn is an invalid arn")
            }
        })?;
        Ok(Some(Self { target }))
    }
}

impl std::fmt::Display for SubscriptionRedrivePolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{{\"deadLetterTargetArn\":{}}}",
            Value::String(self.target.to_string())
        )
    }
}

/// The validated changes of one request. `None` leaves a value unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionAttributes {
    raw_message_delivery: Option<bool>,
    filter_policy: Option<Option<String>>,
    filter_policy_scope: Option<FilterPolicyScope>,
    redrive_policy: Option<Option<SubscriptionRedrivePolicy>>,
    delivery_policy: Option<Option<String>>,
}

impl SubscriptionAttributes {
    pub fn parse(
        attributes: &HashMap<String, String>,
        identity: &MessagingIdentity,
    ) -> PubsubResult<Self> {
        let mut parsed = Self::default();
        for (name, value) in attributes {
            match name.as_str() {
                "RawMessageDelivery" => {
                    parsed.raw_message_delivery = Some(parse_bool(name, value)?);
                }
                "FilterPolicy" => {
                    let policy = (!value.is_empty()).then(|| value.clone());
                    if let Some(policy) = &policy {
                        FilterPolicy::parse(policy)?;
                    }
                    parsed.filter_policy = Some(policy);
                }
                "FilterPolicyScope" => parsed.filter_policy_scope = Some(value.parse()?),
                "RedrivePolicy" => {
                    parsed.redrive_policy =
                        Some(SubscriptionRedrivePolicy::parse(value, identity)?);
                }
                "DeliveryPolicy" => parsed.delivery_policy = Some(parse_delivery_policy(value)?),
                _ => {
                    return Err(PubsubError::validation_with_detail(
                        PubsubValidationKind::UnsupportedAttribute,
                        name,
                    ));
                }
            }
        }
        Ok(parsed)
    }

    /// The attributes of a `Subscribe` request: AWS names them as
    /// `Attributes Reason: ...` there, and `Invalid parameter: <name>: ...`
    /// on `SetSubscriptionAttributes` (suite scripts
    /// `subscribe_with_an_unknown_attribute_rejected_and_creates_nothing`
    /// and `filter_policy_operator_vocabulary_round_trip`).
    pub fn parse_for_subscribe(
        attributes: &HashMap<String, String>,
        identity: &MessagingIdentity,
    ) -> PubsubResult<Self> {
        Self::parse(attributes, identity).map_err(|error| match error {
            PubsubError::Validation {
                kind: PubsubValidationKind::UnsupportedAttribute,
                detail,
            } => invalid(&format!(
                "Attributes Reason: Unknown attribute {}",
                detail.unwrap_or_default()
            )),
            PubsubError::Validation {
                kind: PubsubValidationKind::InvalidAttributeValue,
                detail,
            } => invalid(&format!(
                "Attributes Reason: {}",
                detail.unwrap_or_default()
            )),
            other => other,
        })
    }

    pub fn apply(self, subscription: &mut Subscription) {
        if let Some(raw) = self.raw_message_delivery {
            subscription.raw_message_delivery = raw;
        }
        if let Some(policy) = self.filter_policy {
            subscription.filter_policy = policy;
        }
        if let Some(scope) = self.filter_policy_scope {
            subscription.filter_policy_scope = scope;
        }
        if let Some(policy) = self.redrive_policy {
            subscription.redrive_policy = policy;
        }
        if let Some(policy) = self.delivery_policy {
            subscription.delivery_policy = policy;
        }
    }
}

impl Subscription {
    /// The values that `GetSubscriptionAttributes` returns for the stored
    /// attributes. `FilterPolicyScope` appears only with a `FilterPolicy`.
    pub(crate) fn stored_attributes(&self) -> Vec<(&'static str, String)> {
        let mut attributes = vec![("RawMessageDelivery", self.raw_message_delivery.to_string())];
        if let Some(policy) = &self.filter_policy {
            attributes.push(("FilterPolicy", policy.clone()));
            attributes.push(("FilterPolicyScope", self.filter_policy_scope.to_string()));
        }
        if let Some(policy) = &self.redrive_policy {
            attributes.push(("RedrivePolicy", policy.to_string()));
        }
        if let Some(policy) = &self.delivery_policy {
            attributes.push(("DeliveryPolicy", policy.clone()));
        }
        attributes
    }

    /// Whether a repeated `Subscribe` (`requested`) agrees with this stored
    /// subscription of the same identity. An attribute at its default counts
    /// as omitted and is not compared (suite script
    /// `subscribe_same_endpoint_is_idempotent`).
    #[must_use]
    pub fn has_same_attributes(&self, requested: &Self) -> bool {
        // Policies compare as JSON values, so whitespace and key order do
        // not make a repeated `Subscribe` fail (review C6).
        let normal = |subscription: &Self| {
            subscription
                .stored_attributes()
                .into_iter()
                .map(|(name, value)| {
                    let value =
                        serde_json::from_str::<Value>(&value).unwrap_or(Value::String(value));
                    (name, value)
                })
                .collect::<Vec<_>>()
        };
        let stored = normal(self);
        normal(requested)
            .into_iter()
            .filter(|(name, value)| {
                !(*name == "RawMessageDelivery" && *value == Value::Bool(false))
            })
            .all(|attribute| stored.contains(&attribute))
    }

    /// Whether the policy of this subscription accepts a message. A stored
    /// policy was validated when it was set, so an unreadable one matches.
    #[must_use]
    pub fn accepts(&self, request: &crate::PublishRequest) -> bool {
        let Some(policy) = self
            .filter_policy
            .as_deref()
            .and_then(|raw| FilterPolicy::parse(raw).ok())
        else {
            return true;
        };
        match self.filter_policy_scope {
            FilterPolicyScope::MessageAttributes => {
                policy.matches_attributes(&request.message_attributes)
            }
            FilterPolicyScope::MessageBody => policy.matches_body(&request.message),
        }
    }
}

fn parse_bool(name: &str, value: &str) -> PubsubResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid(&format!(
            "{name}: Invalid value [{value}]. Must be true or false."
        ))),
    }
}

/// AWS accepts and validates `DeliveryPolicy` on a subscription; `{}` is
/// valid. It is stored and returned; its retry numbers are not applied yet.
fn parse_delivery_policy(value: &str) -> PubsubResult<Option<String>> {
    if value.is_empty() {
        return Ok(None);
    }
    let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(value) else {
        return Err(invalid("DeliveryPolicy: failed to parse JSON."));
    };
    for (section, content) in &fields {
        let numeric = match section.as_str() {
            "healthyRetryPolicy" | "sicklyRetryPolicy" => &[
                "minDelayTarget",
                "maxDelayTarget",
                "numRetries",
                "numMaxDelayRetries",
                "numNoDelayRetries",
                "numMinDelayRetries",
            ][..],
            "throttlePolicy" => &["maxReceivesPerSecond"][..],
            "requestPolicy" => &["headerContentType"][..],
            "guaranteed" if content.is_boolean() => continue,
            _ => {
                return Err(invalid(&format!(
                    "DeliveryPolicy: Unrecognized section {section}"
                )));
            }
        };
        let Value::Object(entries) = content else {
            return Err(invalid(&format!(
                "DeliveryPolicy: {section} must be an object"
            )));
        };
        for (key, entry) in entries {
            let is_valid = match key.as_str() {
                "backoffFunction" => matches!(
                    entry.as_str(),
                    Some("linear" | "arithmetic" | "geometric" | "exponential")
                ),
                "headerContentType" => entry.is_string(),
                known if numeric.contains(&known) => entry.as_u64().is_some(),
                _ => false,
            };
            if !is_valid {
                return Err(invalid(&format!(
                    "DeliveryPolicy: Invalid value for {section}.{key}"
                )));
            }
        }
    }
    // `{}` stays as sent: a recorded read-back of it is not known.
    if fields.is_empty() {
        return Ok(Some(value.to_owned()));
    }
    Ok(Some(normalize_delivery_policy(&fields).to_string()))
}

/// The stored form of AWS (suite script
/// `subscription_delivery_policy_reads_back_normalized_on_an_sqs_subscription`):
/// every section and field present, unset ones `null`, `guaranteed` false.
fn normalize_delivery_policy(fields: &serde_json::Map<String, Value>) -> Value {
    const RETRY: [&str; 7] = [
        "minDelayTarget",
        "maxDelayTarget",
        "numRetries",
        "numMaxDelayRetries",
        "numNoDelayRetries",
        "numMinDelayRetries",
        "backoffFunction",
    ];
    let sections: [(&str, &[&str]); 4] = [
        ("healthyRetryPolicy", &RETRY),
        ("sicklyRetryPolicy", &RETRY),
        ("throttlePolicy", &["maxReceivesPerSecond"]),
        ("requestPolicy", &["headerContentType"]),
    ];
    let mut normalized = serde_json::Map::new();
    for (section, keys) in sections {
        let value = match fields.get(section) {
            Some(Value::Object(entries)) => Value::Object(
                keys.iter()
                    .map(|key| {
                        (
                            (*key).to_owned(),
                            entries.get(*key).cloned().unwrap_or(Value::Null),
                        )
                    })
                    .collect(),
            ),
            _ => Value::Null,
        };
        normalized.insert(section.to_owned(), value);
    }
    let guaranteed = fields.get("guaranteed").and_then(Value::as_bool);
    normalized.insert(
        "guaranteed".to_owned(),
        Value::Bool(guaranteed.unwrap_or(false)),
    );
    Value::Object(normalized)
}

fn invalid(detail: &str) -> PubsubError {
    PubsubError::validation_with_detail(PubsubValidationKind::InvalidAttributeValue, detail)
}
