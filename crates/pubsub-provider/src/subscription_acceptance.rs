//! Select subscription and confirmation facts before either is persisted.

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    DeliveryRecord, DeliveryRecordId, DeliveryRecordKind, DeliveryStatus, DeliveryTarget,
    PubsubAttempt, PubsubMessageId, SubscribeResponse, Subscription, SubscriptionIdentity,
    SubscriptionRecord, delivery_operations::one_delivery, pubsub_port::read_topic,
};

/// Captured subscription identity, token, time, and optional confirmation
/// nonce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionAcceptanceInput {
    subscription: Subscription,
    confirmation_message_id: Option<PubsubMessageId>,
}

impl SubscriptionAcceptanceInput {
    pub fn try_new(
        subscription: Subscription,
        confirmation_message_id: Option<PubsubMessageId>,
    ) -> StorageResult<Self> {
        if subscription.confirmation.pending_confirmation() != confirmation_message_id.is_some()
            || subscription.confirmation.token().is_some_and(str::is_empty)
            || confirmation_message_id
                .as_ref()
                .is_some_and(|id| id.as_str().is_empty())
        {
            return Err(StorageError::internal(
                "subscription confirmation inputs differ",
            ));
        }
        Ok(Self {
            subscription,
            confirmation_message_id,
        })
    }

    pub fn capture(subscription: Subscription) -> StorageResult<Self> {
        let message_id = subscription
            .confirmation
            .pending_confirmation()
            .then(PubsubMessageId::new);
        Self::try_new(subscription, message_id)
    }

    pub fn subscription(&self) -> &Subscription {
        &self.subscription
    }

    pub(crate) fn into_parts(self) -> (Subscription, Option<PubsubMessageId>) {
        (self.subscription, self.confirmation_message_id)
    }
}

impl<'de> Deserialize<'de> for SubscriptionAcceptanceInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireInput {
            subscription: Subscription,
            confirmation_message_id: Option<PubsubMessageId>,
        }
        let input = WireInput::deserialize(deserializer)?;
        Self::try_new(input.subscription, input.confirmation_message_id).map_err(D::Error::custom)
    }
}

enum SubscriptionSelection {
    Existing(SubscriptionRecord),
    Create(SubscriptionRecord),
}

impl SubscriptionSelection {
    fn record(&self) -> &SubscriptionRecord {
        match self {
            Self::Existing(record) | Self::Create(record) => record,
        }
    }
}

/// Protected selection retains the native attempt that observed it.
#[must_use = "apply or drop the subscription in its original native attempt"]
pub struct PreparedSubscription<'native, A: PubsubAttempt> {
    attempt: &'native mut A,
    selection: SubscriptionSelection,
    confirmation: Option<DeliveryRecord>,
}

impl<'native, A: PubsubAttempt> PreparedSubscription<'native, A> {
    pub async fn prepare(
        attempt: &'native mut A,
        input: SubscriptionAcceptanceInput,
    ) -> StorageResult<Option<Self>> {
        let (subscription, message_id) = input.into_parts();
        let Some(topic) = read_topic(attempt, &subscription.topic_arn).await? else {
            return Ok(None);
        };
        let accepted_at = subscription.created_at;
        let identity = SubscriptionIdentity {
            topic_incarnation: topic.incarnation,
            protocol: subscription.protocol,
            endpoint: &subscription.endpoint,
        };
        let selection = match attempt.find_subscription_by_identity(identity).await? {
            Some(record) => SubscriptionSelection::Existing(record),
            None => SubscriptionSelection::Create(SubscriptionRecord {
                subscription,
                topic_incarnation: topic.incarnation,
            }),
        };
        let confirmation = DeliveryRecord::pending_confirmation_at(
            &selection.record().subscription,
            message_id.as_ref(),
            accepted_at,
        )?;
        let confirmation = if let Some(record) = confirmation {
            let existing = one_delivery(attempt.read_deliveries(vec![record.id.clone()]).await?)?;
            existing.is_none().then_some(record)
        } else {
            None
        };
        Ok(Some(Self {
            attempt,
            selection,
            confirmation,
        }))
    }

    pub fn record(&self) -> &SubscriptionRecord {
        self.selection.record()
    }

    pub fn confirmation(&self) -> Option<&DeliveryRecord> {
        self.confirmation.as_ref()
    }

    pub async fn apply(self) -> StorageResult<Subscription> {
        let subscription = match self.selection {
            SubscriptionSelection::Existing(record) => record.subscription,
            SubscriptionSelection::Create(record) => {
                let subscription = record.subscription.clone();
                self.attempt.write_subscription(record, None).await?;
                subscription
            }
        };
        if let Some(record) = self.confirmation {
            self.attempt.write_delivery_records(vec![record]).await?;
        }
        Ok(subscription)
    }
}

impl From<&Subscription> for SubscribeResponse {
    fn from(subscription: &Subscription) -> Self {
        Self {
            subscription_arn: if subscription.confirmation.pending_confirmation() {
                "pending confirmation".to_string()
            } else {
                subscription.subscription_arn.to_string()
            },
        }
    }
}

impl DeliveryRecord {
    pub(crate) fn pending_confirmation_at(
        subscription: &Subscription,
        message_id: Option<&PubsubMessageId>,
        accepted_at: TimestampMillis,
    ) -> StorageResult<Option<Self>> {
        let Some(token) = subscription.confirmation.token() else {
            return Ok(None);
        };
        let message_id = message_id
            .filter(|id| !id.as_str().is_empty())
            .filter(|_| !token.is_empty())
            .ok_or_else(|| StorageError::internal("subscription confirmation inputs differ"))?;
        Ok(Some(Self {
            id: DeliveryRecordId(format!(
                "{}:{}:confirmation",
                subscription.subscription_arn, message_id
            )),
            kind: DeliveryRecordKind::SubscriptionConfirmation {
                token: token.to_string(),
            },
            message_id: message_id.clone(),
            subscription_arn: subscription.subscription_arn.clone(),
            subscription: None,
            message_body: None,
            subject: None,
            message_attributes: std::collections::HashMap::new(),
            target: DeliveryTarget::BuiltIn,
            status: DeliveryStatus::Pending,
            attempts: 0,
            claim_generation: 0,
            next_attempt_at: None,
            lease_owner: None,
            lease_expires_at: None,
            last_error: None,
            created_at: accepted_at,
            updated_at: accepted_at,
        }))
    }
}
