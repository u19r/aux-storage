//! Shared external delivery; returns an outcome without saving native state.

use std::collections::HashMap;

use http_request::StatusCode;
use pubsub_provider::{
    DeliveryRecord, DeliveryRecordId, DeliveryRecordKind, DeliveryRecordUpdate, DeliveryStatus,
    DeliveryTarget, PublishRequest, PubsubError, PubsubMessageId, PubsubResult, Subscription,
    SubscriptionProtocol,
};
use queue_provider::{MessageAttributeValue, SendMessageRequest};
use storage_types::TimestampMillis;
use stream_provider::{SubscriptionDestination, SubscriptionMessage, SubscriptionSendOutcome};

use super::{
    manager::{PubsubManager, duration_millis_i64},
    notification::{
        ConfirmationRenderContext, NotificationRenderContext, confirmation_body,
        confirmation_headers, notification_body, notification_headers,
    },
};

struct DeliveryClaim {
    id: DeliveryRecordId,
    message_id: PubsubMessageId,
    target: DeliveryTarget,
    attempts: u32,
    claim_generation: u64,
    lease_owner: String,
    // The existing durable claim owns this observation. Settlement and retry
    // retain it instead of mixing scheduler and machine clocks.
    observed_at: TimestampMillis,
}

impl DeliveryClaim {
    fn update(self, status: DeliveryStatus, last_error: Option<String>) -> DeliveryRecordUpdate {
        DeliveryRecordUpdate {
            record_id: self.id,
            expected_claim_generation: self.claim_generation,
            expected_lease_owner: self.lease_owner,
            status,
            attempts: self.attempts.saturating_add(1),
            next_attempt_at: None,
            last_error,
            updated_at: self.observed_at,
        }
    }
}

impl PubsubManager {
    pub fn delivery_lease_deadline(&self, now: TimestampMillis) -> TimestampMillis {
        TimestampMillis::from_timestamp(
            now.timestamp_millis()
                .saturating_add(duration_millis_i64(self.delivery_config.lease_duration)),
        )
    }

    /// Uses the existing sender and retry policy, but never saves an outcome.
    /// The caller owns the durable claim and settlement boundary. Its claim
    /// timestamp anchors retry backoff; no second clock enters the outcome.
    pub async fn deliver_claimed_record(
        &self,
        record: DeliveryRecord,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        let DeliveryRecord {
            id,
            message_id,
            target,
            attempts,
            claim_generation,
            lease_owner,
            updated_at: observed_at,
            kind,
            subscription_arn,
            subscription,
            message_body,
            subject,
            message_attributes,
            ..
        } = record;
        if claim_generation == 0 {
            return Err(PubsubError::delivery_claim_lost(id.0));
        }
        let claim = DeliveryClaim {
            id,
            message_id,
            target,
            attempts,
            claim_generation,
            lease_owner: lease_owner
                .ok_or_else(|| PubsubError::delivery_claim_lost(subscription_arn.to_string()))?,
            observed_at,
        };
        let subscription = match subscription {
            Some(subscription) => subscription,
            None => {
                let Some(subscription) = self.provider.get_subscription(&subscription_arn).await?
                else {
                    return self.create_delivery_outcome(
                        claim,
                        DeliveryStatus::Failed,
                        Some("subscription not found".to_string()),
                    );
                };
                subscription
            }
        };
        if let DeliveryRecordKind::SubscriptionConfirmation { token } = kind {
            return self.send_confirmation(claim, subscription, token).await;
        }
        let Some(message_body) = message_body else {
            return self.create_delivery_outcome(
                claim,
                DeliveryStatus::Failed,
                Some("message body missing".to_string()),
            );
        };
        let request = PublishRequest {
            topic_arn: subscription.topic_arn.clone(),
            message: message_body,
            subject,
            message_attributes,
        };

        self.send_notification(claim, subscription, request).await
    }

    async fn send_notification(
        &self,
        claim: DeliveryClaim,
        subscription: Subscription,
        request: PublishRequest,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        match claim.target {
            DeliveryTarget::CustomSender => {
                self.send_custom_delivery(claim, subscription, request)
                    .await
            }
            DeliveryTarget::BuiltIn => {
                self.send_builtin_delivery(claim, subscription, request)
                    .await
            }
        }
    }

    async fn send_custom_delivery(
        &self,
        claim: DeliveryClaim,
        subscription: Subscription,
        request: PublishRequest,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        let Some(sender) = self.subscription_message_sender.as_ref() else {
            return self.plan_retry_outcome(claim, "custom sender is not configured");
        };
        let destination = subscription_destination(&subscription);
        let message = SubscriptionMessage::new(
            subscription.subscription_arn.to_string(),
            claim.message_id.to_string(),
            destination,
            request.message.into_bytes(),
        )
        .with_attributes(request.message_attributes);
        match sender.send_subscription_message(message).await {
            Ok(SubscriptionSendOutcome::Delivered) => {
                self.create_delivery_outcome(claim, DeliveryStatus::Delivered, None)
            }
            Ok(SubscriptionSendOutcome::AcceptedForDelivery) => {
                self.create_delivery_outcome(claim, DeliveryStatus::AcceptedByCustomSender, None)
            }
            Err(error) => self.plan_retry_outcome(claim, &error.to_string()),
        }
    }

    async fn send_builtin_delivery(
        &self,
        claim: DeliveryClaim,
        subscription: Subscription,
        request: PublishRequest,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        match subscription.protocol {
            SubscriptionProtocol::Queue => {
                self.send_queue_delivery(claim, subscription, request).await
            }
            SubscriptionProtocol::Http | SubscriptionProtocol::Https => {
                let body =
                    self.pubsub_notification_body(&request, &claim.message_id, &subscription)?;
                let headers = notification_headers(&claim.message_id, &subscription, &request)?;
                self.send_http_delivery(claim, &subscription.endpoint, headers, body)
                    .await
            }
        }
    }

    async fn send_queue_delivery(
        &self,
        claim: DeliveryClaim,
        subscription: Subscription,
        request: PublishRequest,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        let Some(manager) = &self.queue_manager else {
            return self.plan_retry_outcome(claim, "queue manager is not configured");
        };
        let message_attributes = if subscription.raw_message_delivery {
            queue_message_attributes(&request.message_attributes)
        } else {
            None
        };
        let message_body = if subscription.raw_message_delivery {
            request.message
        } else {
            self.pubsub_notification_body(&request, &claim.message_id, &subscription)?
        };
        match manager
            .send_message(SendMessageRequest {
                queue_url: subscription.endpoint,
                message_body,
                delay_seconds: None,
                message_attributes,
            })
            .await
        {
            Ok(_) => self.create_delivery_outcome(claim, DeliveryStatus::Delivered, None),
            Err(error) => self.plan_retry_outcome(claim, &error.to_string()),
        }
    }

    async fn send_http_delivery(
        &self,
        claim: DeliveryClaim,
        endpoint: &str,
        headers: http_request::reqwest::header::HeaderMap,
        body: String,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        let Some(client) = &self.http_client else {
            return self.plan_retry_outcome(claim, "HTTP client is not configured");
        };
        match client
            .post(endpoint)
            .headers(headers)
            .timeout(self.delivery_config.request_timeout)
            .body(body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                self.create_delivery_outcome(claim, DeliveryStatus::Delivered, None)
            }
            Ok(response) => {
                let status = response.status();
                if is_retryable_status(status) {
                    self.plan_retry_outcome(claim, &format!("HTTP delivery status {status}"))
                } else {
                    self.create_delivery_outcome(
                        claim,
                        DeliveryStatus::Failed,
                        Some(format!("HTTP delivery status {status}")),
                    )
                }
            }
            Err(error) => self.plan_retry_outcome(claim, &error.to_string()),
        }
    }

    async fn send_confirmation(
        &self,
        claim: DeliveryClaim,
        subscription: Subscription,
        token: String,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        if !matches!(
            subscription.protocol,
            SubscriptionProtocol::Http | SubscriptionProtocol::Https
        ) {
            return self.create_delivery_outcome(
                claim,
                DeliveryStatus::Failed,
                Some("subscription confirmation is only supported for HTTP endpoints".to_string()),
            );
        }
        if !subscription.confirmation.pending_confirmation() {
            return self.create_delivery_outcome(claim, DeliveryStatus::Delivered, None);
        }
        if self.http_client.is_none() {
            return self.plan_retry_outcome(claim, "HTTP client is not configured");
        }
        let body = confirmation_body(ConfirmationRenderContext {
            topic_arn: &subscription.topic_arn,
            message_id: &claim.message_id,
            token: &token,
            delivery_config: &self.delivery_config,
            signer: self.notification_signer.as_deref(),
        })?;
        let headers = confirmation_headers(&claim.message_id, &subscription.topic_arn)?;
        self.send_http_delivery(claim, &subscription.endpoint, headers, body)
            .await
    }

    fn plan_retry_outcome(
        &self,
        claim: DeliveryClaim,
        error: &str,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        let attempts = claim.attempts.saturating_add(1);
        let status = if attempts >= self.delivery_config.max_attempts {
            DeliveryStatus::Failed
        } else {
            DeliveryStatus::RetryScheduled
        };
        let mut outcome = claim.update(status, Some(error.to_string()));
        if status == DeliveryStatus::RetryScheduled {
            outcome.next_attempt_at = Some(TimestampMillis::from_timestamp(
                outcome
                    .updated_at
                    .timestamp_millis()
                    .saturating_add(self.retry_delay_millis(attempts)),
            ));
        }
        Ok(outcome)
    }

    fn create_delivery_outcome(
        &self,
        claim: DeliveryClaim,
        status: DeliveryStatus,
        last_error: Option<String>,
    ) -> PubsubResult<DeliveryRecordUpdate> {
        Ok(claim.update(status, last_error))
    }

    fn retry_delay_millis(&self, attempts: u32) -> i64 {
        let exponent = attempts.saturating_sub(1).min(10);
        duration_millis_i64(self.delivery_config.retry_base_delay)
            .saturating_mul(2_i64.saturating_pow(exponent))
    }

    fn pubsub_notification_body(
        &self,
        request: &PublishRequest,
        message_id: &PubsubMessageId,
        subscription: &Subscription,
    ) -> PubsubResult<String> {
        notification_body(NotificationRenderContext {
            request,
            message_id,
            subscription,
            delivery_config: &self.delivery_config,
            signer: self.notification_signer.as_deref(),
        })
    }
}

fn queue_message_attributes(
    attributes: &HashMap<String, String>,
) -> Option<HashMap<String, MessageAttributeValue>> {
    if attributes.is_empty() {
        return None;
    }
    Some(
        attributes
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    MessageAttributeValue {
                        string_value: Some(value.clone()),
                        binary_value: None,
                        data_type: "String".to_string(),
                    },
                )
            })
            .collect(),
    )
}

fn subscription_destination(subscription: &Subscription) -> SubscriptionDestination {
    SubscriptionDestination::new(
        subscription.protocol.as_str(),
        subscription.endpoint.clone(),
    )
    .with_extra_json(subscription.extra_json.clone())
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}
