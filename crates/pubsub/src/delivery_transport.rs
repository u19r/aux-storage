//! One claimed delivery: attempt it, then settle the result under the claim
//! fence (W3 design, sections 1 and 5). An SQS destination on the same store
//! is delivered and settled in one native transaction; other destinations
//! are attempted here and settled in one attempt after the request.

use std::{collections::HashMap, time::Duration};

use http_request::{HttpRequestError, HttpRequestErrorKind};
use pubsub_provider::{
    ClaimedDelivery, DeliveryAttemptResult, DeliveryFailureCode, DeliveryPayload,
    DeliveryRecordKind, DeliveryTarget, LocalDelivery, LocalDeliveryItem, LocalOutcome,
    PublishRequest, PubsubMessageId, PubsubResult, RenderedQueueMessage, SettleInput,
    SettleOutcome, SettleReport, Subscription, SubscriptionProtocol,
};
use queue_provider::{MessageAttributeValue, QueueArn};
use stream_provider::{SubscriptionDestination, SubscriptionMessage, SubscriptionSendOutcome};

use super::{
    delivery_scheduler::BreakerSignal,
    manager::PubsubManager,
    notification::{
        ConfirmationRenderContext, NotificationRenderContext, confirmation_body,
        confirmation_headers, notification_body, notification_headers,
    },
};

/// SQS accepts at most ten message attributes on a raw delivery.
const MAX_RAW_QUEUE_ATTRIBUTES: usize = 10;

/// The payload of a claimed notification, from the record's snapshot.
struct ClaimedNotification {
    message_id: PubsubMessageId,
    subscription: Subscription,
    request: PublishRequest,
}

/// The deliveries of one claim for one queue of this store.
pub(crate) struct LocalBatch {
    pub(crate) queue: QueueArn,
    pub(crate) items: Vec<LocalDeliveryItem>,
}

/// The settle reports of one local batch, and its records left to lease
/// expiry because their lease no longer covered the attempt.
#[derive(Debug)]
pub(crate) struct LocalBatchPass {
    pub(crate) reports: Vec<SettleReport>,
    pub(crate) lease_too_short: usize,
}

/// One claimed record after its pass: settled, or left to lease expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryPass {
    Settled {
        class: BreakerSignal,
        report: SettleReport,
    },
    /// The lease no longer covers one request and its settle; nothing was
    /// attempted and the record becomes due when the lease ends.
    LeaseTooShort,
}

impl PubsubManager {
    /// Attempt and settle one claimed record. A record is attempted only
    /// while its lease covers the request and the settle (design 4.1).
    pub(crate) async fn deliver_and_settle(
        &self,
        claimed: ClaimedDelivery,
    ) -> PubsubResult<DeliveryPass> {
        let timing = self.delivery_config.timing;
        if !timing.covers_attempt(lease_end(&claimed), self.provider.now()) {
            return Ok(DeliveryPass::LeaseTooShort);
        }
        let jitter = storage_types::ambient::uuid_now_v7().as_u128() as u32;
        let local = self.local_item(&claimed);
        let (class, report) = match local {
            Ok(Some((queue, item))) => {
                let mut pass = self.deliver_local_batch(queue, vec![item]).await?;
                let Some(report) = pass.reports.pop() else {
                    return Ok(DeliveryPass::LeaseTooShort);
                };
                return Ok(DeliveryPass::Settled {
                    class: BreakerSignal::Answered,
                    report,
                });
            }
            unattempted => {
                // An error before or while preparing the attempt (rendering,
                // signing) is settled as transient under the fence (review
                // D5), so the record reaches its deadline instead of being
                // claimed again every lease.
                let prepared = unattempted
                    .map(|_| ())
                    .and_then(|()| self.dead_letter_message(&claimed.payload));
                let (result, dead_letter) = match prepared {
                    Ok(dead_letter) => match self.attempt_delivery(&claimed).await {
                        Ok(result) => (result, dead_letter),
                        Err(error) => (prepare_failure(&error), dead_letter),
                    },
                    Err(error) => (prepare_failure(&error), None),
                };
                let class = match result {
                    DeliveryAttemptResult::Delivered | DeliveryAttemptResult::Permanent(_) => {
                        BreakerSignal::Answered
                    }
                    DeliveryAttemptResult::Transient(_) => BreakerSignal::Failed,
                };
                let report = self
                    .provider
                    .settle_delivery(SettleInput {
                        message_id: claimed.payload.message_id.clone(),
                        claimed: claimed.state,
                        fence: claimed.fence,
                        result,
                        dead_letter,
                        jitter,
                        identity: self.identity.clone(),
                    })
                    .await?;
                (class, report)
            }
        };
        self.after_settle(&report);
        Ok(DeliveryPass::Settled { class, report })
    }

    /// Split claimed records into deliveries to queues of this store,
    /// grouped by queue (review E1), and the other records.
    pub(crate) fn split_local(
        &self,
        claims: Vec<ClaimedDelivery>,
    ) -> (Vec<LocalBatch>, Vec<ClaimedDelivery>) {
        let mut batches: Vec<LocalBatch> = Vec::new();
        let mut others = Vec::new();
        for claimed in claims {
            match self.local_item(&claimed) {
                Ok(Some((queue, item))) => {
                    match batches.iter_mut().find(|batch| batch.queue == queue) {
                        Some(batch) => batch.items.push(item),
                        None => batches.push(LocalBatch {
                            queue,
                            items: vec![item],
                        }),
                    }
                }
                // An error while rendering is settled on the other path
                // (review D5).
                Ok(None) | Err(_) => others.push(claimed),
            }
        }
        (batches, others)
    }

    /// Deliver every record of one queue in one native attempt (review E1).
    /// A record whose lease no longer covers the attempt is left to lease
    /// expiry and counted. A store failure is not a destination failure, so
    /// local batches pass neither the per-destination limit nor the breaker.
    /// A batch over the record or byte bound, or one that the store rejects
    /// for its size, is split in halves; one record that is still too large
    /// is settled as a store failure (finding 4).
    pub(crate) async fn deliver_local_batch(
        &self,
        queue: QueueArn,
        items: Vec<LocalDeliveryItem>,
    ) -> PubsubResult<LocalBatchPass> {
        let now = self.provider.now();
        let timing = self.delivery_config.timing;
        let mut lease_too_short = 0;
        let items: Vec<LocalDeliveryItem> = items
            .into_iter()
            .filter_map(|item| {
                let until = item.claimed.lease.as_ref().map(|lease| lease.until);
                if timing.covers_attempt(until, now) {
                    Some(item)
                } else {
                    lease_too_short += 1;
                    None
                }
            })
            .collect();
        let mut reports = Vec::with_capacity(items.len());
        let mut pending = vec![items];
        while let Some(items) = pending.pop() {
            if items.is_empty() {
                continue;
            }
            let delivery = LocalDelivery {
                queue: queue.clone(),
                items,
                jitter: storage_types::ambient::uuid_now_v7().as_u128() as u32,
                identity: self.identity.clone(),
            };
            match self
                .provider
                .deliver_to_local_queue(delivery.clone())
                .await?
            {
                LocalOutcome::Settled(settled) => reports.extend(settled),
                LocalOutcome::TooLarge if delivery.items.len() > 1 => {
                    let mut first = delivery.items;
                    let second = first.split_off(first.len() / 2);
                    pending.push(second);
                    pending.push(first);
                }
                LocalOutcome::TooLarge => {
                    reports.push(self.settle_too_large(delivery).await?);
                }
            }
        }
        reports.iter().for_each(|report| self.after_settle(report));
        Ok(LocalBatchPass {
            reports,
            lease_too_short,
        })
    }

    /// One record that the store rejects for its size: a transient store
    /// failure, so it reaches its deadline instead of looping.
    async fn settle_too_large(&self, delivery: LocalDelivery) -> PubsubResult<SettleReport> {
        let Some(item) = delivery.items.into_iter().next() else {
            return Err(pubsub_provider::PubsubError::storage(
                "a local delivery has no item",
            ));
        };
        self.provider
            .settle_delivery(SettleInput {
                claimed: item.claimed,
                message_id: item.message_id,
                fence: item.fence,
                result: DeliveryAttemptResult::Transient(
                    DeliveryFailureCode::QueueStoreUnavailable,
                ),
                dead_letter: Some(item.message),
                jitter: delivery.jitter,
                identity: delivery.identity,
            })
            .await
    }

    /// An SQS notification on the built-in target is delivered in the store's
    /// own transaction. A raw delivery with too many attributes, or an
    /// endpoint that is no longer a queue ARN of this deployment, is a
    /// permanent failure that the settle path decides.
    fn local_item(
        &self,
        claimed: &ClaimedDelivery,
    ) -> PubsubResult<Option<(QueueArn, LocalDeliveryItem)>> {
        let Some(notification) = claimed_notification(&claimed.payload) else {
            return Ok(None);
        };
        if claimed.payload.target != DeliveryTarget::BuiltIn
            || notification.subscription.protocol != SubscriptionProtocol::Queue
            || is_raw_over_attribute_limit(&notification)
        {
            return Ok(None);
        }
        let Some(queue) = QueueArn::parse(&notification.subscription.endpoint, &self.identity).ok()
        else {
            return Ok(None);
        };
        Ok(Some((
            queue,
            LocalDeliveryItem {
                claimed: claimed.state.clone(),
                fence: claimed.fence.clone(),
                message_id: claimed.payload.message_id.clone(),
                message: self.queue_rendering(&notification)?,
            },
        )))
    }

    /// The dead-letter message has the form of a normal delivery of the
    /// subscription (AWS, section 14). A confirmation never moves.
    fn dead_letter_message(
        &self,
        payload: &DeliveryPayload,
    ) -> PubsubResult<Option<RenderedQueueMessage>> {
        claimed_notification(payload)
            .map(|notification| self.queue_rendering(&notification))
            .transpose()
    }

    async fn attempt_delivery(
        &self,
        claimed: &ClaimedDelivery,
    ) -> PubsubResult<DeliveryAttemptResult> {
        let record = &claimed.payload;
        if let DeliveryRecordKind::SubscriptionConfirmation { token } = &record.kind {
            let Some(subscription) = self
                .provider
                .get_subscription(&claimed.state.subscription_arn)
                .await?
            else {
                return Ok(DeliveryAttemptResult::Permanent(
                    DeliveryFailureCode::Internal,
                ));
            };
            return self
                .send_confirmation(&record.message_id, &subscription, token)
                .await;
        }
        let Some(notification) = claimed_notification(record) else {
            return Ok(DeliveryAttemptResult::Permanent(
                DeliveryFailureCode::Internal,
            ));
        };
        match (record.target, notification.subscription.protocol) {
            (DeliveryTarget::CustomSender, _) => Ok(self.send_custom_delivery(notification).await),
            (DeliveryTarget::BuiltIn, SubscriptionProtocol::Queue) => {
                let failure = if is_raw_over_attribute_limit(&notification) {
                    DeliveryFailureCode::RawAttributeLimit
                } else {
                    DeliveryFailureCode::DestinationMissing
                };
                Ok(DeliveryAttemptResult::Permanent(failure))
            }
            (DeliveryTarget::BuiltIn, SubscriptionProtocol::Http | SubscriptionProtocol::Https) => {
                let body = self.envelope(&notification)?;
                let headers = notification_headers(
                    &notification.message_id,
                    &notification.subscription,
                    &notification.request,
                )?;
                Ok(self
                    .send_http_delivery(&notification.subscription.endpoint, headers, body)
                    .await)
            }
        }
    }

    async fn send_custom_delivery(
        &self,
        notification: ClaimedNotification,
    ) -> DeliveryAttemptResult {
        let Some(sender) = self.subscription_message_sender.as_ref() else {
            return not_configured("custom sender");
        };
        let destination = subscription_destination(&notification.subscription);
        let message = SubscriptionMessage::new(
            notification.subscription.subscription_arn.to_string(),
            notification.message_id.to_string(),
            destination,
            notification.request.message.into_bytes(),
        )
        .with_attributes(
            notification
                .request
                .message_attributes
                .into_iter()
                .map(|(name, value)| (name, value.wire_value().into_owned()))
                .collect(),
        );
        // A hung sender ends at the request timeout (review D6), so its
        // destination slot is freed and the lease still covers the settle.
        match tokio::time::timeout(
            self.delivery_config.timing.request_timeout(),
            sender.send_subscription_message(message),
        )
        .await
        {
            Ok(Ok(
                SubscriptionSendOutcome::Delivered | SubscriptionSendOutcome::AcceptedForDelivery,
            )) => DeliveryAttemptResult::Delivered,
            Ok(Err(_)) | Err(_) => {
                DeliveryAttemptResult::Transient(DeliveryFailureCode::CustomSender)
            }
        }
    }

    async fn send_http_delivery(
        &self,
        endpoint: &str,
        headers: http_request::reqwest::header::HeaderMap,
        body: String,
    ) -> DeliveryAttemptResult {
        let Some(client) = &self.http_client else {
            return not_configured("HTTP client");
        };
        match client
            .post(endpoint)
            .headers(headers)
            .timeout(self.delivery_config.timing.request_timeout())
            .body(body)
            .send()
            .await
        {
            Ok(response) => DeliveryAttemptResult::from_http_status(response.status().as_u16()),
            Err(HttpRequestError::Transport {
                kind: HttpRequestErrorKind::Timeout,
            }) => DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpTimeout),
            Err(HttpRequestError::Transport {
                kind: HttpRequestErrorKind::Connect,
            }) => DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpConnect),
            Err(_) => DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpTransport),
        }
    }

    async fn send_confirmation(
        &self,
        message_id: &PubsubMessageId,
        subscription: &Subscription,
        token: &str,
    ) -> PubsubResult<DeliveryAttemptResult> {
        if !matches!(
            subscription.protocol,
            SubscriptionProtocol::Http | SubscriptionProtocol::Https
        ) {
            return Ok(DeliveryAttemptResult::Permanent(
                DeliveryFailureCode::Internal,
            ));
        }
        if !subscription.confirmation.pending_confirmation() {
            return Ok(DeliveryAttemptResult::Delivered);
        }
        let body = confirmation_body(ConfirmationRenderContext {
            topic_arn: &subscription.topic_arn,
            message_id,
            token,
            delivery_config: &self.delivery_config,
            signer: self.notification_signer.as_deref(),
        })?;
        let headers = confirmation_headers(message_id, &subscription.topic_arn)?;
        Ok(self
            .send_http_delivery(&subscription.endpoint, headers, body)
            .await)
    }

    /// The queue message of a subscription: the raw body with the original
    /// attributes, or the notification envelope with no attributes.
    fn queue_rendering(
        &self,
        notification: &ClaimedNotification,
    ) -> PubsubResult<RenderedQueueMessage> {
        if notification.subscription.raw_message_delivery {
            return Ok(RenderedQueueMessage {
                body: notification.request.message.clone(),
                attributes: queue_message_attributes(&notification.request.message_attributes),
            });
        }
        Ok(RenderedQueueMessage {
            body: self.envelope(notification)?,
            attributes: None,
        })
    }

    fn envelope(&self, notification: &ClaimedNotification) -> PubsubResult<String> {
        notification_body(NotificationRenderContext {
            request: &notification.request,
            message_id: &notification.message_id,
            subscription: &notification.subscription,
            delivery_config: &self.delivery_config,
            signer: self.notification_signer.as_deref(),
        })
    }

    /// Wake the receives of a queue that the commit wrote to, and log a
    /// drop with its identifiers only (no body, no attributes).
    fn after_settle(&self, report: &SettleReport) {
        if let (Some(queue_url), Some(queue_wake)) = (&report.woken_queue, &self.queue_wake) {
            queue_wake.signal_committed_send(queue_url, Duration::ZERO);
        }
        if let SettleOutcome::Dropped(reason) = report.outcome {
            tracing::warn!(
                reason = reason.label(),
                subscription_arn = report.subscription_arn.as_ref().map(|arn| arn.as_str()),
                message_id = report.message_id.as_ref().map(|id| id.as_str()),
                "pubsub delivery dropped"
            );
        }
    }
}

/// The lease end of a claimed delivery.
fn lease_end(claimed: &ClaimedDelivery) -> Option<storage_types::TimestampMillis> {
    claimed.state.lease.as_ref().map(|lease| lease.until)
}

fn claimed_notification(record: &DeliveryPayload) -> Option<ClaimedNotification> {
    if !matches!(record.kind, DeliveryRecordKind::Notification) {
        return None;
    }
    let subscription = record.subscription.clone()?;
    let message = record.message_body.clone()?;
    Some(ClaimedNotification {
        message_id: record.message_id.clone(),
        request: PublishRequest {
            topic_arn: subscription.topic_arn.clone(),
            message,
            subject: record.subject.clone(),
            message_attributes: record.message_attributes.clone(),
        },
        subscription,
    })
}

fn is_raw_over_attribute_limit(notification: &ClaimedNotification) -> bool {
    notification.subscription.raw_message_delivery
        && notification.request.message_attributes.len() > MAX_RAW_QUEUE_ATTRIBUTES
}

/// A failure before the request: logged without payload, retried until the
/// deadline.
fn prepare_failure(error: &pubsub_provider::PubsubError) -> DeliveryAttemptResult {
    tracing::error!(
        error_type = error.aws_query_error_type(),
        "pubsub delivery could not be prepared"
    );
    DeliveryAttemptResult::Transient(DeliveryFailureCode::Internal)
}

/// A missing transport is a configuration defect; it is retried, not
/// dropped, and logged.
fn not_configured(what: &'static str) -> DeliveryAttemptResult {
    tracing::error!(
        missing = what,
        "pubsub delivery transport is not configured"
    );
    DeliveryAttemptResult::Transient(DeliveryFailureCode::NotConfigured)
}

fn queue_message_attributes(
    attributes: &HashMap<String, pubsub_provider::MessageAttribute>,
) -> Option<HashMap<String, MessageAttributeValue>> {
    if attributes.is_empty() {
        return None;
    }
    Some(
        attributes
            .iter()
            .map(|(name, value)| {
                let binary = matches!(value, pubsub_provider::MessageAttribute::Binary(_));
                let data_type = value.data_type().to_owned();
                (
                    name.clone(),
                    MessageAttributeValue {
                        string_value: (!binary).then(|| value.wire_value().into_owned()),
                        binary_value: binary.then(|| value.wire_value().into_owned()),
                        data_type,
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
