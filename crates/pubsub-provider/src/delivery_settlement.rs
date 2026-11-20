//! Fenced settlement of claimed deliveries and the local SQS delivery (W3S
//! plan 5.6; W3 design sections 1.4, 1.5, 2.4, 5). Each operation is one
//! native attempt per try. A settle decides from the claimed state and
//! writes under the fence `(id, generation, nonce)`: the fenced state write
//! or delete comes first, and every other effect (a dead-letter message, a
//! queue message) is written only for a fence that holds, in the same
//! attempt.
//!
//! Durability: every attempt here is `Relaxed` (decision D7). It reads the
//! state (written by Relaxed claims), the subscription and the queue catalog
//! (written by Durable operations). Rule DUR-F6 is kept: no Durable read
//! depends on these writes; a lost commit returns the record (at least once).

use std::{collections::HashMap, sync::Arc};

use queue_provider::{
    MessageAttributeValue, MessageId, QueueArn, QueueAttempt, QueueMessage, QueueOperation,
};
use storage_common::telemetry::record_provider_count;
use storage_types::{
    MessagingIdentity, ReadProtection, StorageError, StorageResult, TimestampMillis,
};

use crate::{
    DeliveryAttemptResult, DeliveryDecision, DeliveryFailureCode, DeliveryFence, DeliveryState,
    DropReason, PubsubAttempt, PubsubError, PubsubMessageId, PubsubPort, PubsubResult, StateChange,
    Subscription, SubscriptionArn,
    constants::{DELIVERY_SETTLE, LOCAL_BATCH_MAX_BODY_BYTES, LOCAL_BATCH_MAX_RECORDS},
    pubsub_port::{RecordReads, read_topic_at},
};

/// A queue message in the form that the subscription receives: the envelope
/// with no attributes, or the raw body with the original attributes. A
/// dead-letter message has the same form (AWS, section 14).
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct RenderedQueueMessage {
    pub body: String,
    pub attributes: Option<HashMap<String, MessageAttributeValue>>,
}

/// What one local batch gave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalOutcome {
    /// One report for each item, in item order.
    Settled(Vec<SettleReport>),
    /// The batch is over a bound, or the store rejected its size. Nothing
    /// was written and no record was settled; the caller splits the batch
    /// (finding 4).
    TooLarge,
}

impl LocalOutcome {
    /// The reports, or `None` for `TooLarge`.
    #[must_use]
    pub fn settled(self) -> Option<Vec<SettleReport>> {
        match self {
            Self::Settled(reports) => Some(reports),
            Self::TooLarge => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleOutcome {
    Delivered,
    Retried,
    DeadLettered,
    Dropped(DropReason),
    /// The fence failed: another claim owns the record, or it is gone.
    ClaimLost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleReport {
    pub outcome: SettleOutcome,
    /// The failure that made a drop or a dead-letter move.
    pub failure: Option<DeliveryFailureCode>,
    /// A queue that received a message in this commit; the caller wakes its
    /// waiting receives.
    pub woken_queue: Option<String>,
    pub message_id: Option<PubsubMessageId>,
    pub subscription_arn: Option<SubscriptionArn>,
    /// The due time of a retry that the settle wrote: a timer hint.
    pub retry_due: Option<TimestampMillis>,
}

/// One settle after an attempt that the caller made (HTTP, custom sender,
/// confirmation, or a failed local delivery).
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct SettleInput {
    /// The state that the claim wrote; the fence says whether it is still
    /// the stored state.
    pub claimed: DeliveryState,
    pub fence: DeliveryFence,
    pub message_id: PubsubMessageId,
    pub result: DeliveryAttemptResult,
    /// Written to the subscription's dead-letter queue when the decision is
    /// `DeadLetter`.
    pub dead_letter: Option<RenderedQueueMessage>,
    pub jitter: u32,
    /// A dead-letter target in another region is missing (review D9).
    pub identity: MessagingIdentity,
}

/// One claimed delivery of a local batch and the message it enqueues.
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct LocalDeliveryItem {
    pub claimed: DeliveryState,
    pub fence: DeliveryFence,
    pub message_id: PubsubMessageId,
    pub message: RenderedQueueMessage,
}

/// Deliveries to ONE queue on the same store (review E1), at most
/// `LOCAL_BATCH_MAX_RECORDS` items and `LOCAL_BATCH_MAX_BODY_BYTES` of
/// bodies (finding 4).
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct LocalDelivery {
    pub queue: QueueArn,
    pub items: Vec<LocalDeliveryItem>,
    pub jitter: u32,
    pub identity: MessagingIdentity,
}

impl LocalDelivery {
    #[must_use]
    pub fn is_within_bounds(&self) -> bool {
        let bytes: usize = self.items.iter().map(|item| item.message.body.len()).sum();
        self.items.len() <= LOCAL_BATCH_MAX_RECORDS && bytes <= LOCAL_BATCH_MAX_BODY_BYTES
    }
}

/// Settle one attempt result in one attempt per try. A success is one fenced
/// delete pair; a failure reads the current subscription and topic, then
/// writes one small state row or deletes the delivery.
pub async fn settle_delivery<P: PubsubPort>(
    port: &P,
    input: SettleInput,
) -> PubsubResult<SettleReport> {
    match settle_once(port, input.clone()).await {
        // The store refused the size of a dead-letter insert (W3S review):
        // the record settles as `DeadLetterRejected` without the message,
        // so a lease does not attempt the endpoint again forever.
        Err(PubsubError::Storage(error))
            if is_capacity_rejection(&error) && input.dead_letter.is_some() =>
        {
            settle_once(
                port,
                SettleInput {
                    dead_letter: None,
                    ..input
                },
            )
            .await
        }
        settled => settled,
    }
}

struct SettleOnceInput {
    settle: SettleInput,
    now: TimestampMillis,
}

async fn settle_once<P: PubsubPort>(port: &P, input: SettleInput) -> PubsubResult<SettleReport> {
    let now = port.now();
    let input = Arc::new(SettleOnceInput { settle: input, now });
    let report = port
        .run(DELIVERY_SETTLE, input, |attempt, input, _| {
            Box::pin(settle_in_attempt(attempt, &input.settle, input.now))
        })
        .await?;
    count(&report);
    Ok(report)
}

async fn settle_in_attempt<A: PubsubAttempt + QueueAttempt>(
    attempt: &mut A,
    input: &SettleInput,
    now: TimestampMillis,
) -> StorageResult<SettleReport> {
    let current = match input.result {
        DeliveryAttemptResult::Transient(_) | DeliveryAttemptResult::Permanent(_) => {
            current_subscription(attempt, &input.claimed.subscription_arn).await?
        }
        DeliveryAttemptResult::Delivered => None,
    };
    let decision = input
        .claimed
        .decide(input.result.clone(), current.as_ref(), now, input.jitter);
    let dead_letter = DeadLetter {
        message: input.dead_letter.as_ref(),
        identity: &input.identity,
    };
    apply(attempt, input, decision, dead_letter, now).await
}

/// Apply a decision under the fence. Nothing else is written when the
/// fence does not hold.
async fn apply<A: PubsubAttempt + QueueAttempt>(
    attempt: &mut A,
    input: &SettleInput,
    decision: DeliveryDecision,
    dead_letter: DeadLetter<'_>,
    now: TimestampMillis,
) -> StorageResult<SettleReport> {
    let report = |outcome| report(input, outcome, None);
    let next = match &decision {
        DeliveryDecision::Retry {
            next_attempt_at,
            attempts,
            failure,
        } => Some(
            input
                .claimed
                .rescheduled(*next_attempt_at, *attempts, *failure),
        ),
        _ => None,
    };
    if let Some(next) = next {
        let change = StateChange {
            expected: input.fence.clone(),
            next,
        };
        let held = attempt.store_delivery_states(vec![change]).await?;
        return Ok(match (held.first(), decision) {
            (
                Some(true),
                DeliveryDecision::Retry {
                    next_attempt_at, ..
                },
            ) => {
                let mut retried = report(SettleOutcome::Retried);
                retried.retry_due = Some(next_attempt_at);
                retried
            }
            _ => report(SettleOutcome::ClaimLost),
        });
    }
    let held = attempt.delete_deliveries(vec![input.fence.clone()]).await?;
    if held.first() != Some(&true) {
        return Ok(report(SettleOutcome::ClaimLost));
    }
    let (outcome, woken, failure) = match decision {
        DeliveryDecision::DeadLetter { target, failure } => {
            let (outcome, woken) = move_to_dead_letter(attempt, &target, dead_letter, now).await?;
            (outcome, woken, Some(failure))
        }
        DeliveryDecision::Drop { reason, failure } => {
            (SettleOutcome::Dropped(reason), None, failure)
        }
        _ => (SettleOutcome::Delivered, None, None),
    };
    let mut settled = self::report(input, outcome, woken);
    settled.failure = failure;
    Ok(settled)
}

struct LocalInput {
    delivery: LocalDelivery,
    messages: Vec<QueueMessage>,
    now: TimestampMillis,
}

/// Enqueue into a queue on the same store and delete the deliveries, for all
/// items of one queue in ONE attempt (review E1): one read wave
/// of the fenced states and the catalog row, then the fenced deletes (record
/// id order, finding 6) and one batched insert for the held items, both
/// carried with the commit (W3 C8). Every local failure is permanent, so every
/// held item is terminal: a failed item moves to its dead-letter queue or is
/// dropped in the same attempt, with one subscription read for each
/// subscription (finding 7). A known abort after the retry budget, or an
/// unknown commit outcome, is settled as transient for each item through its
/// fence (review C5); a capacity rejection settles nothing (finding 4).
pub async fn deliver_to_local_queue<P: PubsubPort>(
    port: &P,
    mut delivery: LocalDelivery,
) -> PubsubResult<LocalOutcome> {
    if delivery.items.is_empty() {
        return Err(PubsubError::storage("a local delivery has no item"));
    }
    if !delivery.is_within_bounds() {
        return Ok(LocalOutcome::TooLarge);
    }
    delivery
        .items
        .sort_by(|left, right| left.claimed.id.cmp(&right.claimed.id));
    if delivery
        .items
        .windows(2)
        .any(|pair| pair[0].claimed.id == pair[1].claimed.id)
    {
        return Err(PubsubError::storage("a local delivery repeats a record"));
    }
    let now = port.now();
    // One message ID for each item, stable across conflict retries.
    let messages: Vec<QueueMessage> = delivery
        .items
        .iter()
        .map(|item| queue_message(&item.message, MessageId::random(), now))
        .collect();
    let input = Arc::new(LocalInput {
        delivery,
        messages,
        now,
    });
    let attempted = port
        .run(DELIVERY_SETTLE, Arc::clone(&input), |attempt, input, _| {
            Box::pin(deliver_in_attempt(
                attempt,
                input.delivery.clone(),
                input.messages.clone(),
                input.now,
            ))
        })
        .await;
    match attempted {
        Ok(reports) => {
            reports.iter().for_each(count);
            Ok(LocalOutcome::Settled(reports))
        }
        Err(error) if is_capacity_rejection(&error) => Ok(LocalOutcome::TooLarge),
        Err(error) => {
            let Some(DeliveryAttemptResult::Transient(failure)) =
                DeliveryAttemptResult::from_queue_store_error(&error)
            else {
                return Err(error.into());
            };
            let mut reports = Vec::with_capacity(input.delivery.items.len());
            for item in &input.delivery.items {
                reports.push(
                    settle_delivery(port, local_failure(&input.delivery, item, failure)).await?,
                );
            }
            Ok(LocalOutcome::Settled(reports))
        }
    }
}

async fn deliver_in_attempt<A: PubsubAttempt + QueueAttempt>(
    attempt: &mut A,
    delivery: LocalDelivery,
    messages: Vec<QueueMessage>,
    now: TimestampMillis,
) -> StorageResult<Vec<SettleReport>> {
    let fences: Vec<DeliveryFence> = delivery
        .items
        .iter()
        .map(|item| item.fence.clone())
        .collect();
    // An ARN in another region names no queue of this store (review D9).
    let queue_name = delivery
        .queue
        .is_in_region(&delivery.identity)
        .then(|| delivery.queue.queue_name());
    let message_ids: Vec<MessageId> = messages.iter().map(|message| message.message_id).collect();
    // One wave: the fenced states (Conflict), the catalog row (Snapshot)
    // and, when the backend can, the absence of the new message ids.
    let observed = attempt
        .load_records(RecordReads {
            fences: &fences,
            queue_name,
            message_ids: &message_ids,
        })
        .await?;
    let mut reports: Vec<Option<SettleReport>> = vec![None; observed.held.len()];
    let mut held_items = Vec::with_capacity(observed.held.len());
    let mut held_fences = Vec::with_capacity(observed.held.len());
    for (index, (is_held, fence)) in observed.held.iter().zip(fences).enumerate() {
        if *is_held {
            held_items.push(index);
            held_fences.push(fence);
        } else {
            reports[index] = Some(report_of(&delivery.items[index], SettleOutcome::ClaimLost));
        }
    }
    if !held_fences.is_empty() {
        attempt.stage_delivery_deletes(held_fences)?;
    }
    let queue = match observed.queue {
        Some(record) if !held_items.is_empty() => {
            Some(QueueOperation::for_messages(attempt, record))
        }
        _ => None,
    };
    let mut failures = Vec::new();
    match queue {
        None => failures.extend(
            held_items
                .into_iter()
                .map(|index| (index, DeliveryFailureCode::DestinationMissing)),
        ),
        Some(queue) => {
            let url = queue.queue_url().to_owned();
            let mut sendable = Vec::with_capacity(held_items.len());
            let mut batch = Vec::with_capacity(held_items.len());
            for index in held_items {
                let mut message = messages[index].clone();
                if message.validate_for_send().is_err() {
                    failures.push((index, DeliveryFailureCode::PayloadRejected));
                    continue;
                }
                message.queue_url.clone_from(&url);
                batch.push(message);
                sendable.push(index);
            }
            let results = if batch.is_empty() {
                Vec::new()
            } else {
                queue.send_messages(batch, true).await?
            };
            if results.len() != sendable.len() {
                return Err(StorageError::internal(
                    "queue batch send returned another count",
                ));
            }
            for (index, result) in sendable.into_iter().zip(results) {
                match result {
                    Ok(_) => {
                        let mut delivered =
                            report_of(&delivery.items[index], SettleOutcome::Delivered);
                        delivered.woken_queue = Some(url.clone());
                        reports[index] = Some(delivered);
                    }
                    Err(_) => failures.push((index, DeliveryFailureCode::PayloadRejected)),
                }
            }
        }
    }
    let mut subscriptions: HashMap<SubscriptionArn, Option<Subscription>> = HashMap::new();
    for (index, failure) in failures {
        let item = &delivery.items[index];
        let arn = &item.claimed.subscription_arn;
        let current = match subscriptions.get(arn) {
            Some(current) => current.clone(),
            None => {
                let current = current_subscription(attempt, arn).await?;
                subscriptions.insert(arn.clone(), current.clone());
                current
            }
        };
        let result = DeliveryAttemptResult::Permanent(failure);
        let decision = item
            .claimed
            .decide(result, current.as_ref(), now, delivery.jitter);
        let dead_letter = DeadLetter {
            message: Some(&item.message),
            identity: &delivery.identity,
        };
        reports[index] = Some(terminal(attempt, item, decision, dead_letter, now).await?);
    }
    reports
        .into_iter()
        .map(|report| report.ok_or_else(|| StorageError::internal("a delivery has no outcome")))
        .collect()
}

/// The effect of a terminal decision of an item whose delivery is already
/// deleted in this attempt.
async fn terminal<A: QueueAttempt>(
    attempt: &mut A,
    item: &LocalDeliveryItem,
    decision: DeliveryDecision,
    dead_letter: DeadLetter<'_>,
    now: TimestampMillis,
) -> StorageResult<SettleReport> {
    let (outcome, woken, failure) = match decision {
        DeliveryDecision::DeadLetter { target, failure } => {
            let (outcome, woken) = move_to_dead_letter(attempt, &target, dead_letter, now).await?;
            (outcome, woken, Some(failure))
        }
        DeliveryDecision::Drop { reason, failure } => {
            (SettleOutcome::Dropped(reason), None, failure)
        }
        _ => return Err(StorageError::internal("a local failure decided a retry")),
    };
    let mut settled = report_of(item, outcome);
    settled.woken_queue = woken;
    settled.failure = failure;
    Ok(settled)
}

/// Read at `Snapshot` (design 7.5): a failing delivery takes no lock on the
/// subscription or the topic, so it never blocks `Publish` or `Subscribe`.
async fn current_subscription<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &SubscriptionArn,
) -> StorageResult<Option<Subscription>> {
    let Some(record) = attempt
        .read_subscription(arn, ReadProtection::Snapshot)
        .await?
    else {
        return Ok(None);
    };
    let topic = read_topic_at(
        attempt,
        &record.subscription.topic_arn,
        ReadProtection::Snapshot,
    )
    .await?;
    Ok(topic
        .is_some_and(|topic| topic.incarnation == record.topic_incarnation)
        .then_some(record.subscription))
}

/// The dead-letter message and the identity that its target must match.
struct DeadLetter<'a> {
    message: Option<&'a RenderedQueueMessage>,
    identity: &'a MessagingIdentity,
}

/// The dead-letter queue is a queue on the same store; the insert joins the
/// settle attempt. A missing queue or a rejected message drops the record.
async fn move_to_dead_letter<A: QueueAttempt>(
    attempt: &mut A,
    target: &QueueArn,
    dead_letter: DeadLetter<'_>,
    now: TimestampMillis,
) -> StorageResult<(SettleOutcome, Option<String>)> {
    if !target.is_in_region(dead_letter.identity) {
        return Ok((SettleOutcome::Dropped(DropReason::DeadLetterMissing), None));
    }
    let Some(message) = dead_letter.message else {
        return Ok((SettleOutcome::Dropped(DropReason::DeadLetterRejected), None));
    };
    let Some(queue) =
        QueueOperation::load_by_name_for_messages(attempt, target.queue_name()).await?
    else {
        return Ok((SettleOutcome::Dropped(DropReason::DeadLetterMissing), None));
    };
    let queue_url = queue.queue_url().to_owned();
    let mut message = queue_message(message, MessageId::random(), now);
    if message.validate_for_send().is_err() {
        return Ok((SettleOutcome::Dropped(DropReason::DeadLetterRejected), None));
    }
    message.queue_url.clone_from(&queue_url);
    match queue.send_messages(vec![message], true).await?.pop() {
        Some(Ok(_)) => Ok((SettleOutcome::DeadLettered, Some(queue_url))),
        _ => Ok((SettleOutcome::Dropped(DropReason::DeadLetterRejected), None)),
    }
}

/// The store refused the size of the transaction: a smaller batch can pass,
/// so this is not a failure of any record.
fn is_capacity_rejection(error: &StorageError) -> bool {
    error.attempt_kind() == Some(storage_types::AttemptKind::Capacity)
}

fn local_failure(
    delivery: &LocalDelivery,
    item: &LocalDeliveryItem,
    failure: DeliveryFailureCode,
) -> SettleInput {
    let result = match failure {
        DeliveryFailureCode::QueueStoreUnavailable => DeliveryAttemptResult::Transient(failure),
        _ => DeliveryAttemptResult::Permanent(failure),
    };
    SettleInput {
        claimed: item.claimed.clone(),
        fence: item.fence.clone(),
        message_id: item.message_id.clone(),
        result,
        dead_letter: Some(item.message.clone()),
        jitter: delivery.jitter,
        identity: delivery.identity.clone(),
    }
}

fn queue_message(
    message: &RenderedQueueMessage,
    id: MessageId,
    now: TimestampMillis,
) -> QueueMessage {
    QueueMessage {
        receive_count: 0,
        message_id: id,
        queue_url: String::new(),
        body: message.body.clone(),
        message_attributes: message.attributes.clone(),
        receipt_handle: None,
        created_at: now,
        visibility_timestamp: None,
        trace_header: None,
    }
}

fn report(
    input: &SettleInput,
    outcome: SettleOutcome,
    woken_queue: Option<String>,
) -> SettleReport {
    SettleReport {
        outcome,
        failure: None,
        woken_queue,
        message_id: Some(input.message_id.clone()),
        subscription_arn: Some(input.claimed.subscription_arn.clone()),
        retry_due: None,
    }
}

fn report_of(item: &LocalDeliveryItem, outcome: SettleOutcome) -> SettleReport {
    SettleReport {
        outcome,
        failure: None,
        woken_queue: None,
        message_id: Some(item.message_id.clone()),
        subscription_arn: Some(item.claimed.subscription_arn.clone()),
        retry_due: None,
    }
}

/// Fixed-cardinality outcome counters.
fn count(report: &SettleReport) {
    let label = match report.outcome {
        SettleOutcome::Delivered => "delivery_delivered",
        SettleOutcome::Retried => "delivery_retry",
        SettleOutcome::DeadLettered => "delivery_dead_lettered",
        SettleOutcome::Dropped(_) => "delivery_dropped",
        SettleOutcome::ClaimLost => "delivery_claim_lost",
    };
    record_provider_count("pubsub", label, 1);
    if let SettleOutcome::Dropped(reason) = report.outcome {
        record_provider_count("pubsub_delivery_dropped", reason.label(), 1);
    }
}
