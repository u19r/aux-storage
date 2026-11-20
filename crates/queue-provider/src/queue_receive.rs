//! Queue selection, original responses, and lease effects share one native
//! attempt.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use storage_types::{
    AttemptKind, AttemptTry, DurationSeconds, StorageError, StorageResult, TimestampMillis,
};

use super::{
    MessageResponse, Queue, QueueAttempt, QueueDueBatch, QueueDueRequest, QueueError,
    QueueIncarnation, QueueMessageRecord, QueueMessageState, QueuePort, QueueResult,
    QueueValidationKind, ReceiptHandle,
    constants::{
        DEAD_LETTER_SOURCE_ARN_ATTRIBUTE, DEFAULT_RECEIVE_VISIBILITY_TIMEOUT_SECS,
        DEFAULT_RECEIVE_WAIT_TIME_SECONDS, MAX_RECEIVE_MESSAGES, MAX_VISIBILITY_TIMEOUT_SECONDS,
        MAX_WAIT_TIME_SECONDS, MESSAGE_CLAIM, QUEUE_CLAIM_WINDOW, QUEUE_CLAIM_WRITE_BYTES,
        QUEUE_DUE_HEAD, QUEUE_RECEIVE_WAIT_TIME_ATTRIBUTE, QUEUE_VISIBILITY_TIMEOUT_ATTRIBUTE,
        RECEIVE_PROBE,
    },
    queue_message::{earliest_due, queue_not_found, visibility_end},
    queue_redrive::{ClaimRedrive, RedriveDecision, RedrivePolicy},
};

/// The messages one receive claimed and what its attempt observed.
pub struct ReceivedMessages {
    pub messages: Vec<MessageResponse>,
    /// The queue's `ReceiveMessageWaitTimeSeconds`. A provider that performs
    /// the long poll itself reports zero.
    pub wait_time_seconds: u32,
    /// No due message that this claim could see remains (decision D15). A
    /// claim that lost a conflict, skipped a locked row, or took one slot of
    /// a deep queue is not drained, even when it returns nothing.
    pub drained: bool,
    /// Time from the claim's captured time until the earliest message that
    /// the claim left becomes due (decision D16). `None` means none is known.
    pub next_due_in: Option<Duration>,
    /// The name of the dead-letter queue that this claim moved at least one
    /// message into (design D10). The caller wakes its waiters as after a send.
    pub moved_to: Option<String>,
}

/// What one read-only receive probe observed, without a write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiveProbe {
    /// The queue's `ReceiveMessageWaitTimeSeconds`.
    pub wait_time_seconds: u32,
    /// Time from the probe's captured time until the earliest message becomes
    /// due. Zero means a message is due now. `None` means the queue has no
    /// message.
    pub next_due_in: Option<Duration>,
}

/// Receive defaults that a request without the parameter takes from its queue.
struct QueueReceiveDefaults {
    visibility_timeout: DurationSeconds,
    wait_time_seconds: u32,
}

impl QueueReceiveDefaults {
    /// Stored attributes were validated when set; an unreadable value keeps
    /// the SQS default.
    fn of(queue: &Queue) -> Self {
        let attribute = |name: &str, default: u32, maximum: u32| {
            queue
                .attributes
                .get(name)
                .and_then(|value| value.parse::<u32>().ok())
                .map_or(default, |value| value.min(maximum))
        };
        Self {
            visibility_timeout: attribute(
                QUEUE_VISIBILITY_TIMEOUT_ATTRIBUTE,
                DEFAULT_RECEIVE_VISIBILITY_TIMEOUT_SECS,
                MAX_VISIBILITY_TIMEOUT_SECONDS,
            )
            .into(),
            wait_time_seconds: attribute(
                QUEUE_RECEIVE_WAIT_TIME_ATTRIBUTE,
                DEFAULT_RECEIVE_WAIT_TIME_SECONDS,
                MAX_WAIT_TIME_SECONDS,
            ),
        }
    }
}

/// The response and effects of one claim attempt.
struct ClaimPlan {
    messages: Vec<MessageResponse>,
    /// Source rows that leave the queue (expired, dropped, moved) and target
    /// rows that a move replaces.
    removed: Vec<QueueMessageState>,
    /// New leases: `(claimed, previous)`.
    leases: Vec<(QueueMessageState, QueueMessageState)>,
    /// Dead-letter copies inserted into the target.
    inserts: Vec<QueueMessageRecord>,
    moved: usize,
    /// Dead-letter copies reached `QUEUE_CLAIM_WRITE_BYTES`; the later due
    /// records were left untouched and stay due.
    over_budget: bool,
    /// Binds each issued receipt handle to the claiming queue.
    receipt_scope: String,
}

impl ClaimPlan {
    /// Expired rows are deleted and use their slot; the redrive decision
    /// (design D10) moves or drops an exhausted message, which also uses its
    /// slot and is not returned; the others get one fixed receipt entropy
    /// each and the lease end of this attempt.
    fn new(
        due: Vec<QueueMessageRecord>,
        entropy: &[uuid::Uuid],
        lease: ClaimLease,
        redrive: Option<&ClaimRedrive>,
        receipt_scope: String,
    ) -> Self {
        let mut plan = Self {
            messages: Vec::with_capacity(due.len()),
            removed: Vec::new(),
            leases: Vec::with_capacity(due.len()),
            inserts: Vec::new(),
            moved: 0,
            over_budget: false,
            receipt_scope,
        };
        let mut moved_bytes = 0usize;
        for (index, (previous, entropy)) in due.into_iter().zip(entropy).enumerate() {
            if previous.expires_at <= lease.captured_at {
                plan.removed.push(previous.state());
                continue;
            }
            match redrive.map_or(RedriveDecision::Deliver, |redrive| {
                redrive.decide(&previous, lease.captured_at)
            }) {
                RedriveDecision::Deliver => plan.deliver(previous, *entropy, lease.next_visible),
                RedriveDecision::Drop => plan.removed.push(previous.state()),
                RedriveDecision::Move { moved, replaced } => {
                    let bytes = stored_record_bytes(&moved);
                    if index > 0 && moved_bytes.saturating_add(bytes) > QUEUE_CLAIM_WRITE_BYTES {
                        plan.over_budget = true;
                        break;
                    }
                    moved_bytes = moved_bytes.saturating_add(bytes);
                    plan.removed.push(previous.state());
                    if let Some(replaced) = replaced {
                        plan.removed.push(*replaced);
                    }
                    plan.inserts.push(*moved);
                    plan.moved += 1;
                }
            }
        }
        plan
    }

    fn deliver(
        &mut self,
        previous: QueueMessageRecord,
        entropy: uuid::Uuid,
        next_visible: TimestampMillis,
    ) {
        let receipt = ReceiptHandle::new(
            next_visible.timestamp_millis(),
            entropy,
            &self.receipt_scope,
        );
        let previous_state = previous.state();
        let mut claimed = previous;
        claimed.message.receive_count = claimed.message.receive_count.saturating_add(1);
        claimed.message.receipt_handle = Some(receipt.clone());
        claimed.message.visibility_timestamp = Some(next_visible);
        let claimed_state = claimed.state();
        let dead_letter_source_arn = claimed.dead_letter_source_arn;
        let mut response = MessageResponse::from_message(claimed.message, receipt);
        if let (Some(arn), Some(attributes)) =
            (dead_letter_source_arn, response.attributes.as_mut())
        {
            attributes.insert(DEAD_LETTER_SOURCE_ARN_ATTRIBUTE.to_owned(), arn);
        }
        self.messages.push(response);
        self.leases.push((claimed_state, previous_state));
    }

    /// Deletes, then lease writes, then dead-letter inserts, in the claim's
    /// attempt.
    async fn apply<A: QueueAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<(Vec<MessageResponse>, usize)> {
        attempt.delete_messages(self.removed).await?;
        attempt.write_message_states(self.leases).await?;
        attempt.insert_messages(self.inserts).await?;
        Ok((self.messages, self.moved))
    }
}

/// The fixed claim time and the lease end of one attempt.
#[derive(Clone, Copy)]
struct ClaimLease {
    captured_at: TimestampMillis,
    next_visible: TimestampMillis,
}

/// The values of one receive that survive its retries.
struct ReceiveInput {
    queue_url: String,
    limit: NonZeroU32,
    visibility_timeout: Option<DurationSeconds>,
    captured_at: TimestampMillis,
    clock_skew_millis: i64,
    entropy: Vec<uuid::Uuid>,
    /// The queue wait default that the latest try observed, for an empty answer
    /// after contention.
    observed_wait: std::sync::atomic::AtomicU32,
}

/// Claim due messages in one Relaxed item attempt per try (decisions D1 to
/// D4, D10, D11, D14 to D16). A dead-letter move commits in the same attempt.
///
/// The captured time and the receipt entropy survive retries; each attempt
/// reads the catalog again, chooses its own slot, and computes its lease end
/// from its own time, so retries do not shorten the client's lease. A claim
/// that loses every conflict retry returns an empty, undrained receive: a
/// conflict proves that nothing committed.
pub(super) async fn receive_messages<P: QueuePort>(
    port: &P,
    queue_url: &str,
    max_messages: u32,
    visibility_timeout: Option<DurationSeconds>,
) -> QueueResult<ReceivedMessages> {
    if !(1..=MAX_RECEIVE_MESSAGES).contains(&max_messages)
        || visibility_timeout.is_some_and(|timeout| *timeout > MAX_VISIBILITY_TIMEOUT_SECONDS)
    {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    let limit = NonZeroU32::new(max_messages)
        .ok_or_else(|| StorageError::internal("validated queue receive limit is zero"))?;
    let captured_at = port.now();
    let clock_skew_millis = port.max_clock_skew_millis();
    let entropy = (0..max_messages)
        .map(|_| storage_types::ambient::uuid_now_v7())
        .collect::<Vec<_>>();
    let input = Arc::new(ReceiveInput {
        queue_url: queue_url.to_owned(),
        limit,
        visibility_timeout,
        captured_at,
        clock_skew_millis,
        entropy,
        observed_wait: std::sync::atomic::AtomicU32::new(0),
    });
    let received = port
        .run(
            MESSAGE_CLAIM,
            Arc::clone(&input),
            |attempt, input, attempt_try| Box::pin(claim_in_attempt(attempt, input, attempt_try)),
        )
        .await;
    match received {
        Ok(Some(received)) => Ok(received),
        Ok(None) => Err(queue_not_found(input.queue_url.clone())),
        Err(error) if error.attempt_kind() == Some(AttemptKind::Conflict) => {
            storage_common::telemetry::record_provider_count("queue", "claim_contention_empty", 1);
            Ok(ReceivedMessages {
                messages: Vec::new(),
                wait_time_seconds: input
                    .observed_wait
                    .load(std::sync::atomic::Ordering::Relaxed),
                drained: false,
                next_due_in: Some(Duration::ZERO),
                moved_to: None,
            })
        }
        Err(error) => Err(error.into()),
    }
}

/// One claim try: the queue row without a lock, the due rows, then the lease
/// writes and any dead-letter move, all in this attempt.
async fn claim_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &ReceiveInput,
    attempt_try: AttemptTry,
) -> StorageResult<Option<ReceivedMessages>> {
    let Some(queue) = attempt
        .read_queue_for_messages(crate::queue_catalog::queue_name_of(&input.queue_url))
        .await?
    else {
        return Ok(None);
    };
    let defaults = QueueReceiveDefaults::of(&queue.queue);
    let policy = RedrivePolicy::of(&queue.queue);
    input.observed_wait.store(
        defaults.wait_time_seconds,
        std::sync::atomic::Ordering::Relaxed,
    );
    let request = QueueDueRequest {
        incarnation: queue.incarnation,
        captured_at: input.captured_at,
        clock_skew_millis: input.clock_skew_millis,
        limit: input.limit,
        window: QUEUE_CLAIM_WINDOW,
        head: QUEUE_DUE_HEAD,
        first_attempt: attempt_try.is_first(),
        rotation: storage_types::ambient::random_u64() as u32,
        dead_letter_target: ClaimRedrive::target_name(policy.as_ref(), &queue.queue),
    };
    let QueueDueBatch {
        mut records,
        drained,
        next_due,
        dead_letter_target,
    } = attempt.read_due_messages(&request).await?;
    // A lock re-check can return rows out of order.
    records.sort_by_key(|record| (record.visible_at(), record.message.message_id));
    validate_due_records(&records, &request)?;
    let mut redrive = ClaimRedrive::observe(policy, &queue.queue, dead_letter_target)?;
    if let Some(redrive) = redrive.as_mut() {
        redrive
            .load_held(attempt, &records, input.captured_at)
            .await?;
    }
    let lease = ClaimLease {
        captured_at: input.captured_at,
        next_visible: visibility_end(
            attempt_try.started_at.max(input.captured_at),
            input
                .visibility_timeout
                .unwrap_or(defaults.visibility_timeout),
            input.clock_skew_millis,
        ),
    };
    let next_visible = lease.next_visible;
    let plan = ClaimPlan::new(
        records,
        &input.entropy,
        lease,
        redrive.as_ref(),
        queue.receipt_scope(),
    );
    let over_budget = plan.over_budget;
    let (messages, moved) = plan.apply(attempt).await?;
    let moved_to = redrive
        .as_ref()
        .and_then(ClaimRedrive::moved_to)
        .filter(|_| moved > 0)
        .map(str::to_owned);
    if let Some(target) = moved_to.as_deref() {
        attempt.stage_queue_wake(crate::QueueWakeSlot::of_queue(target))?;
    }
    // The next due time includes this claim's own leases.
    // A zero visibility timeout makes the claimed messages due
    // at once; the millisecond difference between the capture
    // and the attempt must not report them as not yet due.
    let is_zero_lease = *input
        .visibility_timeout
        .unwrap_or(defaults.visibility_timeout)
        == 0
        && !messages.is_empty();
    let leases = (!messages.is_empty()).then_some(crate::QueueDueMark {
        due_at: next_visible,
        leased: true,
    });
    let next_due = earliest_due(
        next_due
            .map(|due_at| crate::QueueDueMark {
                due_at,
                leased: false,
            })
            .iter()
            .chain(leases.iter()),
        input.clock_skew_millis,
    );
    Ok(Some(ReceivedMessages {
        messages,
        wait_time_seconds: defaults.wait_time_seconds,
        drained: drained && !over_budget,
        next_due_in: if is_zero_lease || over_budget {
            Some(Duration::ZERO)
        } else {
            next_due.map(|due| due_in(input.captured_at, due))
        },
        moved_to,
    }))
}

struct ProbeInput {
    queue_url: String,
    captured_at: TimestampMillis,
    clock_skew_millis: i64,
}

/// Read the queue defaults and the earliest due time in one read-only
/// attempt. The answer is a hint, so the attempt is `Relaxed` (rule DUR-F6):
/// it may see a Relaxed claim that a crash later removes, and then a waiter
/// only probes or claims again.
pub(super) async fn probe_receive<P: QueuePort>(
    port: &P,
    queue_url: &str,
) -> QueueResult<Option<ReceiveProbe>> {
    let captured_at = port.now();
    let clock_skew_millis = port.max_clock_skew_millis();
    let input = Arc::new(ProbeInput {
        queue_url: queue_url.to_owned(),
        captured_at,
        clock_skew_millis,
    });
    // A read-only attempt can also be aborted (FoundationDB
    // `transaction_too_old`); a known abort reads again.
    port.run(RECEIVE_PROBE, input, |attempt, input, _| {
        Box::pin(probe_in_attempt(attempt, input))
    })
    .await
    .map_err(QueueError::from)
}

async fn probe_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &ProbeInput,
) -> StorageResult<Option<ReceiveProbe>> {
    let Some(queue) = attempt
        .read_queue_for_messages(crate::queue_catalog::queue_name_of(&input.queue_url))
        .await?
    else {
        return Ok(None);
    };
    let head = attempt
        .read_due_head(queue.incarnation, QUEUE_DUE_HEAD)
        .await?;
    Ok(Some(ReceiveProbe {
        wait_time_seconds: QueueReceiveDefaults::of(&queue.queue).wait_time_seconds,
        next_due_in: earliest_due(head.iter(), input.clock_skew_millis)
            .map(|due| due_in(input.captured_at, due)),
    }))
}

fn due_in(captured_at: TimestampMillis, due: TimestampMillis) -> Duration {
    let millis = due
        .timestamp_millis()
        .saturating_sub(captured_at.timestamp_millis());
    Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

/// Stored bytes of the parts of a record that vary with the message (W7-0).
/// Binary attribute values count as stored (base64), not decoded.
pub(crate) fn stored_record_bytes(record: &QueueMessageRecord) -> usize {
    let message = &record.message;
    let attributes = message.message_attributes.as_ref().map_or(0, |attributes| {
        attributes
            .iter()
            .map(|(name, value)| {
                name.len()
                    + value.data_type.len()
                    + value.string_value.as_deref().map_or(0, str::len)
                    + value.binary_value.as_deref().map_or(0, str::len)
            })
            .sum()
    });
    message.body.len()
        + attributes
        + message.trace_header.as_deref().map_or(0, str::len)
        + record.checkpoint_data.as_deref().map_or(0, str::len)
}

fn validate_due_records(
    due: &[QueueMessageRecord],
    request: &QueueDueRequest,
) -> StorageResult<()> {
    if due.len() > request.limit.get() as usize {
        return Err(StorageError::internal(
            "queue due result exceeds captured receive limit",
        ));
    }
    let incarnation: QueueIncarnation = request.incarnation;
    let mut previous = None;
    for record in due {
        let cursor = (record.visible_at(), record.message.message_id);
        if record.incarnation != incarnation
            || !request.is_due(crate::QueueDueMark {
                due_at: cursor.0,
                leased: record.message.receipt_handle.is_some(),
            })
            || previous.is_some_and(|previous| cursor <= previous)
        {
            return Err(StorageError::internal(
                "queue due result differs from captured selection",
            ));
        }
        previous = Some(cursor);
    }
    Ok(())
}
