//! Dead-letter queues (W12, design D10): queue ARNs, the redrive policy, and
//! the redrive decision of a claim attempt.
//!
//! The policy is stored in normal form in the source queue's attributes. A
//! claim reads the target catalog row in the same read wave as its due rows;
//! a message whose receive count reached `maxReceiveCount` moves to the
//! target in the same native attempt as the claim (delete from the source,
//! insert into the target with the same `MessageId`, body, and attributes).

use std::{collections::HashMap, fmt, str::FromStr};

use http_error::HttpApiError;
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use crate::{
    MessageId, Queue, QueueArn, QueueAttempt, QueueCatalogRecord, QueueError, QueueIncarnation,
    QueueMessageRecord, QueueMessageState, QueueResult,
    constants::{MAX_REDRIVE_RECEIVE_COUNT, QUEUE_REDRIVE_POLICY_ATTRIBUTE},
    queue_send::QueueSendPolicy,
};

/// A validated redrive policy. `Display` gives the stored normal form, so
/// `"maxReceiveCount": "1"` reads back as the number 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedrivePolicy {
    target: QueueArn,
    max_receive_count: u32,
}

impl RedrivePolicy {
    /// The stored policy of `queue`. The value was validated when it was set,
    /// so an unreadable value means that the queue has no policy.
    #[must_use]
    pub fn of(queue: &Queue) -> Option<Self> {
        queue
            .attributes
            .get(QUEUE_REDRIVE_POLICY_ATTRIBUTE)?
            .parse()
            .ok()
    }

    #[must_use]
    pub fn target(&self) -> &QueueArn {
        &self.target
    }

    /// SQS moves a message on the receive after `maxReceiveCount` receives.
    ///
    /// Each hop delivers at least once: a message that a move put in this
    /// queue and that no receive of this queue has taken yet is delivered,
    /// even when its kept count is at the limit. Without this rule, a cycle
    /// (A to B and B to A) or a chain whose next `maxReceiveCount` is at or
    /// below the kept count moves the message on each receive and no consumer
    /// gets it. Only a move clears the receipt
    /// (`QueueSendPolicy::prepare_moved`), and a claim always sets one, so
    /// "moved here and no receipt" means "not received here since the move".
    fn should_move(&self, record: &QueueMessageRecord) -> bool {
        let is_unreceived_arrival =
            record.dead_letter_source_arn.is_some() && record.message.receipt_handle.is_none();
        record.message.receive_count >= self.max_receive_count && !is_unreceived_arrival
    }
}

impl FromStr for RedrivePolicy {
    type Err = HttpApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = |reason: &str| {
            HttpApiError::validation_error(format!(
                "Value {value} for parameter RedrivePolicy is invalid. Reason: {reason}"
            ))
        };
        // Probe 9: codes, texts, and check order.
        let Ok(serde_json::Value::Object(fields)) = serde_json::from_str(value) else {
            return Err(HttpApiError::aws_query_error(
                crate::constants::SQS_INVALID_ATTRIBUTE_VALUE_ERROR_TYPE,
                "Invalid value for the parameter RedrivePolicy. Reason: Redrive policy is not a \
                 valid JSON map.",
                400,
            ));
        };
        let missing = |member: &str| {
            invalid(&format!(
                "Redrive policy does not contain mandatory attribute: {member}."
            ))
        };
        let Some(count) = fields.get("maxReceiveCount") else {
            return Err(missing("maxReceiveCount"));
        };
        let Some(target) = fields.get("deadLetterTargetArn") else {
            return Err(missing("deadLetterTargetArn"));
        };
        if fields.len() > 2 {
            return Err(invalid(
                "Only following attributes are supported: [maxReceiveCount, deadLetterTargetArn].",
            ));
        }
        let target = match target {
            serde_json::Value::String(arn) => arn
                .parse::<QueueArn>()
                .map_err(|reason: String| invalid(&reason))?,
            _ => return Err(invalid("Invalid value for deadLetterTargetArn.")),
        };
        let shown = match count {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let max_receive_count = shown
            .parse::<u32>()
            .ok()
            .filter(|count| (1..=MAX_REDRIVE_RECEIVE_COUNT).contains(count))
            .ok_or_else(|| {
                invalid(&format!(
                    "Invalid value for maxReceiveCount: {shown}, valid values are from 1 to \
                     {MAX_REDRIVE_RECEIVE_COUNT} both inclusive."
                ))
            })?;
        Ok(Self {
            target,
            max_receive_count,
        })
    }
}

impl fmt::Display for RedrivePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{{\"deadLetterTargetArn\":{},\"maxReceiveCount\":{}}}",
            serde_json::Value::String(self.target.to_string()),
            self.max_receive_count
        )
    }
}

/// Rewrite a supplied `RedrivePolicy` into its normal form and return its
/// target, which the caller's catalog attempt must find. An empty value is a
/// removal and stays empty. The source's own ARN is accepted (probe 9; then
/// the claim delivers as usual). `check_redrive_identity` owns the
/// partition, account, and region rule.
pub(crate) fn normalize_redrive_policy(
    attributes: &mut HashMap<String, String>,
) -> QueueResult<Option<RedrivePolicy>> {
    let Some(value) = attributes.get_mut(QUEUE_REDRIVE_POLICY_ATTRIBUTE) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    let policy: RedrivePolicy = value.parse().map_err(invalid_parameter)?;
    *value = policy.to_string();
    Ok(Some(policy))
}

/// The one deployment rule for a supplied `RedrivePolicy` (probe 9): the
/// target is in the configured partition and account, then in the
/// configured region. The target's existence is checked by the provider.
pub fn check_redrive_identity(
    attributes: &HashMap<String, String>,
    identity: &storage_types::MessagingIdentity,
) -> QueueResult<()> {
    let Some(value) = attributes
        .get(QUEUE_REDRIVE_POLICY_ATTRIBUTE)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let policy: RedrivePolicy = value.parse().map_err(invalid_parameter)?;
    let reason = if !policy.target.is_owned_by(identity) {
        "Dead-letter target owner should be same as the source."
    } else if !policy.target.is_in_region(identity) {
        "Dead-letter target must be in same region as the source."
    } else {
        return Ok(());
    };
    Err(invalid_parameter(HttpApiError::validation_error(format!(
        "Value {value} for parameter RedrivePolicy is invalid. Reason: {reason}"
    ))))
}

/// In a catalog attempt that sets a policy: the target queue must exist,
/// unless it is the source itself.
pub(crate) async fn find_redrive_target<A: QueueAttempt>(
    attempt: &mut A,
    policy: &RedrivePolicy,
    source_name: &str,
) -> StorageResult<QueueResult<()>> {
    let target = &policy.target;
    if target.queue_name() == source_name
        || attempt
            .read_queue_by_name(target.queue_name(), ReadProtection::Snapshot)
            .await?
            .is_some()
    {
        return Ok(Ok(()));
    }
    Ok(Err(invalid_parameter(HttpApiError::validation_error(
        format!(
            "Value {policy} for parameter RedrivePolicy is invalid. Reason: Dead letter target \
             does not exist."
        ),
    ))))
}

/// Keeps `InvalidAttributeValue` (a policy that is not JSON, probe 9).
fn invalid_parameter(error: HttpApiError) -> QueueError {
    crate::request_validation::queue_validation_error(error)
}

/// What one claim attempt does with one due, unexpired message.
pub(crate) enum RedriveDecision {
    Deliver,
    /// Delete from the source only: the message is past the target's
    /// retention, which counts from its first send.
    Drop,
    /// Delete from the source and insert `moved` into the target. `replaced`
    /// is an expired target row with the same `MessageId` that cleanup has
    /// not removed yet; the insert overwrites it.
    Move {
        moved: Box<QueueMessageRecord>,
        replaced: Option<Box<QueueMessageState>>,
    },
}

/// The redrive facts of one claim attempt: the source's policy and the target
/// row that the due read returned, with the target rows that already have the
/// `MessageId` of a message to move.
pub(crate) struct ClaimRedrive {
    policy: RedrivePolicy,
    source_arn: String,
    target: Option<RedriveTarget>,
}

struct RedriveTarget {
    /// Moved rows address the target by name (a URL is never an identity).
    name: String,
    incarnation: QueueIncarnation,
    send: QueueSendPolicy,
    /// Target rows with the ID of a message to move, live or expired.
    found: HashMap<MessageId, QueueMessageState>,
}

impl ClaimRedrive {
    /// The name of the target catalog row that the claim reads in its due
    /// wave; `None` without a policy or when the target is the source.
    #[must_use]
    pub(crate) fn target_name(policy: Option<&RedrivePolicy>, source: &Queue) -> Option<String> {
        policy
            .map(|policy| policy.target.queue_name().to_owned())
            .filter(|name| *name != source.queue_name)
    }

    /// `None` when the source has no policy. A missing target, or the source
    /// as its own target, gives a policy with no target: exhausted messages
    /// are delivered as usual and their count keeps increasing (as in SQS).
    pub(crate) fn observe(
        policy: Option<RedrivePolicy>,
        source: &Queue,
        target: Option<QueueCatalogRecord>,
    ) -> StorageResult<Option<Self>> {
        let Some(policy) = policy else {
            return Ok(None);
        };
        let target = target
            .filter(|target| target.queue.queue_name == policy.target.queue_name())
            .map(|target| {
                Ok::<_, storage_types::StorageError>(RedriveTarget {
                    send: QueueSendPolicy::try_from(&target)?,
                    incarnation: target.incarnation,
                    name: target.queue.queue_name,
                    found: HashMap::new(),
                })
            })
            .transpose()?;
        // The source ARN takes the partition and region of the validated
        // policy, which are the deployment's (`QueueManager` checks them).
        let source_arn = policy.target.sibling(source).to_string();
        Ok(Some(Self {
            policy,
            source_arn,
            target,
        }))
    }

    /// Read the target rows with the IDs of the messages to move, in one
    /// batch. Only a claim with a message to move pays this read. The read is
    /// necessary: a stable-identity `PublishMessage` can put the same
    /// `MessageId` in the source and in the target.
    pub(crate) async fn load_held<A: QueueAttempt>(
        &mut self,
        attempt: &mut A,
        due: &[QueueMessageRecord],
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        let Some(target) = self.target.as_mut() else {
            return Ok(());
        };
        let ids = due
            .iter()
            .filter(|record| record.expires_at > captured_at && self.policy.should_move(record))
            .map(|record| record.message.message_id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(());
        }
        let found = attempt
            .read_message_states_by_id(target.incarnation, ids.clone())
            .await?;
        target.found = found
            .zip(ids)
            .filter_map(|(id, found)| found.map(|record| (id, record)))
            .collect();
        Ok(())
    }

    /// The decision for one due, unexpired message. A message that the target
    /// holds live, or with no usable target, is delivered as usual. A target
    /// row that is past its retention is not held: the move replaces it.
    pub(crate) fn decide(
        &self,
        record: &QueueMessageRecord,
        captured_at: TimestampMillis,
    ) -> RedriveDecision {
        if !self.policy.should_move(record) {
            return RedriveDecision::Deliver;
        }
        let Some(target) = self.target.as_ref() else {
            storage_common::telemetry::record_provider_count("queue", "redrive_skipped", 1);
            return RedriveDecision::Deliver;
        };
        let replaced = target.found.get(&record.message.message_id);
        if replaced.is_some_and(|row| row.expires_at > captured_at) {
            storage_common::telemetry::record_provider_count("queue", "redrive_skipped", 1);
            return RedriveDecision::Deliver;
        }
        let moved =
            target
                .send
                .prepare_moved(record, &target.name, self.source_arn.clone(), captured_at);
        if moved.expires_at <= captured_at {
            return RedriveDecision::Drop;
        }
        RedriveDecision::Move {
            moved: Box::new(moved),
            replaced: replaced.cloned().map(Box::new),
        }
    }

    /// The target queue name, for the wake signal after a committed move.
    #[must_use]
    pub(crate) fn moved_to(&self) -> Option<&str> {
        self.target.as_ref().map(|target| target.name.as_str())
    }
}
