//! W3 tests 2 and 3: attempt classification and the delivery decision.

use std::{collections::HashMap, time::Duration};

use storage_types::{MessagingIdentity, StorageEnum, StorageError, TimestampMillis};

use crate::{
    DeliveryAttemptResult, DeliveryDecision, DeliveryFailureCode, DeliveryRecordId,
    DeliveryRetryPolicy, DeliveryState, DestinationKey, DropReason, PubsubMessageId,
    SubscribeRequest, Subscription, SubscriptionProtocol, TopicArn, delivery_policy::after,
};

const QUEUE_ARN: &str = "arn:aws:sqs:us-east-1:000000000000:orders";
const DLQ_ARN: &str = "arn:aws:sqs:us-east-1:000000000000:orders-dlq";

fn subscription(protocol: SubscriptionProtocol, endpoint: &str, dead_letter: bool) -> Subscription {
    let mut attributes = HashMap::new();
    if dead_letter {
        attributes.insert(
            "RedrivePolicy".to_owned(),
            format!("{{\"deadLetterTargetArn\":\"{DLQ_ARN}\"}}"),
        );
    }
    Subscription::try_new(
        SubscribeRequest {
            topic_arn: TopicArn::new("arn:aws:sns:us-east-1:000000000000:orders").unwrap(),
            protocol,
            endpoint: endpoint.to_owned(),
            attributes,
            extra_json: serde_json::Value::Null,
        },
        &MessagingIdentity::default(),
    )
    .unwrap()
}

fn record(subscription: &Subscription, created_at: TimestampMillis) -> DeliveryState {
    DeliveryState {
        id: DeliveryRecordId(format!(
            "{}:{}",
            subscription.subscription_arn,
            PubsubMessageId::new()
        )),
        destination: DestinationKey::of_endpoint(subscription.protocol, &subscription.endpoint),
        due_at: created_at,
        subscription_arn: subscription.subscription_arn.clone(),
        retry_deadline: DeliveryRetryPolicy::for_protocol(subscription.protocol)
            .deadline(created_at),
        attempts: 0,
        generation: 0,
        lease: None,
        last_failure: None,
    }
}

#[test]
fn given_attempt_outcomes_when_classified_then_each_row_of_table_1_2_holds() {
    for (status, expected) in [
        (200, DeliveryAttemptResult::Delivered),
        (204, DeliveryAttemptResult::Delivered),
        (
            301,
            DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(301)),
        ),
        (
            400,
            DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400)),
        ),
        (
            404,
            DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(404)),
        ),
        (
            429,
            DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(429)),
        ),
        (
            500,
            DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(500)),
        ),
        (
            503,
            DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(503)),
        ),
    ] {
        assert_eq!(
            DeliveryAttemptResult::from_http_status(status),
            expected,
            "{status}"
        );
    }
    let unavailable = DeliveryAttemptResult::Transient(DeliveryFailureCode::QueueStoreUnavailable);
    let conflict: StorageError = StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
        storage_types::ConflictCause::Logical("test"),
    ))
    .into();
    let capacity: StorageError =
        StorageEnum::Attempt(storage_types::AttemptFailure::CAPACITY).into();
    assert_eq!(
        DeliveryAttemptResult::from_queue_store_error(&conflict),
        Some(unavailable.clone())
    );
    assert_eq!(
        DeliveryAttemptResult::from_queue_store_error(&capacity),
        Some(unavailable)
    );
    assert_eq!(
        DeliveryAttemptResult::from_queue_store_error(&StorageError::internal("unknown commit")),
        Some(DeliveryAttemptResult::Transient(
            DeliveryFailureCode::QueueStoreUnavailable
        )),
        "an unknown outcome is settled as transient; the fence decides"
    );
    let durability: StorageError =
        StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
            storage_types::NativeUnavailable::DurabilityFailed,
        ))
        .into();
    assert_eq!(
        DeliveryAttemptResult::from_queue_store_error(&durability),
        None
    );
}

#[test]
fn given_attempts_when_delayed_then_jitter_stays_inside_half_and_full_delay() {
    for policy in [DeliveryRetryPolicy::SQS, DeliveryRetryPolicy::HTTP] {
        for attempts in 1..=12 {
            let full = policy
                .base_delay
                .saturating_mul(1 << (attempts - 1))
                .min(policy.max_delay);
            for jitter in [0, 1, 777, u32::MAX] {
                let delay = policy.next_delay(attempts, jitter);
                assert!(
                    delay >= full / 2 && delay <= full,
                    "{attempts} {jitter}: {delay:?}"
                );
            }
        }
    }
    assert_eq!(
        DeliveryRetryPolicy::SQS.lifetime,
        Duration::from_secs(23 * 24 * 3600)
    );
    assert_eq!(
        DeliveryRetryPolicy::HTTP.lifetime,
        Duration::from_secs(3600)
    );
}

#[test]
fn given_failures_when_decided_then_retry_until_deadline_then_dead_letter_or_drop() {
    let created = TimestampMillis::from_timestamp(1_000_000);
    let with_dlq = subscription(SubscriptionProtocol::Https, "https://example.com", true);
    let plain = subscription(SubscriptionProtocol::Queue, QUEUE_ARN, false);
    let transient = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(503));
    let permanent = DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400));

    let early = record(&with_dlq, created);
    match early.decide(transient.clone(), Some(&with_dlq), created, 0) {
        DeliveryDecision::Retry {
            attempts,
            next_attempt_at,
            ..
        } => {
            assert_eq!(attempts, 1);
            assert!(next_attempt_at > created && next_attempt_at < early.retry_deadline);
        }
        other => panic!("{other:?}"),
    }
    let at_deadline = early.retry_deadline;
    assert!(matches!(
        early.decide(transient.clone(), Some(&with_dlq), at_deadline, 0),
        DeliveryDecision::DeadLetter { ref target, .. } if target.to_string() == DLQ_ARN
    ));
    let queue_record = record(&plain, created);
    assert_eq!(
        queue_record.decide(
            transient.clone(),
            Some(&plain),
            queue_record.retry_deadline,
            0
        ),
        DeliveryDecision::Drop {
            reason: DropReason::DeadlineNoDeadLetter,
            failure: Some(DeliveryFailureCode::HttpStatus(503)),
        }
    );
    assert!(matches!(
        early.decide(permanent.clone(), Some(&with_dlq), created, 0),
        DeliveryDecision::DeadLetter { .. }
    ));
    assert!(matches!(
        queue_record.decide(permanent.clone(), Some(&plain), created, 0),
        DeliveryDecision::Drop {
            reason: DropReason::PermanentNoDeadLetter,
            ..
        }
    ));
    assert!(matches!(
        early.decide(transient, None, created, 0),
        DeliveryDecision::Drop {
            reason: DropReason::SubscriptionDeleted,
            ..
        }
    ));
}

/// A record past its deadline still gets its attempt: the deadline is
/// checked only after a failure.
#[test]
fn given_record_past_deadline_when_delivered_then_it_completes() {
    let created = TimestampMillis::from_timestamp(1_000_000);
    let with_dlq = subscription(SubscriptionProtocol::Https, "https://example.com", true);
    let backlog = record(&with_dlq, created);
    let long_after = after(backlog.retry_deadline, Duration::from_secs(3600));
    assert_eq!(
        backlog.decide(
            DeliveryAttemptResult::Delivered,
            Some(&with_dlq),
            long_after,
            0
        ),
        DeliveryDecision::Complete,
    );
}

#[test]
fn given_confirmation_when_it_fails_then_it_never_dead_letters() {
    let created = TimestampMillis::from_timestamp(1_000_000);
    let with_dlq = subscription(SubscriptionProtocol::Https, "https://example.com", true);
    let mut confirmation = record(&with_dlq, created);
    confirmation.destination = crate::DestinationKey::of_confirmation(&with_dlq.subscription_arn);
    assert!(matches!(
        confirmation.decide(
            DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400)),
            Some(&with_dlq),
            created,
            0
        ),
        DeliveryDecision::Drop {
            reason: DropReason::PermanentNoDeadLetter,
            ..
        }
    ));
}
