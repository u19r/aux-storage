//! Pure decisions of the delivery schedule (W3S plan 5.2).

use std::num::NonZeroU32;

use storage_types::TimestampMillis;
use uuid::Uuid;

use crate::{
    AskMode, ClaimTerms, DeliveryAttemptResult, DeliveryClaimRequest, DeliveryDecision,
    DeliveryFailureCode, DeliveryFence, DeliveryRecordId, DeliveryState, DestinationAsk,
    DestinationId, DestinationKey, DestinationKind, DropReason, SubscriptionArn,
    SubscriptionProtocol,
};

fn at(millis: i64) -> TimestampMillis {
    TimestampMillis::from_timestamp(millis)
}

fn state(destination: DestinationKey) -> DeliveryState {
    DeliveryState {
        id: DeliveryRecordId("arn:aws:sns:us-east-1:000000000000:t:s:m".to_owned()),
        destination,
        due_at: at(1_000),
        subscription_arn: SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:t:s").unwrap(),
        retry_deadline: at(3_601_000),
        attempts: 0,
        generation: 0,
        lease: None,
        last_failure: None,
    }
}

fn terms(owner: &str, now: i64) -> ClaimTerms {
    ClaimTerms {
        owner: owner.to_owned(),
        now: at(now),
        lease_until: at(now + 30_000),
    }
}

fn http() -> DestinationKey {
    DestinationKey::of_endpoint(SubscriptionProtocol::Https, "HTTPS://A.Example/path?q=1")
}

#[test]
fn given_endpoints_when_keyed_then_kind_and_origin_follow_the_protocol() {
    assert_eq!(http().as_str(), "http:https://a.example:443");
    assert_eq!(http().kind(), DestinationKind::Http);
    let queue = DestinationKey::of_endpoint(
        SubscriptionProtocol::Queue,
        "arn:aws:sqs:us-east-1:000000000000:q",
    );
    assert_eq!(queue.kind(), DestinationKind::LocalQueue);
    let confirmation = DestinationKey::of_confirmation(
        &SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:t:s").unwrap(),
    );
    assert_eq!(confirmation.kind(), DestinationKind::Confirmation);
    assert_eq!(
        confirmation.as_str().parse::<DestinationKey>().unwrap(),
        confirmation
    );
    let id = queue.id();
    assert_eq!(id.to_string().parse::<DestinationId>().unwrap(), id);
    assert_ne!(queue.id(), http().id());
}

#[test]
fn given_due_state_when_claimed_twice_then_generation_nonce_and_due_change_and_old_fence_fails() {
    let first = state(http())
        .claim(&terms("a", 1_000), Uuid::new_v4())
        .unwrap();
    assert_eq!(
        (first.generation, first.due_at, first.attempts),
        (1, at(31_000), 0)
    );
    let fence = DeliveryFence::of(&first).unwrap();
    assert!(fence.holds(&first));
    assert_eq!(fence.locator, first.locator());
    let taken = first.claim(&terms("a", 31_000), Uuid::new_v4()).unwrap();
    assert!(
        !fence.holds(&taken),
        "same owner, new nonce: the fence fails"
    );
    assert!(
        DeliveryFence::of(&state(http())).is_err(),
        "no lease, no fence"
    );
    assert!(state(http()).claim(&terms("", 0), Uuid::new_v4()).is_err());
}

#[test]
fn given_state_with_lease_and_failure_when_value_round_trips_then_due_and_id_come_from_key() {
    let mut claimed = state(http())
        .claim(&terms("a", 1_000), Uuid::new_v4())
        .unwrap();
    claimed.last_failure = Some(DeliveryFailureCode::HttpStatus(503));
    let bytes = claimed.encode_value().unwrap();
    assert!(bytes.len() < 512, "a state value is small: {}", bytes.len());
    let decoded = DeliveryState::decode_value(&claimed.locator(), &bytes).unwrap();
    assert_eq!(decoded, claimed);
    let mut other = claimed.locator();
    other.destination = DestinationKey::of_confirmation(
        &SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:t:x").unwrap(),
    )
    .id();
    assert!(DeliveryState::decode_value(&other, &bytes).is_err());
}

#[test]
fn given_failures_when_decided_on_state_then_retry_until_deadline_then_drop() {
    let subscription = crate::Subscription {
        subscription_arn: SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:t:s").unwrap(),
        topic_arn: crate::TopicArn::new("arn:aws:sns:us-east-1:000000000000:t").unwrap(),
        protocol: SubscriptionProtocol::Https,
        endpoint: "https://a.example".to_owned(),
        raw_message_delivery: false,
        filter_policy: None,
        filter_policy_scope: crate::FilterPolicyScope::default(),
        redrive_policy: None,
        delivery_policy: None,
        confirmation: crate::SubscriptionConfirmation::Confirmed,
        extra_json: serde_json::Value::Null,
        created_at: at(0),
    };
    let current = state(http());
    let transient = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpTimeout);
    let DeliveryDecision::Retry { attempts, .. } =
        current.decide(transient.clone(), Some(&subscription), at(2_000), 0)
    else {
        panic!("a transient failure before the deadline retries");
    };
    assert_eq!(attempts, 1);
    assert_eq!(
        current.decide(transient.clone(), Some(&subscription), at(3_601_000), 0),
        DeliveryDecision::Drop {
            reason: DropReason::DeadlineNoDeadLetter,
            failure: Some(DeliveryFailureCode::HttpTimeout),
        }
    );
    assert!(matches!(
        current.decide(transient, None, at(2_000), 0),
        DeliveryDecision::Drop {
            reason: DropReason::SubscriptionDeleted,
            ..
        }
    ));
    assert_eq!(
        "http_status_503".parse::<DeliveryFailureCode>().unwrap(),
        DeliveryFailureCode::HttpStatus(503)
    );
}

#[test]
fn given_asks_when_validated_then_total_take_and_distinct_destinations_are_required() {
    let ask = |destination: DestinationKey, take: u32| DestinationAsk {
        destination,
        take: NonZeroU32::new(take).unwrap(),
        mode: AskMode::Due,
    };
    let queue = DestinationKey::of_endpoint(SubscriptionProtocol::Queue, "arn:q");
    let request = |asks| DeliveryClaimRequest {
        terms: terms("a", 0),
        asks,
    };
    assert!(
        request(vec![ask(http(), 8), ask(queue.clone(), 24)])
            .validate()
            .is_ok()
    );
    assert!(
        request(vec![ask(http(), 8), ask(queue.clone(), 25)])
            .validate()
            .is_err()
    );
    assert!(
        request(vec![ask(http(), 1), ask(http(), 1)])
            .validate()
            .is_err()
    );
    assert!(request(Vec::new()).validate().is_err());
}
