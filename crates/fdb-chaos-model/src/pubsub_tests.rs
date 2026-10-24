use std::collections::BTreeSet;

use crate::pubsub::{
    PublishAttempt, PublishAuditPhase, PublishDeliveryAudit, PublishIntentAudit, PublishOutcome,
    check_publish_attempts, check_publish_progress,
};

fn make_attempt(outcome: PublishOutcome) -> PublishAttempt {
    PublishAttempt {
        message_id: "message-1".into(),
        outcome,
        message_body: "exact payload".into(),
        subscription_arns: vec!["arn:subscription:a".into(), "arn:subscription:b".into()],
    }
}

fn make_intent(attempt: &PublishAttempt) -> PublishIntentAudit {
    PublishIntentAudit {
        message_id: attempt.message_id.clone(),
        message_body: attempt.message_body.clone(),
        subscription_arns: attempt.subscription_arns.clone(),
    }
}

fn make_deliveries(attempt: &PublishAttempt) -> Vec<PublishDeliveryAudit> {
    attempt
        .subscription_arns
        .iter()
        .map(|subscription_arn| PublishDeliveryAudit {
            record_id: format!("{subscription_arn}:{}", attempt.message_id),
            message_id: attempt.message_id.clone(),
            subscription_arn: subscription_arn.clone(),
            message_body: Some(attempt.message_body.clone()),
            terminal: true,
        })
        .collect()
}

#[test]
fn given_unknown_attempt_when_no_effects_then_every_phase_accepts() {
    for phase in [
        PublishAuditPhase::Accepted,
        PublishAuditPhase::Materialized,
        PublishAuditPhase::Terminal,
    ] {
        assert!(
            check_publish_attempts(&[make_attempt(PublishOutcome::Unknown)], &[], &[], phase)
                .is_ok()
        );
    }
}

#[test]
fn given_unknown_attempt_when_intent_persists_then_every_phase_accepts_recoverable_work() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let intent = make_intent(&attempt);
    for phase in [
        PublishAuditPhase::Accepted,
        PublishAuditPhase::Materialized,
        PublishAuditPhase::Terminal,
    ] {
        assert!(
            check_publish_attempts(
                std::slice::from_ref(&attempt),
                std::slice::from_ref(&intent),
                &[],
                phase
            )
            .is_ok()
        );
    }
}

#[test]
fn given_unknown_attempt_when_complete_fanout_exists_then_every_phase_accepts() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let deliveries = make_deliveries(&attempt);
    for phase in [
        PublishAuditPhase::Accepted,
        PublishAuditPhase::Materialized,
        PublishAuditPhase::Terminal,
    ] {
        assert!(
            check_publish_attempts(std::slice::from_ref(&attempt), &[], &deliveries, phase).is_ok()
        );
    }
}

#[test]
fn given_acknowledged_attempt_when_effects_are_lost_then_every_phase_rejects() {
    for phase in [
        PublishAuditPhase::Accepted,
        PublishAuditPhase::Materialized,
        PublishAuditPhase::Terminal,
    ] {
        assert!(
            check_publish_attempts(
                &[make_attempt(PublishOutcome::Acknowledged)],
                &[],
                &[],
                phase
            )
            .is_err()
        );
    }
}

#[test]
fn given_acknowledged_attempt_when_intent_persists_then_only_accepted_phase_accepts() {
    let attempt = make_attempt(PublishOutcome::Acknowledged);
    let intents = [make_intent(&attempt)];
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &intents,
            &[],
            PublishAuditPhase::Accepted
        )
        .is_ok()
    );
    for phase in [PublishAuditPhase::Materialized, PublishAuditPhase::Terminal] {
        assert!(
            check_publish_attempts(std::slice::from_ref(&attempt), &intents, &[], phase).is_err()
        );
    }
}

#[test]
fn given_acknowledged_attempt_when_complete_fanout_exists_then_every_phase_accepts() {
    let attempt = make_attempt(PublishOutcome::Acknowledged);
    let deliveries = make_deliveries(&attempt);
    for phase in [
        PublishAuditPhase::Accepted,
        PublishAuditPhase::Materialized,
        PublishAuditPhase::Terminal,
    ] {
        assert!(
            check_publish_attempts(std::slice::from_ref(&attempt), &[], &deliveries, phase).is_ok()
        );
    }
}

#[test]
fn given_acknowledged_empty_snapshot_when_no_effects_then_terminal_accepts() {
    let mut attempt = make_attempt(PublishOutcome::Acknowledged);
    attempt.subscription_arns.clear();
    assert!(check_publish_attempts(&[attempt], &[], &[], PublishAuditPhase::Terminal).is_ok());
}

#[test]
fn given_duplicate_attempt_ids_when_checked_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    assert!(
        check_publish_attempts(
            &[attempt.clone(), attempt],
            &[],
            &[],
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
}

#[test]
fn given_unknown_attempt_when_duplicate_intents_exist_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let intent = make_intent(&attempt);
    assert!(
        check_publish_attempts(
            &[attempt],
            &[intent.clone(), intent],
            &[],
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
}

#[test]
fn given_unknown_attempt_when_duplicate_delivery_exists_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let mut deliveries = make_deliveries(&attempt);
    deliveries.push(deliveries[0].clone());
    assert!(
        check_publish_attempts(&[attempt], &[], &deliveries, PublishAuditPhase::Accepted).is_err()
    );
}

#[test]
fn given_unknown_attempt_when_intent_and_deliveries_coexist_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let intents = [make_intent(&attempt)];
    let deliveries = make_deliveries(&attempt);
    assert!(
        check_publish_attempts(
            &[attempt],
            &intents,
            &deliveries,
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
}

#[test]
fn given_unknown_attempt_when_fanout_is_partial_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let deliveries = make_deliveries(&attempt);
    assert!(
        check_publish_attempts(
            &[attempt],
            &[],
            &deliveries[..1],
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
}

#[test]
fn given_unknown_attempt_when_fanout_has_extra_subscriber_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let mut deliveries = make_deliveries(&attempt);
    let mut extra = deliveries[0].clone();
    extra.subscription_arn = "arn:subscription:extra".into();
    extra.record_id = format!("{}:{}", extra.subscription_arn, extra.message_id);
    deliveries.push(extra);
    assert!(
        check_publish_attempts(&[attempt], &[], &deliveries, PublishAuditPhase::Accepted).is_err()
    );
}

#[test]
fn given_unknown_attempt_when_intent_snapshot_is_malformed_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    for subscriptions in [
        vec!["arn:subscription:a".into()],
        vec!["arn:subscription:a".into(), "arn:subscription:a".into()],
        vec!["arn:subscription:a".into(), "arn:subscription:extra".into()],
    ] {
        let mut intent = make_intent(&attempt);
        intent.subscription_arns = subscriptions;
        assert!(
            check_publish_attempts(
                std::slice::from_ref(&attempt),
                &[intent],
                &[],
                PublishAuditPhase::Accepted
            )
            .is_err()
        );
    }
}

#[test]
fn given_duplicate_expected_subscriptions_when_checked_then_rejects() {
    let mut attempt = make_attempt(PublishOutcome::Unknown);
    attempt
        .subscription_arns
        .push(attempt.subscription_arns[0].clone());
    assert!(check_publish_attempts(&[attempt], &[], &[], PublishAuditPhase::Accepted).is_err());
}

#[test]
fn given_unknown_attempt_when_intent_body_differs_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let mut intent = make_intent(&attempt);
    intent.message_body = "wrong payload".into();
    assert!(
        check_publish_attempts(&[attempt], &[intent], &[], PublishAuditPhase::Accepted).is_err()
    );
}

#[test]
fn given_unknown_attempt_when_delivery_body_is_wrong_or_absent_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    for body in [None, Some("wrong payload".into())] {
        let mut deliveries = make_deliveries(&attempt);
        deliveries[0].message_body = body;
        assert!(
            check_publish_attempts(
                std::slice::from_ref(&attempt),
                &[],
                &deliveries,
                PublishAuditPhase::Terminal
            )
            .is_err()
        );
    }
}

#[test]
fn given_unknown_attempt_when_record_id_is_malformed_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let mut deliveries = make_deliveries(&attempt);
    deliveries[0].record_id = "malformed".into();
    assert!(
        check_publish_attempts(&[attempt], &[], &deliveries, PublishAuditPhase::Accepted).is_err()
    );
}

#[test]
fn given_unattempted_message_when_intent_or_delivery_exists_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    assert!(
        check_publish_attempts(
            &[],
            &[make_intent(&attempt)],
            &[],
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
    assert!(
        check_publish_attempts(
            &[],
            &[],
            &make_deliveries(&attempt),
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
}

#[test]
fn given_rejected_attempt_when_any_effect_exists_then_rejects() {
    let attempt = make_attempt(PublishOutcome::Rejected);
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &[make_intent(&attempt)],
            &[],
            PublishAuditPhase::Accepted
        )
        .is_err()
    );
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &[],
            &make_deliveries(&attempt),
            PublishAuditPhase::Terminal
        )
        .is_err()
    );
    assert!(check_publish_attempts(&[attempt], &[], &[], PublishAuditPhase::Terminal).is_ok());
}

#[test]
fn given_complete_fanout_when_one_delivery_is_pending_then_terminal_rejects() {
    for outcome in [PublishOutcome::Acknowledged, PublishOutcome::Unknown] {
        let attempt = make_attempt(outcome);
        let mut deliveries = make_deliveries(&attempt);
        deliveries[0].terminal = false;
        assert!(
            check_publish_attempts(
                std::slice::from_ref(&attempt),
                &[],
                &deliveries,
                PublishAuditPhase::Materialized
            )
            .is_ok()
        );
        assert!(
            check_publish_attempts(&[attempt], &[], &deliveries, PublishAuditPhase::Terminal)
                .is_err()
        );
    }
}

#[test]
fn given_reordered_intent_snapshot_when_checked_then_accepts_set_equivalence() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let mut intent = make_intent(&attempt);
    intent.subscription_arns.reverse();
    assert!(
        check_publish_attempts(&[attempt], &[intent], &[], PublishAuditPhase::Accepted).is_ok()
    );
}

#[test]
fn given_distinct_messages_when_delivery_record_ids_collide_then_rejects() {
    let mut first = make_attempt(PublishOutcome::Unknown);
    first.message_id = "b:c".into();
    first.subscription_arns = vec!["a".into()];
    let mut second = make_attempt(PublishOutcome::Unknown);
    second.message_id = "c".into();
    second.subscription_arns = vec!["a:b".into()];
    let mut deliveries = make_deliveries(&first);
    deliveries.extend(make_deliveries(&second));
    assert!(
        check_publish_attempts(
            &[first, second],
            &[],
            &deliveries,
            PublishAuditPhase::Terminal
        )
        .is_err()
    );
}

#[test]
fn given_complete_fanout_when_subscriber_is_replaced_then_rejects_same_count() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let mut deliveries = make_deliveries(&attempt);
    deliveries[0].subscription_arn = "arn:subscription:other".into();
    deliveries[0].record_id = format!("{}:{}", deliveries[0].subscription_arn, attempt.message_id);
    assert!(
        check_publish_attempts(&[attempt], &[], &deliveries, PublishAuditPhase::Accepted).is_err()
    );
}

#[test]
fn given_observed_unknown_intent_when_effects_disappear_then_progress_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let intents = [make_intent(&attempt)];
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &intents,
            &[],
            PublishAuditPhase::Accepted
        )
        .is_ok()
    );
    let accepted = check_publish_progress(&BTreeSet::new(), &intents, &[]).unwrap();
    assert_eq!(accepted, BTreeSet::from([attempt.message_id.clone()]));
    assert!(check_publish_attempts(&[attempt], &[], &[], PublishAuditPhase::Terminal).is_ok());
    assert!(check_publish_progress(&accepted, &[], &[]).is_err());
}

#[test]
fn given_observed_unknown_fanout_when_effects_disappear_then_progress_rejects() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let deliveries = make_deliveries(&attempt);
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &[],
            &deliveries,
            PublishAuditPhase::Materialized
        )
        .is_ok()
    );
    let accepted = check_publish_progress(&BTreeSet::new(), &[], &deliveries).unwrap();
    assert_eq!(accepted, BTreeSet::from([attempt.message_id.clone()]));
    assert!(check_publish_attempts(&[attempt], &[], &[], PublishAuditPhase::Terminal).is_ok());
    assert!(check_publish_progress(&accepted, &[], &[]).is_err());
}

#[test]
fn given_observed_unknown_intent_when_complete_fanout_replaces_it_then_progress_accepts() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    let intents = [make_intent(&attempt)];
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &intents,
            &[],
            PublishAuditPhase::Accepted
        )
        .is_ok()
    );
    let accepted = check_publish_progress(&BTreeSet::new(), &intents, &[]).unwrap();
    let deliveries = make_deliveries(&attempt);
    assert!(
        check_publish_attempts(&[attempt], &[], &deliveries, PublishAuditPhase::Terminal).is_ok()
    );
    assert_eq!(
        check_publish_progress(&accepted, &[], &deliveries).unwrap(),
        accepted
    );
}

#[test]
fn given_unobserved_unknown_attempt_when_effects_remain_absent_then_progress_accepts() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &[],
            &[],
            PublishAuditPhase::Accepted
        )
        .is_ok()
    );
    let accepted = check_publish_progress(&BTreeSet::new(), &[], &[]).unwrap();
    assert!(accepted.is_empty());
    assert!(check_publish_attempts(&[attempt], &[], &[], PublishAuditPhase::Terminal).is_ok());
    assert!(
        check_publish_progress(&accepted, &[], &[])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn given_unobserved_unknown_attempt_when_intent_appears_then_progress_retains_acceptance() {
    let attempt = make_attempt(PublishOutcome::Unknown);
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &[],
            &[],
            PublishAuditPhase::Accepted
        )
        .is_ok()
    );
    let accepted = check_publish_progress(&BTreeSet::new(), &[], &[]).unwrap();
    let intents = [make_intent(&attempt)];
    assert!(
        check_publish_attempts(
            std::slice::from_ref(&attempt),
            &intents,
            &[],
            PublishAuditPhase::Terminal
        )
        .is_ok()
    );
    assert_eq!(
        check_publish_progress(&accepted, &intents, &[]).unwrap(),
        BTreeSet::from([attempt.message_id])
    );
}
