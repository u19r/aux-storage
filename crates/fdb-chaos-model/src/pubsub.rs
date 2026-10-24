use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublishOutcome {
    Acknowledged,
    Rejected,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishAttempt {
    pub message_id: String,
    pub outcome: PublishOutcome,
    pub message_body: String,
    pub subscription_arns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishIntentAudit {
    pub message_id: String,
    pub message_body: String,
    pub subscription_arns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishDeliveryAudit {
    pub record_id: String,
    pub message_id: String,
    pub subscription_arn: String,
    pub message_body: Option<String>,
    pub terminal: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublishAuditPhase {
    Accepted,
    Materialized,
    Terminal,
}

/// Checks one complete durable snapshot against retained publish attempts.
/// An unknown attempt may retain its recoverable intent even in the terminal
/// phase; this is a possible acceptance, never proof that the attempt had no
/// effect.
pub fn check_publish_attempts(
    attempts: &[PublishAttempt],
    intents: &[PublishIntentAudit],
    deliveries: &[PublishDeliveryAudit],
    phase: PublishAuditPhase,
) -> Result<(), String> {
    let mut message_ids = BTreeSet::new();
    for attempt in attempts {
        if !message_ids.insert(attempt.message_id.as_str()) {
            return Err(format!("duplicate publish attempt {}", attempt.message_id));
        }
    }
    for message_id in intents
        .iter()
        .map(|intent| intent.message_id.as_str())
        .chain(
            deliveries
                .iter()
                .map(|delivery| delivery.message_id.as_str()),
        )
    {
        if !message_ids.contains(message_id) {
            return Err(format!("unexpected publish effect for {message_id}"));
        }
    }
    let mut record_ids = BTreeSet::new();
    for delivery in deliveries {
        if !record_ids.insert(delivery.record_id.as_str()) {
            return Err(format!("duplicate delivery record {}", delivery.record_id));
        }
    }
    for attempt in attempts {
        check_attempt_effects(attempt, intents, deliveries, phase)
            .map_err(|error| format!("publish {}: {error}", attempt.message_id))?;
    }
    Ok(())
}

/// Preserves observed acceptance across complete durable snapshots.
/// Call `check_publish_attempts` first to validate the current snapshot's
/// evidence.
pub fn check_publish_progress(
    previously_accepted: &BTreeSet<String>,
    intents: &[PublishIntentAudit],
    deliveries: &[PublishDeliveryAudit],
) -> Result<BTreeSet<String>, String> {
    let observed: BTreeSet<_> = intents
        .iter()
        .map(|intent| intent.message_id.clone())
        .chain(
            deliveries
                .iter()
                .map(|delivery| delivery.message_id.clone()),
        )
        .collect();
    if let Some(message_id) = previously_accepted.difference(&observed).next() {
        return Err(format!(
            "publish {message_id}: previously observed acceptance lost all durable evidence"
        ));
    }
    Ok(observed)
}

fn check_attempt_effects(
    attempt: &PublishAttempt,
    intents: &[PublishIntentAudit],
    deliveries: &[PublishDeliveryAudit],
    phase: PublishAuditPhase,
) -> Result<(), String> {
    let expected_subscriptions = collect_unique_subscriptions(&attempt.subscription_arns)?;
    let intents: Vec<_> = intents
        .iter()
        .filter(|intent| intent.message_id == attempt.message_id)
        .collect();
    let deliveries: Vec<_> = deliveries
        .iter()
        .filter(|delivery| delivery.message_id == attempt.message_id)
        .collect();
    if intents.len() > 1 {
        return Err("duplicate publish intent".into());
    }
    if !intents.is_empty() && !deliveries.is_empty() {
        return Err("intent and delivery fanout coexist".into());
    }
    if let Some(intent) = intents.first()
        && (intent.message_body != attempt.message_body
            || collect_unique_subscriptions(&intent.subscription_arns)? != expected_subscriptions)
    {
        return Err("intent body or subscription snapshot differs from attempt".into());
    }
    check_delivery_fanout(attempt, &deliveries, phase)?;
    check_attempt_acceptance(attempt, !intents.is_empty(), deliveries.len(), phase)
}

fn collect_unique_subscriptions(subscriptions: &[String]) -> Result<BTreeSet<&str>, String> {
    let unique: BTreeSet<_> = subscriptions.iter().map(String::as_str).collect();
    if unique.len() != subscriptions.len() {
        return Err("duplicate subscription in publish snapshot".into());
    }
    Ok(unique)
}

fn check_delivery_fanout(
    attempt: &PublishAttempt,
    deliveries: &[&PublishDeliveryAudit],
    phase: PublishAuditPhase,
) -> Result<(), String> {
    let mut subscriptions = BTreeSet::new();
    for delivery in deliveries {
        let expected_id = format!("{}:{}", delivery.subscription_arn, attempt.message_id);
        if delivery.record_id != expected_id {
            return Err("delivery record ID differs from subscription and message".into());
        }
        if delivery.message_body.as_deref() != Some(attempt.message_body.as_str()) {
            return Err("delivery body differs from attempt".into());
        }
        subscriptions.insert(delivery.subscription_arn.as_str());
        if phase == PublishAuditPhase::Terminal && !delivery.terminal {
            return Err("delivery is not terminal".into());
        }
    }
    if !deliveries.is_empty()
        && subscriptions
            != attempt
                .subscription_arns
                .iter()
                .map(String::as_str)
                .collect()
    {
        return Err("delivery fanout differs from subscription snapshot".into());
    }
    Ok(())
}

fn check_attempt_acceptance(
    attempt: &PublishAttempt,
    has_intent: bool,
    delivery_count: usize,
    phase: PublishAuditPhase,
) -> Result<(), String> {
    match attempt.outcome {
        PublishOutcome::Rejected if has_intent || delivery_count > 0 => {
            Err("rejected attempt has durable effects".into())
        }
        PublishOutcome::Acknowledged if has_intent && phase != PublishAuditPhase::Accepted => {
            Err("acknowledged intent was not materialized".into())
        }
        PublishOutcome::Acknowledged
            if !has_intent && delivery_count != attempt.subscription_arns.len() =>
        {
            Err("acknowledged attempt lost its intent and complete delivery fanout".into())
        }
        _ => Ok(()),
    }
}
