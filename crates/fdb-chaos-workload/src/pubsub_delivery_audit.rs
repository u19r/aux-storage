use fdb_chaos_model::{
    PublishAttempt, PublishAuditPhase, PublishDeliveryAudit, PublishIntentAudit, PublishOutcome,
    check_publish_attempts, check_publish_progress,
};

use crate::{imports::*, pubsub_delivery::*};

impl PubsubDeliveryWorkload {
    pub(crate) async fn publish_for_delivery_audit(
        &mut self,
        provider: &Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>,
        topic_arn: &TopicArn,
    ) -> Result<PubsubMessageId, String> {
        // An independent publish provides coverage if the first acknowledgement
        // is lost.
        for sequence in 0..2 {
            let message_id = PubsubMessageId::new();
            let message_body = self.message_body(sequence);
            // A retryable outcome (a conflict or an expired attempt deadline)
            // proves nothing committed: publish again under the same ID.
            let mut result = Ok(());
            for _ in 0..6 {
                result = provider
                    .accept_publish(
                        PublishRequest {
                            topic_arn: topic_arn.clone(),
                            message: message_body.clone(),
                            subject: None,
                            message_attributes: HashMap::new(),
                        },
                        message_id.clone(),
                        true,
                    )
                    .await;
                if !matches!(&result, Err(error) if is_known_not_committed_retryable(error)) {
                    break;
                }
            }
            let outcome = match &result {
                Ok(()) => PublishOutcome::Acknowledged,
                Err(error) if is_unknown_commit(error) => PublishOutcome::Unknown,
                Err(_) => PublishOutcome::Rejected,
            };
            self.publish_attempts.push(PublishAttempt {
                message_id: message_id.to_string(),
                outcome,
                message_body,
                subscription_arns: self
                    .subscription_arns
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            });
            self.write_publish_audit(
                "publish-attempts.json",
                &serde_json::json!(self.publish_attempts),
            )?;
            match result {
                Ok(()) => {
                    self.publish_count += 1;
                    return Ok(message_id);
                }
                Err(error) if is_unknown_commit(&error) => {
                    self.unknown_publish_count += 1;
                }
                Err(error) => return Err(format!("publish rejected: {error}")),
            }
        }
        Err("independent publish control was also unknown; delivery coverage incomplete".into())
    }

    pub(crate) async fn audit_publish_effects(
        &mut self,
        db: SimDatabase,
        phase: PublishAuditPhase,
    ) -> Result<Vec<DeliveryPayload>, String> {
        let (candidates, records) =
            super::pubsub_snapshot::read_pubsub_snapshot(self.driver(db)?).await?;
        let intents = candidates
            .into_iter()
            .map(|candidate| PublishIntentAudit {
                message_id: candidate.intent.acceptance.message_id.to_string(),
                message_body: candidate.intent.acceptance.request.message,
                subscription_arns: candidate
                    .subscriptions
                    .iter()
                    .map(|subscription| subscription.subscription_arn.to_string())
                    .collect(),
            })
            .collect::<Vec<_>>();
        let deliveries = records
            .iter()
            .map(|record| PublishDeliveryAudit {
                record_id: record.id.0.clone(),
                message_id: record.message_id.to_string(),
                subscription_arn: record
                    .subscription
                    .as_ref()
                    .map(|subscription| subscription.subscription_arn.to_string())
                    .unwrap_or_default(),
                message_body: record.message_body.clone(),
                // A stored record is pending; terminal records are deleted.
                terminal: false,
            })
            .collect::<Vec<_>>();
        self.publish_audit_count += 1;
        self.write_publish_audit(&format!("publish-audit-{:02}-{phase:?}.json", self.publish_audit_count),
            &serde_json::json!({"attempts": self.publish_attempts, "previously_observed_acceptances": self.observed_publish_acceptances, "intents": intents, "deliveries": deliveries}))?;
        check_publish_attempts(&self.publish_attempts, &intents, &deliveries, phase)?;
        self.observed_publish_acceptances =
            check_publish_progress(&self.observed_publish_acceptances, &intents, &deliveries)?;
        Ok(records)
    }

    pub(crate) async fn materialize_for_delivery_audit(
        &mut self,
        provider: &Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>,
        db: SimDatabase,
    ) -> Result<(), String> {
        let started_at = TimestampMillis::now();
        let simulated_start = self.context.now();
        let mut now = started_at;
        for pass in 0..2 {
            let request = pubsub_provider::PublishChunkClaimRequest {
                limit: pubsub_provider::PublishChunkClaimLimit::try_from(10usize)
                    .map_err(|error| error.to_string())?,
                now,
            };
            // A failed chunk is reported, not returned (review D8).
            let outcome = provider
                .materialize_publish_chunks(request)
                .await
                .and_then(|report| report.first_error.map_or(Ok(()), Err));
            match outcome {
                Ok(()) => return Ok(()),
                Err(error) if is_unknown_commit(&error) => {
                    self.unknown_materialization_count += 1;
                    let records = self
                        .audit_publish_effects(db.clone(), PublishAuditPhase::Accepted)
                        .await?;
                    if self
                        .publish_attempts
                        .iter()
                        .filter(|attempt| attempt.outcome == PublishOutcome::Acknowledged)
                        .all(|attempt| {
                            records
                                .iter()
                                .filter(|record| record.message_id.as_str() == attempt.message_id)
                                .count()
                                == attempt.subscription_arns.len()
                        })
                    {
                        return Ok(());
                    }
                    if pass == 1 {
                        return Err(format!(
                            "materialization recovery remained unknown: {error}"
                        ));
                    }
                    self.context
                        .delay(std::time::Duration::from_millis(
                            pubsub_provider::PUBLISH_INTENT_LEASE_MILLIS as u64 + 1,
                        ))
                        .await
                        .map_err(|error| {
                            format!("materialization recovery delay failed: {error}")
                        })?;
                    now = recovery_timestamp(started_at, simulated_start, self.context.now())?;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        unreachable!("bounded materialization passes return on the second attempt")
    }

    /// Claim up to `take` due records of one destination. An unknown claim
    /// commit is recovered by claiming again after the lease: the fence
    /// makes a lost or applied claim harmless (W3S R4, R5).
    pub(crate) async fn claim_for_audit(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
        owner: &str,
        destination: &DestinationKey,
        now: TimestampMillis,
    ) -> Result<(Vec<ClaimedDelivery>, TimestampMillis), String> {
        let mut now = now;
        for _ in 0..6 {
            let request = DeliveryClaimRequest {
                terms: ClaimTerms {
                    owner: owner.to_owned(),
                    now,
                    lease_until: now + CLAIM_LEASE_MS,
                },
                asks: vec![DestinationAsk {
                    destination: destination.clone(),
                    take: std::num::NonZeroU32::new(10).expect("nonzero take"),
                    mode: AskMode::Due,
                }],
            };
            match provider.claim_deliveries(request).await {
                Ok(mut claims) => {
                    let claimed = claims.pop().map(|claim| claim.claimed).unwrap_or_default();
                    return Ok((claimed, now));
                }
                Err(error) if is_unknown_commit(&error) => {
                    self.unknown_claim_count += 1;
                    now = now + CLAIM_LEASE_MS + 1;
                }
                // A conflict proves that nothing committed: claim again.
                Err(error) if is_retryable(&error) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("delivery claim failed six times".into())
    }

    fn write_publish_audit(&self, name: &str, value: &serde_json::Value) -> Result<(), String> {
        let root = PathBuf::from(&self.artifact_root).join(format!("client-{}", self.client_id));
        fs::create_dir_all(&root)
            .map_err(|error| format!("create publish audit directory: {error}"))?;
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| format!("encode publish audit: {error}"))?;
        fs::write(root.join(name), bytes).map_err(|error| format!("write publish audit: {error}"))
    }

    /// The first subscription (in ARN order) is delivered; the other fails.
    pub(crate) fn expects_delivery(&self, subscription: &SubscriptionArn) -> bool {
        let first = self
            .subscription_arns
            .iter()
            .min_by_key(|arn| arn.to_string());
        first == Some(subscription)
    }
}

pub(crate) fn recovery_timestamp(
    started_at: TimestampMillis,
    simulated_start: f64,
    simulated_end: f64,
) -> Result<TimestampMillis, String> {
    let elapsed_ms = (simulated_end - simulated_start) * 1_000.0;
    if !elapsed_ms.is_finite() || elapsed_ms < 0.0 || elapsed_ms >= i64::MAX as f64 {
        return Err("invalid simulated materialization clock".into());
    }
    Ok(TimestampMillis::from_timestamp(
        started_at
            .timestamp_millis()
            .saturating_add(elapsed_ms as i64),
    ))
}

/// A settle or local delivery that lost every conflict retry, expired its
/// attempt deadline (known not committed), or whose commit outcome is
/// unknown, is tried again under the same fence.
pub(crate) fn is_retryable(error: &pubsub_provider::PubsubError) -> bool {
    is_unknown_commit(error) || is_known_not_committed_retryable(error)
}

/// Known not committed, and the public contract says clients retry.
pub(crate) fn is_known_not_committed_retryable(error: &pubsub_provider::PubsubError) -> bool {
    matches!(error, pubsub_provider::PubsubError::Storage(source)
        if source.class().is_retryable())
}

pub(crate) fn is_unknown_commit(error: &pubsub_provider::PubsubError) -> bool {
    matches!(error, pubsub_provider::PubsubError::Storage(source)
        if source.is_commit_unknown())
}

/// The lease of the workload's claims.
pub(crate) const CLAIM_LEASE_MS: i64 = 30_000;
