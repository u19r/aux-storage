use fdb_chaos_model::{
    PublishAttempt, PublishAuditPhase, PublishDeliveryAudit, PublishIntentAudit, PublishOutcome,
    check_publish_attempts, check_publish_progress,
};
use pubsub_provider::{PubsubAttempt, PubsubPort};

use crate::{common::*, imports::*, pubsub_delivery::*};

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
            let result = provider
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
    ) -> Result<Vec<DeliveryRecord>, String> {
        let (candidates, records) =
            super::pubsub_snapshot::read_pubsub_snapshot(self.driver(db)).await?;
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
                subscription_arn: record.subscription_arn.to_string(),
                message_body: record.message_body.clone(),
                terminal: matches!(
                    record.status,
                    DeliveryStatus::Delivered | DeliveryStatus::Failed
                ),
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
            match provider.materialize_publish_chunks(request).await {
                Ok(_) => return Ok(()),
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

    pub(crate) async fn claim_delivery_for_audit(
        &mut self,
        provider: &Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>,
        phase: &str,
        request: ClaimDeliveryRecordsRequest,
    ) -> Result<(ClaimDeliveryRecordsResponse, TimestampMillis), String> {
        let started_at = request.now;
        let simulated_start = self.context.now();
        let before = read_claim_records(provider, self.delivery_record_ids.clone()).await?;
        match provider.claim_delivery_records(request.clone()).await {
            Ok(response) => Ok((response, started_at)),
            Err(error) if is_unknown_commit(&error) => {
                self.unknown_claim_count += 1;
                let after = read_claim_records(provider, self.delivery_record_ids.clone()).await?;
                verify_unknown_claim(&request, &before, &after)?;
                self.claim_audit_count += 1;
                self.write_publish_audit(&format!("claim-audit-{:02}-{phase}.json", self.claim_audit_count),
                    &serde_json::json!({"request": request, "before": before, "after": after, "outcome": "ambiguous"}))?;
                let lease_ms = request
                    .lease_expires_at
                    .timestamp_millis()
                    .saturating_sub(started_at.timestamp_millis())
                    .max(1);
                self.context
                    .delay(std::time::Duration::from_millis(lease_ms as u64 + 1))
                    .await
                    .map_err(|error| format!("claim recovery delay failed: {error}"))?;
                let now = recovery_timestamp(started_at, simulated_start, self.context.now())?;
                if now <= request.lease_expires_at {
                    return Err("claim recovery clock did not expire original lease".into());
                }
                let retry = ClaimDeliveryRecordsRequest {
                    owner: request.owner,
                    now,
                    lease_expires_at: now + lease_ms,
                    limit: request.limit,
                };
                let response = provider
                    .claim_delivery_records(retry)
                    .await
                    .map_err(|error| format!("claim recovery failed: {error}"))?;
                Ok((response, now))
            }
            Err(error) => Err(error.to_string()),
        }
    }

    fn write_publish_audit(&self, name: &str, value: &serde_json::Value) -> Result<(), String> {
        let root = PathBuf::from(&self.artifact_root).join(format!("client-{}", self.client_id));
        fs::create_dir_all(&root)
            .map_err(|error| format!("create publish audit directory: {error}"))?;
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| format!("encode publish audit: {error}"))?;
        fs::write(root.join(name), bytes).map_err(|error| format!("write publish audit: {error}"))
    }

    pub(crate) fn expected_delivery_status(
        &self,
        subscription: &SubscriptionArn,
    ) -> DeliveryStatus {
        let first = self
            .subscription_arns
            .iter()
            .min_by_key(|arn| arn.to_string());
        if first == Some(subscription) {
            DeliveryStatus::Delivered
        } else {
            DeliveryStatus::Failed
        }
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

fn is_unknown_commit(error: &pubsub_provider::PubsubError) -> bool {
    matches!(error, pubsub_provider::PubsubError::Storage(source)
        if matches!(source.as_ref(), StorageEnum::SyncSubmissionAmbiguous { .. }))
}

async fn read_claim_records(
    provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ids: Vec<DeliveryRecordId>,
) -> Result<Vec<DeliveryRecord>, String> {
    let count = ids.len();
    let rows = provider
        .with_pubsub_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { attempt.read_deliveries(ids).await })
        })
        .await
        .map_err(|error| storage_error_detail(&error))?;
    if rows.len() != count || rows.iter().any(Option::is_none) {
        return Err("claim audit lost a durable delivery".into());
    }
    Ok(rows.into_iter().flatten().collect())
}

fn verify_unknown_claim(
    request: &ClaimDeliveryRecordsRequest,
    before: &[DeliveryRecord],
    after: &[DeliveryRecord],
) -> Result<(), String> {
    if before.len() > usize::from(request.limit) {
        return Err("claim fixture exceeds one atomic batch".into());
    }
    let expected = before
        .iter()
        .map(|record| {
            let mut next = record.clone();
            if record.is_claimable(request.now) {
                next.claim_generation = next
                    .claim_generation
                    .checked_add(1)
                    .ok_or_else(|| "claim generation overflow".to_string())?;
                next.lease_owner = Some(request.owner.clone());
                next.lease_expires_at = Some(request.lease_expires_at);
                next.updated_at = request.now;
            }
            Ok(next)
        })
        .collect::<Result<Vec<_>, String>>()?;
    if after != before && after != expected {
        return Err("unknown claim has partial or invalid durable effects".into());
    }
    Ok(())
}
