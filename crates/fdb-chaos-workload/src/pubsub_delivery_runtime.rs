use fdb_chaos_model::PublishAuditPhase;

use crate::{common::*, imports::*, pubsub_delivery::*};

impl RustWorkload for PubsubDeliveryWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if self.client_id != 0 {
            return;
        }
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("setup", err);
                return;
            }
        };

        self.trace_operation("setup", "create_topic");
        let topic = match provider
            .create_topic(CreateTopicRequest {
                name: match TopicName::new(self.topic_name()) {
                    Ok(name) => name,
                    Err(err) => {
                        self.trace_error("create_topic", err.to_string());
                        return;
                    }
                },
                attributes: HashMap::new(),
            })
            .await
        {
            Ok(topic) => topic,
            Err(err) => {
                self.trace_error("create_topic", err.to_string());
                return;
            }
        };

        let mut subscription_arns = Vec::new();
        for subscription_index in 0..2 {
            self.trace_operation("setup", "create_subscription");
            let subscription = match provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: self.endpoint_for(subscription_index),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
                .await
            {
                Ok(subscription) => subscription,
                Err(err) => {
                    self.trace_error("create_subscription", err.to_string());
                    return;
                }
            };
            subscription_arns.push(subscription.subscription_arn);
        }
        self.topic_arn = Some(topic.topic_arn);
        self.subscription_arns = subscription_arns;
    }

    async fn start(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
        if !self.is_active_client() {
            return;
        }
        let Some(topic_arn) = self.topic_arn.clone() else {
            self.trace_error("start", "missing setup topic arn".to_string());
            return;
        };
        let subscription_arns = self.subscription_arns.clone();
        if subscription_arns.len() != 2 {
            self.trace_error(
                "start",
                format!(
                    "expected two setup subscription arns, found {}",
                    subscription_arns.len()
                ),
            );
            return;
        }
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("start", err);
                return;
            }
        };

        let message_id = match self.publish_for_delivery_audit(&provider, &topic_arn).await {
            Ok(message_id) => message_id,
            Err(error) => {
                self.trace_error("publish", error);
                return;
            }
        };
        self.message_id = Some(message_id);
        if let Err(error) = self
            .audit_publish_effects(db.clone(), PublishAuditPhase::Accepted)
            .await
        {
            self.trace_error("publish_acceptance_audit", error);
            return;
        }

        if let Err(error) = self
            .materialize_for_delivery_audit(&provider, db.clone())
            .await
        {
            let audit = self
                .audit_publish_effects(db.clone(), PublishAuditPhase::Materialized)
                .await;
            self.trace_error(
                "materialize_publish_intents",
                format!(
                    "{error}; durable audit: {:?}",
                    audit.as_ref().map(|records| records.len())
                ),
            );
            return;
        }
        let materialized = match self
            .audit_publish_effects(db.clone(), PublishAuditPhase::Materialized)
            .await
        {
            Ok(records) => records,
            Err(error) => {
                self.trace_error("publish_materialization_audit", error);
                return;
            }
        };
        let delivery_record_ids = materialized
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        self.delivery_record_ids = delivery_record_ids.clone();

        self.trace_operation("start", "claim_delivery");
        let mut now = TimestampMillis::now();
        let Some(limit) = DeliveryClaimLimit::try_from(10usize).ok() else {
            return;
        };
        let claim = match self
            .claim_delivery_for_audit(
                &provider,
                "claim_delivery",
                ClaimDeliveryRecordsRequest {
                    owner: "fdb-chaos-pubsub-worker".to_string(),
                    now,
                    lease_expires_at: now + 30,
                    limit,
                },
            )
            .await
        {
            Ok((claim, next_now)) => {
                now = next_now;
                claim
            }
            Err(err) => {
                self.trace_error("claim_delivery", err.to_string());
                return;
            }
        };
        if claim.records.len() != delivery_record_ids.len() {
            self.trace_error(
                "claim_delivery",
                format!(
                    "expected all materialized delivery records, found {}",
                    claim.records.len()
                ),
            );
            return;
        }
        let mut records = claim.records;
        records.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        let actual_ids = records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        if actual_ids != delivery_record_ids {
            self.trace_error(
                "claim_delivery",
                format!(
                    "delivery record id mismatch: expected {:?} actual {:?}",
                    delivery_record_ids, actual_ids
                ),
            );
            return;
        }
        self.claim_count += records.len() as u64;

        self.trace_operation("start", "claim_duplicate");
        let Some(limit) = DeliveryClaimLimit::try_from(10usize).ok() else {
            return;
        };
        let duplicate = match self
            .claim_delivery_for_audit(
                &provider,
                "claim_duplicate",
                ClaimDeliveryRecordsRequest {
                    owner: "fdb-chaos-pubsub-worker-duplicate".to_string(),
                    now: now + 1,
                    lease_expires_at: now + 31,
                    limit,
                },
            )
            .await
        {
            Ok((claim, next_now)) => {
                now = next_now;
                claim
            }
            Err(err) => {
                self.trace_error("claim_duplicate", err.to_string());
                return;
            }
        };
        if !duplicate.records.is_empty() {
            self.trace_error(
                "claim_duplicate",
                format!(
                    "duplicate claim returned {} leased delivery records",
                    duplicate.records.len()
                ),
            );
            return;
        }
        self.duplicate_claim_reject_count += 1;

        let mut retry_record_ids = Vec::new();
        for record in records {
            let delivered = self.expected_delivery_status(&record.subscription_arn)
                == DeliveryStatus::Delivered;
            let Some(owner) = record.lease_owner else {
                self.trace_error(
                    "update_delivery",
                    "claimed delivery has no lease owner".into(),
                );
                return;
            };
            if !delivered {
                retry_record_ids.push(record.id.clone());
            }
            let update = DeliveryRecordUpdate {
                record_id: record.id,
                expected_claim_generation: record.claim_generation,
                expected_lease_owner: owner,
                status: if delivered {
                    DeliveryStatus::Delivered
                } else {
                    DeliveryStatus::RetryScheduled
                },
                attempts: record.attempts.saturating_add(1),
                next_attempt_at: if delivered { None } else { Some(now + 10) },
                last_error: if delivered {
                    None
                } else {
                    Some("fdb-chaos synthetic retry".into())
                },
                updated_at: now + 3,
            };
            if let Err(error) = provider.update_delivery_record(update).await {
                self.trace_error("update_delivery", error.to_string());
                return;
            }
            if delivered {
                self.delivered_count += 1;
            } else {
                self.retry_reschedule_count += 1;
            }
        }

        self.trace_operation("start", "claim_terminal_and_retry_not_due");
        let Some(limit) = DeliveryClaimLimit::try_from(10usize).ok() else {
            return;
        };
        let not_due = match self
            .claim_delivery_for_audit(
                &provider,
                "claim_terminal_and_retry_not_due",
                ClaimDeliveryRecordsRequest {
                    owner: "fdb-chaos-pubsub-worker-terminal".to_string(),
                    now: now + 4,
                    lease_expires_at: now + 34,
                    limit,
                },
            )
            .await
        {
            Ok((claim, next_now)) => {
                now = next_now;
                claim
            }
            Err(err) => {
                self.trace_error("claim_terminal_and_retry_not_due", err.to_string());
                return;
            }
        };
        if !not_due.records.is_empty() {
            self.trace_error(
                "claim_terminal_and_retry_not_due",
                format!(
                    "terminal or retry-not-due claim returned {} delivery records",
                    not_due.records.len()
                ),
            );
            return;
        }
        self.terminal_duplicate_reject_count += 1;

        self.trace_operation("start", "claim_retry_due");
        let Some(limit) = DeliveryClaimLimit::try_from(10usize).ok() else {
            return;
        };
        let retry_claim = match self
            .claim_delivery_for_audit(
                &provider,
                "claim_retry_due",
                ClaimDeliveryRecordsRequest {
                    owner: "fdb-chaos-pubsub-worker-retry".to_string(),
                    now: now + 11,
                    lease_expires_at: now + 41,
                    limit,
                },
            )
            .await
        {
            Ok((claim, next_now)) => {
                now = next_now;
                claim
            }
            Err(err) => {
                self.trace_error("claim_retry_due", err.to_string());
                return;
            }
        };
        let mut retry_ids = retry_claim
            .records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        retry_ids.sort_by(|left, right| left.0.cmp(&right.0));
        if retry_ids != retry_record_ids {
            self.trace_error(
                "claim_retry_due",
                "retry claim differs from scheduled record set".into(),
            );
            return;
        }
        self.retry_claim_count += retry_claim.records.len() as u64;
        self.trace_operation("start", "mark_failed");
        for record in retry_claim.records {
            let Some(owner) = record.lease_owner else {
                self.trace_error("mark_failed", "claimed delivery has no lease owner".into());
                return;
            };
            let update = DeliveryRecordUpdate {
                record_id: record.id,
                expected_claim_generation: record.claim_generation,
                expected_lease_owner: owner,
                status: DeliveryStatus::Failed,
                attempts: record.attempts.saturating_add(1),
                next_attempt_at: None,
                last_error: Some("fdb-chaos synthetic failure".into()),
                updated_at: now + 12,
            };
            if let Err(error) = provider.update_delivery_record(update).await {
                self.trace_error("mark_failed", error.to_string());
                return;
            }
            self.failed_count += 1;
        }

        self.trace_operation("start", "claim_terminal_failed");
        let Some(limit) = DeliveryClaimLimit::try_from(10usize).ok() else {
            return;
        };
        let failed_terminal = match self
            .claim_delivery_for_audit(
                &provider,
                "claim_terminal_failed",
                ClaimDeliveryRecordsRequest {
                    owner: "fdb-chaos-pubsub-worker-failed-terminal".to_string(),
                    now: now + 13,
                    lease_expires_at: now + 43,
                    limit,
                },
            )
            .await
        {
            Ok((claim, _next_now)) => claim,
            Err(err) => {
                self.trace_error("claim_terminal_failed", err.to_string());
                return;
            }
        };
        if !failed_terminal.records.is_empty() {
            self.trace_error(
                "claim_terminal_failed",
                format!(
                    "failed terminal claim returned {} delivery records",
                    failed_terminal.records.len()
                ),
            );
            return;
        }
        self.terminal_duplicate_reject_count += 1;

        self.topic_arn = Some(topic_arn);
        self.subscription_arns = subscription_arns;
        self.delivery_record_ids = delivery_record_ids;
    }

    async fn check(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if self.client_id != 0 {
            return;
        }
        if self.message_id.is_none() || self.delivery_record_ids.len() < 2 {
            self.trace_error(
                "check",
                "acknowledged delivery coverage was not initialized".into(),
            );
            return;
        }
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(error) => {
                self.trace_error("check", error);
                return;
            }
        };
        let records = match self
            .audit_publish_effects(db, PublishAuditPhase::Terminal)
            .await
        {
            Ok(records) => records,
            Err(error) => {
                self.trace_error("publish_terminal_audit", error);
                return;
            }
        };
        self.direct_scan_count += records.len() as u64;
        let direct_ids = records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        if direct_ids != self.delivery_record_ids {
            self.trace_error(
                "check",
                "terminal delivery set differs from materialized set".into(),
            );
            return;
        }
        for direct in records {
            let expected = self.expected_delivery_status(&direct.subscription_arn);
            if direct.status != expected {
                self.trace_error(
                    "check",
                    "direct delivery has incorrect terminal outcome".into(),
                );
                return;
            }
            match provider.get_delivery_record(&direct.id).await {
                Ok(Some(record)) if record == direct => self.orphan_check_count += 1,
                Ok(_) => {
                    self.trace_error(
                        "check",
                        "provider delivery differs from durable audit".into(),
                    );
                    return;
                }
                Err(error) => {
                    self.trace_error("check", error.to_string());
                    return;
                }
            }
        }
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64("aux_storage_pubsub_delivery_setup_count", self.setup_count),
            metric_val_u64("aux_storage_pubsub_delivery_start_count", self.start_count),
            metric_val_u64("aux_storage_pubsub_delivery_check_count", self.check_count),
            metric_val_u64(
                "aux_storage_pubsub_delivery_publish_count",
                self.publish_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_unknown_publish_count",
                self.unknown_publish_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_unknown_materialization_count",
                self.unknown_materialization_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_unknown_claim_count",
                self.unknown_claim_count,
            ),
            metric_val_u64("aux_storage_pubsub_delivery_claim_count", self.claim_count),
            metric_val_u64(
                "aux_storage_pubsub_delivery_duplicate_claim_reject_count",
                self.duplicate_claim_reject_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_delivered_count",
                self.delivered_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_failed_count",
                self.failed_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_retry_reschedule_count",
                self.retry_reschedule_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_retry_claim_count",
                self.retry_claim_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_direct_scan_count",
                self.direct_scan_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_terminal_duplicate_reject_count",
                self.terminal_duplicate_reject_count,
            ),
            metric_val_u64(
                "aux_storage_pubsub_delivery_orphan_check_count",
                self.orphan_check_count,
            ),
            metric_val_u64("aux_storage_pubsub_delivery_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
