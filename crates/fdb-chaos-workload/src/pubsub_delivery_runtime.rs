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
        let manager = match self.manager(db.clone()).await {
            Ok(manager) => manager,
            Err(err) => {
                self.trace_error("start", err);
                return;
            }
        };
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("start", err);
                return;
            }
        };

        self.trace_operation("start", "publish");
        let publish = match manager
            .publish(PublishRequest {
                topic_arn: topic_arn.clone(),
                message: self.message_body(0),
                subject: None,
                message_attributes: HashMap::new(),
            })
            .await
        {
            Ok(publish) => publish,
            Err(err) => {
                self.trace_error("publish", err.to_string());
                return;
            }
        };
        self.publish_count += 1;
        let mut delivery_record_ids = subscription_arns
            .iter()
            .map(|subscription_arn| {
                DeliveryRecordId(format!("{}:{}", subscription_arn, publish.message_id))
            })
            .collect::<Vec<_>>();
        delivery_record_ids.sort_by(|left, right| left.0.cmp(&right.0));

        self.trace_operation("start", "claim_delivery");
        let now = TimestampMillis::now();
        let claim = match provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: "fdb-chaos-pubsub-worker".to_string(),
                now,
                lease_expires_at: now + 30,
                limit: 10,
            })
            .await
        {
            Ok(claim) => claim,
            Err(err) => {
                self.trace_error("claim_delivery", err.to_string());
                return;
            }
        };
        if claim.records.len() != 2 {
            self.trace_error(
                "claim_delivery",
                format!(
                    "expected two delivery records, found {}",
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
        let duplicate = match provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: "fdb-chaos-pubsub-worker-duplicate".to_string(),
                now: now + 1,
                lease_expires_at: now + 31,
                limit: 10,
            })
            .await
        {
            Ok(claim) => claim,
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

        self.trace_operation("start", "mark_delivered");
        let mut delivered_record = records.remove(0);
        delivered_record.status = DeliveryStatus::Delivered;
        delivered_record.updated_at = now + 2;
        delivered_record.lease_owner = None;
        delivered_record.lease_expires_at = None;
        if let Err(err) = provider.update_delivery_record(delivered_record).await {
            self.trace_error("mark_delivered", err.to_string());
            return;
        }
        self.delivered_count += 1;

        self.trace_operation("start", "schedule_retry");
        let mut retry_record = records.remove(0);
        retry_record.status = DeliveryStatus::RetryScheduled;
        retry_record.attempts += 1;
        retry_record.next_attempt_at = Some(now + 10);
        retry_record.updated_at = now + 3;
        retry_record.lease_owner = None;
        retry_record.lease_expires_at = None;
        retry_record.last_error = Some("fdb-chaos synthetic retry".to_string());
        let retry_record_id = retry_record.id.clone();
        if let Err(err) = provider.update_delivery_record(retry_record).await {
            self.trace_error("schedule_retry", err.to_string());
            return;
        }
        self.retry_reschedule_count += 1;

        self.trace_operation("start", "claim_terminal_and_retry_not_due");
        let not_due = match provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: "fdb-chaos-pubsub-worker-terminal".to_string(),
                now: now + 4,
                lease_expires_at: now + 34,
                limit: 10,
            })
            .await
        {
            Ok(claim) => claim,
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
        let retry_claim = match provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: "fdb-chaos-pubsub-worker-retry".to_string(),
                now: now + 11,
                lease_expires_at: now + 41,
                limit: 10,
            })
            .await
        {
            Ok(claim) => claim,
            Err(err) => {
                self.trace_error("claim_retry_due", err.to_string());
                return;
            }
        };
        if retry_claim.records.len() != 1 || retry_claim.records[0].id != retry_record_id {
            self.trace_error(
                "claim_retry_due",
                format!(
                    "expected retry delivery {}, found {:?}",
                    retry_record_id.0, retry_claim.records
                ),
            );
            return;
        }
        self.retry_claim_count += 1;

        self.trace_operation("start", "mark_failed");
        let mut failed_record = retry_claim.records[0].clone();
        failed_record.status = DeliveryStatus::Failed;
        failed_record.updated_at = now + 12;
        failed_record.lease_owner = None;
        failed_record.lease_expires_at = None;
        failed_record.last_error = Some("fdb-chaos synthetic failure".to_string());
        if let Err(err) = provider.update_delivery_record(failed_record).await {
            self.trace_error("mark_failed", err.to_string());
            return;
        }
        self.failed_count += 1;

        self.trace_operation("start", "claim_terminal_failed");
        let failed_terminal = match provider
            .claim_delivery_records(ClaimDeliveryRecordsRequest {
                owner: "fdb-chaos-pubsub-worker-failed-terminal".to_string(),
                now: now + 13,
                lease_expires_at: now + 43,
                limit: 10,
            })
            .await
        {
            Ok(claim) => claim,
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
        self.message_id = Some(publish.message_id);
        self.delivery_record_ids = delivery_record_ids;
    }

    async fn check(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if self.client_id != 0 {
            return;
        }
        let delivery_record_ids = self.delivery_record_ids.clone();
        if delivery_record_ids.len() != 2 {
            self.trace_error(
                "check",
                format!(
                    "expected two delivery record ids, found {}",
                    delivery_record_ids.len()
                ),
            );
            return;
        }
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("check", err);
                return;
            }
        };

        self.trace_operation("check", "delivery_record_terminal");
        for (record_id, expected_status) in [
            (&delivery_record_ids[0], DeliveryStatus::Delivered),
            (&delivery_record_ids[1], DeliveryStatus::Failed),
        ] {
            match provider.get_delivery_record(record_id).await {
                Ok(Some(record)) if record.status == expected_status => {
                    self.orphan_check_count += 1;
                }
                Ok(Some(record)) => {
                    self.trace_error(
                        "delivery_record_terminal",
                        format!(
                            "expected terminal record {} status {:?}, found {:?}",
                            record_id.0, expected_status, record.status
                        ),
                    );
                    return;
                }
                Ok(None) => {
                    self.trace_error(
                        "delivery_record_terminal",
                        format!("delivery record {} is missing", record_id.0),
                    );
                    return;
                }
                Err(err) => {
                    self.trace_error("delivery_record_terminal", err.to_string());
                    return;
                }
            }
        }

        self.trace_operation("check", "direct_delivery_record_scan");
        let direct_records = match self.direct_delivery_records(db).await {
            Ok(records) => records,
            Err(err) => {
                self.trace_error("direct_delivery_record_scan", err);
                return;
            }
        };
        self.direct_scan_count += direct_records.len() as u64;
        for (record_id, expected_status) in [
            (&delivery_record_ids[0], DeliveryStatus::Delivered),
            (&delivery_record_ids[1], DeliveryStatus::Failed),
        ] {
            match direct_records.iter().find(|record| record.id == *record_id) {
                Some(record) if record.status == expected_status => {}
                Some(record) => {
                    self.trace_error(
                        "direct_delivery_record_scan",
                        format!(
                            "direct scan expected record {} status {:?}, found {:?}",
                            record_id.0, expected_status, record.status
                        ),
                    );
                    return;
                }
                None => {
                    self.trace_error(
                        "direct_delivery_record_scan",
                        format!("direct scan did not find delivery record {}", record_id.0),
                    );
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
