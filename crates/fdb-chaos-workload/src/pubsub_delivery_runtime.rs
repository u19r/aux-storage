use fdb_chaos_model::PublishAuditPhase;

use crate::{
    constants::{QUEUE_IDEMPOTENT_CALL_ATTEMPTS, QUEUE_REDELIVERY_POLL_MILLIS},
    imports::*,
    pubsub_delivery::*,
};

/// Far beyond `MIN_DEFERRAL` and the wall time of one simulation run.
/// Past the longest first back-off (1 s for SQS).
const RETRY_WAIT_MS: i64 = 60_000;

impl PubsubDeliveryWorkload {
    pub(super) async fn setup_phase(&mut self, db: SimDatabase) {
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

        // The delivered subscription writes into a real queue on the same
        // store, so the check phase audits copies, not the absence of records.
        self.trace_operation("setup", "create_queue");
        if let Err(err) = QueueManager::new(provider.clone())
            .create_queue(CreateQueueRequest {
                queue_name: self.queue_name(0),
                attributes: Some(HashMap::new()),
                ..Default::default()
            })
            .await
        {
            self.trace_error("create_queue", err.to_string());
            return;
        }

        self.trace_operation("setup", "create_topic");
        let topic = match TopicName::new(self.topic_name()).and_then(|name| {
            Topic::try_new(
                CreateTopicRequest {
                    name,
                    attributes: HashMap::new(),
                    tags: std::collections::BTreeMap::new(),
                },
                &storage_types::MessagingIdentity::default(),
            )
        }) {
            Ok(topic) => topic,
            Err(err) => {
                self.trace_error("create_topic", err.to_string());
                return;
            }
        };
        let topic = match provider.create_topic(topic).await {
            Ok(topic) => topic,
            Err(err) => {
                self.trace_error("create_topic", err.to_string());
                return;
            }
        };

        let mut subscription_arns = Vec::new();
        for subscription_index in 0..2 {
            self.trace_operation("setup", "create_subscription");
            let request = SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: self.endpoint_for(subscription_index),
                attributes: HashMap::new(),
                extra_json: serde_json::Value::Null,
            };
            let subscription = match Subscription::try_new(
                request,
                &storage_types::MessagingIdentity::default(),
            ) {
                Ok(subscription) => provider.create_subscription(subscription).await,
                Err(err) => Err(err),
            };
            let subscription = match subscription {
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

    pub(super) async fn start_phase(&mut self, db: SimDatabase) {
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

        if let Err(error) = self
            .deliver_and_fail(&provider, db.clone(), &materialized)
            .await
        {
            self.trace_error("deliver", error);
            return;
        }

        self.topic_arn = Some(topic_arn);
        self.subscription_arns = subscription_arns;
        self.delivery_record_ids = delivery_record_ids;
    }

    pub(super) async fn check_phase(&mut self, db: SimDatabase) {
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
        // Terminal records are deleted (W3 design 1.5). The audit compares
        // the expected effects of every publish attempt with a direct scan of
        // the durable store (no intent or delivery record remains) and with
        // the copies in the delivered subscription's queue.
        for id in self.delivery_record_ids.clone() {
            match provider.get_delivery(&id).await {
                Ok(None) => self.orphan_check_count += 1,
                Ok(Some(_)) => {
                    self.trace_error("check", "a settled delivery record remains".into());
                    return;
                }
                Err(error) => {
                    self.trace_error("check", error.to_string());
                    return;
                }
            }
        }
        let copies = match self.queue_copies(provider.clone()).await {
            Ok(copies) => copies,
            Err(error) => {
                self.trace_error("check", error);
                return;
            }
        };
        if let Err(error) = self.audit_terminal_effects(db, &copies).await {
            self.trace_error("terminal_effects_audit", error);
        }
    }

    /// Two destinations (W3S plan 6): the first is delivered to its queue;
    /// the second always fails, first transiently (one small state write,
    /// no payload write), then permanently with no dead-letter queue.
    async fn deliver_and_fail(
        &mut self,
        provider: &Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>,
        db: SimDatabase,
        materialized: &[DeliveryPayload],
    ) -> Result<(), String> {
        self.trace_operation("start", "claim_delivery");
        let mut destinations: Vec<DestinationKey> = materialized
            .iter()
            .map(|payload| payload.destination.clone())
            .collect();
        destinations.sort();
        destinations.dedup();
        let mut now = TimestampMillis::now();
        let mut claimed = Vec::new();
        for destination in &destinations {
            let (records, at) = self
                .claim_for_audit(provider, "fdb-chaos-pubsub-worker", destination, now)
                .await?;
            now = at;
            claimed.extend(records);
        }
        let mut ids: Vec<DeliveryRecordId> = claimed
            .iter()
            .map(|record| record.state.id.clone())
            .collect();
        ids.sort_by(|left, right| left.0.cmp(&right.0));
        if ids != self.delivery_record_ids {
            return Err(format!(
                "claimed {ids:?}, expected {:?}",
                self.delivery_record_ids
            ));
        }
        self.claim_count += claimed.len() as u64;
        for destination in &destinations {
            let (duplicate, _) = self
                .claim_for_audit(
                    provider,
                    "fdb-chaos-pubsub-worker-duplicate",
                    destination,
                    now + 1,
                )
                .await?;
            if !duplicate.is_empty() {
                return Err("a duplicate claim took leased records".into());
            }
        }
        self.duplicate_claim_reject_count += 1;
        let mut failing = None;
        for record in claimed {
            if self.expects_delivery(&record.state.subscription_arn) {
                self.deliver_locally(provider.as_ref(), &record).await?;
                self.delivered_count += 1;
                continue;
            }
            failing = Some(record.state.destination.clone());
            let failure = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpConnect);
            self.settle(provider.as_ref(), &record, failure, SettleOutcome::Retried)
                .await?;
            self.retry_reschedule_count += 1;
        }
        let failing = failing.ok_or("no failing destination")?;
        // R1: the retry wrote the state only; every payload is unchanged.
        // The audit read has no retry of its own; a simulated fault can
        // reject it, so it is tried again.
        let mut snapshot =
            super::pubsub_snapshot::read_pubsub_snapshot(self.driver(db.clone())?).await;
        for _ in 0..5 {
            if snapshot.is_ok() {
                break;
            }
            snapshot = super::pubsub_snapshot::read_pubsub_snapshot(self.driver(db.clone())?).await;
        }
        let (_, payloads) = snapshot?;
        for payload in payloads {
            if !materialized.contains(&payload) {
                return Err("a payload changed after materialization".into());
            }
        }
        let (early, _) = self
            .claim_for_audit(
                provider,
                "fdb-chaos-pubsub-worker-terminal",
                &failing,
                now + 4,
            )
            .await?;
        if !early.is_empty() {
            return Err("a retry was claimed before its back-off".into());
        }
        self.terminal_duplicate_reject_count += 1;
        let later = now + RETRY_WAIT_MS;
        let (retried, at) = self
            .claim_for_audit(provider, "fdb-chaos-pubsub-worker-retry", &failing, later)
            .await?;
        self.retry_claim_count += retried.len() as u64;
        for record in retried {
            let failure = DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400));
            let expected = SettleOutcome::Dropped(pubsub::DropReason::PermanentNoDeadLetter);
            self.settle(provider.as_ref(), &record, failure, expected)
                .await?;
            self.failed_count += 1;
        }
        for destination in &destinations {
            let (left, _) = self
                .claim_for_audit(
                    provider,
                    "fdb-chaos-pubsub-worker-final",
                    destination,
                    at + 13,
                )
                .await?;
            if !left.is_empty() {
                return Err("a terminal delivery was claimed again".into());
            }
        }
        self.terminal_duplicate_reject_count += 1;
        Ok(())
    }

    /// Settle one synthetic outcome and check what the commit did. An
    /// unknown commit is settled again through the fence: `ClaimLost` then
    /// means the first commit applied.
    async fn settle(
        &self,
        provider: &impl PubsubProvider,
        record: &ClaimedDelivery,
        result: DeliveryAttemptResult,
        expected: SettleOutcome,
    ) -> Result<(), String> {
        let input = SettleInput {
            claimed: record.state.clone(),
            fence: record.fence.clone(),
            message_id: record.payload.message_id.clone(),
            result,
            dead_letter: None,
            jitter: 0,
            identity: storage_types::MessagingIdentity::default(),
        };
        let mut attempt = provider.settle_delivery(input.clone()).await;
        for _ in 0..5 {
            match &attempt {
                Err(error) if super::pubsub_delivery_audit::is_retryable(error) => {
                    attempt = provider.settle_delivery(input.clone()).await;
                }
                _ => break,
            }
        }
        let report = attempt.map_err(|error| error.to_string())?;
        if report.outcome != expected && report.outcome != SettleOutcome::ClaimLost {
            return Err(format!(
                "settle gave {:?}, expected {expected:?}",
                report.outcome
            ));
        }
        Ok(())
    }

    /// Enqueue the SNS message ID as the body and delete the record in one
    /// attempt. An unknown commit is resolved by reading the record: gone
    /// means committed; present (same claim) means the attempt is repeated
    /// under the same fence.
    async fn deliver_locally(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
        record: &ClaimedDelivery,
    ) -> Result<(), String> {
        let mut record = record.clone();
        let record = &mut record;
        let identity = storage_types::MessagingIdentity::default();
        let endpoint = record
            .payload
            .subscription
            .as_ref()
            .map(|subscription| subscription.endpoint.clone())
            .ok_or("delivery record has no subscription snapshot")?;
        let queue =
            queue::QueueArn::parse(&endpoint, &identity).map_err(|error| error.to_string())?;
        for _ in 0..6 {
            let delivery = LocalDelivery {
                queue: queue.clone(),
                items: vec![LocalDeliveryItem {
                    claimed: record.state.clone(),
                    fence: record.fence.clone(),
                    message_id: record.payload.message_id.clone(),
                    message: RenderedQueueMessage {
                        body: record.payload.message_id.to_string(),
                        attributes: None,
                    },
                }],
                jitter: 0,
                identity: storage_types::MessagingIdentity::default(),
            };
            let outcome = provider
                .deliver_to_local_queue(delivery)
                .await
                .map(|outcome| outcome.settled().map(|mut reports| reports.remove(0)));
            // An unknown commit is settled as transient through the fence
            // (review C5): `ClaimLost` means the first commit applied (the
            // record is gone) or another claim holds it; `Retried` means it
            // did not apply, so the record is claimed again (finding 5).
            match outcome {
                Ok(Some(report)) if report.outcome == SettleOutcome::Delivered => return Ok(()),
                Ok(Some(report)) if report.outcome == SettleOutcome::ClaimLost => {
                    return match provider.get_delivery(&record.state.id).await {
                        Ok(None) => Ok(()),
                        Ok(Some(_)) => Err("local delivery lost its claim to another".into()),
                        Err(error) => Err(error.to_string()),
                    };
                }
                Ok(Some(report)) if report.outcome == SettleOutcome::Retried => {
                    self.unknown_delivery_count += 1;
                    *record = self.claim_again(provider, record).await?;
                }
                Ok(other) => {
                    return Err(format!("local delivery gave {other:?}"));
                }
                Err(error) if super::pubsub_delivery_audit::is_retryable(&error) => {
                    self.unknown_delivery_count += 1;
                    match provider.get_delivery(&record.state.id).await {
                        Ok(None) => return Ok(()),
                        Ok(Some(_)) => {}
                        Err(error) => return Err(error.to_string()),
                    }
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("local delivery stayed unknown six times".into())
    }

    /// Claim one record again after a `Retried` settle: the claim time is
    /// past the longest first back-off (1 s for SQS).
    async fn claim_again(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
        record: &ClaimedDelivery,
    ) -> Result<ClaimedDelivery, String> {
        let now = TimestampMillis::now() + RETRY_WAIT_MS;
        let (claimed, _) = self
            .claim_for_audit(
                provider,
                "fdb-chaos-pubsub-worker-again",
                &record.state.destination,
                now,
            )
            .await?;
        claimed
            .into_iter()
            .find(|claimed| claimed.state.id == record.state.id)
            .ok_or_else(|| "a retried local delivery was not claimed again".to_string())
    }

    /// Receive every message of the delivered subscription's queue; each
    /// body is the SNS message ID of one copy. The audit claims through the
    /// store, not the `QueueManager`: the manager's short poll opens a claim
    /// only on probe evidence younger than its probe interval, and a
    /// simulated probe can take longer than that.
    async fn queue_copies(
        &mut self,
        provider: Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>,
    ) -> Result<Vec<fdb_chaos_model::QueueCopyAudit>, String> {
        use queue::QueueOperations as _;

        let delivered = self
            .subscription_arns
            .iter()
            .find(|arn| self.expects_delivery(arn))
            .cloned()
            .ok_or("no delivered subscription")?;
        let clock = Arc::clone(
            self.clock
                .as_ref()
                .ok_or("simulation phase clock is absent")?,
        );
        let queue_name = self.queue_name(0);
        let queue_url = crate::common::retry_idempotent_queue_call(clock.as_ref(), || {
            provider.get_queue_url_by_name(&queue_name)
        })
        .await
        .map_err(|error| queue_error_detail(&error))?
        .ok_or("delivered subscription queue is missing")?;
        // A receive with an unknown commit outcome may have claimed copies
        // until their visibility timeout ends; the audit then waits for that
        // and collects every copy again. A conflicted or undrained receive
        // claimed nothing that it did not return.
        let visibility = 300_u32;
        let pause = std::time::Duration::from_millis(u64::from(QUEUE_REDELIVERY_POLL_MILLIS));
        let mut retries = 0_u32;
        let mut copies = Vec::new();
        loop {
            let wait = match provider
                .receive_messages(&queue_url, 10, Some(visibility.into()), None)
                .await
            {
                Ok(received) if received.messages.is_empty() && received.drained => {
                    return Ok(copies);
                }
                Ok(received) => {
                    let empty = received.messages.is_empty();
                    copies.extend(received.messages.into_iter().map(|message| {
                        fdb_chaos_model::QueueCopyAudit {
                            message_id: message.body,
                            subscription_arn: delivered.to_string(),
                        }
                    }));
                    if !empty {
                        continue;
                    }
                    pause
                }
                Err(error) => match crate::common::queue_retry_kind(&error) {
                    Some(crate::common::QueueRetry::Conflict) => pause,
                    Some(crate::common::QueueRetry::Ambiguous) => {
                        copies.clear();
                        std::time::Duration::from_secs(u64::from(visibility) + 1)
                    }
                    None => return Err(queue_error_detail(&error)),
                },
            };
            retries += 1;
            if retries > QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
                return Err("the delivered queue did not drain".into());
            }
            storage_types::Clock::sleep(clock.as_ref(), wait)
                .await
                .map_err(|error| crate::common::storage_error_detail(&error))?;
        }
    }

    async fn audit_terminal_effects(
        &mut self,
        db: SimDatabase,
        copies: &[fdb_chaos_model::QueueCopyAudit],
    ) -> Result<(), String> {
        let (candidates, records) =
            super::pubsub_snapshot::read_pubsub_snapshot(self.driver(db)?).await?;
        self.direct_scan_count += 1;
        let intents = candidates
            .into_iter()
            .map(|candidate| fdb_chaos_model::PublishIntentAudit {
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
            .map(|record| fdb_chaos_model::PublishDeliveryAudit {
                record_id: record.id.0.clone(),
                message_id: record.message_id.to_string(),
                subscription_arn: record
                    .subscription
                    .as_ref()
                    .map(|subscription| subscription.subscription_arn.to_string())
                    .unwrap_or_default(),
                message_body: record.message_body.clone(),
                terminal: false,
            })
            .collect::<Vec<_>>();
        let delivered = self
            .subscription_arns
            .iter()
            .filter(|arn| self.expects_delivery(arn))
            .map(ToString::to_string)
            .collect();
        fdb_chaos_model::check_terminal_effects(
            &self.publish_attempts,
            &intents,
            &deliveries,
            &delivered,
            copies,
        )?;
        self.terminal_effect_audit_count += 1;
        Ok(())
    }
}

fn queue_error_detail(error: &QueueError) -> String {
    match error {
        QueueError::StorageError(error) => crate::common::storage_error_detail(error),
        error => error.to_string(),
    }
}
