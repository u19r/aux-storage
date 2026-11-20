use std::collections::BTreeMap;

use crate::{
    common::*,
    constants::*,
    imports::*,
    queue_claim::{NodeLog, receive_count_of, run_node, simulation_queue_manager},
    queue_claim_audit::{
        ClaimEvidence, ClaimSeen, ReleaseKind, ReleaseOutcome, ReleaseSeen, audit_claims,
    },
    queue_dead_letter_audit::{TargetSeen, audit_dead_letter},
};

const QUEUE_DLQ_PREFIX: &[u8] = b"aux-storage/fdb-chaos/queue-dead-letter/";
const QUEUE_DLQ_SOURCE_NAME: &str = "FdbChaosDeadLetterSource";
const QUEUE_DLQ_TARGET_NAME: &str = "FdbChaosDeadLetterTarget";

pub(crate) struct QueueDeadLetterWorkload {
    name: String,
    profile: String,
    operation_count: u64,
    node_count: usize,
    message_count: u64,
    artifact_root: String,
    client_id: i32,
    evidence: ClaimEvidence,
    /// Body to message id, from every source claim.
    source_ids: BTreeMap<String, String>,
    target: Vec<TargetSeen>,
    incomplete: bool,
    start_claim_count: u64,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    sent_count: u64,
    unknown_send_count: u64,
    source_claim_count: u64,
    unknown_receive_count: u64,
    release_count: u64,
    abandon_count: u64,
    push_poll_count: u64,
    moved_count: u64,
    error_count: u64,
    incomplete_count: u64,
    context: std::rc::Rc<WorkloadContext>,
    clock: Option<Arc<crate::simulation_clock::SimulationClock>>,
}

impl QueueDeadLetterWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 12_u64).max(1);
        let node_count =
            option_or_default(&context, OPTION_NODE_COUNT, 6_i64).clamp(5, 12) as usize;
        let message_count = option_or_default(&context, OPTION_MESSAGE_COUNT, 12_u64).max(1);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let _: i32 = option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        Self {
            name,
            profile,
            operation_count,
            node_count,
            message_count,
            artifact_root,
            client_id,
            evidence: ClaimEvidence::default(),
            source_ids: BTreeMap::new(),
            target: Vec::new(),
            incomplete: false,
            start_claim_count: 0,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            sent_count: 0,
            unknown_send_count: 0,
            source_claim_count: 0,
            unknown_receive_count: 0,
            release_count: 0,
            abandon_count: 0,
            push_poll_count: 0,
            moved_count: 0,
            error_count: 0,
            incomplete_count: 0,
            context: std::rc::Rc::new(context),
            clock: None,
        }
    }

    async fn node(&self, db: &SimDatabase) -> Result<QueueManager, String> {
        simulation_queue_manager(db, QUEUE_DLQ_PREFIX, self.clock.as_ref()).await
    }

    async fn queue_url(&self, manager: &QueueManager, queue_name: &str) -> Result<String, String> {
        crate::common::queue_url_retrying(self.clock.as_ref(), manager, queue_name).await
    }

    fn is_active_client(&self) -> bool {
        self.client_id == 0
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosQueueDeadLetterPhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "NodeCount" => self.node_count as u64,
                "OperationCount" => self.operation_count,
                "MessageCount" => self.message_count,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosQueueDeadLetterError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Error" => error,
            ],
        );
    }

    /// Unknown outcomes left the rest of the work unprovable. Not an error:
    /// the store answered correctly.
    fn trace_incomplete(&mut self, phase: &'static str, reason: String) {
        self.incomplete_count += 1;
        self.incomplete = true;
        self.context.trace(
            Severity::Warn,
            "AuxStorageFdbChaosQueueDeadLetterIncomplete",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Reason" => reason,
            ],
        );
    }

    async fn pause(&self, millis: u64) -> Result<(), String> {
        let clock = self
            .clock
            .as_ref()
            .ok_or("simulation phase clock is absent")?;
        storage_types::Clock::sleep(clock.as_ref(), std::time::Duration::from_millis(millis))
            .await
            .map_err(|error| storage_error_detail(&error))
    }

    /// Record the message id of a source claim. One body with two ids is a
    /// broken invariant.
    fn note_source_id(&mut self, body: &str, message_id: &str) {
        match self.source_ids.get(body) {
            Some(known) if known != message_id => {
                let detail =
                    format!("D2 two source identities: body={body} ids {known} and {message_id}");
                self.trace_error("audit", detail);
            }
            Some(_) => {}
            None => {
                self.source_ids
                    .insert(body.to_owned(), message_id.to_owned());
            }
        }
    }

    fn merge(&mut self, log: NodeLog) {
        let claims = log.claims.len() as u64;
        self.start_claim_count += claims;
        self.source_claim_count += claims;
        self.evidence.claims.extend(log.claims);
        self.evidence.releases.extend(log.releases);
        self.unknown_receive_count += log.unknown_receive;
        self.release_count += log.release;
        self.abandon_count += log.abandon;
        for (body, message_id) in log.message_ids {
            self.note_source_id(&body, &message_id);
        }
        for failure in log.failures {
            self.trace_error("node", failure);
        }
    }

    async fn setup_phase(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if !self.is_active_client() {
            return;
        }
        let manager = match self.node(&db).await {
            Ok(manager) => manager,
            Err(error) => {
                self.trace_error("setup", error);
                return;
            }
        };
        let clock = match self.clock.as_ref() {
            Some(clock) => Arc::clone(clock),
            None => {
                self.trace_error("setup", "simulation phase clock is absent".to_owned());
                return;
            }
        };
        let result = retry_idempotent_queue_call(clock.as_ref(), || {
            manager.create_queue(CreateQueueRequest {
                queue_name: QUEUE_DLQ_TARGET_NAME.to_owned(),
                attributes: Some(HashMap::new()),
                ..Default::default()
            })
        })
        .await;
        match QueueCall::from(result) {
            QueueCall::Done(_) => {}
            QueueCall::Unknown | QueueCall::Aborted => {
                self.trace_incomplete(
                    "create_target",
                    "create outcome unknown after retries".to_owned(),
                );
                return;
            }
            QueueCall::Failed(error) => {
                self.trace_error("create_target", error.to_string());
                return;
            }
        }
        let target_url = match self.queue_url(&manager, QUEUE_DLQ_TARGET_NAME).await {
            Ok(url) => url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        let arn = match manager
            .get_queue_attributes(queue::GetQueueAttributesRequest {
                queue_url: target_url,
                attribute_names: Some(vec!["QueueArn".to_owned()]),
            })
            .await
        {
            Ok(response) => match response.attributes.get("QueueArn") {
                Some(arn) => arn.clone(),
                None => {
                    self.trace_error("target_arn", "QueueArn is missing".to_owned());
                    return;
                }
            },
            Err(error) => {
                self.trace_error("target_arn", error.to_string());
                return;
            }
        };
        let policy = format!(
            r#"{{"maxReceiveCount":"{QUEUE_DLQ_MAX_RECEIVE_COUNT}","deadLetterTargetArn":"{arn}"}}"#
        );
        let result = retry_idempotent_queue_call(clock.as_ref(), || {
            manager.create_queue(CreateQueueRequest {
                queue_name: QUEUE_DLQ_SOURCE_NAME.to_owned(),
                attributes: Some(HashMap::from([(
                    "RedrivePolicy".to_owned(),
                    policy.clone(),
                )])),
                ..Default::default()
            })
        })
        .await;
        match QueueCall::from(result) {
            QueueCall::Done(_) => {}
            QueueCall::Unknown | QueueCall::Aborted => {
                self.trace_incomplete(
                    "create_source",
                    "create outcome unknown after retries".to_owned(),
                );
                return;
            }
            QueueCall::Failed(error) => {
                self.trace_error("create_source", error.to_string());
                return;
            }
        }
        let source_url = match self.queue_url(&manager, QUEUE_DLQ_SOURCE_NAME).await {
            Ok(url) => url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        for sequence in 0..self.message_count {
            let body = format!("queue-dead-letter-{sequence}");
            let mut resolved = false;
            for _ in 0..=QUEUE_MAX_AMBIGUOUS_SENDS {
                let result = manager
                    .send_message(SendMessageRequest {
                        queue_url: source_url.clone(),
                        message_body: body.clone(),
                        delay_seconds: Some(0),
                        message_attributes: None,
                        ..Default::default()
                    })
                    .await;
                match QueueCall::from(result) {
                    QueueCall::Done(_) => {
                        self.sent_count += 1;
                        self.evidence.acked_bodies.insert(body.clone());
                        resolved = true;
                        break;
                    }
                    QueueCall::Unknown => {
                        self.unknown_send_count += 1;
                        self.evidence.unknown_bodies.insert(body.clone());
                        resolved = true;
                        break;
                    }
                    QueueCall::Aborted => {}
                    QueueCall::Failed(error) => {
                        self.trace_error("send_message", error.to_string());
                        return;
                    }
                }
            }
            if !resolved {
                self.trace_error("send_message", "send aborted past cap".to_owned());
                return;
            }
        }
    }

    async fn start_phase(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
        if !self.is_active_client() || self.incomplete {
            return;
        }
        let mut managers = Vec::with_capacity(self.node_count);
        for _ in 0..self.node_count {
            match self.node(&db).await {
                Ok(manager) => managers.push(manager),
                Err(error) => {
                    self.trace_error("start", error);
                    return;
                }
            }
        }
        let source_url = match self.queue_url(&managers[0], QUEUE_DLQ_SOURCE_NAME).await {
            Ok(url) => url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        let clock = Arc::clone(self.clock.as_ref().expect("phase clock"));
        let operations = self.operation_count;
        let logs =
            futures::future::join_all(managers.iter().enumerate().map(|(index, manager)| {
                run_node(
                    index as u16,
                    manager,
                    &source_url,
                    clock.as_ref(),
                    operations,
                    QUEUE_DLQ_VISIBILITY_SECONDS,
                    false,
                )
            }))
            .await;
        for log in logs {
            self.merge(log);
        }
    }

    async fn check_phase(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if !self.is_active_client() || self.incomplete {
            return;
        }
        let manager = match self.node(&db).await {
            Ok(manager) => manager,
            Err(error) => {
                self.trace_error("check", error);
                return;
            }
        };
        let source_url = match self.queue_url(&manager, QUEUE_DLQ_SOURCE_NAME).await {
            Ok(url) => url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        let target_url = match self.queue_url(&manager, QUEUE_DLQ_TARGET_NAME).await {
            Ok(url) => url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        let settle_millis = u64::from(QUEUE_DLQ_VISIBILITY_SECONDS) * 1_000
            + queue::DEFAULT_MAX_CLOCK_SKEW_MILLIS as u64
            + 1_000;
        if let Err(error) = self.pause(settle_millis).await {
            self.trace_error("check", error);
            return;
        }
        self.push_source(&manager, &source_url).await;
        if self.error_count > 0 {
            return;
        }
        self.drain_target(&manager, &target_url).await;
        if self.error_count > 0 {
            return;
        }
        self.audit();
    }

    /// Receive the source with visibility 0 until two empty polls in a row, at
    /// most `QUEUE_DLQ_PUSH_POLLS` polls. A receive at the maximum count moves
    /// the message instead of delivering it, so the source ends empty.
    async fn push_source(&mut self, manager: &QueueManager, queue_url: &str) {
        let Some(clock) = self.clock.as_ref().map(Arc::clone) else {
            self.trace_error("push", "simulation phase clock is absent".to_owned());
            return;
        };
        let mut empty_in_a_row = 0_u32;
        let mut unknown_seen = false;
        for _ in 0..QUEUE_DLQ_PUSH_POLLS {
            self.push_poll_count += 1;
            let called_at = storage_types::Clock::now(clock.as_ref()).timestamp_millis();
            let result = manager
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.to_owned(),
                    max_number_of_messages: Some(10),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                    attribute_names: Some(vec!["ApproximateReceiveCount".to_owned()]),
                    message_attribute_names: None,
                })
                .await;
            let returned_at = storage_types::Clock::now(clock.as_ref()).timestamp_millis();
            let messages = match QueueCall::from(result) {
                QueueCall::Done(response) => response.messages,
                QueueCall::Unknown | QueueCall::Aborted => {
                    unknown_seen = true;
                    self.unknown_receive_count += 1;
                    empty_in_a_row = 0;
                    continue;
                }
                QueueCall::Failed(error) => {
                    self.trace_error("push", format!("receive: {error}"));
                    return;
                }
            };
            if messages.is_empty() {
                empty_in_a_row += 1;
                if empty_in_a_row >= 2 {
                    return;
                }
                if let Err(error) = self.pause(QUEUE_CLAIM_EMPTY_PAUSE_MILLIS).await {
                    self.trace_error("push", error);
                    return;
                }
                continue;
            }
            empty_in_a_row = 0;
            for message in messages {
                let Some(receive_count) = receive_count_of(&message) else {
                    self.trace_error("push", "ApproximateReceiveCount missing".to_owned());
                    return;
                };
                self.source_claim_count += 1;
                self.note_source_id(&message.body, &message.message_id);
                self.evidence.claims.push(ClaimSeen {
                    body: message.body.clone(),
                    receive_count,
                    node: u16::MAX,
                    called_at,
                    returned_at,
                });
                // Visibility 0 releases the claim at once.
                self.evidence.releases.push(ReleaseSeen {
                    body: message.body.clone(),
                    receive_count,
                    kind: ReleaseKind::Visibility,
                    outcome: ReleaseOutcome::Acked,
                    called_at,
                    returned_at,
                });
            }
        }
        if unknown_seen {
            self.trace_incomplete("push", "source not empty after unknown receives".to_owned());
        } else {
            self.trace_error(
                "push",
                format!("source not empty after {QUEUE_DLQ_PUSH_POLLS} polls"),
            );
        }
    }

    /// Receive and delete the target until two empty polls in a row, at most
    /// `QUEUE_COLLECT_POLLS * 4` polls.
    async fn drain_target(&mut self, manager: &QueueManager, queue_url: &str) {
        let Some(clock) = self.clock.as_ref().map(Arc::clone) else {
            self.trace_error("drain", "simulation phase clock is absent".to_owned());
            return;
        };
        let mut empty_in_a_row = 0_u32;
        for _ in 0..QUEUE_COLLECT_POLLS * 4 {
            let result = manager
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.to_owned(),
                    max_number_of_messages: Some(10),
                    visibility_timeout: Some(30),
                    wait_time_seconds: Some(0),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await;
            let messages = match QueueCall::from(result) {
                QueueCall::Done(response) => response.messages,
                QueueCall::Unknown | QueueCall::Aborted => {
                    self.unknown_receive_count += 1;
                    continue;
                }
                QueueCall::Failed(error) => {
                    self.trace_error("drain", format!("receive: {error}"));
                    return;
                }
            };
            if messages.is_empty() {
                empty_in_a_row += 1;
                if empty_in_a_row >= 2 {
                    return;
                }
                if let Err(error) = self.pause(QUEUE_CLAIM_EMPTY_PAUSE_MILLIS).await {
                    self.trace_error("drain", error);
                    return;
                }
                continue;
            }
            empty_in_a_row = 0;
            for message in messages {
                let receipt = message.receipt_handle.as_str();
                let result = retry_idempotent_queue_call(clock.as_ref(), || {
                    manager.delete_message(DeleteMessageRequest {
                        queue_url: queue_url.to_owned(),
                        receipt_handle: ReceiptHandle::from(receipt),
                    })
                })
                .await
                .map(|_| ());
                let delete_acked = match QueueCall::from(result) {
                    QueueCall::Done(()) => true,
                    QueueCall::Unknown | QueueCall::Aborted => {
                        self.trace_incomplete("drain", "drain delete outcome unknown".to_owned());
                        false
                    }
                    QueueCall::Failed(error) => {
                        self.trace_error("drain", format!("delete: {error}"));
                        return;
                    }
                };
                self.target.push(TargetSeen {
                    body: message.body,
                    message_id: message.message_id,
                    delete_acked,
                });
            }
        }
    }

    fn audit(&mut self) {
        match audit_dead_letter(
            &self.evidence.claims,
            &self.source_ids,
            &self.target,
            QUEUE_DLQ_MAX_RECEIVE_COUNT,
        ) {
            Ok(moved) => self.moved_count = moved,
            Err(error) => self.trace_error("audit", error),
        }
        self.evidence.drained = self.target.iter().map(|seen| seen.body.clone()).collect();
        if let Err(error) = audit_claims(
            &self.evidence,
            i64::from(QUEUE_DLQ_VISIBILITY_SECONDS) * 1_000,
            queue::DEFAULT_MAX_CLOCK_SKEW_MILLIS,
        ) {
            if self.incomplete && error.starts_with("C4") {
                self.trace_incomplete("audit", error);
            } else {
                self.trace_error("audit", error);
            }
        }
        // As in the concurrent-claim audit: an unknown send that did not
        // commit leaves fewer messages than planned, so only acknowledged
        // sends must be claimed, and an incomplete run is not judged.
        let acked = self.evidence.acked_bodies.len().max(1) as u64;
        if !self.incomplete && (self.target.is_empty() || self.start_claim_count < acked) {
            self.trace_error(
                "audit",
                format!(
                    "vacuous run: {} target messages, {} start claims for {} messages",
                    self.target.len(),
                    self.start_claim_count,
                    self.message_count
                ),
            );
        }
    }

    async fn run_simulation_phase(
        &mut self,
        db: SimDatabase,
        phase: crate::simulation_clock::SimulationPhase,
    ) {
        let context = std::rc::Rc::clone(&self.context);
        let (clock, requests) = crate::simulation_clock::SimulationClock::new();
        self.clock = Some(clock);
        let operation = async {
            match phase {
                crate::simulation_clock::SimulationPhase::Setup => self.setup_phase(db).await,
                crate::simulation_clock::SimulationPhase::Start => self.start_phase(db).await,
                crate::simulation_clock::SimulationPhase::Check => self.check_phase(db).await,
            }
        };
        crate::simulation_clock::drive_operation(context, requests, operation).await;
        self.clock = None;
    }
}

impl RustWorkload for QueueDeadLetterWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.run_simulation_phase(db, crate::simulation_clock::SimulationPhase::Setup)
            .await;
    }

    async fn start(&mut self, db: SimDatabase) {
        self.run_simulation_phase(db, crate::simulation_clock::SimulationPhase::Start)
            .await;
    }

    async fn check(&mut self, db: SimDatabase) {
        self.run_simulation_phase(db, crate::simulation_clock::SimulationPhase::Check)
            .await;
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64(
                "aux_storage_queue_dead_letter_node_count",
                self.node_count as u64,
            ),
            metric_val_u64("aux_storage_queue_dead_letter_sent", self.sent_count),
            metric_val_u64(
                "aux_storage_queue_dead_letter_unknown_send",
                self.unknown_send_count,
            ),
            metric_val_u64(
                "aux_storage_queue_dead_letter_source_claim",
                self.source_claim_count,
            ),
            metric_val_u64(
                "aux_storage_queue_dead_letter_unknown_receive",
                self.unknown_receive_count,
            ),
            metric_val_u64("aux_storage_queue_dead_letter_release", self.release_count),
            metric_val_u64("aux_storage_queue_dead_letter_abandon", self.abandon_count),
            metric_val_u64(
                "aux_storage_queue_dead_letter_push_poll",
                self.push_poll_count,
            ),
            metric_val_u64("aux_storage_queue_dead_letter_moved", self.moved_count),
            metric_val_u64("aux_storage_queue_dead_letter_error", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        120.0
    }
}
