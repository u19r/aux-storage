use crate::{
    common::*,
    constants::*,
    imports::*,
    queue_claim_audit::{
        ClaimEvidence, ClaimSeen, ReleaseKind, ReleaseOutcome, ReleaseSeen, audit_claims,
    },
};

const QUEUE_CLAIM_PREFIX: &[u8] = b"aux-storage/fdb-chaos/queue-claim/";
const QUEUE_CLAIM_QUEUE_NAME: &str = "FdbChaosQueueClaim";

/// What one node saw. Nodes run together, so each fills its own log and the
/// workload merges the logs afterwards.
#[derive(Default)]
pub(crate) struct NodeLog {
    pub(crate) claims: Vec<ClaimSeen>,
    /// (body, message id) next to each claim.
    pub(crate) message_ids: Vec<(String, String)>,
    pub(crate) releases: Vec<ReleaseSeen>,
    pub(crate) unknown_receive: u64,
    pub(crate) aborted_receive: u64,
    pub(crate) release: u64,
    pub(crate) abandon: u64,
    pub(crate) empty_poll: u64,
    pub(crate) failures: Vec<String>,
}

/// One node: its own backend, store, caches and manager on the shared
/// database and prefix.
pub(crate) async fn simulation_queue_manager(
    db: &SimDatabase,
    prefix: &'static [u8],
    clock: Option<&Arc<crate::simulation_clock::SimulationClock>>,
) -> Result<QueueManager, String> {
    let clock = clock.ok_or("simulation phase clock is absent")?;
    // Format initialization reads its key and stages only what is absent, so
    // calling it again after an unknown commit is a fresh observation.
    let mut unknown_detail = String::new();
    for _ in 0..QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
        let driver = kv_backend::foundationdb::FoundationDbBackend::from_simulation(
            Arc::clone(db),
            Some(prefix),
            None,
            storage_types::ReadVersionPolicy::Fresh,
            crate::simulation_clock::phase_clock(Some(clock))?,
        );
        match kv_backend::KvStore::initialize_format_with_clock(
            driver,
            storage_types::InitialStreamLineages::ONE,
            clock.clone(),
        )
        .await
        {
            Ok(store) => {
                let store = store.with_metadata_cache_ttl(SIMULATION_METADATA_CACHE_TTL);
                return Ok(QueueManager::new(Arc::new(store)));
            }
            Err(error) if error.is_commit_unknown() => {
                unknown_detail = storage_error_detail(&error);
            }
            Err(error) => return Err(storage_error_detail(&error)),
        }
    }
    Err(format!(
        "format initialization outcome unknown after {QUEUE_IDEMPOTENT_CALL_ATTEMPTS} attempts: \
         {unknown_detail}"
    ))
}

pub(crate) struct QueueClaimWorkload {
    name: String,
    profile: String,
    operation_count: u64,
    node_count: usize,
    message_count: u64,
    artifact_root: String,
    client_id: i32,
    evidence: ClaimEvidence,
    incomplete: bool,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    sent_count: u64,
    unknown_send_count: u64,
    claim_count: u64,
    unknown_receive_count: u64,
    aborted_receive_count: u64,
    delete_effective_count: u64,
    delete_undetermined_count: u64,
    release_count: u64,
    abandon_count: u64,
    drained_count: u64,
    empty_poll_count: u64,
    error_count: u64,
    incomplete_count: u64,
    context: std::rc::Rc<WorkloadContext>,
    clock: Option<Arc<crate::simulation_clock::SimulationClock>>,
}

impl QueueClaimWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 16_u64).max(1);
        let node_count =
            option_or_default(&context, OPTION_NODE_COUNT, 8_i64).clamp(5, 12) as usize;
        let message_count = option_or_default(&context, OPTION_MESSAGE_COUNT, 24_u64).max(1);
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
            incomplete: false,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            sent_count: 0,
            unknown_send_count: 0,
            claim_count: 0,
            unknown_receive_count: 0,
            aborted_receive_count: 0,
            delete_effective_count: 0,
            delete_undetermined_count: 0,
            release_count: 0,
            abandon_count: 0,
            drained_count: 0,
            empty_poll_count: 0,
            error_count: 0,
            incomplete_count: 0,
            context: std::rc::Rc::new(context),
            clock: None,
        }
    }

    async fn node(&self, db: &SimDatabase) -> Result<QueueManager, String> {
        simulation_queue_manager(db, QUEUE_CLAIM_PREFIX, self.clock.as_ref()).await
    }

    async fn queue_url(&self, manager: &QueueManager) -> Result<String, String> {
        crate::common::queue_url_retrying(self.clock.as_ref(), manager, QUEUE_CLAIM_QUEUE_NAME)
            .await
    }

    fn is_active_client(&self) -> bool {
        self.client_id == 0
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosQueueClaimPhase",
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
            "AuxStorageFdbChaosQueueClaimError",
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
            "AuxStorageFdbChaosQueueClaimIncomplete",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Reason" => reason,
            ],
        );
    }

    fn merge(&mut self, log: NodeLog) {
        self.claim_count += log.claims.len() as u64;
        self.evidence.claims.extend(log.claims);
        self.evidence.releases.extend(log.releases);
        self.unknown_receive_count += log.unknown_receive;
        self.aborted_receive_count += log.aborted_receive;
        self.release_count += log.release;
        self.abandon_count += log.abandon;
        self.empty_poll_count += log.empty_poll;
        for failure in log.failures {
            self.trace_error("node", failure);
        }
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
                queue_name: QUEUE_CLAIM_QUEUE_NAME.to_owned(),
                attributes: Some(HashMap::new()),
                ..Default::default()
            })
        })
        .await;
        match QueueCall::from(result) {
            QueueCall::Done(_) => {}
            QueueCall::Unknown | QueueCall::Aborted => {
                self.trace_incomplete(
                    "create_queue",
                    "create outcome unknown after retries".to_owned(),
                );
                return;
            }
            QueueCall::Failed(error) => {
                self.trace_error("create_queue", error.to_string());
                return;
            }
        }
        let queue_url = match self.queue_url(&manager).await {
            Ok(queue_url) => queue_url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        for sequence in 0..self.message_count {
            let body = format!("queue-claim-{sequence}");
            let mut resolved = false;
            for _ in 0..=QUEUE_MAX_AMBIGUOUS_SENDS {
                let result = manager
                    .send_message(SendMessageRequest {
                        queue_url: queue_url.clone(),
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
        let queue_url = match self.queue_url(&managers[0]).await {
            Ok(queue_url) => queue_url,
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
                    &queue_url,
                    clock.as_ref(),
                    operations,
                    QUEUE_CLAIM_VISIBILITY_SECONDS,
                    true,
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
        let queue_url = match self.queue_url(&manager).await {
            Ok(queue_url) => queue_url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error);
                return;
            }
        };
        let settle_millis = u64::from(QUEUE_CLAIM_VISIBILITY_SECONDS) * 1_000
            + queue::DEFAULT_MAX_CLOCK_SKEW_MILLIS as u64
            + 1_000;
        if let Err(error) = self.pause(settle_millis).await {
            self.trace_error("check", error);
            return;
        }
        self.drain(&manager, &queue_url).await;
        if self.error_count > 0 {
            return;
        }
        match audit_claims(
            &self.evidence,
            i64::from(QUEUE_CLAIM_VISIBILITY_SECONDS) * 1_000,
            queue::DEFAULT_MAX_CLOCK_SKEW_MILLIS,
        ) {
            Ok(counts) => {
                self.delete_effective_count = counts.effective_deletes;
                self.delete_undetermined_count = counts.undetermined_deletes;
            }
            Err(error) if self.incomplete && error.starts_with("C4") => {
                self.trace_incomplete("audit", error);
            }
            Err(error) => self.trace_error("audit", error),
        }
        // An unknown send that did not commit leaves fewer messages than
        // planned; only acknowledged sends must each be claimed for the run to
        // count.
        if !self.incomplete && self.evidence.claims.len() < self.evidence.acked_bodies.len().max(1)
        {
            self.trace_error(
                "audit",
                format!(
                    "vacuous run: {} claims for {} messages",
                    self.evidence.claims.len(),
                    self.message_count
                ),
            );
        }
    }

    /// Receive and delete until two empty polls in a row, at most
    /// `QUEUE_CLAIM_DRAIN_POLLS` polls. Drain receives are claims and enter
    /// the audit; their deletes do not.
    async fn drain(&mut self, manager: &QueueManager, queue_url: &str) {
        let Some(clock) = self.clock.as_ref().map(Arc::clone) else {
            self.trace_error("drain", "simulation phase clock is absent".to_owned());
            return;
        };
        let mut empty_in_a_row = 0_u32;
        for _ in 0..QUEUE_CLAIM_DRAIN_POLLS {
            let called_at = storage_types::Clock::now(clock.as_ref()).timestamp_millis();
            let result = manager
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.to_owned(),
                    max_number_of_messages: Some(10),
                    visibility_timeout: Some(30),
                    wait_time_seconds: Some(0),
                    attribute_names: Some(vec!["ApproximateReceiveCount".to_owned()]),
                    message_attribute_names: None,
                })
                .await;
            let returned_at = storage_types::Clock::now(clock.as_ref()).timestamp_millis();
            let messages = match QueueCall::from(result) {
                QueueCall::Done(response) => response.messages,
                QueueCall::Unknown => {
                    self.unknown_receive_count += 1;
                    continue;
                }
                QueueCall::Aborted => {
                    self.aborted_receive_count += 1;
                    continue;
                }
                QueueCall::Failed(error) => {
                    self.trace_error("drain", format!("receive: {error}"));
                    return;
                }
            };
            if messages.is_empty() {
                self.empty_poll_count += 1;
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
                let Some(receive_count) = receive_count_of(&message) else {
                    self.trace_error("drain", "ApproximateReceiveCount missing".to_owned());
                    return;
                };
                self.claim_count += 1;
                self.evidence.claims.push(ClaimSeen {
                    body: message.body.clone(),
                    receive_count,
                    node: u16::MAX,
                    called_at,
                    returned_at,
                });
                self.evidence.drained.insert(message.body.clone());
                let receipt = message.receipt_handle.as_str();
                let result = retry_idempotent_queue_call(clock.as_ref(), || {
                    manager.delete_message(DeleteMessageRequest {
                        queue_url: queue_url.to_owned(),
                        receipt_handle: ReceiptHandle::from(receipt),
                    })
                })
                .await
                .map(|_| ());
                match QueueCall::from(result) {
                    QueueCall::Done(()) => self.drained_count += 1,
                    QueueCall::Unknown | QueueCall::Aborted => {
                        self.trace_incomplete("drain", "drain delete outcome unknown".to_owned());
                    }
                    QueueCall::Failed(error) => {
                        self.trace_error("drain", format!("delete: {error}"));
                        return;
                    }
                }
            }
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

pub(crate) async fn run_node(
    node: u16,
    manager: &QueueManager,
    queue_url: &str,
    clock: &crate::simulation_clock::SimulationClock,
    operations: u64,
    visibility_seconds: u32,
    deletes: bool,
) -> NodeLog {
    let mut log = NodeLog::default();
    let now = || storage_types::Clock::now(clock).timestamp_millis();
    for _ in 0..operations {
        let called_at = now();
        let result = manager
            .receive_message(ReceiveMessageRequest {
                queue_url: queue_url.to_owned(),
                max_number_of_messages: Some(QUEUE_CLAIM_MAX_MESSAGES),
                visibility_timeout: Some(i64::from(visibility_seconds)),
                wait_time_seconds: Some(0),
                attribute_names: Some(vec!["ApproximateReceiveCount".to_owned()]),
                message_attribute_names: None,
            })
            .await;
        let returned_at = now();
        let messages = match QueueCall::from(result) {
            QueueCall::Done(response) => response.messages,
            QueueCall::Unknown => {
                log.unknown_receive += 1;
                continue;
            }
            QueueCall::Aborted => {
                log.aborted_receive += 1;
                continue;
            }
            QueueCall::Failed(error) => {
                log.failures.push(format!("node {node} receive: {error}"));
                return log;
            }
        };
        if messages.is_empty() {
            log.empty_poll += 1;
            let pause = std::time::Duration::from_millis(QUEUE_CLAIM_EMPTY_PAUSE_MILLIS);
            if let Err(error) = storage_types::Clock::sleep(clock, pause).await {
                log.failures.push(storage_error_detail(&error));
                return log;
            }
            continue;
        }
        for message in messages {
            let Some(receive_count) = receive_count_of(&message) else {
                log.failures
                    .push(format!("node {node}: ApproximateReceiveCount missing"));
                return log;
            };
            log.claims.push(ClaimSeen {
                body: message.body.clone(),
                receive_count,
                node,
                called_at,
                returned_at,
            });
            log.message_ids
                .push((message.body.clone(), message.message_id.clone()));
            let roll = storage_types::ambient::random_u64() % 4;
            match (deletes, roll) {
                (true, 0 | 1) => {
                    release(
                        &mut log,
                        manager,
                        queue_url,
                        clock,
                        &message,
                        receive_count,
                        ReleaseKind::Delete,
                    )
                    .await
                }
                (true, 2) | (false, 0 | 1) => {
                    log.release += 1;
                    release(
                        &mut log,
                        manager,
                        queue_url,
                        clock,
                        &message,
                        receive_count,
                        ReleaseKind::Visibility,
                    )
                    .await
                }
                _ => log.abandon += 1,
            }
            if !log.failures.is_empty() {
                return log;
            }
        }
    }
    log
}

pub(crate) fn receive_count_of(message: &MessageResponse) -> Option<u64> {
    message
        .attributes
        .as_ref()?
        .get("ApproximateReceiveCount")?
        .parse()
        .ok()
}

/// One delete or visibility-0 call with the claim's receipt, retried as an
/// idempotent call. A stale-receipt visibility answer is a correct outcome
/// (the lease ended): it is recorded as undetermined.
pub(crate) async fn release(
    log: &mut NodeLog,
    manager: &QueueManager,
    queue_url: &str,
    clock: &crate::simulation_clock::SimulationClock,
    message: &MessageResponse,
    receive_count: u64,
    kind: ReleaseKind,
) {
    let called_at = storage_types::Clock::now(clock).timestamp_millis();
    let receipt = ReceiptHandle::from(message.receipt_handle.as_str());
    let result = match kind {
        ReleaseKind::Delete => retry_idempotent_queue_call(clock, || {
            manager.delete_message(DeleteMessageRequest {
                queue_url: queue_url.to_owned(),
                receipt_handle: receipt.clone(),
            })
        })
        .await
        .map(|_| ()),
        ReleaseKind::Visibility => retry_idempotent_queue_call(clock, || {
            manager.change_message_visibility(ChangeMessageVisibilityRequest {
                queue_url: queue_url.to_owned(),
                receipt_handle: receipt.clone(),
                visibility_timeout: 0,
            })
        })
        .await
        .map(|_| ()),
    };
    let returned_at = storage_types::Clock::now(clock).timestamp_millis();
    let outcome = match QueueCall::from(result) {
        QueueCall::Done(()) => ReleaseOutcome::Acked,
        QueueCall::Unknown | QueueCall::Aborted => ReleaseOutcome::Undetermined,
        QueueCall::Failed(QueueError::Validation { .. }) if kind == ReleaseKind::Visibility => {
            ReleaseOutcome::Undetermined
        }
        QueueCall::Failed(error) => {
            log.failures.push(format!("{kind:?}: {error}"));
            return;
        }
    };
    log.releases.push(ReleaseSeen {
        body: message.body.clone(),
        receive_count,
        kind,
        outcome,
        called_at,
        returned_at,
    });
}

impl RustWorkload for QueueClaimWorkload {
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
            metric_val_u64("aux_storage_queue_claim_node_count", self.node_count as u64),
            metric_val_u64("aux_storage_queue_claim_sent", self.sent_count),
            metric_val_u64(
                "aux_storage_queue_claim_unknown_send",
                self.unknown_send_count,
            ),
            metric_val_u64("aux_storage_queue_claim_claim", self.claim_count),
            metric_val_u64(
                "aux_storage_queue_claim_unknown_receive",
                self.unknown_receive_count,
            ),
            metric_val_u64(
                "aux_storage_queue_claim_aborted_receive",
                self.aborted_receive_count,
            ),
            metric_val_u64(
                "aux_storage_queue_claim_delete_effective",
                self.delete_effective_count,
            ),
            metric_val_u64(
                "aux_storage_queue_claim_delete_undetermined",
                self.delete_undetermined_count,
            ),
            metric_val_u64("aux_storage_queue_claim_release", self.release_count),
            metric_val_u64("aux_storage_queue_claim_abandon", self.abandon_count),
            metric_val_u64("aux_storage_queue_claim_drained", self.drained_count),
            metric_val_u64("aux_storage_queue_claim_empty_poll", self.empty_poll_count),
            metric_val_u64("aux_storage_queue_claim_error", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        120.0
    }
}
