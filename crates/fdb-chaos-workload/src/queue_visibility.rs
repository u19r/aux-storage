use crate::{common::*, constants::*, imports::*};

pub(crate) struct QueueVisibilityWorkload {
    name: String,
    profile: String,
    operation_count: u64,
    artifact_root: String,
    client_id: i32,
    client_count: i32,
    active_client_count: i32,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    send_count: u64,
    receive_count: u64,
    redelivery_count: u64,
    stale_receipt_noop_count: u64,
    /// Sends whose commit outcome FoundationDB did not report. The message
    /// may exist, so the round sends another and receives up to ten.
    ambiguous_send_count: u64,
    /// Receives whose commit outcome FoundationDB did not report.
    ambiguous_receive_count: u64,
    /// Receives that conflicted and committed nothing.
    conflict_receive_count: u64,
    delete_count: u64,
    empty_check_count: u64,
    error_count: u64,
    /// Sends that aborted and were sent again with the same body.
    aborted_send_count: u64,
    /// Other messages that a redelivery poll met and deleted.
    leftover_delete_count: u64,
    /// Messages of unknown sends or unknown deletes found and deleted by the
    /// drain.
    unknown_found_count: u64,
    /// Rounds or phases that unknown outcomes left unprovable.
    incomplete_count: u64,
    check_skipped_count: u64,
    incomplete: bool,
    /// Bodies whose send outcome is unknown.
    unknown_bodies: BTreeSet<String>,
    /// Bodies whose delete outcome is unknown after the idempotent retries.
    unknown_deleted_bodies: BTreeSet<String>,
    /// Bodies whose delete was acknowledged while this client held the only
    /// receipt.
    deleted_bodies: BTreeSet<String>,
    context: std::rc::Rc<WorkloadContext>,
    clock: Option<Arc<crate::simulation_clock::SimulationClock>>,
}

impl QueueVisibilityWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 1_u64).max(1);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();
        let active_client_count =
            option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32).clamp(1, client_count);
        Self {
            name,
            profile,
            operation_count,
            artifact_root,
            client_id,
            client_count,
            active_client_count,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            send_count: 0,
            receive_count: 0,
            redelivery_count: 0,
            stale_receipt_noop_count: 0,
            ambiguous_send_count: 0,
            ambiguous_receive_count: 0,
            conflict_receive_count: 0,
            delete_count: 0,
            empty_check_count: 0,
            error_count: 0,
            aborted_send_count: 0,
            leftover_delete_count: 0,
            unknown_found_count: 0,
            incomplete_count: 0,
            check_skipped_count: 0,
            incomplete: false,
            unknown_bodies: BTreeSet::new(),
            unknown_deleted_bodies: BTreeSet::new(),
            deleted_bodies: BTreeSet::new(),
            context: std::rc::Rc::new(context),
            clock: None,
        }
    }

    async fn manager(&self, db: SimDatabase) -> Result<QueueManager, String> {
        let driver = kv_backend::foundationdb::FoundationDbBackend::from_simulation(
            db,
            Some(b"aux-storage/fdb-chaos/queue-visibility/"),
            None,
            storage_types::ReadVersionPolicy::Fresh,
            crate::simulation_clock::phase_clock(self.clock.as_ref())?,
        );
        let clock = self
            .clock
            .as_ref()
            .ok_or("simulation phase clock is absent")?;
        let store = kv_backend::KvStore::initialize_format_with_clock(
            driver,
            storage_types::InitialStreamLineages::ONE,
            clock.clone(),
        )
        .await
        .map_err(|error| storage_error_detail(&error))?
        .with_metadata_cache_ttl(SIMULATION_METADATA_CACHE_TTL);
        Ok(QueueManager::new(Arc::new(store)))
    }

    fn queue_name(&self) -> String {
        format!("FdbChaosQueueVisibility{}", self.client_id)
    }

    async fn queue_url(&self, manager: &QueueManager) -> Result<String, String> {
        crate::common::queue_url_retrying(self.clock.as_ref(), manager, &self.queue_name()).await
    }

    fn message_body(&self, sequence: u64) -> String {
        format!("queue-visibility-{}-{sequence}", self.client_id)
    }

    fn messages_per_round(&self) -> u64 {
        4
    }

    fn is_active_client(&self) -> bool {
        self.client_id < self.active_client_count
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosQueueVisibilityPhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ActiveClientCount" => self.active_client_count,
                "OperationCount" => self.operation_count,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    fn trace_operation(&self, phase: &'static str, operation: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosQueueVisibilityOperation",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "Operation" => operation,
                "ClientId" => self.client_id,
            ],
        );
    }

    fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosQueueVisibilityError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Error" => error,
            ],
        );
    }

    /// Unknown outcomes left the rest of this client's work unprovable. Not an
    /// error: the store answered correctly.
    fn trace_incomplete(&mut self, phase: &'static str, reason: String) {
        self.incomplete_count += 1;
        self.incomplete = true;
        self.context.trace(
            Severity::Warn,
            "AuxStorageFdbChaosQueueVisibilityIncomplete",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Reason" => reason,
            ],
        );
    }

    fn trace_phase_end(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosQueueVisibilityPhaseEnd",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ErrorCount" => self.error_count,
                "IncompleteCount" => self.incomplete_count,
            ],
        );
    }
}

impl QueueVisibilityWorkload {
    async fn setup_phase(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if !self.is_active_client() {
            return;
        }
        let manager = match self.manager(db).await {
            Ok(manager) => manager,
            Err(err) => {
                self.trace_error("setup", err);
                return;
            }
        };
        self.trace_operation("setup", "create_queue");
        let clock = match self.clock.as_ref() {
            Some(clock) => Arc::clone(clock),
            None => {
                self.trace_error("setup", "simulation phase clock is absent".to_owned());
                return;
            }
        };
        let queue_name = self.queue_name();
        let result = retry_idempotent_queue_call(clock.as_ref(), || {
            manager.create_queue(CreateQueueRequest {
                queue_name: queue_name.clone(),
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
            }
            QueueCall::Failed(error) => self.trace_error("create_queue", error.to_string()),
        }
    }

    async fn start_phase(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
        if !self.is_active_client() {
            return;
        }
        let manager = match self.manager(db).await {
            Ok(manager) => manager,
            Err(err) => {
                self.trace_error("start", err);
                return;
            }
        };
        let queue_url = match self.queue_url(&manager).await {
            Ok(queue_url) => queue_url,
            Err(err) => {
                self.trace_error("resolve_queue_url", err);
                return;
            }
        };
        if self.incomplete {
            return;
        }
        let mut body_sequence = 0_u64;
        for _ in 0..self.operation_count {
            if let Err(error) = self.round(&manager, &queue_url, &mut body_sequence).await {
                self.trace_error("round", error);
                return;
            }
            if self.incomplete {
                return;
            }
        }
    }

    /// One round: send, collect at least two, keep one, visibility 0,
    /// redelivery, stale delete is a no-op, current receipt is present,
    /// delete it.
    async fn round(
        &mut self,
        manager: &QueueManager,
        queue_url: &str,
        body_sequence: &mut u64,
    ) -> Result<(), String> {
        let acked = self.send_round(manager, queue_url, body_sequence).await?;
        if acked < 2 {
            self.trace_incomplete(
                "send_message",
                format!("only {acked} acknowledged sends in a round"),
            );
            return Ok(());
        }
        self.trace_operation("start", "receive_message");
        let mut received = self.collect(manager, queue_url, 2).await?;
        if self.incomplete {
            return Ok(());
        }
        if received.len() < 2 {
            return Err(format!(
                "expected at least two of {acked} acknowledged messages after \
                 {QUEUE_COLLECT_POLLS} short polls, got {}",
                received.len()
            ));
        }
        let first = received.remove(0);
        self.receive_count += 1;
        for extra in received {
            if !self.delete_tracked(manager, queue_url, &extra).await? {
                return Ok(());
            }
        }
        self.trace_operation("start", "change_message_visibility_zero");
        match self
            .change_visibility(manager, queue_url, first.receipt_handle.as_str(), 0)
            .await
        {
            QueueCall::Done(()) => {}
            QueueCall::Unknown | QueueCall::Aborted => {
                self.trace_incomplete(
                    "change_message_visibility",
                    "visibility 0 outcome unknown".to_owned(),
                );
                return Ok(());
            }
            QueueCall::Failed(error) => return Err(format!("change_message_visibility: {error}")),
        }
        self.trace_operation("start", "receive_redelivery");
        let Some(redelivery) = self
            .receive_redelivery(manager, queue_url, &first.message_id)
            .await?
        else {
            if self.incomplete {
                return Ok(());
            }
            return Err(format!(
                "message was not redelivered within {QUEUE_REDELIVERY_WAIT_SECONDS}s after \
                 visibility timeout zero"
            ));
        };
        if redelivery.body != first.body {
            return Err(format!(
                "redelivery body mismatch: id={} first_body={} redelivered_body={}",
                first.message_id, first.body, redelivery.body
            ));
        }
        self.receive_count += 1;
        self.redelivery_count += 1;
        // A stale receipt deletes nothing and succeeds (W2 D7). The current
        // receipt still changes the visibility, which proves the message is
        // present.
        self.trace_operation("start", "delete_stale_receipt");
        match self
            .delete(manager, queue_url, first.receipt_handle.as_str())
            .await
        {
            QueueCall::Done(()) => {}
            QueueCall::Unknown | QueueCall::Aborted => {
                self.trace_incomplete(
                    "delete_stale_receipt",
                    "stale delete outcome unknown".to_owned(),
                );
                return Ok(());
            }
            QueueCall::Failed(error) => return Err(format!("delete_stale_receipt: {error}")),
        }
        self.trace_operation("start", "current_receipt_presence");
        match self
            .change_visibility(manager, queue_url, redelivery.receipt_handle.as_str(), 30)
            .await
        {
            QueueCall::Done(()) => {}
            QueueCall::Unknown | QueueCall::Aborted => {
                self.trace_incomplete(
                    "current_receipt_presence",
                    "presence outcome unknown".to_owned(),
                );
                return Ok(());
            }
            QueueCall::Failed(error) => {
                return Err(format!(
                    "stale receipt delete removed the current claim: {error}"
                ));
            }
        }
        self.stale_receipt_noop_count += 1;
        self.trace_operation("start", "delete_current_receipt");
        self.delete_tracked(manager, queue_url, &redelivery).await?;
        Ok(())
    }

    /// Send `messages_per_round` messages with at most
    /// `messages_per_round + QUEUE_MAX_AMBIGUOUS_SENDS` calls. A known abort
    /// sends the same body again; an unknown outcome records the body (the
    /// message may exist) and the next call sends a new body.
    async fn send_round(
        &mut self,
        manager: &QueueManager,
        queue_url: &str,
        body_sequence: &mut u64,
    ) -> Result<u64, String> {
        let mut acked = 0_u64;
        let mut calls = 0_u64;
        while acked < self.messages_per_round()
            && calls < self.messages_per_round() + QUEUE_MAX_AMBIGUOUS_SENDS
        {
            calls += 1;
            self.trace_operation("start", "send_message");
            let body = self.message_body(*body_sequence);
            let result = manager
                .send_message(SendMessageRequest {
                    queue_url: queue_url.to_owned(),
                    message_body: body.clone(),
                    delay_seconds: Some(0),
                    message_attributes: None,
                    ..Default::default()
                })
                .await;
            match QueueCall::from(result) {
                QueueCall::Done(_) => {
                    self.send_count += 1;
                    acked += 1;
                    *body_sequence += 1;
                }
                QueueCall::Unknown => {
                    self.ambiguous_send_count += 1;
                    self.unknown_bodies.insert(body);
                    *body_sequence += 1;
                }
                QueueCall::Aborted => self.aborted_send_count += 1,
                QueueCall::Failed(error) => return Err(format!("send_message: {error}")),
            }
        }
        Ok(acked)
    }

    /// Short polls until at least `min` messages are held, at most
    /// `QUEUE_COLLECT_POLLS` polls with a pause between empty polls.
    async fn collect(
        &mut self,
        manager: &QueueManager,
        queue_url: &str,
        min: usize,
    ) -> Result<Vec<MessageResponse>, String> {
        let mut held = Vec::new();
        for _ in 0..QUEUE_COLLECT_POLLS {
            let messages = self.receive(manager, queue_url, 10).await?;
            if self.incomplete {
                break;
            }
            if messages.is_empty() {
                self.pause().await?;
            }
            held.extend(messages);
            if held.len() >= min {
                break;
            }
        }
        Ok(held)
    }

    /// Delete a held message and record its body. Returns false when the
    /// outcome stayed unknown (the client is then incomplete).
    async fn delete_tracked(
        &mut self,
        manager: &QueueManager,
        queue_url: &str,
        message: &MessageResponse,
    ) -> Result<bool, String> {
        match self
            .delete(manager, queue_url, message.receipt_handle.as_str())
            .await
        {
            QueueCall::Done(()) => {
                self.delete_count += 1;
                self.deleted_bodies.insert(message.body.clone());
                Ok(true)
            }
            QueueCall::Unknown | QueueCall::Aborted => {
                self.unknown_deleted_bodies.insert(message.body.clone());
                self.trace_incomplete(
                    "delete_message",
                    "delete outcome unknown after retries".to_owned(),
                );
                Ok(false)
            }
            QueueCall::Failed(error) => Err(format!("delete_message: {error}")),
        }
    }

    async fn pause(&self) -> Result<(), String> {
        let clock = self
            .clock
            .as_ref()
            .ok_or("simulation phase clock is absent")?;
        let pause = std::time::Duration::from_millis(u64::from(QUEUE_REDELIVERY_POLL_MILLIS));
        storage_types::Clock::sleep(clock.as_ref(), pause)
            .await
            .map_err(|error| storage_error_detail(&error))
    }

    async fn delete(
        &self,
        manager: &QueueManager,
        queue_url: &str,
        receipt_handle: &str,
    ) -> QueueCall<()> {
        let Some(clock) = self.clock.as_ref() else {
            return QueueCall::Failed(QueueError::StorageError(StorageError::internal(
                "simulation phase clock is absent",
            )));
        };
        let result = retry_idempotent_queue_call(clock.as_ref(), || {
            manager.delete_message(DeleteMessageRequest {
                queue_url: queue_url.to_owned(),
                receipt_handle: ReceiptHandle::from(receipt_handle),
            })
        })
        .await
        .map(|_| ());
        QueueCall::from(result)
    }

    async fn change_visibility(
        &self,
        manager: &QueueManager,
        queue_url: &str,
        receipt_handle: &str,
        visibility_timeout: i64,
    ) -> QueueCall<()> {
        let Some(clock) = self.clock.as_ref() else {
            return QueueCall::Failed(QueueError::StorageError(StorageError::internal(
                "simulation phase clock is absent",
            )));
        };
        let result = retry_idempotent_queue_call(clock.as_ref(), || {
            manager.change_message_visibility(ChangeMessageVisibilityRequest {
                queue_url: queue_url.to_owned(),
                receipt_handle: ReceiptHandle::from(receipt_handle),
                visibility_timeout,
            })
        })
        .await
        .map(|_| ());
        QueueCall::from(result)
    }

    /// Short polls on the simulation clock. The simulator has no Tokio
    /// reactor, so the workload never asks the manager for a long poll, which
    /// waits on Tokio time. A short poll can be empty after a lost claim
    /// conflict; the next poll claims again. Other messages that a poll meets
    /// are deleted.
    async fn receive_redelivery(
        &mut self,
        manager: &QueueManager,
        queue_url: &str,
        message_id: &str,
    ) -> Result<Option<MessageResponse>, String> {
        let polls = QUEUE_REDELIVERY_WAIT_SECONDS * 1_000 / QUEUE_REDELIVERY_POLL_MILLIS;
        for _ in 0..polls {
            let messages = self.receive(manager, queue_url, 10).await?;
            if self.incomplete {
                return Ok(None);
            }
            let mut wanted = None;
            for message in messages {
                if message.message_id == message_id {
                    wanted = Some(message);
                    continue;
                }
                let deleted = self.delete_tracked(manager, queue_url, &message).await?;
                self.leftover_delete_count += 1;
                if !deleted {
                    return Ok(None);
                }
            }
            if wanted.is_some() {
                return Ok(wanted);
            }
            self.pause().await?;
        }
        Ok(None)
    }

    /// Receive with a 30 s visibility timeout. A receive whose commit outcome
    /// FoundationDB did not report may have claimed the messages, so the
    /// client waits until that claim expires and receives again.
    async fn receive(
        &mut self,
        manager: &QueueManager,
        queue_url: &str,
        max_number_of_messages: i64,
    ) -> Result<Vec<MessageResponse>, String> {
        loop {
            let result = manager
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.to_owned(),
                    max_number_of_messages: Some(max_number_of_messages),
                    visibility_timeout: Some(i64::from(QUEUE_RECEIVE_VISIBILITY_SECONDS)),
                    wait_time_seconds: Some(0),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await;
            match QueueCall::from(result) {
                QueueCall::Done(response) => return Ok(response.messages),
                QueueCall::Unknown
                    if self.ambiguous_receive_count < QUEUE_MAX_AMBIGUOUS_RECEIVES =>
                {
                    self.ambiguous_receive_count += 1;
                    let clock = self
                        .clock
                        .as_ref()
                        .ok_or("simulation phase clock is absent")?;
                    let claim_expiry = std::time::Duration::from_secs(
                        u64::from(QUEUE_RECEIVE_VISIBILITY_SECONDS) + 1,
                    );
                    storage_types::Clock::sleep(clock.as_ref(), claim_expiry)
                        .await
                        .map_err(|error| storage_error_detail(&error))?;
                }
                QueueCall::Unknown => {
                    self.trace_incomplete(
                        "receive_message",
                        "unknown receive cap reached".to_owned(),
                    );
                    return Ok(Vec::new());
                }
                QueueCall::Aborted
                    if self.conflict_receive_count < u64::from(QUEUE_IDEMPOTENT_CALL_ATTEMPTS) =>
                {
                    self.conflict_receive_count += 1;
                    self.pause().await?;
                }
                QueueCall::Aborted => {
                    return Err("receive aborted past the conflict cap".to_owned());
                }
                QueueCall::Failed(error) => return Err(error.to_string()),
            }
        }
    }

    async fn check_phase(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if !self.is_active_client() {
            return;
        }
        if self.incomplete {
            self.check_skipped_count += 1;
            return;
        }
        let manager = match self.manager(db).await {
            Ok(manager) => manager,
            Err(err) => {
                self.trace_error("check", err);
                return;
            }
        };
        let queue_url = match self.queue_url(&manager).await {
            Ok(queue_url) => queue_url,
            Err(err) => {
                self.trace_error("resolve_queue_url", err);
                return;
            }
        };
        self.trace_operation("check", "drain");
        for _ in 0..QUEUE_COLLECT_POLLS {
            let messages = match self.receive(&manager, &queue_url, 10).await {
                Ok(messages) => messages,
                Err(error) => {
                    self.trace_error("drain", error);
                    return;
                }
            };
            if messages.is_empty() {
                break;
            }
            for message in messages {
                if self.deleted_bodies.contains(&message.body) {
                    self.trace_error(
                        "drain",
                        format!("acknowledged delete reappeared: body={}", message.body),
                    );
                    return;
                }
                if !self.unknown_bodies.contains(&message.body)
                    && !self.unknown_deleted_bodies.contains(&message.body)
                {
                    self.trace_error(
                        "drain",
                        format!(
                            "message of an acknowledged round remained: body={}",
                            message.body
                        ),
                    );
                    return;
                }
                match self
                    .delete(&manager, &queue_url, message.receipt_handle.as_str())
                    .await
                {
                    QueueCall::Done(()) => self.unknown_found_count += 1,
                    QueueCall::Unknown | QueueCall::Aborted => {
                        self.trace_incomplete("drain", "drain delete outcome unknown".to_owned());
                        return;
                    }
                    QueueCall::Failed(error) => {
                        self.trace_error("drain", error.to_string());
                        return;
                    }
                }
            }
        }
        if !self.incomplete {
            self.empty_check_count += 1;
        }
    }

    async fn run_simulation_phase(
        &mut self,
        db: SimDatabase,
        phase: crate::simulation_clock::SimulationPhase,
    ) {
        let phase_name = match phase {
            crate::simulation_clock::SimulationPhase::Setup => "setup",
            crate::simulation_clock::SimulationPhase::Start => "start",
            crate::simulation_clock::SimulationPhase::Check => "check",
        };
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
        self.trace_phase_end(phase_name);
    }
}

impl RustWorkload for QueueVisibilityWorkload {
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
            metric_val_u64("aux_storage_queue_visibility_setup_count", self.setup_count),
            metric_val_u64("aux_storage_queue_visibility_start_count", self.start_count),
            metric_val_u64("aux_storage_queue_visibility_check_count", self.check_count),
            metric_val_u64("aux_storage_queue_visibility_send_count", self.send_count),
            metric_val_u64(
                "aux_storage_queue_visibility_receive_count",
                self.receive_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_redelivery_count",
                self.redelivery_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_stale_receipt_noop_count",
                self.stale_receipt_noop_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_ambiguous_send_count",
                self.ambiguous_send_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_ambiguous_receive_count",
                self.ambiguous_receive_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_delete_count",
                self.delete_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_empty_check_count",
                self.empty_check_count,
            ),
            metric_val_u64("aux_storage_queue_visibility_error_count", self.error_count),
            metric_val_u64(
                "aux_storage_queue_visibility_aborted_send_count",
                self.aborted_send_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_leftover_delete_count",
                self.leftover_delete_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_unknown_found_count",
                self.unknown_found_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_incomplete_count",
                self.incomplete_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_check_skipped_count",
                self.check_skipped_count,
            ),
            metric_val_u64(
                "aux_storage_queue_visibility_conflict_receive_count",
                self.conflict_receive_count,
            ),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
