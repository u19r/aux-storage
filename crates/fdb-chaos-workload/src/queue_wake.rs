use crate::{
    common::*,
    constants::*,
    imports::*,
    queue_claim::simulation_queue_manager,
    queue_wake_audit::{CallResult, SlotValues, changed_slots, judge},
};

fn store_prefix() -> Vec<u8> {
    use foundationdb::tuple::{Bytes, Element, pack};
    pack(&(
        Element::Bytes(Bytes::from(QUEUE_WAKE_PREFIX_BYTES)),
        Element::Nil,
    ))
}

/// All slot keys of the workload store, read in one read-only transaction.
async fn read_slots(db: &SimDatabase) -> Result<SlotValues, String> {
    use futures::TryStreamExt as _;
    let begin = [store_prefix().as_slice(), QUEUE_WAKE_FAMILY].concat();
    let mut end = begin.clone();
    *end.last_mut().expect("family is not empty") += 1; // '/' + 1: just past the family
    let mut last_error = String::new();
    for _ in 0..QUEUE_WAKE_READ_ATTEMPTS {
        let transaction = db.create_trx().map_err(|error| error.to_string())?;
        let range = foundationdb::RangeOption::from((begin.as_slice(), end.as_slice()));
        let mut values = SlotValues::new();
        let mut stream = transaction.get_ranges_keyvalues(range, false);
        let read: Result<(), foundationdb::FdbError> = async {
            while let Some(pair) = stream.try_next().await? {
                let suffix = &pair.key()[begin.len()..];
                if let Ok(bytes) = <[u8; 2]>::try_from(suffix) {
                    values.insert(u16::from_be_bytes(bytes), pair.value().to_vec());
                }
            }
            Ok(())
        }
        .await;
        match read {
            Ok(()) => return Ok(values),
            Err(error) if error.is_retryable() => last_error = error.to_string(),
            Err(error) => return Err(error.to_string()),
        }
    }
    Err(format!("slot read did not finish: {last_error}"))
}

/// Deterministic queue names with distinct slots.
pub(crate) fn wake_queues() -> Vec<(String, u16)> {
    let mut chosen: Vec<(String, u16)> = Vec::new();
    let mut suffix = 0_u32;
    while chosen.len() < QUEUE_WAKE_QUEUE_COUNT {
        let name = format!("FdbChaosWake{suffix}");
        let slot = queue::QueueWakeSlot::of_queue(&name).index();
        if chosen.iter().all(|(_, used)| *used != slot) {
            chosen.push((name, slot));
        }
        suffix += 1;
    }
    chosen
}

pub(crate) struct QueueWakeWorkload {
    name: String,
    profile: String,
    operation_count: u64,
    artifact_root: String,
    client_id: i32,
    /// Name and slot of the four queues: source, traffic, attributes, target.
    queues: Vec<(String, u16)>,
    urls: Vec<String>,
    incomplete: bool,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    call_count: u64,
    staged_call_count: u64,
    unknown_count: u64,
    aborted_count: u64,
    move_count: u64,
    error_count: u64,
    incomplete_count: u64,
    context: std::rc::Rc<WorkloadContext>,
    clock: Option<Arc<crate::simulation_clock::SimulationClock>>,
}

impl QueueWakeWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 3_u64).max(1);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let _: i32 = option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        Self {
            name,
            profile,
            operation_count,
            artifact_root,
            client_id,
            queues: wake_queues(),
            urls: Vec::new(),
            incomplete: false,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            call_count: 0,
            staged_call_count: 0,
            unknown_count: 0,
            aborted_count: 0,
            move_count: 0,
            error_count: 0,
            incomplete_count: 0,
            context: std::rc::Rc::new(context),
            clock: None,
        }
    }

    fn is_active_client(&self) -> bool {
        self.client_id == 0
    }

    /// Slot of queue `index`.
    fn slot(&self, index: usize) -> u16 {
        self.queues[index].1
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosQueueWakePhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "OperationCount" => self.operation_count,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosQueueWakeError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Error" => error,
            ],
        );
    }

    /// Unknown outcomes left the rest of the script unprovable. Not an error:
    /// the store answered correctly.
    fn trace_incomplete(&mut self, phase: &'static str, reason: String) {
        self.incomplete_count += 1;
        self.incomplete = true;
        self.context.trace(
            Severity::Warn,
            "AuxStorageFdbChaosQueueWakeIncomplete",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Reason" => reason,
            ],
        );
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

    /// Read slots, run `call`, read slots, judge. Returns the call's value
    /// when it is `Done`. A broken invariant is traced as an error and
    /// returns `None`; an unknown outcome marks the run incomplete.
    async fn step<T>(
        &mut self,
        db: &SimDatabase,
        operation: &'static str,
        expected: &[u16],
        call: impl std::future::Future<Output = Result<T, QueueError>>,
    ) -> Option<T> {
        let before = match read_slots(db).await {
            Ok(values) => values,
            Err(error) => {
                self.trace_error(operation, error);
                return None;
            }
        };
        let outcome = QueueCall::from(call.await);
        let after = match read_slots(db).await {
            Ok(values) => values,
            Err(error) => {
                self.trace_error(operation, error);
                return None;
            }
        };
        self.call_count += 1;
        let (result, value) = match outcome {
            QueueCall::Done(value) => (CallResult::Done, Some(value)),
            QueueCall::Aborted => {
                self.aborted_count += 1;
                (CallResult::Aborted, None)
            }
            QueueCall::Unknown => {
                self.unknown_count += 1;
                (CallResult::Unknown, None)
            }
            QueueCall::Failed(error) => {
                self.trace_error(operation, error.to_string());
                return None;
            }
        };
        let expected: BTreeSet<u16> = expected.iter().copied().collect();
        let judged = changed_slots(&before, &after)
            .and_then(|changed| judge(operation, result, &expected, &changed));
        if let Err(error) = judged {
            self.trace_error(operation, error);
            return None;
        }
        if !expected.is_empty() && result == CallResult::Done {
            self.staged_call_count += 1;
        }
        if result == CallResult::Unknown {
            self.trace_incomplete(
                operation,
                "unknown outcome; script state is no longer known".to_owned(),
            );
        }
        value
    }

    /// `step`, called again after a known abort, at most
    /// `QUEUE_IDEMPOTENT_CALL_ATTEMPTS` times.
    async fn step_retry<T, F, Fut>(
        &mut self,
        db: &SimDatabase,
        operation: &'static str,
        expected: &[u16],
        mut call: F,
    ) -> Option<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, QueueError>>,
    {
        for _ in 0..QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
            let aborted_before = self.aborted_count;
            let errors_before = self.error_count;
            let value = self.step(db, operation, expected, call()).await;
            if value.is_some() {
                return value;
            }
            if self.aborted_count == aborted_before || self.error_count != errors_before {
                return None;
            }
        }
        self.trace_error(operation, "aborted past the retry cap".to_owned());
        None
    }

    /// Receive one message with visibility 30: short polls, at most
    /// `QUEUE_COLLECT_POLLS`, each expected to change no slot.
    async fn receive_one(
        &mut self,
        db: &SimDatabase,
        manager: &QueueManager,
        queue_url: &str,
        operation: &'static str,
    ) -> Option<MessageResponse> {
        for _ in 0..QUEUE_COLLECT_POLLS {
            let response = self
                .step_retry(db, operation, &[], || {
                    manager.receive_message(ReceiveMessageRequest {
                        queue_url: queue_url.to_owned(),
                        max_number_of_messages: Some(1),
                        visibility_timeout: Some(30),
                        wait_time_seconds: Some(0),
                        attribute_names: None,
                        message_attribute_names: None,
                    })
                })
                .await?;
            if let Some(message) = response.messages.into_iter().next() {
                return Some(message);
            }
            if let Err(error) = self.pause().await {
                self.trace_error(operation, error);
                return None;
            }
        }
        self.trace_error(
            operation,
            format!("no message after {QUEUE_COLLECT_POLLS} short polls"),
        );
        None
    }

    async fn setup_phase(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if !self.is_active_client() {
            return;
        }
        let manager =
            match simulation_queue_manager(&db, QUEUE_WAKE_PREFIX_BYTES, self.clock.as_ref()).await
            {
                Ok(manager) => manager,
                Err(error) => {
                    self.trace_error("setup", error);
                    return;
                }
            };
        let names: Vec<String> = self.queues.iter().map(|(name, _)| name.clone()).collect();
        let slots: Vec<u16> = self.queues.iter().map(|(_, slot)| *slot).collect();
        if self
            .step_retry(&db, "create_target", &[slots[3]], || {
                manager.create_queue(CreateQueueRequest {
                    queue_name: names[3].clone(),
                    attributes: Some(HashMap::new()),
                    ..Default::default()
                })
            })
            .await
            .is_none()
        {
            return;
        }
        let target_url = match manager
            .get_queue_url(GetQueueUrlRequest {
                queue_name: names[3].clone(),
                ..Default::default()
            })
            .await
        {
            Ok(response) => response.queue_url,
            Err(error) => {
                self.trace_error("resolve_queue_url", error.to_string());
                return;
            }
        };
        let Some(attributes) = self
            .step_retry(&db, "get_target_arn", &[], || {
                manager.get_queue_attributes(queue::GetQueueAttributesRequest {
                    queue_url: target_url.clone(),
                    attribute_names: Some(vec!["QueueArn".to_owned()]),
                })
            })
            .await
        else {
            return;
        };
        let Some(arn) = attributes.attributes.get("QueueArn").cloned() else {
            self.trace_error("get_target_arn", "QueueArn is missing".to_owned());
            return;
        };
        let policy = format!(r#"{{"maxReceiveCount":"1","deadLetterTargetArn":"{arn}"}}"#);
        if self
            .step_retry(&db, "create_source", &[slots[0]], || {
                manager.create_queue(CreateQueueRequest {
                    queue_name: names[0].clone(),
                    attributes: Some(HashMap::from([(
                        "RedrivePolicy".to_owned(),
                        policy.clone(),
                    )])),
                    ..Default::default()
                })
            })
            .await
            .is_none()
        {
            return;
        }
        for index in [1_usize, 2] {
            if self
                .step_retry(&db, "create_queue", &[slots[index]], || {
                    manager.create_queue(CreateQueueRequest {
                        queue_name: names[index].clone(),
                        attributes: Some(HashMap::new()),
                        ..Default::default()
                    })
                })
                .await
                .is_none()
            {
                return;
            }
        }
        let mut urls = Vec::with_capacity(names.len());
        for name in &names {
            let Some(response) = self
                .step_retry(&db, "get_queue_url", &[], || {
                    manager.get_queue_url(GetQueueUrlRequest {
                        queue_name: name.clone(),
                        ..Default::default()
                    })
                })
                .await
            else {
                return;
            };
            urls.push(response.queue_url);
        }
        self.urls = urls;
    }

    async fn start_phase(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
        if !self.is_active_client() || self.incomplete || self.urls.len() != QUEUE_WAKE_QUEUE_COUNT
        {
            return;
        }
        let manager =
            match simulation_queue_manager(&db, QUEUE_WAKE_PREFIX_BYTES, self.clock.as_ref()).await
            {
                Ok(manager) => manager,
                Err(error) => {
                    self.trace_error("start", error);
                    return;
                }
            };
        for round in 0..self.operation_count {
            if !self.round(&db, &manager, round).await {
                return;
            }
        }
        let (s1, s2) = (self.slot(1), self.slot(2));
        let urls = self.urls.clone();
        if self
            .step_retry(&db, "purge_queue", &[s2], || {
                manager.purge_queue(queue::PurgeQueueRequest {
                    queue_url: urls[2].clone(),
                })
            })
            .await
            .is_none()
        {
            return;
        }
        let _ = self
            .step_retry(&db, "delete_queue", &[s1], || {
                manager.delete_queue(queue::DeleteQueueRequest {
                    queue_url: urls[1].clone(),
                })
            })
            .await;
    }

    /// One round of the script. Returns false when the phase ends.
    async fn round(&mut self, db: &SimDatabase, manager: &QueueManager, round: u64) -> bool {
        let slots: Vec<u16> = (0..QUEUE_WAKE_QUEUE_COUNT)
            .map(|index| self.slot(index))
            .collect();
        let urls = self.urls.clone();
        let traffic_body = format!("wake-{round}");
        let move_body = format!("wake-move-{round}");
        // a: send makes the traffic queue's slot change.
        if self
            .step_retry(db, "send_message", &[slots[1]], || {
                manager.send_message(SendMessageRequest {
                    queue_url: urls[1].clone(),
                    message_body: traffic_body.clone(),
                    delay_seconds: Some(0),
                    message_attributes: None,
                    ..Default::default()
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // b: a receive stages nothing.
        let Some(first) = self
            .receive_one(db, manager, &urls[1], "receive_message")
            .await
        else {
            return false;
        };
        // c: due later, no slot change.
        let receipt = first.receipt_handle.clone();
        if self
            .step_retry(db, "visibility_later", &[], || {
                manager.change_message_visibility(ChangeMessageVisibilityRequest {
                    queue_url: urls[1].clone(),
                    receipt_handle: ReceiptHandle::from(receipt.as_str()),
                    visibility_timeout: 60,
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // d: due sooner changes the slot.
        if self
            .step_retry(db, "visibility_sooner", &[slots[1]], || {
                manager.change_message_visibility(ChangeMessageVisibilityRequest {
                    queue_url: urls[1].clone(),
                    receipt_handle: ReceiptHandle::from(receipt.as_str()),
                    visibility_timeout: 0,
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // e, f: receive again and delete.
        let Some(second) = self
            .receive_one(db, manager, &urls[1], "receive_message")
            .await
        else {
            return false;
        };
        let receipt = second.receipt_handle.clone();
        if self
            .step_retry(db, "delete_message", &[], || {
                manager.delete_message(DeleteMessageRequest {
                    queue_url: urls[1].clone(),
                    receipt_handle: ReceiptHandle::from(receipt.as_str()),
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // g: attributes change the slot of their queue.
        let timeout = (30 + round).to_string();
        if self
            .step_retry(db, "set_queue_attributes", &[slots[2]], || {
                manager.set_queue_attributes(queue::SetQueueAttributesRequest {
                    queue_url: urls[2].clone(),
                    attributes: HashMap::from([("VisibilityTimeout".to_owned(), timeout.clone())]),
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // h: send to the source queue.
        if self
            .step_retry(db, "send_message_source", &[slots[0]], || {
                manager.send_message(SendMessageRequest {
                    queue_url: urls[0].clone(),
                    message_body: move_body.clone(),
                    delay_seconds: Some(0),
                    message_attributes: None,
                    ..Default::default()
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // i: the first receive delivers (receive count becomes 1).
        let Some(delivered) = self
            .receive_one(db, manager, &urls[0], "receive_source")
            .await
        else {
            return false;
        };
        // j: due sooner changes the source slot.
        let receipt = delivered.receipt_handle.clone();
        if self
            .step_retry(db, "visibility_source", &[slots[0]], || {
                manager.change_message_visibility(ChangeMessageVisibilityRequest {
                    queue_url: urls[0].clone(),
                    receipt_handle: ReceiptHandle::from(receipt.as_str()),
                    visibility_timeout: 0,
                })
            })
            .await
            .is_none()
        {
            return false;
        }
        // k: the claim moves the message to the target and stages its slot.
        let Some(response) = self
            .step_retry(db, "dead_letter_move", &[slots[3]], || {
                manager.receive_message(ReceiveMessageRequest {
                    queue_url: urls[0].clone(),
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(30),
                    wait_time_seconds: Some(0),
                    attribute_names: None,
                    message_attribute_names: None,
                })
            })
            .await
        else {
            return false;
        };
        if response.messages.is_empty() {
            self.move_count += 1;
        } else {
            self.trace_error(
                "dead_letter_move",
                "expected a move, got a delivery".to_owned(),
            );
            return false;
        }
        // l: the target holds the moved message; delete it.
        let Some(moved) = self
            .receive_one(db, manager, &urls[3], "receive_target")
            .await
        else {
            return false;
        };
        let receipt = moved.receipt_handle.clone();
        self.step_retry(db, "delete_target_message", &[], || {
            manager.delete_message(DeleteMessageRequest {
                queue_url: urls[3].clone(),
                receipt_handle: ReceiptHandle::from(receipt.as_str()),
            })
        })
        .await
        .is_some()
    }

    async fn check_phase(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if !self.is_active_client() {
            return;
        }
        let slots = match read_slots(&db).await {
            Ok(slots) => slots,
            Err(error) => {
                self.trace_error("check", error);
                return;
            }
        };
        let own: BTreeSet<u16> = self.queues.iter().map(|(_, slot)| *slot).collect();
        if let Some(stray) = slots.keys().find(|slot| !own.contains(slot)) {
            self.trace_error("audit", format!("W4 stray slot key {stray}"));
            return;
        }
        if self.incomplete || self.error_count > 0 {
            return;
        }
        if let Some(absent) = own.iter().find(|slot| !slots.contains_key(slot)) {
            self.trace_error(
                "audit",
                format!("W4 slot {absent} of a workload queue is absent"),
            );
            return;
        }
        let rounds = self.operation_count;
        let expected_staged = 4 + 6 * rounds + 2;
        if self.staged_call_count != expected_staged || self.move_count != rounds {
            self.trace_error(
                "audit",
                format!(
                    "vacuous or miscounted run: staged {} of {expected_staged}, moves {} of \
                     {rounds}",
                    self.staged_call_count, self.move_count
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

impl RustWorkload for QueueWakeWorkload {
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
            metric_val_u64("aux_storage_queue_wake_call", self.call_count),
            metric_val_u64("aux_storage_queue_wake_staged_call", self.staged_call_count),
            metric_val_u64("aux_storage_queue_wake_unknown", self.unknown_count),
            metric_val_u64("aux_storage_queue_wake_aborted", self.aborted_count),
            metric_val_u64("aux_storage_queue_wake_move", self.move_count),
            metric_val_u64("aux_storage_queue_wake_error", self.error_count),
            metric_val_u64("aux_storage_queue_wake_incomplete", self.incomplete_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
