use crate::{common::*, imports::*};

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
    stale_receipt_reject_count: u64,
    delete_count: u64,
    empty_check_count: u64,
    error_count: u64,
    context: WorkloadContext,
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
            stale_receipt_reject_count: 0,
            delete_count: 0,
            empty_check_count: 0,
            error_count: 0,
            context,
        }
    }

    fn store(&self, db: SimDatabase) -> Result<FoundationDbKvStore, String> {
        FoundationDbKvStore::from_database(
            FoundationDbConfig {
                cluster_file_path: None,
                tenant_name: None,
                subspace_prefix: Some(b"aux-storage/fdb-chaos/queue-visibility/".to_vec()),
                cache_read_version_ms: 0,
                immediate_gsi_consistency: false,
                report_conflicting_keys: false,
            },
            db,
        )
        .map_err(|err| storage_error_detail(&err))
    }

    async fn provider(&self, db: SimDatabase) -> Result<Arc<FdbChaosProvider>, String> {
        let provider = Arc::new(
            SortedKvDbStorageProvider::new(self.store(db)?).with_database_jobs_enabled(false),
        );
        queue::QueueProvider::initialize(provider.as_ref())
            .await
            .map_err(|err| err.to_string())?;
        Ok(provider)
    }

    async fn manager(&self, db: SimDatabase) -> Result<QueueManager, String> {
        Ok(QueueManager::new(self.provider(db).await?))
    }

    fn queue_name(&self) -> String {
        "FdbChaosQueueVisibility".to_string()
    }

    async fn queue_url(&self, manager: &QueueManager) -> Result<String, String> {
        manager
            .get_queue_url(GetQueueUrlRequest {
                queue_name: self.queue_name(),
            })
            .await
            .map(|response| response.queue_url)
            .map_err(|err| err.to_string())
    }

    fn message_body(&self, sequence: u64) -> String {
        format!("queue-visibility-message-{sequence}")
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
}

impl RustWorkload for QueueVisibilityWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if self.client_id != 0 {
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
        if let Err(err) = manager
            .create_queue(CreateQueueRequest {
                queue_name: self.queue_name(),
                attributes: Some(HashMap::new()),
            })
            .await
        {
            self.trace_error("create_queue", err.to_string());
        }
    }

    async fn start(&mut self, db: SimDatabase) {
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
        for sequence in 0..self.operation_count {
            for offset in 0..self.messages_per_round() {
                self.trace_operation("start", "send_message");
                if let Err(err) = manager
                    .send_message(SendMessageRequest {
                        queue_url: queue_url.clone(),
                        message_body: self.message_body(
                            sequence
                                .saturating_mul(self.messages_per_round())
                                .saturating_add(offset),
                        ),
                        delay_seconds: Some(0),
                        message_attributes: None,
                    })
                    .await
                {
                    self.trace_error("send_message", err.to_string());
                    return;
                }
                self.send_count += 1;
            }

            self.trace_operation("start", "receive_message");
            let mut received = match manager
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.clone(),
                    max_number_of_messages: Some(self.messages_per_round() as u32),
                    visibility_timeout: Some(30),
                    wait_time_seconds: Some(0),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await
            {
                Ok(response) => response.messages,
                Err(err) => {
                    self.trace_error("receive_message", err.to_string());
                    return;
                }
            };
            if received.len() < 2 {
                self.trace_error(
                    "receive_message",
                    format!(
                        "expected multiple seeded queue messages, got {}",
                        received.len()
                    ),
                );
                return;
            }
            let first = received.remove(0);
            self.receive_count += 1;
            for extra in received {
                if let Err(err) = manager
                    .delete_message(DeleteMessageRequest {
                        queue_url: queue_url.clone(),
                        receipt_handle: ReceiptHandle::from(extra.receipt_handle.as_str()),
                    })
                    .await
                {
                    self.trace_error("delete_extra_receipt", err.to_string());
                    return;
                }
                self.delete_count += 1;
            }

            self.trace_operation("start", "change_message_visibility_zero");
            if let Err(err) = manager
                .change_message_visibility(ChangeMessageVisibilityRequest {
                    queue_url: queue_url.clone(),
                    receipt_handle: ReceiptHandle::from(first.receipt_handle.as_str()),
                    visibility_timeout: 0,
                })
                .await
            {
                self.trace_error("change_message_visibility", err.to_string());
                return;
            }

            self.trace_operation("start", "receive_redelivery");
            let redelivery = match manager
                .receive_message(ReceiveMessageRequest {
                    queue_url: queue_url.clone(),
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(30),
                    wait_time_seconds: Some(QUEUE_REDELIVERY_WAIT_SECONDS),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await
            {
                Ok(response) => response.messages,
                Err(err) => {
                    self.trace_error("receive_redelivery", err.to_string());
                    return;
                }
            };
            let Some(redelivery) = redelivery.into_iter().next() else {
                self.trace_error(
                    "receive_redelivery",
                    format!(
                        "message was not redelivered within {QUEUE_REDELIVERY_WAIT_SECONDS}s \
                         after visibility timeout zero"
                    ),
                );
                return;
            };
            if redelivery.message_id != first.message_id || redelivery.body != first.body {
                self.trace_error(
                    "receive_redelivery",
                    format!(
                        "redelivery mismatch: first_id={} redelivered_id={} first_body={} \
                         redelivered_body={}",
                        first.message_id, redelivery.message_id, first.body, redelivery.body
                    ),
                );
                return;
            }
            self.receive_count += 1;
            self.redelivery_count += 1;

            self.trace_operation("start", "delete_stale_receipt");
            match manager
                .delete_message(DeleteMessageRequest {
                    queue_url: queue_url.clone(),
                    receipt_handle: ReceiptHandle::from(first.receipt_handle.as_str()),
                })
                .await
            {
                Ok(()) => {
                    self.trace_error(
                        "delete_stale_receipt",
                        "stale receipt handle deleted the current claim".to_string(),
                    );
                    return;
                }
                Err(_) => {
                    self.stale_receipt_reject_count += 1;
                }
            }

            self.trace_operation("start", "delete_current_receipt");
            if let Err(err) = manager
                .delete_message(DeleteMessageRequest {
                    queue_url: queue_url.clone(),
                    receipt_handle: ReceiptHandle::from(redelivery.receipt_handle.as_str()),
                })
                .await
            {
                self.trace_error("delete_current_receipt", err.to_string());
                return;
            }
            self.delete_count += 1;
        }
    }

    async fn check(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if self.client_id != 0 {
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
        self.trace_operation("check", "receive_empty_check");
        match manager
            .receive_message(ReceiveMessageRequest {
                queue_url,
                max_number_of_messages: Some(10),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
        {
            Ok(response) if response.messages.is_empty() => {
                self.empty_check_count += 1;
            }
            Ok(response) => {
                self.trace_error(
                    "empty_check",
                    format!(
                        "expected queue to be empty after acknowledged delete, found {} messages",
                        response.messages.len()
                    ),
                );
            }
            Err(err) => {
                self.trace_error("empty_check", err.to_string());
            }
        }
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
                "aux_storage_queue_visibility_stale_receipt_reject_count",
                self.stale_receipt_reject_count,
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
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
