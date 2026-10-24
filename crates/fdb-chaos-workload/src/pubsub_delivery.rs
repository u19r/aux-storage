use crate::{common::*, constants::*, imports::*};

pub(crate) struct PubsubDeliveryWorkload {
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) operation_count: u64,
    pub(crate) artifact_root: String,
    pub(crate) client_id: i32,
    pub(crate) client_count: i32,
    pub(crate) active_client_count: i32,
    pub(crate) setup_count: u64,
    pub(crate) start_count: u64,
    pub(crate) check_count: u64,
    pub(crate) publish_count: u64,
    pub(crate) unknown_publish_count: u64,
    pub(crate) unknown_materialization_count: u64,
    pub(crate) publish_audit_count: u64,
    pub(crate) publish_attempts: Vec<fdb_chaos_model::PublishAttempt>,
    pub(crate) observed_publish_acceptances: BTreeSet<String>,
    pub(crate) claim_count: u64,
    pub(crate) unknown_claim_count: u64,
    pub(crate) claim_audit_count: u64,
    pub(crate) duplicate_claim_reject_count: u64,
    pub(crate) delivered_count: u64,
    pub(crate) failed_count: u64,
    pub(crate) retry_reschedule_count: u64,
    pub(crate) retry_claim_count: u64,
    pub(crate) direct_scan_count: u64,
    pub(crate) terminal_duplicate_reject_count: u64,
    pub(crate) orphan_check_count: u64,
    pub(crate) error_count: u64,
    pub(crate) topic_arn: Option<TopicArn>,
    pub(crate) subscription_arns: Vec<SubscriptionArn>,
    pub(crate) message_id: Option<PubsubMessageId>,
    pub(crate) delivery_record_ids: Vec<DeliveryRecordId>,
    pub(crate) context: WorkloadContext,
}

impl PubsubDeliveryWorkload {
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
            publish_count: 0,
            unknown_publish_count: 0,
            unknown_materialization_count: 0,
            publish_audit_count: 0,
            publish_attempts: Vec::new(),
            observed_publish_acceptances: BTreeSet::new(),
            claim_count: 0,
            unknown_claim_count: 0,
            claim_audit_count: 0,
            duplicate_claim_reject_count: 0,
            delivered_count: 0,
            failed_count: 0,
            retry_reschedule_count: 0,
            retry_claim_count: 0,
            direct_scan_count: 0,
            terminal_duplicate_reject_count: 0,
            orphan_check_count: 0,
            error_count: 0,
            topic_arn: None,
            subscription_arns: Vec::new(),
            message_id: None,
            delivery_record_ids: Vec::new(),
            context,
        }
    }

    pub(crate) fn driver(&self, db: SimDatabase) -> kv_backend::foundationdb::FoundationDbBackend {
        kv_backend::foundationdb::FoundationDbBackend::from_simulation(
            db,
            Some(b"aux-storage/fdb-chaos/pubsub-delivery/"),
            None,
        )
    }

    pub(crate) async fn provider(
        &self,
        db: SimDatabase,
    ) -> Result<Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>, String>
    {
        let store = kv_backend::KvStore::initialize_format(self.driver(db))
            .await
            .map_err(|error| storage_error_detail(&error))?;
        Ok(Arc::new(store))
    }

    pub(crate) fn is_active_client(&self) -> bool {
        self.client_id < self.active_client_count
    }

    pub(crate) fn topic_name(&self) -> String {
        "FdbChaosPubsubDelivery".to_string()
    }

    pub(crate) fn endpoint_for(&self, index: u64) -> String {
        format!("arn:aws:sqs:us-east-1:000000000000:FdbChaosPubsubDeliveryQueue{index}")
    }

    pub(crate) fn message_body(&self, sequence: u64) -> String {
        format!("pubsub-delivery-message-{sequence}")
    }

    pub(crate) fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosPubsubDeliveryPhase",
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

    pub(crate) fn trace_operation(&self, phase: &'static str, operation: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosPubsubDeliveryOperation",
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

    pub(crate) fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosPubsubDeliveryError",
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
