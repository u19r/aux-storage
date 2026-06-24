use crate::{common::*, imports::*};

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
    pub(crate) claim_count: u64,
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
            claim_count: 0,
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

    pub(crate) fn store(&self, db: SimDatabase) -> Result<FoundationDbKvStore, String> {
        FoundationDbKvStore::from_database(
            FoundationDbConfig {
                cluster_file_path: None,
                tenant_name: None,
                subspace_prefix: Some(b"aux-storage/fdb-chaos/pubsub-delivery/".to_vec()),
                cache_read_version_ms: 0,
                immediate_gsi_consistency: false,
                report_conflicting_keys: false,
            },
            db,
        )
        .map_err(|err| storage_error_detail(&err))
    }

    pub(crate) async fn provider(&self, db: SimDatabase) -> Result<Arc<FdbChaosProvider>, String> {
        let provider = Arc::new(
            SortedKvDbStorageProvider::new(self.store(db)?).with_database_jobs_enabled(false),
        );
        pubsub::PubsubProvider::initialize(provider.as_ref())
            .await
            .map_err(|err| err.to_string())?;
        Ok(provider)
    }

    pub(crate) async fn manager(&self, db: SimDatabase) -> Result<PubsubManager, String> {
        Ok(PubsubManager::new(self.provider(db).await?))
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

    pub(crate) async fn direct_delivery_records(
        &self,
        db: SimDatabase,
    ) -> Result<Vec<DeliveryRecord>, String> {
        let store = self.store(db)?;
        let delivery_prefix = compact::pubsub_kind_prefix(PubsubRecordKind::Delivery).start;
        let scan = store
            .direct_audit_scan_prefix(&delivery_prefix, 1024)
            .await
            .map_err(|err| storage_error_detail(&err))?;
        let mut records = Vec::new();
        for (key, value) in scan.items {
            if !is_direct_delivery_record_key(&key) {
                continue;
            }
            let record: DeliveryRecord = storage_types::storage_serde::from_bytes(&value)
                .map_err(|err| format!("decode pubsub delivery record key={key:?}: {err:?}"))?;
            records.push(record);
        }
        records.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        Ok(records)
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

pub(crate) fn is_direct_delivery_record_key(key: &[u8]) -> bool {
    key.len() == 8 && key[2..8].iter().any(|byte| *byte != 0)
}
