use crate::{common::*, imports::*};

pub(crate) struct NoopWorkload {
    name: String,
    profile: String,
    operation_count: u64,
    history_sample_limit: u64,
    artifact_root: String,
    client_id: i32,
    client_count: i32,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    context: WorkloadContext,
}

impl NoopWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 0_u64);
        let history_sample_limit = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();

        Self {
            name,
            profile,
            operation_count,
            history_sample_limit,
            artifact_root,
            client_id,
            client_count,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            context,
        }
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosNoopPhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "OperationCount" => self.operation_count,
                "HistorySampleLimit" => self.history_sample_limit,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }
}

impl RustWorkload for NoopWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
    }

    async fn start(&mut self, _db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64("aux_storage_noop_setup_count", self.setup_count),
            metric_val_u64("aux_storage_noop_start_count", self.start_count),
            metric_val_u64("aux_storage_noop_check_count", self.check_count),
            metric_val_u64("aux_storage_noop_operation_count", self.operation_count),
            metric_val_u64(
                "aux_storage_noop_history_sample_limit",
                self.history_sample_limit,
            ),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}

pub(crate) struct InvalidWorkload {
    requested_name: String,
    context: WorkloadContext,
}

impl InvalidWorkload {
    pub(crate) fn new(requested_name: String, context: WorkloadContext) -> Self {
        consume_noop_options(&context);
        Self {
            requested_name,
            context,
        }
    }
}

impl RustWorkload for InvalidWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosUnknownWorkload",
            details![
                "Layer" => "aux-storage",
                "RequestedWorkload" => &self.requested_name,
                "SupportedWorkloads" => "noop,kv_smoke,table_atomicity,queue_visibility,pubsub_delivery,partition_family",
            ],
        );
    }

    async fn start(&mut self, _db: SimDatabase) {}

    async fn check(&mut self, _db: SimDatabase) {}

    fn get_metrics(&self, mut out: Metrics) {
        out.push(metric_val_u64("aux_storage_unknown_workload", 1));
    }

    fn get_check_timeout(&self) -> f64 {
        1.0
    }
}
