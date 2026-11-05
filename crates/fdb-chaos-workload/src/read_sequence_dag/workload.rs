use super::constants;
use crate::{common::*, constants::*, imports::*};

/// FoundationDB simulator workload for the production ReadSequence graph.
///
/// The fixture compares ordinary reads and shared graph execution against the
/// same deterministic oracle, including public continuation and node
/// permutation.
pub(crate) struct ReadSequenceDagWorkload {
    pub(super) name: String,
    pub(super) profile: String,
    pub(super) client_id: i32,
    pub(super) client_count: i32,
    pub(super) active_client_count: i32,
    pub(super) operation_count: u64,
    pub(super) attempts: u64,
    pub(super) published: u64,
    pub(super) retries: u64,
    pub(super) mismatches: u64,
    pub(super) errors: u64,
    pub(super) oracle_checks: u64,
    pub(super) ordinary_attempts: u64,
    pub(super) mapped_attempts: u64,
    pub(super) mapped_selected: u64,
    pub(super) continuation_pages: u64,
    pub(super) permuted_plans: u64,
    pub(super) seed: u64,
    pub(super) buggify: bool,
    pub(super) context: std::rc::Rc<WorkloadContext>,
    pub(super) clock: Option<Arc<crate::simulation_clock::SimulationClock>>,
}

pub(super) type NormalizedResult =
    std::collections::BTreeMap<String, Vec<Vec<std::collections::HashMap<String, AttributeValue>>>>;

impl ReadSequenceDagWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let _: String =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();
        Self {
            name,
            profile: option_or_default_string(&context, OPTION_PROFILE, "read-sequence-dag"),
            client_id,
            client_count,
            active_client_count: option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32)
                .clamp(1, client_count),
            operation_count: option_or_default(&context, OPTION_OPERATION_COUNT, 1_u64),
            attempts: 0,
            published: 0,
            retries: 0,
            mismatches: 0,
            errors: 0,
            oracle_checks: 0,
            ordinary_attempts: 0,
            mapped_attempts: 0,
            mapped_selected: 0,
            continuation_pages: 0,
            permuted_plans: 0,
            seed: option_or_default(&context, OPTION_SEED, 1_u64),
            buggify: option_or_default_string(&context, OPTION_BUGGIFY, "off") == "on",
            context: std::rc::Rc::new(context),
            clock: None,
        }
    }

    pub(super) fn mapped_tenant(&self) -> Vec<u8> {
        format!(
            "aux-storage/fdb-chaos/read-sequence/{}/{}",
            self.seed, self.client_id
        )
        .into_bytes()
    }

    pub(super) fn mapped_table_name(&self) -> TableName {
        TableName::new(&format!("ReadSequenceDag{}{}", self.seed, self.client_id))
    }

    pub(super) fn mapped_index_name(&self) -> IndexName {
        IndexName::new(constants::MAPPED_INDEX_NAME)
    }

    pub(super) async fn mapped_provider(
        &self,
        db: SimDatabase,
    ) -> Result<Arc<dyn storage_provider::DynamoOperations>, String> {
        let tenant = self.mapped_tenant();
        let driver =
            kv_backend::foundationdb::FoundationDbBackend::from_simulation(db, None, Some(&tenant));
        let clock = self
            .clock
            .as_ref()
            .ok_or("simulation phase clock is absent")?;
        let store = kv_backend::KvStore::initialize_format_with_clock(driver, clock.clone())
            .await
            .map_err(|error| storage_error_detail(&error))?;
        Ok(Arc::new(store))
    }
}

impl ReadSequenceDagWorkload {
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

impl RustWorkload for ReadSequenceDagWorkload {
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

    fn get_check_timeout(&self) -> f64 {
        30.0
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64("aux_storage_read_sequence_dag_attempts", self.attempts),
            metric_val_u64("aux_storage_read_sequence_dag_published", self.published),
            metric_val_u64("aux_storage_read_sequence_dag_retries", self.retries),
            metric_val_u64("aux_storage_read_sequence_dag_mismatches", self.mismatches),
            metric_val_u64(
                "aux_storage_read_sequence_dag_oracle_checks",
                self.oracle_checks,
            ),
            metric_val_u64(
                "aux_storage_read_sequence_dag_ordinary_attempts",
                self.ordinary_attempts,
            ),
            metric_val_u64(
                "aux_storage_read_sequence_dag_mapped_attempts",
                self.mapped_attempts,
            ),
            metric_val_u64(
                "aux_storage_read_sequence_dag_mapped_selected",
                self.mapped_selected,
            ),
            metric_val_u64(
                "aux_storage_read_sequence_dag_continuation_pages",
                self.continuation_pages,
            ),
            metric_val_u64(
                "aux_storage_read_sequence_dag_permuted_plans",
                self.permuted_plans,
            ),
            metric_val_u64("aux_storage_read_sequence_dag_errors", self.errors),
        ]);
    }
}
