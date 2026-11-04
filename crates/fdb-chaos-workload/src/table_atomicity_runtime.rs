use crate::{common::*, imports::*, table_atomicity::*};

enum Phase {
    Setup,
    Start,
    Check,
}

impl TableAtomicityWorkload {
    async fn run_phase(&mut self, db: SimDatabase, phase: Phase) {
        let context = std::rc::Rc::clone(&self.context);
        let (clock, requests) = crate::simulation_clock::SimulationClock::new();
        self.clock = Some(clock);
        let operation = async {
            match phase {
                Phase::Setup => self.setup_phase(db).await,
                Phase::Start => self.start_phase(db).await,
                Phase::Check => self.check_phase(db).await,
            }
        };
        crate::simulation_clock::drive_operation(context, requests, operation).await;
        self.clock = None;
    }
}

impl RustWorkload for TableAtomicityWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.run_phase(db, Phase::Setup).await;
    }

    async fn start(&mut self, db: SimDatabase) {
        self.run_phase(db, Phase::Start).await;
    }

    async fn check(&mut self, db: SimDatabase) {
        self.run_phase(db, Phase::Check).await;
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64(
                "aux_storage_table_atomicity_operation_count",
                self.history.events().len() as u64,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_committed_count",
                self.history.committed_count() as u64,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_failed_count",
                self.history.failed_count() as u64,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_condition_failed_count",
                self.history.condition_failed_count() as u64,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_unknown_count",
                self.history.unknown_count() as u64,
            ),
            metric_val_u64("aux_storage_table_atomicity_audit_count", self.audit_count),
            metric_val_u64(
                "aux_storage_table_atomicity_gsi_audit_count",
                self.gsi_audit_count,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_gsi_unclassified_partition_count",
                self.gsi_unclassified_partitions.len() as u64,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_trim_audit_count",
                self.trim_audit_count,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_trim_execution_count",
                self.trim_execution_count,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_stream_audit_count",
                self.stream_audit_count,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_direct_stream_pointer_audit_count",
                self.direct_stream_pointer_audit_count,
            ),
            metric_val_u64(
                "aux_storage_table_atomicity_direct_stream_pointer_decoupled_target_count",
                self.direct_stream_pointer_decoupled_target_count,
            ),
            metric_val_u64("aux_storage_table_atomicity_error_count", self.error_count),
            metric_val_u64(
                "aux_storage_table_atomicity_shared_operation_count",
                self.shared_operation_count,
            ),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        180.0
    }
}
