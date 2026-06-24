use crate::{common::*, imports::*, table_atomicity::*};

impl RustWorkload for TableAtomicityWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.setup_phase(db).await;
    }

    async fn start(&mut self, db: SimDatabase) {
        self.start_phase(db).await;
    }

    async fn check(&mut self, db: SimDatabase) {
        self.check_phase(db).await;
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
                "aux_storage_table_atomicity_trim_unclassified_scope_count",
                self.trim_model.unclassified_count() as u64,
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
