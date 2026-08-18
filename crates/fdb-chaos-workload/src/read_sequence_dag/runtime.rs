use crate::{
    common::*,
    imports::*,
    read_sequence_dag::{
        FdbChaosProvider, NormalizedResult, ReadSequenceDagWorkload,
        constants::{MAPPED_FALLBACK_SHAPE_ID, MAPPED_SHAPE_ID},
    },
};

struct OperationExpectation<'a> {
    request: &'a ReadSequenceRequest,
    expected: &'a NormalizedResult,
    ordinary: &'a NormalizedResult,
    shape_id: &'static str,
    status: &'a str,
    permuted: bool,
}

impl ReadSequenceDagWorkload {
    async fn run_operation(
        &mut self,
        provider: &Arc<FdbChaosProvider>,
        table_name: &TableName,
        index_name: &IndexName,
        status: &str,
        permuted: bool,
    ) -> Result<(), String> {
        let request = self.read_sequence_request(table_name, index_name, status, permuted);
        let plan = plan_read_sequence(&request).map_err(|error| format!("plan: {error}"))?;
        let expected = ReadSequenceDagWorkload::expected_result(status);
        let ordinary = self.run_ordinary_fixture(provider, &request, &plan).await?;
        self.ordinary_attempts += 1;
        self.oracle_checks += 1;
        let shape_id = shape_id(status);
        let expectation = OperationExpectation {
            request: &request,
            expected: &expected,
            ordinary: &ordinary,
            shape_id,
            status,
            permuted,
        };
        self.validate_ordinary(&expectation)?;
        self.trace("ordinary-complete");
        self.run_mapped_operation(provider, &plan, expectation)
            .await
    }

    fn validate_ordinary(&self, expectation: &OperationExpectation<'_>) -> Result<(), String> {
        if expectation.ordinary != expectation.expected {
            return Err(format!(
                "ordinary result mismatch for {}: expected {:?}, got {:?}",
                expectation.shape_id, expectation.expected, expectation.ordinary
            ));
        }
        self.trace_operation(
            expectation.permuted,
            expectation.shape_id,
            "ordinary_dag",
            "matched",
        );
        Ok(())
    }

    async fn run_mapped_operation(
        &mut self,
        provider: &Arc<FdbChaosProvider>,
        plan: &storage_types::ReadSequencePlan,
        expectation: OperationExpectation<'_>,
    ) -> Result<(), String> {
        let execution = provider
            .execute_read_sequence_plan(plan, ReadSequenceConsistency::Eventual, None)
            .await
            .map_err(|error| storage_error_detail(&error))?;
        self.mapped_attempts += 1;
        self.trace("mapped-complete");
        self.handle_mapped_execution(expectation, execution)
    }

    fn handle_mapped_execution(
        &mut self,
        expectation: OperationExpectation<'_>,
        execution: ReadSequenceExecution,
    ) -> Result<(), String> {
        match execution {
            ReadSequenceExecution::Executed(execution) => {
                self.mapped_selected += 1;
                let mapped = Self::normalize_mapped_response(expectation.request, &execution.rows);
                validate_mapped_result(expectation.shape_id, expectation.expected, &mapped)?;
                self.trace_operation(
                    expectation.permuted,
                    expectation.shape_id,
                    "fdb_mapped_range",
                    "matched",
                );
            }
            ReadSequenceExecution::Unsupported(
                storage_provider::ReadSequenceUnsupportedReason::Continuation,
            ) if expectation.status == "closed" => {
                self.mapped_fallbacks += 1;
                self.trace_operation(
                    expectation.permuted,
                    expectation.shape_id,
                    "ordinary_dag",
                    "ordinary_fallback",
                );
            }
            ReadSequenceExecution::Unsupported(reason) => {
                return Err(format!(
                    "canonical Tuple fixture unexpectedly fell back for {}: {reason:?}",
                    expectation.shape_id
                ));
            }
        }
        Ok(())
    }

    fn trace_operation(
        &self,
        permuted: bool,
        shape_id: &'static str,
        strategy: &'static str,
        outcome: &'static str,
    ) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosReadSequenceDagOperation",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "ShapeId" => shape_id,
                "Strategy" => strategy,
                "Outcome" => outcome,
                "Permutation" => permuted,
                "Seed" => self.seed,
                "ClientId" => self.client_id,
            ],
        );
    }

    fn trace(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosReadSequenceDag",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ActiveClientCount" => self.active_client_count,
                "Attempts" => self.attempts,
                "Published" => self.published,
                "Retries" => self.retries,
                "Mismatches" => self.mismatches,
                "OrdinaryAttempts" => self.ordinary_attempts,
                "MappedAttempts" => self.mapped_attempts,
                "MappedSelected" => self.mapped_selected,
                "MappedFallbacks" => self.mapped_fallbacks,
                "OracleChecks" => self.oracle_checks,
                "PermutedPlans" => self.permuted_plans,
            ],
        );
    }

    fn error(&mut self, phase: &'static str, error: String) {
        self.errors += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosReadSequenceDagError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ShapeId" => MAPPED_SHAPE_ID,
                "Error" => error,
            ],
        );
    }

    async fn run_operations(
        &mut self,
        provider: &Arc<FdbChaosProvider>,
        table_name: &TableName,
        index_name: &IndexName,
    ) {
        for operation_index in 0..self.operation_count.max(1) {
            for status in ["open", "closed"] {
                self.run_operation_with_retries(
                    provider,
                    table_name,
                    index_name,
                    status,
                    operation_index,
                )
                .await;
            }
        }
    }

    async fn run_operation_with_retries(
        &mut self,
        provider: &Arc<FdbChaosProvider>,
        table_name: &TableName,
        index_name: &IndexName,
        status: &str,
        operation_index: u64,
    ) {
        self.attempts += 1;
        let permuted =
            deterministic_roll(self.seed, operation_index, u64::from(status == "closed"))
                .is_multiple_of(2);
        self.permuted_plans += u64::from(permuted);
        self.trace("operation-start");
        let mut result = Err("operation did not run".to_string());
        for retry in 0..=2_u8 {
            result = self
                .run_operation(provider, table_name, index_name, status, permuted)
                .await;
            if result.is_ok() || !self.buggify || retry == 2 {
                break;
            }
            self.retries += 1;
        }
        match result {
            Ok(()) => self.published += 1,
            Err(error) => {
                self.mismatches += 1;
                self.error("operation", error);
            }
        }
    }
}

impl RustWorkload for ReadSequenceDagWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        self.trace("setup");
    }

    async fn start(&mut self, db: SimDatabase) {
        self.trace("start");
        if self.client_id >= self.active_client_count {
            return;
        }
        let provider = match self.mapped_provider(db).await {
            Ok(provider) => provider,
            Err(error) => {
                self.error("provider", error);
                return;
            }
        };
        self.trace("provider-ready");
        let table_name = self.mapped_table_name();
        let index_name = self.mapped_index_name();
        if let Err(error) = self.seed_mapped_table(&provider, &table_name).await {
            self.error("seed", error);
            return;
        }
        self.trace("seeded");
        self.run_operations(&provider, &table_name, &index_name)
            .await;
        if let Err(error) = provider.delete_table(&table_name).await {
            self.error("cleanup", storage_error_detail(&error));
        }
        self.trace("complete");
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.trace("check");
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
                "aux_storage_read_sequence_dag_mapped_fallbacks",
                self.mapped_fallbacks,
            ),
            metric_val_u64(
                "aux_storage_read_sequence_dag_permuted_plans",
                self.permuted_plans,
            ),
            metric_val_u64("aux_storage_read_sequence_dag_errors", self.errors),
        ]);
    }
}

fn validate_mapped_result(
    shape_id: &'static str,
    expected: &NormalizedResult,
    mapped: &NormalizedResult,
) -> Result<(), String> {
    if mapped == expected {
        return Ok(());
    }
    Err(format!(
        "mapped result mismatch for {shape_id}: expected {expected:?}, mapped {mapped:?}"
    ))
}

fn shape_id(status: &str) -> &'static str {
    if status == "closed" {
        MAPPED_FALLBACK_SHAPE_ID
    } else {
        MAPPED_SHAPE_ID
    }
}

fn deterministic_roll(seed: u64, attempt: u64, ordinal: u64) -> u64 {
    let mut value = seed
        ^ attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ ordinal.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod runtime_tests;
