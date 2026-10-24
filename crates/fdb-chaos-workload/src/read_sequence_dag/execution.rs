use std::{collections::BTreeSet, future::Future, pin::Pin};

use crate::{
    common::*,
    imports::*,
    read_sequence_dag::{
        NormalizedResult, ReadSequenceDagWorkload,
        constants::{MAPPED_FALLBACK_SHAPE_ID, MAPPED_SHAPE_ID},
        ordinary::OrdinaryFailure,
    },
};

const MAX_BUGGIFIED_RETRIES: u8 = 2;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OperationFailure {
    RetryableStorage(String),
    Fatal(String),
}

impl OperationFailure {
    fn retryable_storage(error: impl Into<String>) -> Self {
        Self::RetryableStorage(error.into())
    }

    fn fatal(error: impl Into<String>) -> Self {
        Self::Fatal(error.into())
    }

    fn into_message(self) -> String {
        match self {
            Self::RetryableStorage(error) | Self::Fatal(error) => error,
        }
    }
}

pub(super) type OperationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), OperationFailure>> + 'a>>;

pub(super) async fn run_with_buggified_retries<T, F>(
    owner: &mut T,
    buggify: bool,
    mut operation: F,
) -> (Result<(), OperationFailure>, u8)
where
    F: for<'a> FnMut(&'a mut T) -> OperationFuture<'a>,
{
    let mut retries = 0;
    loop {
        match operation(owner).await {
            Ok(()) => return (Ok(()), retries),
            Err(failure @ OperationFailure::Fatal(_)) => return (Err(failure), retries),
            Err(_failure @ OperationFailure::RetryableStorage(_))
                if buggify && retries < MAX_BUGGIFIED_RETRIES =>
            {
                retries += 1;
            }
            Err(failure) => return (Err(failure), retries),
        }
    }
}

struct OperationExpectation<'a> {
    request: &'a ReadSequenceRequest,
    expected: &'a NormalizedResult,
    ordinary: &'a NormalizedResult,
    shape_id: &'static str,
    permuted: bool,
}

impl ReadSequenceDagWorkload {
    async fn run_operation(
        &mut self,
        provider: Arc<dyn storage_provider::DynamoOperations>,
        table_name: TableName,
        index_name: IndexName,
        status: String,
        permuted: bool,
    ) -> Result<(), OperationFailure> {
        let request = self.read_sequence_request(&table_name, &index_name, &status, permuted);
        let plan = plan_read_sequence(&request)
            .map_err(|error| OperationFailure::fatal(format!("plan: {error}")))?;
        let expected = ReadSequenceDagWorkload::expected_result(&status);
        let ordinary = self
            .run_ordinary_fixture(&provider, &request, &plan)
            .await
            .map_err(|failure| match failure {
                OrdinaryFailure::Storage(error) => OperationFailure::retryable_storage(error),
                OrdinaryFailure::Fixture(error) => OperationFailure::fatal(error),
            })?;
        self.ordinary_attempts += 1;
        self.oracle_checks += 1;
        let shape_id = shape_id(&status);
        let expectation = OperationExpectation {
            request: &request,
            expected: &expected,
            ordinary: &ordinary,
            shape_id,
            permuted,
        };
        self.validate_ordinary(&expectation)
            .map_err(OperationFailure::fatal)?;
        self.trace("ordinary-complete");
        self.run_mapped_operation(&provider, &plan, expectation)
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
        provider: &Arc<dyn storage_provider::DynamoOperations>,
        _plan: &storage_types::ReadSequencePlan,
        expectation: OperationExpectation<'_>,
    ) -> Result<(), OperationFailure> {
        let mut request = expectation.request.clone();
        let mut results = std::collections::BTreeMap::<
            String,
            std::collections::BTreeMap<u32, Vec<HashMap<String, AttributeValue>>>,
        >::new();
        for _ in 0..256 {
            let response = provider
                .read_sequence(request.clone())
                .await
                .map_err(|error| {
                    OperationFailure::retryable_storage(storage_error_detail(&error))
                })?;
            self.mapped_attempts += 1;
            for node in response.nodes {
                for invocation in node.invocations {
                    let items =
                        normalize_payload(invocation.result).map_err(OperationFailure::fatal)?;
                    results
                        .entry(node.name.clone())
                        .or_default()
                        .entry(invocation.ordinal)
                        .or_default()
                        .extend(items);
                }
            }
            let Some(token) = response.next_sequence_token else {
                let normalized = results
                    .into_iter()
                    .map(|(name, rows)| (name, rows.into_values().collect()))
                    .collect();
                validate_mapped_result(expectation.shape_id, expectation.expected, &normalized)
                    .map_err(OperationFailure::fatal)?;
                self.mapped_selected += 1;
                self.trace_operation(
                    expectation.permuted,
                    expectation.shape_id,
                    "shared_graph",
                    "matched",
                );
                return Ok(());
            };
            self.mapped_fallbacks += 1;
            request.next_sequence_token = Some(token);
        }
        Err(OperationFailure::fatal(
            "ReadSequence exceeded its bounded continuation count",
        ))
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
                "ContinuationPages" => self.mapped_fallbacks,
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
        provider: &Arc<dyn storage_provider::DynamoOperations>,
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
        provider: &Arc<dyn storage_provider::DynamoOperations>,
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
        let buggify = self.buggify;
        let provider = Arc::clone(provider);
        let table_name = table_name.clone();
        let index_name = index_name.clone();
        let status = status.to_owned();
        let (result, retries) = run_with_buggified_retries(self, buggify, move |workload| {
            Box::pin(workload.run_operation(
                Arc::clone(&provider),
                table_name.clone(),
                index_name.clone(),
                status.clone(),
                permuted,
            ))
        })
        .await;
        self.retries += u64::from(retries);
        match result {
            Ok(()) => self.published += 1,
            Err(error) => {
                self.mismatches += 1;
                self.error("operation", error.into_message());
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
        if let Err(error) = provider
            .delete_table(storage_types::DeleteTableRequest {
                table_name: table_name.clone(),
            })
            .await
        {
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
                "aux_storage_read_sequence_dag_continuation_pages",
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

pub(super) fn validate_mapped_result(
    shape_id: &'static str,
    expected: &NormalizedResult,
    mapped: &NormalizedResult,
) -> Result<(), String> {
    if mapped == expected {
        return Ok(());
    }
    let detail = first_normalized_difference(expected, mapped)
        .unwrap_or_else(|| "different result shape".to_string());
    Err(format!("mapped result mismatch for {shape_id}: {detail}"))
}

pub(super) fn first_normalized_difference(
    expected: &NormalizedResult,
    mapped: &NormalizedResult,
) -> Option<String> {
    let section_names: BTreeSet<_> = expected.keys().chain(mapped.keys()).collect();
    for section in section_names {
        let expected_invocations = expected.get(section);
        let mapped_invocations = mapped.get(section);
        let (Some(expected_invocations), Some(mapped_invocations)) =
            (expected_invocations, mapped_invocations)
        else {
            return Some(format!(
                "section {section:?}: expected {}, mapped {}",
                expected_invocations.map_or("missing", |_| "present"),
                mapped_invocations.map_or("missing", |_| "present")
            ));
        };
        if expected_invocations.len() != mapped_invocations.len() {
            return Some(format!(
                "{section:?} invocation count: expected {}, mapped {}",
                expected_invocations.len(),
                mapped_invocations.len()
            ));
        }
        for (invocation_index, (expected_items, mapped_items)) in expected_invocations
            .iter()
            .zip(mapped_invocations)
            .enumerate()
        {
            if expected_items.len() != mapped_items.len() {
                return Some(format!(
                    "{section:?}[{invocation_index}] item count: expected {}, mapped {}",
                    expected_items.len(),
                    mapped_items.len()
                ));
            }
            for (item_index, (expected_item, mapped_item)) in
                expected_items.iter().zip(mapped_items).enumerate()
            {
                let attribute_names: BTreeSet<_> =
                    expected_item.keys().chain(mapped_item.keys()).collect();
                for attribute_name in attribute_names {
                    let expected_value = expected_item.get(attribute_name);
                    let mapped_value = mapped_item.get(attribute_name);
                    if expected_value != mapped_value {
                        return Some(format!(
                            "{section:?}[{invocation_index}][{item_index}].{attribute_name:?}: \
                             expected {expected_value:?}, mapped {mapped_value:?}"
                        ));
                    }
                }
            }
        }
    }
    None
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

fn normalize_payload(
    payload: storage_types::ReadSequenceInvocationPayload,
) -> Result<Vec<HashMap<String, AttributeValue>>, String> {
    let items: Vec<WireItem> = match payload {
        storage_types::ReadSequenceInvocationPayload::Get(response) => {
            response.item.into_iter().collect()
        }
        storage_types::ReadSequenceInvocationPayload::Query(response) => {
            response.items.unwrap_or_default()
        }
        storage_types::ReadSequenceInvocationPayload::BatchGet(response) => response
            .responses
            .unwrap_or_default()
            .into_iter()
            .flat_map(|(_, items)| items)
            .collect(),
    };
    items
        .iter()
        .map(|item| {
            <HashMap<String, AttributeValue> as storage_types::TryFromWireItem>::try_from_wire_item(
                item,
            )
            .map_err(|error| storage_error_detail(&error))
        })
        .collect()
}
