use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use futures::{StreamExt, TryStreamExt, stream};
use http_error::HttpApiError;
use storage_provider::{
    ReadSequenceExecution, ReadSequenceExecutionBudget, ReadSequenceFlatResult,
    ReadSequenceReadContext, ReadSequenceReadLimits, StorageProviderReadContext,
};
use storage_types::{
    AttributeMap, AttributeValue, BatchGetItemRequest, BatchGetItemResponse, ExclusiveStartKey,
    GetItemRequest, GetItemResponse, KeyAttributes, KeysAndAttributes, QueryResponse,
    ReadSequenceConsistency, ReadSequenceInvocationPayload, ReadSequenceInvocationResult,
    ReadSequenceNode, ReadSequenceNodeId, ReadSequenceNodeOperation, ReadSequenceNodeResult,
    ReadSequenceRequest, ReadSequenceResponse, ReadSequenceValidationError, StorageError,
    normalize_dynamodb_number_for_write, plan_read_sequence_with_capabilities,
    project_attribute_map, read_sequence_operation_contains_literal_escape,
};

use crate::{
    batch_get_wire_response::BatchGetWireResponse,
    manager::{
        ReadSequenceExecutionMode, StorageApiManagerImpl,
        storage_manager_impl_batch_get_item::{
            add_empty_batch_get_response_tables, batch_get_needs_decoded_response,
            project_batch_get_response,
        },
        storage_manager_impl_read_sequence_inputs::{
            ResolvedInput, ResolvedInputs, bind_operation, resolve_inputs,
        },
        storage_manager_impl_read_sequence_token::{
            ReadSequenceQueryContinuation, ReadSequenceToken, decode_read_sequence_token,
            encode_read_sequence_token, prepare_resume_token, read_sequence_request_digest,
            validate_resume_token_shape,
        },
    },
    types::Response,
};

const READ_SEQUENCE_CONCURRENCY: usize = 4;
const READ_SEQUENCE_BATCH_GET_LIMIT: usize = 100;
const READ_SEQUENCE_BATCH_GET_MAX_ATTEMPTS: usize = 4;
const READ_SEQUENCE_BATCH_GET_RETRY_BASE_DELAY_MS: u64 = 10;
// This is the provider-neutral default used by the existing DynamoDB-shaped
// Query owners and by the sequence scheduler's read reservation.
const READ_SEQUENCE_DEFAULT_QUERY_ITEMS: u32 = 100;

#[derive(Clone, Copy)]
struct ReadSequenceBudgetLimits {
    max_fanout: u32,
    max_root_items: u32,
    max_intermediate_items: u32,
    max_total_items: u32,
    max_response_bytes: u32,
}

#[derive(Clone, Copy)]
struct WaveReservationLimits {
    remaining_total_items: u32,
    already_read_items: u32,
    total_limit: u32,
    explicit_response_limit: bool,
}

struct ReadSequenceWaveContext<'a> {
    nodes: &'a [ReadSequenceNode],
    completed: &'a [Option<ReadSequenceNodeResult>],
    node_names: &'a [String],
    root_nodes: &'a BTreeSet<ReadSequenceNodeId>,
    resume: Option<&'a ReadSequenceToken>,
    limits: ReadSequenceBudgetLimits,
    active_nodes: &'a BTreeSet<ReadSequenceNodeId>,
}

struct NodeExecutionInput<'a> {
    node: &'a ReadSequenceNode,
    context: &'a ReadSequenceWaveContext<'a>,
    read_context: &'a ReadSequenceApiReadContext<'a>,
    consistency: ReadSequenceConsistency,
    cursor: Option<(u32, ExclusiveStartKey)>,
}

#[derive(Clone, Copy)]
struct ReadSequencePlanInput<'a> {
    request: &'a ReadSequenceRequest,
    plan: &'a storage_types::ReadSequencePlan,
    resume: Option<&'a ReadSequenceToken>,
    request_digest: &'a str,
}

struct OrdinaryReadSequenceState {
    completed: Vec<Option<ReadSequenceNodeResult>>,
    total_read_items: u32,
    next_token: Option<String>,
}

struct OrdinaryReadSequenceExecution<'input, 'context, 'manager> {
    input: ReadSequencePlanInput<'input>,
    read_context: &'context ReadSequenceApiReadContext<'manager>,
    root_nodes: BTreeSet<ReadSequenceNodeId>,
    active_nodes: BTreeSet<ReadSequenceNodeId>,
    state: OrdinaryReadSequenceState,
}

enum WholePlanStrategy {
    Fallback,
    Shadow(ReadSequenceResponse),
    Optimized(ReadSequenceResponse),
}

impl<'input, 'context, 'manager> OrdinaryReadSequenceExecution<'input, 'context, 'manager> {
    fn new(
        input: ReadSequencePlanInput<'input>,
        read_context: &'context ReadSequenceApiReadContext<'manager>,
    ) -> Self {
        let root_nodes = input
            .plan
            .graph
            .dependencies
            .iter()
            .enumerate()
            .filter_map(|(index, dependencies)| {
                dependencies
                    .is_empty()
                    .then_some(ReadSequenceNodeId::from_index(index))
            })
            .collect();
        let active_nodes = input
            .resume
            .map(|token| resume_execution_nodes(input.plan, token))
            .unwrap_or_else(|| {
                (0..input.plan.nodes.len())
                    .map(ReadSequenceNodeId::from_index)
                    .collect()
            });
        Self {
            input,
            read_context,
            root_nodes,
            active_nodes,
            state: OrdinaryReadSequenceState {
                completed: vec![None; input.plan.nodes.len()],
                total_read_items: 0,
                next_token: None,
            },
        }
    }

    async fn run(mut self) -> Result<ReadSequenceResponse, HttpApiError> {
        for wave_index in 0..self.input.plan.graph.waves.len() {
            let wave = self.input.plan.graph.waves[wave_index].clone();
            if !wave
                .iter()
                .any(|node_id| self.active_nodes.contains(node_id))
            {
                continue;
            }
            let output_before_wave =
                completed_output_nodes(self.input.plan, &self.state.completed, self.input.resume)
                    .len();
            let total_before_wave = self.state.total_read_items;
            let (wave_to_execute, deferred_node) = self.reserve_wave(&wave)?;
            let wave_results = self.execute_wave_nodes(&wave_to_execute).await?;
            self.apply_wave_results(wave_results)?;
            if self.enforce_wave_response_limit(
                &wave_to_execute,
                output_before_wave,
                total_before_wave,
            )? {
                break;
            }
            if self.record_query_continuation(&wave_to_execute)? {
                break;
            }
            if self.record_deferred_node(deferred_node)? {
                break;
            }
        }
        self.finish()
    }

    fn reserve_wave(
        &self,
        wave: &[ReadSequenceNodeId],
    ) -> Result<(Vec<ReadSequenceNodeId>, Option<ReadSequenceNodeId>), HttpApiError> {
        let request = self.input.request;
        let total_limit = request
            .max_total_read_items
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS);
        reserve_wave_prefix(
            wave,
            &self.scheduler_context(),
            WaveReservationLimits {
                remaining_total_items: total_limit.saturating_sub(self.state.total_read_items),
                already_read_items: self.state.total_read_items,
                total_limit,
                explicit_response_limit: request.max_response_bytes.is_some(),
            },
        )
    }

    fn scheduler_context(&self) -> ReadSequenceWaveContext<'_> {
        ReadSequenceWaveContext {
            nodes: &self.input.plan.nodes,
            completed: &self.state.completed,
            node_names: &self.input.plan.graph.node_names,
            root_nodes: &self.root_nodes,
            resume: self.input.resume,
            limits: self.budget_limits(),
            active_nodes: &self.active_nodes,
        }
    }

    fn budget_limits(&self) -> ReadSequenceBudgetLimits {
        read_sequence_budget_limits(self.input.request)
    }

    async fn execute_wave_nodes(
        &self,
        wave: &[ReadSequenceNodeId],
    ) -> Result<Vec<(ReadSequenceNodeId, ReadSequenceNodeResult)>, HttpApiError> {
        let scheduler_context = self.scheduler_context();
        execute_wave(
            wave,
            &scheduler_context,
            self.read_context,
            self.input.request.read_consistency,
        )
        .await
    }

    fn apply_wave_results(
        &mut self,
        results: Vec<(ReadSequenceNodeId, ReadSequenceNodeResult)>,
    ) -> Result<(), HttpApiError> {
        for (node_id, node_result) in results {
            self.record_node_result(node_id, node_result)?;
        }
        Ok(())
    }

    fn record_node_result(
        &mut self,
        node_id: ReadSequenceNodeId,
        node_result: ReadSequenceNodeResult,
    ) -> Result<(), HttpApiError> {
        let count = node_result
            .invocations
            .iter()
            .map(|invocation| invocation.result.item_count())
            .sum::<u32>();
        let total_limit = self
            .input
            .request
            .max_total_read_items
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS);
        self.state.total_read_items = self
            .state
            .total_read_items
            .checked_add(count)
            .ok_or_else(|| total_limit_error(total_limit))?;
        if self.state.total_read_items > total_limit {
            return Err(read_sequence_error(
                ReadSequenceValidationError::TotalReadLimitExceeded {
                    actual: self.state.total_read_items,
                    limit: total_limit,
                },
            ));
        }
        self.validate_node_item_limit(node_id, count)?;
        self.state.completed[node_id.index()] = Some(node_result);
        Ok(())
    }

    fn validate_node_item_limit(
        &self,
        node_id: ReadSequenceNodeId,
        count: u32,
    ) -> Result<(), HttpApiError> {
        let request = self.input.request;
        let intermediate_limit = request
            .max_intermediate_items
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS);
        if count > intermediate_limit {
            return Err(read_sequence_error(
                ReadSequenceValidationError::FanoutLimitExceeded {
                    actual: count,
                    limit: intermediate_limit,
                },
            ));
        }
        if self.root_nodes.contains(&node_id) {
            let root_limit = request
                .max_root_items
                .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS);
            if count > root_limit {
                return Err(read_sequence_error(
                    ReadSequenceValidationError::FanoutLimitExceeded {
                        actual: count,
                        limit: root_limit,
                    },
                ));
            }
        }
        Ok(())
    }

    fn enforce_wave_response_limit(
        &mut self,
        wave: &[ReadSequenceNodeId],
        output_before_wave: usize,
        total_before_wave: u32,
    ) -> Result<bool, HttpApiError> {
        let Some(limit) = self.input.request.max_response_bytes else {
            return Ok(false);
        };
        let response = self.response(None, false);
        let Err(error) = enforce_response_limit(&response, Some(limit)) else {
            return Ok(false);
        };
        if output_before_wave == 0
            || !wave.iter().any(|node_id| {
                self.input.plan.graph.outputs.contains(node_id)
                    && self.state.completed[node_id.index()].is_some()
            })
        {
            return Err(error);
        }
        self.rewind_response_overflow(wave, total_before_wave)?;
        Ok(true)
    }

    fn rewind_response_overflow(
        &mut self,
        wave: &[ReadSequenceNodeId],
        total_before_wave: u32,
    ) -> Result<(), HttpApiError> {
        for node_id in wave {
            self.state.completed[node_id.index()] = None;
        }
        self.state.total_read_items = total_before_wave;
        let next_node = wave.first().copied().ok_or_else(|| {
            HttpApiError::internal_server_error("ReadSequence response overflow wave is empty")
        })?;
        self.state.next_token =
            Some(self.encode_frontier_token(next_node, self.completed_nodes())?);
        Ok(())
    }

    fn record_query_continuation(
        &mut self,
        wave: &[ReadSequenceNodeId],
    ) -> Result<bool, HttpApiError> {
        let cursors = find_query_cursors(&self.state.completed, wave);
        if cursors.is_empty() {
            return Ok(false);
        }
        let completed_nodes = self.completed_nodes_without_cursors(&cursors);
        let (next_node, invocation, cursor) = &cursors[0];
        let mut token = self.new_frontier_token(*next_node);
        if cursors.len() == 1 {
            token.invocation_ordinal = Some(*invocation);
            token.query_cursor = Some(cursor.clone());
        } else {
            token.query_continuations = Some(
                cursors
                    .into_iter()
                    .map(
                        |(node, invocation, query_cursor)| ReadSequenceQueryContinuation {
                            node_ordinal: node.index(),
                            invocation_ordinal: invocation,
                            query_cursor,
                        },
                    )
                    .collect(),
            );
        }
        token.completed_nodes = completed_nodes;
        self.state.next_token = Some(encode_read_sequence_token(&token)?);
        Ok(true)
    }

    fn record_deferred_node(
        &mut self,
        deferred_node: Option<ReadSequenceNodeId>,
    ) -> Result<bool, HttpApiError> {
        let Some(next_node) = deferred_node else {
            return Ok(false);
        };
        self.state.next_token =
            Some(self.encode_frontier_token(next_node, self.completed_nodes())?);
        Ok(true)
    }

    fn new_frontier_token(&self, next_node: ReadSequenceNodeId) -> ReadSequenceToken {
        let mut token = ReadSequenceToken::new(
            self.input.request_digest,
            &self.input.plan.graph.structural_digest,
            self.input.request.read_consistency,
        );
        token.next_node_ordinal = next_node.index();
        token
    }

    fn encode_frontier_token(
        &self,
        next_node: ReadSequenceNodeId,
        completed_nodes: Vec<usize>,
    ) -> Result<String, HttpApiError> {
        let mut token = self.new_frontier_token(next_node);
        token.completed_nodes = completed_nodes;
        encode_read_sequence_token(&token)
    }

    fn completed_nodes_without_cursors(
        &self,
        cursors: &[(ReadSequenceNodeId, u32, ExclusiveStartKey)],
    ) -> Vec<usize> {
        self.state
            .completed
            .iter()
            .enumerate()
            .filter_map(|(index, result)| {
                let has_cursor = cursors.iter().any(|(node, _, _)| node.index() == index);
                (!has_cursor && result.is_some()).then_some(index)
            })
            .collect()
    }

    fn completed_nodes(&self) -> Vec<usize> {
        self.state
            .completed
            .iter()
            .enumerate()
            .filter_map(|(index, result)| result.as_ref().map(|_| index))
            .collect()
    }

    fn response(&self, next_token: Option<String>, partial: bool) -> ReadSequenceResponse {
        ReadSequenceResponse {
            nodes: completed_output_nodes(
                self.input.plan,
                &self.state.completed,
                self.input.resume,
            ),
            next_sequence_token: next_token,
            consumed_capacity: read_sequence_consumed_capacity(
                self.input.request.return_consumed_capacity.as_deref(),
                self.state.total_read_items,
            ),
            read_consistency: self.input.request.read_consistency,
            partial,
        }
    }

    fn finish(self) -> Result<ReadSequenceResponse, HttpApiError> {
        let response = self.response(
            self.state.next_token.clone(),
            self.state.next_token.is_some(),
        );
        enforce_response_limit(
            &response,
            Some(
                self.input
                    .request
                    .max_response_bytes
                    .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES),
            ),
        )?;
        Ok(response)
    }
}

impl StorageApiManagerImpl {
    fn prepare_read_sequence_request(
        &self,
        request: &mut ReadSequenceRequest,
    ) -> Result<
        (
            storage_types::ReadSequencePlan,
            Option<ReadSequenceToken>,
            String,
        ),
        HttpApiError,
    > {
        let plan = plan_read_sequence_with_capabilities(
            request,
            self.read_sequence_capabilities.validation_capabilities(),
        )
        .map_err(read_sequence_error)?;
        record_read_sequence_graph_metrics(&plan);
        let resume = prepare_resume_token(request, &plan.graph.structural_digest)?;
        if let Some(token) = resume.as_ref() {
            validate_resume_token_shape(token, &plan)?;
        }
        let request_digest = read_sequence_request_digest(request)?;
        Ok((plan, resume, request_digest))
    }

    async fn choose_whole_plan_strategy(
        &self,
        input: ReadSequencePlanInput<'_>,
    ) -> Result<WholePlanStrategy, HttpApiError> {
        let Some(response) = self.try_execute_whole_plan(input).await? else {
            return Ok(WholePlanStrategy::Fallback);
        };
        if self.read_sequence_execution_mode == ReadSequenceExecutionMode::On {
            metrics::counter!(
                "storage.read_sequence.strategy.total",
                "strategy" => "whole_plan"
            )
            .increment(1);
            return Ok(WholePlanStrategy::Optimized(response));
        }
        metrics::counter!(
            "storage.read_sequence.strategy.total",
            "strategy" => "shadow"
        )
        .increment(1);
        Ok(WholePlanStrategy::Shadow(response))
    }

    pub(super) async fn read_sequence_internal(
        &self,
        mut request: ReadSequenceRequest,
    ) -> Result<Response, HttpApiError> {
        let (plan, resume, request_digest) = self.prepare_read_sequence_request(&mut request)?;
        let strategy = self
            .choose_whole_plan_strategy(ReadSequencePlanInput {
                request: &request,
                plan: &plan,
                resume: resume.as_ref(),
                request_digest: &request_digest,
            })
            .await?;
        let shadow_response = match strategy {
            WholePlanStrategy::Optimized(response) => {
                return Ok(Response::ReadSequence(response));
            }
            WholePlanStrategy::Shadow(response) => Some(response),
            WholePlanStrategy::Fallback => None,
        };

        metrics::counter!("storage.read_sequence.strategy.total", "strategy" => "ordinary_dag")
            .increment(1);
        let response = self
            .execute_ordinary_read_sequence(ReadSequencePlanInput {
                request: &request,
                plan: &plan,
                resume: resume.as_ref(),
                request_digest: &request_digest,
            })
            .await?;
        if let Some(shadow_response) = shadow_response {
            record_read_sequence_shadow_comparison(&shadow_response, &response);
        }
        Ok(Response::ReadSequence(response))
    }

    async fn execute_ordinary_read_sequence(
        &self,
        input: ReadSequencePlanInput<'_>,
    ) -> Result<ReadSequenceResponse, HttpApiError> {
        const READ_SEQUENCE_MAX_RETRY_ATTEMPTS: u8 = 3;
        let mut retry_attempt = 0u8;
        loop {
            let read_context = ReadSequenceApiReadContext::begin(
                self,
                input.request.read_consistency,
                ReadSequenceReadLimits::from_request(input.request),
            )
            .await?;
            match OrdinaryReadSequenceExecution::new(input, &read_context)
                .run()
                .await
            {
                Ok(response) => return Ok(response),
                Err(_error)
                    if retry_attempt < READ_SEQUENCE_MAX_RETRY_ATTEMPTS
                        && read_context.take_retryable_read_failure() =>
                {
                    retry_attempt += 1;
                    metrics::counter!("storage.read_sequence.retry.total", "reason" => "fdb_read")
                        .increment(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn try_execute_whole_plan(
        &self,
        input: ReadSequencePlanInput<'_>,
    ) -> Result<Option<ReadSequenceResponse>, HttpApiError> {
        if self.read_sequence_execution_mode == ReadSequenceExecutionMode::Off {
            return Ok(None);
        }
        if self.is_shadow_skip(&input) {
            metrics::counter!("storage.read_sequence.shadow.total", "outcome" => "skipped")
                .increment(1);
            return Ok(None);
        }
        let Some(budget) = whole_plan_budget(&input) else {
            record_whole_plan_fallback("budget_frontier");
            return Ok(None);
        };
        let execution = self.execute_whole_plan(&input, budget).await?;
        let ReadSequenceExecution::Executed(executed) = execution else {
            return handle_whole_plan_unsupported(execution, input.resume);
        };
        metrics::counter!("storage.read_sequence.optimized.total", "strategy" => "whole_plan")
            .increment(1);
        Ok(Some(build_whole_plan_response(input, executed)?))
    }

    fn is_shadow_skip(&self, input: &ReadSequencePlanInput<'_>) -> bool {
        self.read_sequence_execution_mode == ReadSequenceExecutionMode::Shadow
            && !read_sequence_shadow_sampled(
                input.request_digest,
                self.read_sequence_shadow_sample_percent,
            )
    }

    async fn execute_whole_plan(
        &self,
        input: &ReadSequencePlanInput<'_>,
        budget: ReadSequenceExecutionBudget,
    ) -> Result<ReadSequenceExecution, HttpApiError> {
        let continuation = input
            .resume
            .and_then(|token| token.provider_continuation.as_deref());
        self.db()
            .execute_read_sequence_plan_with_budget(
                input.plan,
                input.request.read_consistency,
                continuation,
                budget,
            )
            .await
            .map_err(HttpApiError::from)
    }
}

fn whole_plan_budget(input: &ReadSequencePlanInput<'_>) -> Option<ReadSequenceExecutionBudget> {
    if input
        .plan
        .nodes
        .iter()
        .any(|node| read_sequence_operation_contains_literal_escape(&node.operation))
        || input.request.return_consumed_capacity.is_some()
        || input
            .resume
            .is_some_and(|token| token.provider_continuation.is_none())
    {
        return None;
    }
    let budget = whole_plan_execution_budget(input.request, input.plan)?;
    (!budget.is_unbounded() || whole_plan_static_budget_fits(input.request, input.plan))
        .then_some(budget)
}

fn record_whole_plan_fallback(reason: &'static str) {
    metrics::counter!(
        "storage.read_sequence.optimization_fallback.total",
        "reason" => reason
    )
    .increment(1);
}

fn handle_whole_plan_unsupported(
    execution: ReadSequenceExecution,
    resume: Option<&ReadSequenceToken>,
) -> Result<Option<ReadSequenceResponse>, HttpApiError> {
    record_whole_plan_fallback(whole_plan_unsupported_reason(execution));
    if resume.is_some_and(|token| token.provider_continuation.is_some()) {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    Ok(None)
}

fn build_whole_plan_response(
    input: ReadSequencePlanInput<'_>,
    executed: storage_provider::ReadSequenceExecuted,
) -> Result<ReadSequenceResponse, HttpApiError> {
    let ValidatedWholePlanRows {
        item_counts,
        mut nodes,
    } = validate_and_decode_whole_plan_rows(input.plan, executed.rows)?;
    let next_sequence_token = encode_whole_plan_continuation(
        input.request_digest,
        input.plan,
        input.request.read_consistency,
        executed.next_continuation,
    )?;
    let response = ReadSequenceResponse {
        nodes: take_whole_plan_output_nodes(input.plan, input.resume, &mut nodes),
        partial: next_sequence_token.is_some(),
        next_sequence_token,
        consumed_capacity: None,
        read_consistency: input.request.read_consistency,
    };
    validate_whole_plan_budgets(input.request, input.plan, &item_counts, &response)?;
    Ok(response)
}

fn decode_whole_plan_nodes(
    plan: &storage_types::ReadSequencePlan,
    rows: Vec<storage_provider::ReadSequenceFlatRow>,
) -> Vec<Option<ReadSequenceNodeResult>> {
    let mut nodes = vec![None; plan.nodes.len()];
    for row in rows {
        let node_id = row.node;
        let invocation = decode_whole_plan_row(plan, row);
        nodes[node_id.index()]
            .get_or_insert_with(|| ReadSequenceNodeResult {
                name: plan
                    .graph
                    .node_name(node_id)
                    .unwrap_or_default()
                    .to_string(),
                invocations: Vec::new(),
            })
            .invocations
            .push(invocation);
    }
    for node in nodes.iter_mut().flatten() {
        node.invocations
            .sort_by_key(|invocation| invocation.ordinal);
    }
    nodes
}

fn decode_whole_plan_row(
    plan: &storage_types::ReadSequencePlan,
    row: storage_provider::ReadSequenceFlatRow,
) -> ReadSequenceInvocationResult {
    ReadSequenceInvocationResult {
        ordinal: row.invocation_ordinal,
        input_refs: row.input_refs,
        result: whole_plan_payload(plan, row.node, row.result),
    }
}

fn whole_plan_payload(
    plan: &storage_types::ReadSequencePlan,
    node_id: ReadSequenceNodeId,
    result: ReadSequenceFlatResult,
) -> ReadSequenceInvocationPayload {
    match result {
        ReadSequenceFlatResult::Get { item } => {
            ReadSequenceInvocationPayload::Get(GetItemResponse { item })
        }
        ReadSequenceFlatResult::BatchGet { responses } => {
            ReadSequenceInvocationPayload::BatchGet(BatchGetItemResponse {
                responses: Some(whole_plan_batch_responses(plan, node_id, responses)),
                unprocessed_keys: None,
                consumed_capacity: None,
            })
        }
        ReadSequenceFlatResult::Query {
            items,
            count,
            scanned_count,
            last_evaluated_key,
        } => ReadSequenceInvocationPayload::Query(QueryResponse {
            items: Some(items),
            count,
            scanned_count,
            last_evaluated_key,
            consumed_capacity: None,
        }),
    }
}

fn whole_plan_batch_responses(
    plan: &storage_types::ReadSequencePlan,
    node_id: ReadSequenceNodeId,
    mut responses: HashMap<storage_types::TableName, Vec<AttributeMap>>,
) -> HashMap<storage_types::TableName, Vec<AttributeMap>> {
    if let ReadSequenceNodeOperation::BatchGet(request) = &plan.nodes[node_id.index()].operation {
        for table_name in request.request_items.keys() {
            responses.entry(table_name.clone()).or_default();
        }
    }
    responses
}

fn encode_whole_plan_continuation(
    request_digest: &str,
    plan: &storage_types::ReadSequencePlan,
    consistency: ReadSequenceConsistency,
    continuation: Option<String>,
) -> Result<Option<String>, HttpApiError> {
    continuation
        .map(|continuation| {
            let mut token =
                ReadSequenceToken::new(request_digest, &plan.graph.structural_digest, consistency);
            token.provider_continuation = Some(continuation);
            encode_read_sequence_token(&token)
        })
        .transpose()
}

fn take_whole_plan_output_nodes(
    plan: &storage_types::ReadSequencePlan,
    resume: Option<&ReadSequenceToken>,
    nodes: &mut [Option<ReadSequenceNodeResult>],
) -> Vec<ReadSequenceNodeResult> {
    plan.graph
        .outputs
        .iter()
        .filter(|node_id| {
            !resume.is_some_and(|token| token.completed_nodes.contains(&node_id.index()))
        })
        .map(|node_id| {
            nodes[node_id.index()]
                .take()
                .unwrap_or_else(|| ReadSequenceNodeResult {
                    name: plan
                        .graph
                        .node_name(*node_id)
                        .unwrap_or_default()
                        .to_string(),
                    invocations: Vec::new(),
                })
        })
        .collect()
}

fn record_read_sequence_graph_metrics(plan: &storage_types::ReadSequencePlan) {
    let width = plan
        .graph
        .waves
        .iter()
        .map(Vec::len)
        .max()
        .unwrap_or_default();
    metrics::histogram!("storage.read_sequence.graph.nodes").record(plan.nodes.len() as f64);
    metrics::histogram!("storage.read_sequence.graph.depth").record(plan.graph.waves.len() as f64);
    metrics::histogram!("storage.read_sequence.graph.width").record(width as f64);
}

pub(super) fn read_sequence_shadow_sampled(request_digest: &str, sample_percent: u8) -> bool {
    if sample_percent == 0 {
        return false;
    }
    if sample_percent >= 100 {
        return true;
    }
    let bucket = request_digest
        .as_bytes()
        .iter()
        .fold(0u8, |state, byte| state.wrapping_add(*byte))
        % 100;
    bucket < sample_percent
}

fn record_read_sequence_shadow_comparison(
    optimized: &ReadSequenceResponse,
    ordinary: &ReadSequenceResponse,
) {
    let outcome = if shadow_responses_equivalent(optimized, ordinary) {
        "match"
    } else {
        "mismatch"
    };
    metrics::counter!("storage.read_sequence.shadow.total", "outcome" => outcome).increment(1);
    if outcome == "mismatch" {
        tracing::warn!(
            target = "storage.read_sequence",
            "shadow comparison mismatch; ordinary DAG response remains authoritative"
        );
    }
}

fn shadow_responses_equivalent(
    optimized: &ReadSequenceResponse,
    ordinary: &ReadSequenceResponse,
) -> bool {
    let continuation = |response: &ReadSequenceResponse| {
        response.next_sequence_token.as_deref().and_then(|raw| {
            decode_read_sequence_token(raw).ok().map(|token| {
                serde_json::json!({
                    "version": token.version,
                    "request_digest": token.request_digest,
                    "metadata_digest": token.metadata_digest,
                    "consistency": token.consistency,
                    "next_node_ordinal": token.next_node_ordinal,
                    "invocation_ordinal": token.invocation_ordinal,
                    "query_cursor": token.query_cursor,
                    "query_continuations": token.query_continuations,
                    "provider_continuation": token.provider_continuation,
                    "completed_nodes": token.completed_nodes,
                })
            })
        })
    };
    serde_json::to_value((
        &optimized.nodes,
        &optimized.consumed_capacity,
        optimized.read_consistency,
        optimized.partial,
        continuation(optimized),
    ))
    .ok()
        == serde_json::to_value((
            &ordinary.nodes,
            &ordinary.consumed_capacity,
            ordinary.read_consistency,
            ordinary.partial,
            continuation(ordinary),
        ))
        .ok()
}

fn whole_plan_unsupported_reason(execution: ReadSequenceExecution) -> &'static str {
    let ReadSequenceExecution::Unsupported(reason) = execution else {
        return "none";
    };
    match reason {
        storage_provider::ReadSequenceUnsupportedReason::BackendCapability => "backend_capability",
        storage_provider::ReadSequenceUnsupportedReason::OperationShape => "operation_shape",
        storage_provider::ReadSequenceUnsupportedReason::ParameterLimit => "parameter_limit",
        storage_provider::ReadSequenceUnsupportedReason::PhysicalLayout => "physical_layout",
        storage_provider::ReadSequenceUnsupportedReason::Continuation => "continuation",
    }
}

/// A continuation stores only the completed-node bitset and the current query
/// frontier.  Replaying every root would issue unrelated reads again; replay
/// exactly the non-completed closure plus the completed ancestors needed to
/// bind those reads.
fn resume_execution_nodes(
    plan: &storage_types::ReadSequencePlan,
    token: &ReadSequenceToken,
) -> BTreeSet<ReadSequenceNodeId> {
    let mut active = (0..plan.nodes.len())
        .map(ReadSequenceNodeId::from_index)
        .filter(|node| !token.completed_nodes.contains(&node.index()))
        .collect::<BTreeSet<_>>();
    let mut pending = active.iter().copied().collect::<Vec<_>>();
    while let Some(node) = pending.pop() {
        for dependency in &plan.graph.dependencies[node.index()] {
            if active.insert(*dependency) {
                pending.push(*dependency);
            }
        }
    }
    active
}

struct ValidatedWholePlanRows {
    item_counts: BTreeMap<(ReadSequenceNodeId, u32), u32>,
    nodes: Vec<Option<ReadSequenceNodeResult>>,
}

fn validate_and_decode_whole_plan_rows(
    plan: &storage_types::ReadSequencePlan,
    rows: Vec<storage_provider::ReadSequenceFlatRow>,
) -> Result<ValidatedWholePlanRows, HttpApiError> {
    let item_counts = validate_whole_plan_row_structure(plan, &rows)?;
    let nodes = decode_whole_plan_nodes(plan, rows);
    validate_whole_plan_invocation_frontier(plan, &nodes)?;
    Ok(ValidatedWholePlanRows { item_counts, nodes })
}

#[cfg(test)]
pub(super) fn consume_whole_plan_rows_for_allocation_test(
    plan: &storage_types::ReadSequencePlan,
    rows: Vec<storage_provider::ReadSequenceFlatRow>,
) -> Result<usize, HttpApiError> {
    let validated = validate_and_decode_whole_plan_rows(plan, rows)?;
    Ok(validated
        .nodes
        .into_iter()
        .flatten()
        .map(|node| node.invocations.len())
        .sum())
}

fn validate_whole_plan_row_structure(
    plan: &storage_types::ReadSequencePlan,
    rows: &[storage_provider::ReadSequenceFlatRow],
) -> Result<BTreeMap<(ReadSequenceNodeId, u32), u32>, HttpApiError> {
    let mut ordinals = BTreeMap::<ReadSequenceNodeId, BTreeSet<u32>>::new();
    for row in rows {
        validate_whole_plan_row(plan, row, &mut ordinals)?;
    }
    validate_whole_plan_ordinals(&ordinals)?;

    let item_counts = rows
        .iter()
        .map(|row| {
            Ok((
                (row.node, row.invocation_ordinal),
                whole_plan_row_item_count(plan, row)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, HttpApiError>>()?;

    validate_whole_plan_required_rows(plan, &ordinals)?;

    validate_whole_plan_input_references(plan, rows, &item_counts)?;

    Ok(item_counts)
}

#[cfg(test)]
pub(super) fn validate_whole_plan_rows(
    plan: &storage_types::ReadSequencePlan,
    rows: &[storage_provider::ReadSequenceFlatRow],
) -> Result<BTreeMap<(ReadSequenceNodeId, u32), u32>, HttpApiError> {
    validate_and_decode_whole_plan_rows(plan, rows.to_vec()).map(|validated| validated.item_counts)
}

fn validate_whole_plan_row(
    plan: &storage_types::ReadSequencePlan,
    row: &storage_provider::ReadSequenceFlatRow,
    ordinals: &mut BTreeMap<ReadSequenceNodeId, BTreeSet<u32>>,
) -> Result<(), HttpApiError> {
    let Some(node) = plan.nodes.get(row.node.index()) else {
        return Err(invalid_whole_plan_result("row references an unknown node"));
    };
    if !ordinals
        .entry(row.node)
        .or_default()
        .insert(row.invocation_ordinal)
    {
        return Err(invalid_whole_plan_result(
            "row repeats an invocation ordinal",
        ));
    }
    validate_whole_plan_result_shape(node, row)?;
    validate_whole_plan_row_inputs(node, row)
}

fn validate_whole_plan_result_shape(
    node: &ReadSequenceNode,
    row: &storage_provider::ReadSequenceFlatRow,
) -> Result<(), HttpApiError> {
    let matches_operation = matches!(
        (&node.operation, &row.result),
        (
            ReadSequenceNodeOperation::Get(_),
            ReadSequenceFlatResult::Get { .. }
        ) | (
            ReadSequenceNodeOperation::BatchGet(_),
            ReadSequenceFlatResult::BatchGet { .. }
        ) | (
            ReadSequenceNodeOperation::Query(_),
            ReadSequenceFlatResult::Query { .. }
        )
    );
    matches_operation
        .then_some(())
        .ok_or_else(|| invalid_whole_plan_result("row result does not match the node operation"))
}

fn validate_whole_plan_row_inputs(
    node: &ReadSequenceNode,
    row: &storage_provider::ReadSequenceFlatRow,
) -> Result<(), HttpApiError> {
    let declared_inputs = node.inputs().keys().collect::<BTreeSet<_>>();
    let returned_inputs = row.input_refs.keys().collect::<BTreeSet<_>>();
    if declared_inputs != returned_inputs {
        return Err(invalid_whole_plan_result(
            "row input references do not match the node inputs",
        ));
    }
    for (input_name, reference) in &row.input_refs {
        let Some(input) = node.inputs().get(input_name) else {
            return Err(invalid_whole_plan_result(
                "row contains an undeclared input reference",
            ));
        };
        if input.from.node != reference.node {
            return Err(invalid_whole_plan_result(
                "row input reference names the wrong source node",
            ));
        }
        if input.on_missing == storage_types::ReadSequenceOnMissing::Skip
            && reference.item_ordinal.is_none()
        {
            return Err(invalid_whole_plan_result(
                "skip input reference points at a missing source item",
            ));
        }
    }
    Ok(())
}

fn validate_whole_plan_ordinals(
    ordinals: &BTreeMap<ReadSequenceNodeId, BTreeSet<u32>>,
) -> Result<(), HttpApiError> {
    for values in ordinals.values() {
        if values
            .iter()
            .copied()
            .enumerate()
            .any(|(expected, actual)| actual != expected as u32)
        {
            return Err(invalid_whole_plan_result(
                "row invocation ordinals are not contiguous",
            ));
        }
    }
    Ok(())
}

fn whole_plan_row_item_count(
    plan: &storage_types::ReadSequencePlan,
    row: &storage_provider::ReadSequenceFlatRow,
) -> Result<u32, HttpApiError> {
    match &row.result {
        ReadSequenceFlatResult::Get { item } => Ok(u32::from(item.is_some())),
        ReadSequenceFlatResult::BatchGet { responses } => {
            validate_whole_plan_batch_tables(plan, row, responses)?;
            Ok(responses.values().map(|items| items.len() as u32).sum())
        }
        ReadSequenceFlatResult::Query {
            items,
            count,
            scanned_count,
            ..
        } => {
            if *count != items.len() as u32 {
                return Err(invalid_whole_plan_result(
                    "query count does not match returned items",
                ));
            }
            if *scanned_count < *count {
                return Err(invalid_whole_plan_result(
                    "query scanned count is less than returned count",
                ));
            }
            Ok(items.len() as u32)
        }
    }
}

fn validate_whole_plan_batch_tables(
    plan: &storage_types::ReadSequencePlan,
    row: &storage_provider::ReadSequenceFlatRow,
    responses: &HashMap<storage_types::TableName, Vec<storage_types::AttributeMap>>,
) -> Result<(), HttpApiError> {
    let ReadSequenceNodeOperation::BatchGet(request) = &plan.nodes[row.node.index()].operation
    else {
        return Ok(());
    };
    if responses
        .keys()
        .any(|table_name| !request.request_items.contains_key(table_name))
    {
        return Err(invalid_whole_plan_result(
            "batch response contains an undeclared table",
        ));
    }
    Ok(())
}

fn validate_whole_plan_required_rows(
    plan: &storage_types::ReadSequencePlan,
    ordinals: &BTreeMap<ReadSequenceNodeId, BTreeSet<u32>>,
) -> Result<(), HttpApiError> {
    for (index, node) in plan.nodes.iter().enumerate() {
        let requires_row = plan.graph.dependencies[index].is_empty() || node.inputs().is_empty();
        if requires_row && !ordinals.contains_key(&ReadSequenceNodeId::from_index(index)) {
            return Err(invalid_whole_plan_result(
                "whole-plan result omitted a required node invocation",
            ));
        }
    }
    Ok(())
}

fn validate_whole_plan_input_references(
    plan: &storage_types::ReadSequencePlan,
    rows: &[storage_provider::ReadSequenceFlatRow],
    item_counts: &BTreeMap<(ReadSequenceNodeId, u32), u32>,
) -> Result<(), HttpApiError> {
    for row in rows {
        for reference in row.input_refs.values() {
            let Some(source_index) = plan
                .graph
                .node_names
                .iter()
                .position(|name| name == &reference.node)
            else {
                return Err(invalid_whole_plan_result(
                    "row input reference names an unknown source node",
                ));
            };
            let source = (
                ReadSequenceNodeId::from_index(source_index),
                reference.invocation_ordinal,
            );
            if !item_counts.contains_key(&source) {
                return Err(invalid_whole_plan_result(
                    "row input reference names an unknown source invocation",
                ));
            }
            if reference
                .item_ordinal
                .is_some_and(|ordinal| ordinal >= item_counts[&source])
            {
                return Err(invalid_whole_plan_result(
                    "row input reference names an unknown source item",
                ));
            }
        }
    }
    Ok(())
}

/// Check the invocation frontier against the same input resolver used by the
/// ordinary DAG.  A flat provider envelope can otherwise look structurally
/// valid while silently omitting a dependent invocation (for example a
/// `ONE`/`ERROR` input whose source item was missing) or publishing an extra
/// invocation for a `SKIP` input.  Optimized results are not public until this
/// semantic check succeeds.
fn validate_whole_plan_invocation_frontier(
    plan: &storage_types::ReadSequencePlan,
    completed: &[Option<ReadSequenceNodeResult>],
) -> Result<(), HttpApiError> {
    for node_id in &plan.graph.topological_order {
        let node = plan
            .nodes
            .get(node_id.index())
            .ok_or_else(|| invalid_whole_plan_result("invocation frontier has an unknown node"))?;
        validate_node_invocation_frontier(*node_id, node, completed, &plan.graph.node_names)?;
    }
    Ok(())
}

fn validate_node_invocation_frontier(
    node_id: ReadSequenceNodeId,
    node: &ReadSequenceNode,
    completed: &[Option<ReadSequenceNodeResult>],
    node_names: &[String],
) -> Result<(), HttpApiError> {
    let resolved = resolve_inputs(node, completed, node_names)
        .map_err(|error| invalid_whole_plan_result(&error.to_string()))?;
    let expected = invocation_bindings(node, &resolved)?;
    let actual = completed[node_id.index()]
        .as_ref()
        .map(|result| result.invocations.as_slice())
        .unwrap_or(&[]);
    if expected.len() != actual.len() {
        return Err(invalid_whole_plan_result(
            "invocation frontier count does not match resolved inputs",
        ));
    }
    for (expected_ordinal, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        validate_frontier_invocation(expected_ordinal, expected, actual)?;
    }
    Ok(())
}

fn validate_frontier_invocation(
    expected_ordinal: usize,
    expected: &InvocationBindings,
    actual: &ReadSequenceInvocationResult,
) -> Result<(), HttpApiError> {
    if actual.ordinal != expected_ordinal as u32 {
        return Err(invalid_whole_plan_result(
            "invocation frontier ordinals are not contiguous",
        ));
    }
    let expected_refs = expected
        .values
        .iter()
        .filter_map(|(name, input)| {
            input
                .reference
                .clone()
                .map(|reference| (name.clone(), reference))
        })
        .collect::<BTreeMap<_, _>>();
    if actual.input_refs != expected_refs {
        return Err(invalid_whole_plan_result(
            "invocation frontier input references do not match resolved inputs",
        ));
    }
    Ok(())
}

fn validate_whole_plan_budgets(
    request: &ReadSequenceRequest,
    plan: &storage_types::ReadSequencePlan,
    item_counts: &BTreeMap<(ReadSequenceNodeId, u32), u32>,
    response: &ReadSequenceResponse,
) -> Result<(), HttpApiError> {
    let limits = read_sequence_budget_limits(request);
    validate_whole_plan_fanout(item_counts, limits.max_fanout)?;
    let node_counts = whole_plan_item_totals(item_counts, limits.max_total_items)?;
    validate_whole_plan_node_limits(
        plan,
        node_counts,
        limits.max_root_items,
        limits.max_intermediate_items,
    )?;
    enforce_response_limit(response, Some(limits.max_response_bytes))
}

fn read_sequence_budget_limits(request: &ReadSequenceRequest) -> ReadSequenceBudgetLimits {
    ReadSequenceBudgetLimits {
        max_fanout: request
            .max_fanout_per_step
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP),
        max_root_items: request
            .max_root_items
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS),
        max_intermediate_items: request
            .max_intermediate_items
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS),
        max_total_items: request
            .max_total_read_items
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS),
        max_response_bytes: request
            .max_response_bytes
            .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES),
    }
}

fn validate_whole_plan_fanout(
    item_counts: &BTreeMap<(ReadSequenceNodeId, u32), u32>,
    configured_fanout: u32,
) -> Result<(), HttpApiError> {
    let mut invocation_counts = BTreeMap::<ReadSequenceNodeId, usize>::new();
    for (node, _invocation) in item_counts.keys() {
        *invocation_counts.entry(*node).or_default() += 1;
    }
    let max_fanout = invocation_counts
        .values()
        .copied()
        .max()
        .unwrap_or_default();
    if max_fanout > configured_fanout as usize {
        return Err(read_sequence_error(
            ReadSequenceValidationError::FanoutLimitExceeded {
                actual: u32::try_from(max_fanout).unwrap_or(u32::MAX),
                limit: configured_fanout,
            },
        ));
    }
    Ok(())
}

fn whole_plan_item_totals(
    item_counts: &BTreeMap<(ReadSequenceNodeId, u32), u32>,
    total_limit: u32,
) -> Result<BTreeMap<ReadSequenceNodeId, u32>, HttpApiError> {
    let mut node_counts = BTreeMap::<ReadSequenceNodeId, u32>::new();
    let mut total = 0u32;
    for ((node, _invocation), count) in item_counts {
        let node_total = node_counts.entry(*node).or_default();
        *node_total = checked_whole_plan_total(*node_total, *count, total_limit)?;
        total = checked_whole_plan_total(total, *count, total_limit)?;
    }
    Ok(node_counts)
}

fn checked_whole_plan_total(
    current: u32,
    additional: u32,
    limit: u32,
) -> Result<u32, HttpApiError> {
    let total = current
        .checked_add(additional)
        .ok_or_else(|| total_limit_error(limit))?;
    if total > limit {
        return Err(read_sequence_error(
            ReadSequenceValidationError::TotalReadLimitExceeded {
                actual: total,
                limit,
            },
        ));
    }
    Ok(total)
}

fn total_limit_error(limit: u32) -> HttpApiError {
    read_sequence_error(ReadSequenceValidationError::TotalReadLimitExceeded {
        actual: u32::MAX,
        limit,
    })
}

fn validate_whole_plan_node_limits(
    plan: &storage_types::ReadSequencePlan,
    node_counts: BTreeMap<ReadSequenceNodeId, u32>,
    root_limit: u32,
    intermediate_limit: u32,
) -> Result<(), HttpApiError> {
    for (node, count) in node_counts {
        if count > intermediate_limit {
            return Err(read_sequence_error(
                ReadSequenceValidationError::FanoutLimitExceeded {
                    actual: count,
                    limit: intermediate_limit,
                },
            ));
        }
        if plan.graph.dependencies[node.index()].is_empty() && count > root_limit {
            return Err(read_sequence_error(
                ReadSequenceValidationError::FanoutLimitExceeded {
                    actual: count,
                    limit: root_limit,
                },
            ));
        }
    }
    Ok(())
}

/// Select the only bounded optimized shape currently supported: one
/// eventual, independent Query root. The provider can lower its requested
/// Query limit to this frontier and return its cursor as the checksummed
/// sequence continuation. The checksum detects corruption but does not
/// authenticate a bearer token. All graph-shaped budgets remain on the ordinary
/// scheduler, whose wave reservation can account for dependent invocations
/// exactly.
pub(super) fn whole_plan_execution_budget(
    request: &ReadSequenceRequest,
    plan: &storage_types::ReadSequencePlan,
) -> Option<ReadSequenceExecutionBudget> {
    if !has_explicit_read_sequence_budget(request) {
        return Some(ReadSequenceExecutionBudget::unbounded());
    }
    let query = single_root_query(plan)?;
    if has_zero_read_sequence_frontier(request) {
        return None;
    }
    bounded_query_items(request, query)
}

fn has_explicit_read_sequence_budget(request: &ReadSequenceRequest) -> bool {
    request.max_root_items.is_some()
        || request.max_fanout_per_step.is_some()
        || request.max_intermediate_items.is_some()
        || request.max_total_read_items.is_some()
        || request.max_child_query_items_per_parent.is_some()
        || request.max_response_bytes.is_some()
}

fn single_root_query(
    plan: &storage_types::ReadSequencePlan,
) -> Option<&storage_types::QueryRequest> {
    let node = plan.nodes.first()?;
    (plan.nodes.len() == 1
        && node.inputs().is_empty()
        && node.iterate.is_none()
        && node.after().is_empty())
    .then_some(&node.operation)
    .and_then(|operation| match operation {
        ReadSequenceNodeOperation::Query(query) => Some(query),
        _ => None,
    })
}

fn has_zero_read_sequence_frontier(request: &ReadSequenceRequest) -> bool {
    request.max_root_items.is_some_and(|limit| limit == 0)
        || request.max_total_read_items.is_some_and(|limit| limit == 0)
        || request.max_fanout_per_step.is_some_and(|limit| limit == 0)
        || request.max_response_bytes.is_some_and(|limit| limit == 0)
}

fn bounded_query_items(
    request: &ReadSequenceRequest,
    query: &storage_types::QueryRequest,
) -> Option<ReadSequenceExecutionBudget> {
    let mut max_items = query.limit.unwrap_or(READ_SEQUENCE_DEFAULT_QUERY_ITEMS);
    for limit in [
        request.max_root_items,
        request.max_total_read_items,
        request.max_intermediate_items,
    ]
    .into_iter()
    .flatten()
    {
        max_items = max_items.min(limit);
    }
    // A byte-bounded response has a variable item size, so one item is the
    // smallest resumable provider frontier.
    if request.max_response_bytes.is_some() {
        max_items = max_items.min(1);
    }
    (max_items > 0).then(|| ReadSequenceExecutionBudget::bounded_items(max_items))
}

/// Prove that a whole-plan provider can stay inside the ordinary scheduler's
/// implicit item and fanout envelope without a resumable budget frontier.
///
/// The graph is already validated, so one `Many` input is the only source of
/// multiple invocations.  Propagating each node's maximum invocation/item
/// count in topological order is conservative for missing rows and filters,
/// and therefore safe as an optimization eligibility check.  Explicit limits
/// outside the provider's bounded Query subset are kept on the ordinary path
/// by `try_execute_whole_plan`, where the checksummed scheduler frontier can
/// page them deterministically.
pub(super) fn whole_plan_static_budget_fits(
    request: &ReadSequenceRequest,
    plan: &storage_types::ReadSequencePlan,
) -> bool {
    // Multiple output roots can cross the implicit response-byte boundary
    // between waves.  Without a byte frontier, let the ordinary scheduler
    // choose a deterministic output prefix instead.
    if request.max_response_bytes.is_none() && plan.graph.outputs.len() > 1 {
        return false;
    }
    let limits = read_sequence_budget_limits(request);
    let mut node_item_bounds = vec![0u32; plan.nodes.len()];
    let mut total_items = 0u32;
    for node_id in &plan.graph.topological_order {
        if !accumulate_static_node_budget(
            plan,
            *node_id,
            &mut node_item_bounds,
            &mut total_items,
            limits,
        ) {
            return false;
        }
    }
    true
}

fn accumulate_static_node_budget(
    plan: &storage_types::ReadSequencePlan,
    node_id: ReadSequenceNodeId,
    node_item_bounds: &mut [u32],
    total_items: &mut u32,
    limits: ReadSequenceBudgetLimits,
) -> bool {
    let Some(node) = plan.nodes.get(node_id.index()) else {
        return false;
    };
    let Some(invocation_bound) = static_invocation_bound(plan, node, node_item_bounds) else {
        return false;
    };
    if invocation_bound > limits.max_fanout {
        return false;
    }
    let items_per_invocation = static_items_per_invocation(node);
    let node_items = invocation_bound.saturating_mul(items_per_invocation);
    let node_limit = if plan.graph.dependencies[node_id.index()].is_empty() {
        limits.max_root_items
    } else {
        limits.max_intermediate_items
    };
    if node_items > node_limit {
        return false;
    }
    *total_items = total_items.saturating_add(node_items);
    if *total_items > limits.max_total_items {
        return false;
    }
    node_item_bounds[node_id.index()] = node_items;
    true
}

fn static_invocation_bound(
    plan: &storage_types::ReadSequencePlan,
    node: &ReadSequenceNode,
    node_item_bounds: &[u32],
) -> Option<u32> {
    let Some(iterate_name) = node.iterate.as_deref() else {
        return Some(1);
    };
    let input = node.inputs().get(iterate_name)?;
    let parent_index = plan
        .graph
        .node_names
        .iter()
        .position(|name| name == &input.from.node)?;
    Some(
        node_item_bounds
            .get(parent_index)
            .copied()
            .unwrap_or_default(),
    )
}

fn static_items_per_invocation(node: &ReadSequenceNode) -> u32 {
    match &node.operation {
        ReadSequenceNodeOperation::Get(_) => 1,
        ReadSequenceNodeOperation::BatchGet(batch) => batch
            .request_items
            .values()
            .try_fold(0u32, |total, keys| {
                total.checked_add(u32::try_from(keys.keys.len()).ok()?)
            })
            .unwrap_or(u32::MAX),
        ReadSequenceNodeOperation::Query(query) => {
            query.limit.unwrap_or(READ_SEQUENCE_DEFAULT_QUERY_ITEMS)
        }
    }
}

fn invalid_whole_plan_result(message: &str) -> HttpApiError {
    HttpApiError::from(StorageError::internal(&format!(
        "ReadSequence provider returned an invalid whole-plan result: {message}"
    )))
}

/// Reserve a deterministic request-ordinal prefix of a wave before issuing
/// any provider reads.  A reservation is deliberately conservative: it uses
/// the maximum number of items the operation can return, not the number which
/// happened to be returned by a previous attempt.  This keeps the budget
/// boundary independent of completion order and leaves any rejected suffix on
/// a canonical-checksum continuation token.  The checksum detects accidental
/// corruption and stale frontiers; it is not an authentication mechanism.
fn reserve_wave_prefix(
    wave: &[ReadSequenceNodeId],
    context: &ReadSequenceWaveContext<'_>,
    limits: WaveReservationLimits,
) -> Result<(Vec<ReadSequenceNodeId>, Option<ReadSequenceNodeId>), HttpApiError> {
    let mut reserved = 0u32;
    let mut selected = Vec::with_capacity(wave.len());
    for node_id in wave {
        if !context.active_nodes.contains(node_id) {
            continue;
        }
        let reservation = estimate_node_item_reservation(*node_id, context)?;
        let fits = reservation <= limits.remaining_total_items.saturating_sub(reserved);
        if !fits {
            if selected.is_empty() {
                return Err(read_sequence_error(
                    ReadSequenceValidationError::TotalReadLimitExceeded {
                        actual: limits
                            .already_read_items
                            .saturating_add(reserved)
                            .saturating_add(reservation),
                        limit: limits.total_limit,
                    },
                ));
            }
            return Ok((selected, Some(*node_id)));
        }
        reserved = reserved.saturating_add(reservation);
        selected.push(*node_id);
        if limits.explicit_response_limit {
            return Ok((selected, next_active_wave_node(wave, *node_id, context)));
        }
    }
    Ok((selected, None))
}

fn next_active_wave_node(
    wave: &[ReadSequenceNodeId],
    current: ReadSequenceNodeId,
    context: &ReadSequenceWaveContext<'_>,
) -> Option<ReadSequenceNodeId> {
    wave.iter()
        .skip_while(|candidate| **candidate != current)
        .skip(1)
        .find(|candidate| context.active_nodes.contains(candidate))
        .copied()
}

fn estimate_node_item_reservation(
    node_id: ReadSequenceNodeId,
    context: &ReadSequenceWaveContext<'_>,
) -> Result<u32, HttpApiError> {
    let node = context
        .nodes
        .get(node_id.index())
        .ok_or_else(|| read_sequence_error(ReadSequenceValidationError::StaleToken))?;
    let resolved =
        resolve_inputs(node, context.completed, context.node_names).map_err(read_sequence_error)?;
    let invocation_count = invocation_bindings(node, &resolved)?.len();
    validate_invocation_fanout(invocation_count, context.limits.max_fanout)?;
    let skipped_invocations =
        node_cursor(context.resume, node_id).map_or(0usize, |(ordinal, _)| ordinal as usize);
    let remaining_invocations = invocation_count.saturating_sub(skipped_invocations);
    let node_limit = node_item_limit(context, node_id);
    let per_invocation = estimate_operation_item_count(&node.operation, node_limit)?;
    let reservation = u32::try_from(remaining_invocations)
        .ok()
        .and_then(|count| count.checked_mul(per_invocation))
        .ok_or_else(|| total_limit_error(u32::MAX))?;
    if reservation > node_limit {
        return Err(read_sequence_error(
            ReadSequenceValidationError::FanoutLimitExceeded {
                actual: reservation,
                limit: node_limit,
            },
        ));
    }
    Ok(reservation)
}

fn validate_invocation_fanout(actual: usize, limit: u32) -> Result<(), HttpApiError> {
    if actual > limit as usize {
        return Err(read_sequence_error(
            ReadSequenceValidationError::FanoutLimitExceeded {
                actual: u32::try_from(actual).unwrap_or(u32::MAX),
                limit,
            },
        ));
    }
    Ok(())
}

fn node_item_limit(context: &ReadSequenceWaveContext<'_>, node_id: ReadSequenceNodeId) -> u32 {
    if context.root_nodes.contains(&node_id) {
        context.limits.max_root_items
    } else {
        context.limits.max_intermediate_items
    }
}

fn estimate_operation_item_count(
    operation: &ReadSequenceNodeOperation,
    node_limit: u32,
) -> Result<u32, HttpApiError> {
    match operation {
        ReadSequenceNodeOperation::Get(_) => Ok(1),
        ReadSequenceNodeOperation::BatchGet(request) => request
            .request_items
            .values()
            .map(|keys| keys.keys.len() as u32)
            .try_fold(0u32, u32::checked_add)
            .ok_or_else(|| {
                read_sequence_error(ReadSequenceValidationError::TotalReadLimitExceeded {
                    actual: u32::MAX,
                    limit: u32::MAX,
                })
            }),
        ReadSequenceNodeOperation::Query(request) => Ok(request
            .limit
            .unwrap_or(READ_SEQUENCE_DEFAULT_QUERY_ITEMS.min(node_limit))),
    }
}

fn node_cursor(
    resume: Option<&ReadSequenceToken>,
    node_id: ReadSequenceNodeId,
) -> Option<(u32, ExclusiveStartKey)> {
    let token = resume?;
    token
        .query_continuations
        .as_ref()
        .and_then(|continuations| {
            continuations
                .iter()
                .find(|continuation| continuation.node_ordinal == node_id.index())
                .map(|continuation| {
                    (
                        continuation.invocation_ordinal,
                        continuation.query_cursor.clone(),
                    )
                })
        })
        .or_else(|| {
            (token.next_node_ordinal == node_id.index())
                .then(|| {
                    token
                        .query_cursor
                        .clone()
                        .map(|cursor| (token.invocation_ordinal.unwrap_or(0), cursor))
                })
                .flatten()
        })
}

async fn execute_wave(
    wave: &[ReadSequenceNodeId],
    context: &ReadSequenceWaveContext<'_>,
    read_context: &ReadSequenceApiReadContext<'_>,
    consistency: ReadSequenceConsistency,
) -> Result<Vec<(ReadSequenceNodeId, ReadSequenceNodeResult)>, HttpApiError> {
    let futures = wave
        .iter()
        .copied()
        .filter(|node_id| context.active_nodes.contains(node_id))
        .map(|node_id| {
            let node = &context.nodes[node_id.index()];
            let cursor = node_cursor(context.resume, node_id);
            async move {
                execute_node(NodeExecutionInput {
                    node,
                    context,
                    read_context,
                    consistency,
                    cursor,
                })
                .await
                .map(|result| (node_id, result))
            }
        });
    stream::iter(futures)
        .buffered(READ_SEQUENCE_CONCURRENCY)
        .try_collect()
        .await
}

async fn execute_node(
    input: NodeExecutionInput<'_>,
) -> Result<ReadSequenceNodeResult, HttpApiError> {
    let resolved = resolve_inputs(
        input.node,
        input.context.completed,
        input.context.node_names,
    )
    .map_err(read_sequence_error)?;
    let bindings = invocation_bindings(input.node, &resolved)?;
    let start_ordinal = validate_node_execution(&input, bindings.len())?;
    let invocations = execute_node_invocations(&input, bindings, start_ordinal).await?;
    Ok(ReadSequenceNodeResult {
        name: input.node.name.clone(),
        invocations,
    })
}

fn validate_node_execution(
    input: &NodeExecutionInput<'_>,
    invocation_count: usize,
) -> Result<usize, HttpApiError> {
    if input.cursor.as_ref().is_some_and(|(cursor_ordinal, _)| {
        !matches!(&input.node.operation, ReadSequenceNodeOperation::Query(_))
            || *cursor_ordinal as usize >= invocation_count
    }) {
        return Err(read_sequence_error(ReadSequenceValidationError::StaleToken));
    }
    validate_invocation_fanout(invocation_count, input.context.limits.max_fanout)?;
    Ok(input
        .cursor
        .as_ref()
        .map_or(0, |(cursor_ordinal, _)| *cursor_ordinal as usize))
}

async fn execute_node_invocations(
    input: &NodeExecutionInput<'_>,
    invocation_bindings: Vec<InvocationBindings>,
    start_ordinal: usize,
) -> Result<Vec<ReadSequenceInvocationResult>, HttpApiError> {
    if matches!(input.node.operation, ReadSequenceNodeOperation::Get(_))
        && invocation_bindings.len().saturating_sub(start_ordinal) > 1
    {
        return execute_batched_get_invocations(input, invocation_bindings, start_ordinal).await;
    }
    let mut invocations = Vec::with_capacity(invocation_bindings.len() - start_ordinal);
    let mut cursor = input.cursor.clone();
    for (ordinal, bindings) in invocation_bindings
        .into_iter()
        .enumerate()
        .skip(start_ordinal)
    {
        let invocation = execute_node_invocation(input, bindings, ordinal, cursor.take()).await?;
        let has_continuation = matches!(
            &invocation.result,
            ReadSequenceInvocationPayload::Query(response)
                if response.last_evaluated_key.is_some()
        );
        invocations.push(invocation);
        // A continuation is a stable page frontier. Do not issue later
        // invocations until this invocation is resumed; otherwise the token
        // would have to encode multiple independent cursors and a retry could
        // duplicate work from an earlier invocation.
        if has_continuation {
            break;
        }
    }
    Ok(invocations)
}

struct BoundGetInvocation {
    ordinal: u32,
    input_refs: BTreeMap<String, storage_types::ReadSequenceInputReference>,
    request: GetItemRequest,
}

async fn execute_batched_get_invocations(
    input: &NodeExecutionInput<'_>,
    invocation_bindings: Vec<InvocationBindings>,
    start_ordinal: usize,
) -> Result<Vec<ReadSequenceInvocationResult>, HttpApiError> {
    let mut bound = Vec::with_capacity(invocation_bindings.len() - start_ordinal);
    for (ordinal, bindings) in invocation_bindings
        .into_iter()
        .enumerate()
        .skip(start_ordinal)
    {
        let bind_values = bindings.resolved_inputs()?;
        let operation =
            bind_operation(&input.node.operation, &bind_values).map_err(read_sequence_error)?;
        let ReadSequenceNodeOperation::Get(request) = operation else {
            return Err(HttpApiError::internal_server_error(
                "ReadSequence batched Get received another operation",
            ));
        };
        bound.push(BoundGetInvocation {
            ordinal: ordinal as u32,
            input_refs: bindings.into_input_refs(),
            request: apply_get_consistency(request, input.consistency),
        });
    }

    let mut results = Vec::with_capacity(bound.len());
    while bound.len() > READ_SEQUENCE_BATCH_GET_LIMIT {
        let remaining = bound.split_off(READ_SEQUENCE_BATCH_GET_LIMIT);
        results.extend(execute_batched_get_chunk(input.read_context, bound).await?);
        bound = remaining;
    }
    results.extend(execute_batched_get_chunk(input.read_context, bound).await?);
    Ok(results)
}

async fn execute_batched_get_chunk(
    read_context: &ReadSequenceApiReadContext<'_>,
    invocations: Vec<BoundGetInvocation>,
) -> Result<Vec<ReadSequenceInvocationResult>, HttpApiError> {
    let Some(first) = invocations.first() else {
        return Ok(Vec::new());
    };
    let table_name = first.request.table_name.clone();
    if invocations
        .iter()
        .any(|invocation| invocation.request.table_name != table_name)
    {
        return Err(HttpApiError::internal_server_error(
            "ReadSequence Get node resolved more than one table",
        ));
    }

    let mut representative_indexes = Vec::<usize>::with_capacity(invocations.len());
    let mut key_indexes = Vec::with_capacity(invocations.len());
    let mut uses = Vec::<usize>::with_capacity(invocations.len());
    for (invocation_index, invocation) in invocations.iter().enumerate() {
        let key_index = representative_indexes.iter().position(|&index| {
            key_attributes_equal(&invocations[index].request.key, &invocation.request.key)
        });
        if let Some(key_index) = key_index {
            uses[key_index] += 1;
            key_indexes.push(key_index);
        } else {
            key_indexes.push(representative_indexes.len());
            representative_indexes.push(invocation_index);
            uses.push(1);
        }
    }

    let request = BatchGetItemRequest {
        request_items: HashMap::from([(
            table_name.clone(),
            KeysAndAttributes {
                keys: representative_indexes
                    .iter()
                    .map(|&index| invocations[index].request.key.clone())
                    .collect(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: first.request.consistent_read,
            },
        )]),
        return_consumed_capacity: None,
    };
    let mut response = read_context.execute_batch_get(request).await?;
    let returned = response
        .responses
        .get_or_insert_with(HashMap::new)
        .remove(&table_name)
        .unwrap_or_default();
    let mut items = vec![None; representative_indexes.len()];
    for item in returned {
        let key_index = representative_indexes
            .iter()
            .position(|&index| item_matches_key(&item, &invocations[index].request.key))
            .ok_or_else(|| {
                HttpApiError::internal_server_error(
                    "BatchGetItem returned an item outside the requested key set",
                )
            })?;
        if items[key_index].is_some() {
            return Err(HttpApiError::internal_server_error(
                "BatchGetItem returned the same key more than once",
            ));
        }
        let request = &first.request;
        items[key_index] = Some(project_attribute_map(
            item,
            request.projection_expression.as_deref(),
            request.attributes_to_get.as_deref(),
            request.expression_attribute_names.as_ref(),
        ));
    }

    let mut results = Vec::with_capacity(invocations.len());
    for (invocation, key_index) in invocations.into_iter().zip(key_indexes) {
        uses[key_index] -= 1;
        let item = if uses[key_index] == 0 {
            items[key_index].take()
        } else {
            items[key_index].clone()
        };
        results.push(ReadSequenceInvocationResult {
            ordinal: invocation.ordinal,
            input_refs: invocation.input_refs,
            result: ReadSequenceInvocationPayload::Get(GetItemResponse { item }),
        });
    }
    Ok(results)
}

fn item_matches_key(item: &AttributeMap, key: &KeyAttributes) -> bool {
    key.iter().all(|(name, value)| {
        item.get(name)
            .is_some_and(|item_value| attribute_values_equal(item_value, value))
    })
}

fn key_attributes_equal(left: &KeyAttributes, right: &KeyAttributes) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, value)| {
            right
                .get(name)
                .is_some_and(|right| attribute_values_equal(value, right))
        })
}

fn attribute_values_equal(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            normalize_dynamodb_number_for_write(left) == normalize_dynamodb_number_for_write(right)
        }
        _ => left == right,
    }
}

async fn execute_node_invocation(
    input: &NodeExecutionInput<'_>,
    bindings: InvocationBindings,
    ordinal: usize,
    cursor: Option<(u32, ExclusiveStartKey)>,
) -> Result<ReadSequenceInvocationResult, HttpApiError> {
    let bind_values = bindings.resolved_inputs()?;
    let mut operation =
        bind_operation(&input.node.operation, &bind_values).map_err(read_sequence_error)?;
    apply_node_query_limit(&mut operation, input);
    if let Some((cursor_ordinal, cursor)) = cursor
        && cursor_ordinal == ordinal as u32
        && let ReadSequenceNodeOperation::Query(request) = &mut operation
    {
        request.exclusive_start_key = Some(cursor);
    }
    let payload = execute_operation(operation, input.read_context, input.consistency).await?;
    Ok(ReadSequenceInvocationResult {
        ordinal: ordinal as u32,
        input_refs: bindings.into_input_refs(),
        result: payload,
    })
}

fn apply_node_query_limit(
    operation: &mut ReadSequenceNodeOperation,
    input: &NodeExecutionInput<'_>,
) {
    let ReadSequenceNodeOperation::Query(request) = operation else {
        return;
    };
    let default_limit = if input.node.inputs().is_empty() && input.node.after().is_empty() {
        input.context.limits.max_root_items
    } else {
        input.context.limits.max_intermediate_items
    };
    request.limit = Some(
        request
            .limit
            .unwrap_or(READ_SEQUENCE_DEFAULT_QUERY_ITEMS.min(default_limit)),
    );
}

pub(super) struct InvocationBindings {
    pub(super) values: BTreeMap<String, BoundInput>,
}

pub(super) struct BoundInput {
    pub(super) value: storage_types::AttributeValue,
    pub(super) reference: Option<storage_types::ReadSequenceInputReference>,
}

impl InvocationBindings {
    fn resolved_inputs(&self) -> Result<BTreeMap<String, ResolvedInput>, HttpApiError> {
        self.values
            .iter()
            .map(|(name, input)| {
                let reference = input.reference.clone().ok_or_else(|| {
                    HttpApiError::internal_server_error(
                        "ReadSequence invocation input reference is missing",
                    )
                })?;
                Ok((
                    name.clone(),
                    ResolvedInput {
                        value: input.value.clone(),
                        reference,
                    },
                ))
            })
            .collect()
    }

    fn into_input_refs(self) -> BTreeMap<String, storage_types::ReadSequenceInputReference> {
        self.values
            .into_iter()
            .filter_map(|(name, input)| input.reference.map(|reference| (name, reference)))
            .collect()
    }
}

fn invocation_bindings(
    node: &ReadSequenceNode,
    resolved: &ResolvedInputs,
) -> Result<Vec<InvocationBindings>, HttpApiError> {
    let Some(iterate) = node.iterate.as_deref() else {
        if resolved.values().any(Vec::is_empty) {
            return Ok(Vec::new());
        }
        let values = resolved
            .iter()
            .map(|(name, values)| {
                let value = values.first().ok_or_else(|| {
                    HttpApiError::internal_server_error(
                        "ReadSequence invocation binding is unexpectedly empty",
                    )
                })?;
                Ok((name.clone(), bound_input(value)))
            })
            .collect::<Result<_, HttpApiError>>()?;
        return Ok(vec![InvocationBindings { values }]);
    };
    iterated_invocation_bindings(resolved, iterate)
}

fn iterated_invocation_bindings(
    resolved: &ResolvedInputs,
    iterate: &str,
) -> Result<Vec<InvocationBindings>, HttpApiError> {
    let iterations = resolved.get(iterate).cloned().unwrap_or_default();
    if resolved
        .iter()
        .any(|(name, values)| name != iterate && values.is_empty())
    {
        return Ok(Vec::new());
    }
    let mut invocations = Vec::with_capacity(iterations.len());
    for iteration in iterations {
        let mut values = BTreeMap::new();
        for (name, inputs) in resolved {
            let input = if name == iterate {
                &iteration
            } else if let Some(input) = inputs.first() {
                input
            } else {
                return Err(HttpApiError::validation_error(
                    "ReadSequence input resolution skipped an invocation",
                ));
            };
            values.insert(name.clone(), bound_input(input));
        }
        invocations.push(InvocationBindings { values });
    }
    Ok(invocations)
}

fn bound_input(input: &ResolvedInput) -> BoundInput {
    BoundInput {
        value: input.value.clone(),
        reference: Some(input.reference.clone()),
    }
}

async fn execute_operation(
    operation: ReadSequenceNodeOperation,
    read_context: &ReadSequenceApiReadContext<'_>,
    consistency: ReadSequenceConsistency,
) -> Result<ReadSequenceInvocationPayload, HttpApiError> {
    match operation {
        ReadSequenceNodeOperation::Get(request) => {
            Ok(ReadSequenceInvocationPayload::Get(GetItemResponse {
                item: read_context
                    .execute_get(apply_get_consistency(request, consistency))
                    .await?,
            }))
        }
        ReadSequenceNodeOperation::BatchGet(request) => {
            let response = read_context
                .execute_batch_get(apply_batch_get_consistency(request, consistency))
                .await?;
            Ok(ReadSequenceInvocationPayload::BatchGet(response))
        }
        ReadSequenceNodeOperation::Query(request) => Ok(ReadSequenceInvocationPayload::Query(
            read_context
                .execute_query(apply_query_consistency(request, consistency))
                .await?,
        )),
    }
}

struct ReadSequenceApiReadContext<'a> {
    manager: &'a StorageApiManagerImpl,
    provider_context: Option<ReadSequenceReadContext>,
}

impl<'a> ReadSequenceApiReadContext<'a> {
    fn take_retryable_read_failure(&self) -> bool {
        self.provider_context
            .as_ref()
            .is_some_and(|context| context.take_retryable_read_failure())
    }

    async fn begin(
        manager: &'a StorageApiManagerImpl,
        consistency: ReadSequenceConsistency,
        limits: ReadSequenceReadLimits,
    ) -> Result<Self, HttpApiError> {
        if consistency == ReadSequenceConsistency::Transactional
            && !manager.read_sequence_capabilities.transactional_snapshots
        {
            return Err(read_sequence_error(
                ReadSequenceValidationError::UnsupportedConsistency { consistency },
            ));
        }
        let provider_context = if consistency == ReadSequenceConsistency::Transactional {
            Some(ReadSequenceReadContext::new(
                manager
                    .db()
                    .begin_read_sequence_read_context(consistency)
                    .await?,
                limits,
            ))
        } else {
            None
        };
        Ok(Self {
            manager,
            provider_context,
        })
    }

    async fn execute_get(
        &self,
        request: storage_types::GetItemRequest,
    ) -> Result<Option<AttributeMap>, HttpApiError> {
        if let Some(context) = self.provider_context.as_ref() {
            let item = context
                .get_item_as::<HashMap<String, AttributeValue>>(
                    request.table_name,
                    request.key,
                    request.consistent_read.unwrap_or(false),
                )
                .await?
                .map(|item| {
                    project_attribute_map(
                        item.into(),
                        request.projection_expression.as_deref(),
                        request.attributes_to_get.as_deref(),
                        request.expression_attribute_names.as_ref(),
                    )
                });
            return Ok(item);
        }
        match self.manager.get_item_internal(request).await? {
            Response::GetItem(response) => Ok(response.item),
            Response::GetWire(response) => Ok(response.into_get_item_response()?.item),
            _ => Err(HttpApiError::internal_server_error(
                "GetItem returned an unexpected response type",
            )),
        }
    }

    async fn execute_batch_get(
        &self,
        mut request: BatchGetItemRequest,
    ) -> Result<BatchGetItemResponse, HttpApiError> {
        let mut responses = HashMap::<storage_types::TableName, Vec<AttributeMap>>::new();
        for attempt in 0..READ_SEQUENCE_BATCH_GET_MAX_ATTEMPTS {
            let mut response = self.execute_batch_get_once(request).await?;
            for (table_name, mut items) in response.responses.take().unwrap_or_default() {
                responses.entry(table_name).or_default().append(&mut items);
            }
            let unprocessed_keys = response.unprocessed_keys.take().unwrap_or_default();
            if unprocessed_keys.values().all(|keys| keys.keys.is_empty()) {
                return Ok(BatchGetItemResponse {
                    responses: Some(responses),
                    unprocessed_keys: None,
                    consumed_capacity: None,
                });
            }
            if attempt + 1 == READ_SEQUENCE_BATCH_GET_MAX_ATTEMPTS {
                return unprocessed_batch_get_error(&unprocessed_keys);
            }
            metrics::counter!("storage.read_sequence.batch_get_retry.total").increment(1);
            tokio::time::sleep(read_sequence_batch_get_retry_delay(attempt)).await;
            request = BatchGetItemRequest {
                request_items: unprocessed_keys,
                return_consumed_capacity: None,
            };
        }
        Err(HttpApiError::internal_server_error(
            "ReadSequence BatchGetItem retry loop exited unexpectedly",
        ))
    }

    async fn execute_batch_get_once(
        &self,
        request: BatchGetItemRequest,
    ) -> Result<BatchGetItemResponse, HttpApiError> {
        if let Some(context) = self.provider_context.as_ref() {
            let shape = request.clone();
            let wire = context.batch_get_item(request).await?;
            let response = if batch_get_needs_decoded_response(&shape) {
                project_batch_get_response(wire, &shape)?
            } else {
                BatchGetWireResponse::from(add_empty_batch_get_response_tables(wire, &shape))
                    .into_batch_get_response()?
            };
            return Ok(response);
        }
        let response = match self.manager.batch_get_item_internal(request).await? {
            Response::BatchGetItem(response) => response,
            Response::BatchGetWire(response) => response.into_batch_get_response()?,
            _ => {
                return Err(HttpApiError::internal_server_error(
                    "BatchGetItem returned an unexpected response type",
                ));
            }
        };
        Ok(response)
    }

    async fn execute_query(
        &self,
        request: storage_types::QueryRequest,
    ) -> Result<QueryResponse, HttpApiError> {
        if let Some(context) = self.provider_context.as_ref() {
            return match self
                .manager
                .query_internal_with_read_context(request, context)
                .await?
            {
                Response::Query(response) => Ok(response),
                Response::QueryWire(response) => Ok(response.into_query_response()?),
                _ => Err(HttpApiError::internal_server_error(
                    "Query returned an unexpected response type",
                )),
            };
        }
        match self
            .manager
            .query_internal_for_read_sequence(request)
            .await?
        {
            Response::Query(response) => Ok(response),
            Response::QueryWire(response) => Ok(response.into_query_response()?),
            _ => Err(HttpApiError::internal_server_error(
                "Query returned an unexpected response type",
            )),
        }
    }
}

fn read_sequence_batch_get_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(
        READ_SEQUENCE_BATCH_GET_RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << attempt.min(8)),
    )
}

fn unprocessed_batch_get_error<T>(
    tables: &HashMap<storage_types::TableName, KeysAndAttributes>,
) -> Result<T, HttpApiError> {
    let count = tables.values().map(|keys| keys.keys.len()).sum::<usize>();
    Err(HttpApiError::throttled_error(
        "ThrottlingException",
        format!("ReadSequence BatchGetItem exhausted retries with {count} unprocessed key(s)"),
    ))
}

fn find_query_cursors(
    results: &[Option<ReadSequenceNodeResult>],
    wave: &[ReadSequenceNodeId],
) -> Vec<(ReadSequenceNodeId, u32, ExclusiveStartKey)> {
    wave.iter()
        .filter_map(|node_id| {
            results[node_id.index()].as_ref().and_then(|node| {
                node.invocations.iter().find_map(|invocation| {
                    if let ReadSequenceInvocationPayload::Query(response) = &invocation.result {
                        response.last_evaluated_key.clone().map(|cursor| {
                            (
                                *node_id,
                                invocation.ordinal,
                                ExclusiveStartKey::from(cursor),
                            )
                        })
                    } else {
                        None
                    }
                })
            })
        })
        .collect::<Vec<_>>()
}

fn apply_get_consistency(
    mut request: storage_types::GetItemRequest,
    consistency: ReadSequenceConsistency,
) -> storage_types::GetItemRequest {
    if requires_consistent_read(consistency) {
        request.consistent_read = Some(true);
    }
    request
}

fn apply_batch_get_consistency(
    mut request: storage_types::BatchGetItemRequest,
    consistency: ReadSequenceConsistency,
) -> storage_types::BatchGetItemRequest {
    if requires_consistent_read(consistency) {
        for keys in request.request_items.values_mut() {
            keys.consistent_read = Some(true);
        }
    }
    request
}

fn apply_query_consistency(
    mut request: storage_types::QueryRequest,
    consistency: ReadSequenceConsistency,
) -> storage_types::QueryRequest {
    if requires_consistent_read(consistency) && request.index_name.is_none() {
        request.consistent_read = Some(true);
    }
    request
}

fn requires_consistent_read(consistency: ReadSequenceConsistency) -> bool {
    matches!(
        consistency,
        ReadSequenceConsistency::Strong | ReadSequenceConsistency::Transactional
    )
}

fn enforce_response_limit(
    response: &ReadSequenceResponse,
    limit: Option<u32>,
) -> Result<(), HttpApiError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let bytes = serde_json::to_vec(response).map_err(|error| {
        HttpApiError::from(StorageError::internal(&format!(
            "serialize ReadSequence response: {error}"
        )))
    })?;
    if bytes.len() > limit as usize {
        return Err(HttpApiError::validation_error(
            "ReadSequence response exceeds MaxResponseBytes",
        ));
    }
    Ok(())
}

fn completed_output_nodes(
    plan: &storage_types::ReadSequencePlan,
    completed: &[Option<ReadSequenceNodeResult>],
    resume: Option<&ReadSequenceToken>,
) -> Vec<ReadSequenceNodeResult> {
    plan.graph
        .outputs
        .iter()
        .filter(|node_id| {
            !resume.is_some_and(|token| token.completed_nodes.contains(&node_id.index()))
        })
        .filter_map(|node_id| completed[node_id.index()].as_ref())
        .cloned()
        .collect()
}

fn read_sequence_error(error: ReadSequenceValidationError) -> HttpApiError {
    if let ReadSequenceValidationError::GraphResolutionInvariant { remaining } = error {
        return HttpApiError::from(StorageError::internal(&format!(
            "ReadSequence graph resolution invariant failed with {remaining} unresolved node(s)"
        )));
    }
    HttpApiError::from(StorageError::from(error))
}

pub(super) fn read_sequence_consumed_capacity(
    return_consumed_capacity: Option<&str>,
    read_count: u32,
) -> Option<serde_json::Value> {
    // The provider facade currently exposes item results, not backend billing
    // units.  Until providers return their measured capacity, this is a
    // provider-neutral item-count estimate for response-shape compatibility,
    // not a billing-accurate DynamoDB consumed-capacity value.
    match return_consumed_capacity? {
        "TOTAL" | "INDEXES" => {
            let units = f64::from(read_count).max(0.5);
            Some(serde_json::json!({
                "TableName": "ReadSequence",
                "CapacityUnits": units,
                "ReadCapacityUnits": units
            }))
        }
        "NONE" => None,
        _ => None,
    }
}

#[cfg(test)]
pub(super) async fn execute_wave_for_test(
    manager: &StorageApiManagerImpl,
    request: &ReadSequenceRequest,
    provider_context: Box<dyn StorageProviderReadContext>,
) -> Result<Vec<(ReadSequenceNodeId, ReadSequenceNodeResult)>, HttpApiError> {
    let plan = storage_types::plan_read_sequence(request).map_err(read_sequence_error)?;
    let wave =
        plan.graph.waves.first().ok_or_else(|| {
            HttpApiError::internal_server_error("ReadSequence test plan has no wave")
        })?;
    let completed = vec![None; plan.nodes.len()];
    let active_nodes = (0..plan.nodes.len())
        .map(ReadSequenceNodeId::from_index)
        .collect::<BTreeSet<_>>();
    let root_nodes = plan
        .graph
        .dependencies
        .iter()
        .enumerate()
        .filter_map(|(index, dependencies)| {
            dependencies
                .is_empty()
                .then_some(ReadSequenceNodeId::from_index(index))
        })
        .collect::<BTreeSet<_>>();
    let context = ReadSequenceWaveContext {
        nodes: &plan.nodes,
        completed: &completed,
        node_names: &plan.graph.node_names,
        root_nodes: &root_nodes,
        resume: None,
        limits: read_sequence_budget_limits(request),
        active_nodes: &active_nodes,
    };
    let read_context = ReadSequenceApiReadContext {
        manager,
        provider_context: Some(ReadSequenceReadContext::new(
            provider_context,
            ReadSequenceReadLimits::from_request(request),
        )),
    };
    execute_wave(wave, &context, &read_context, request.read_consistency).await
}

#[cfg(test)]
pub(super) async fn execute_ordinary_read_sequence_for_test(
    manager: &StorageApiManagerImpl,
    request: &ReadSequenceRequest,
    provider_context: Box<dyn StorageProviderReadContext>,
) -> Result<ReadSequenceResponse, HttpApiError> {
    let plan = storage_types::plan_read_sequence(request).map_err(read_sequence_error)?;
    let read_context = ReadSequenceApiReadContext {
        manager,
        provider_context: Some(ReadSequenceReadContext::new(
            provider_context,
            ReadSequenceReadLimits::from_request(request),
        )),
    };
    OrdinaryReadSequenceExecution::new(
        ReadSequencePlanInput {
            request,
            plan: &plan,
            resume: None,
            request_digest: "read-sequence-test",
        },
        &read_context,
    )
    .run()
    .await
}
