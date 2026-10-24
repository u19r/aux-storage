//! One validated ReadSequence graph over a caller-owned read context.

use std::{cmp::Reverse, collections::BTreeSet};

use async_trait::async_trait;
use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, ExclusiveStartKey, GetItemRequest, GetItemResponse,
    KeyAttributes, QueryRequest, QueryResponse, READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP,
    READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS, READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS,
    READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS, ReadSequenceInvocationPayload, ReadSequenceNodeId,
    ReadSequencePlan, ReadSequenceQueryContinuation, ReadSequenceReplayPage, ReadSequenceRequest,
    ReadSequenceResponse, ReadSequenceToken, ReadSequenceValidationCapabilities,
    ReadSequenceValidationError, StorageError, StorageResult, encode_read_sequence_token,
    invocation_bindings, plan_read_sequence_with_capabilities, prepare_resume_token,
    read_sequence_request_digest, resolve_inputs, validate_resume_token_shape,
};

use super::{
    node::{NodeExecution, NodeOutcome},
    response::{enforce_response_limit, response},
};
use crate::{
    CachePublication, DynamoRangeReadAttempt, GetItemAttempt, ReadSequenceStepReader,
    TableMetadataCache, batch_get_item_in_attempt,
    dynamo_range_read::query_for_read_sequence_in_attempt, get_item_in_attempt,
};

struct Execution<'a> {
    request: &'a ReadSequenceRequest,
    plan: ReadSequencePlan,
    resume: Option<ReadSequenceToken>,
    request_digest: String,
    active: BTreeSet<ReadSequenceNodeId>,
    outcomes: Vec<Option<NodeOutcome>>,
    completed: BTreeSet<usize>,
    replay_pages: Vec<ReadSequenceReplayPage>,
    query_continuations: Vec<ReadSequenceQueryContinuation>,
    total_read_items: u32,
    publications: Vec<CachePublication>,
}

/// Executes every selected graph node within the same native read attempt.
/// The caller commits the attempt before publishing returned cache entries.
pub async fn read_sequence_in_attempt<A>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: ReadSequenceRequest,
    capabilities: ReadSequenceValidationCapabilities,
) -> StorageResult<(ReadSequenceResponse, Vec<CachePublication>)>
where
    A: GetItemAttempt + DynamoRangeReadAttempt,
{
    let mut reader = NativeReadSequenceReader { attempt, cache };
    read_sequence_with_reader(&mut reader, request, capabilities).await
}

struct NativeReadSequenceReader<'a, A> {
    attempt: &'a mut A,
    cache: &'a TableMetadataCache,
}

#[async_trait]
impl<A> ReadSequenceStepReader for NativeReadSequenceReader<'_, A>
where A: GetItemAttempt + DynamoRangeReadAttempt + Send
{
    async fn get(
        &mut self,
        request: GetItemRequest,
    ) -> StorageResult<(GetItemResponse, Option<CachePublication>)> {
        get_item_in_attempt(self.attempt, self.cache, &request).await
    }

    async fn batch_get_once(
        &mut self,
        request: BatchGetItemRequest,
    ) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
        batch_get_item_in_attempt(self.attempt, self.cache, request).await
    }

    async fn query(
        &mut self,
        request: QueryRequest,
    ) -> StorageResult<(
        QueryResponse,
        Option<Vec<KeyAttributes>>,
        Option<CachePublication>,
    )> {
        let (page, publication) =
            query_for_read_sequence_in_attempt(self.attempt, self.cache, request).await?;
        Ok((page.response, page.item_cursors, publication))
    }
}

/// Shared ReadSequence graph, limits, and continuation semantics for native
/// attempts and remote Dynamo operation calls.
pub async fn read_sequence_with_reader<R: ReadSequenceStepReader>(
    reader: &mut R,
    mut request: ReadSequenceRequest,
    capabilities: ReadSequenceValidationCapabilities,
) -> StorageResult<(ReadSequenceResponse, Vec<CachePublication>)> {
    let plan = plan_read_sequence_with_capabilities(&request, capabilities)?;
    let (resume, digest) = prepare_resume_token(&mut request, &plan.graph.structural_digest)?;
    if let Some(token) = &resume {
        validate_resume_token_shape(token, &plan)?;
    }
    let request_digest = match digest {
        Some(digest) => digest,
        None => read_sequence_request_digest(&request)?,
    };
    let mut execution = Execution::new(&request, plan, resume, request_digest);
    let next = execution.run(reader).await?;
    let prior_completed = execution
        .resume
        .as_ref()
        .map_or(&[][..], |token| token.completed_nodes.as_slice());
    let result = response(
        execution.request,
        &execution.plan,
        &execution.outcomes,
        prior_completed,
        execution.total_read_items,
        next,
    );
    enforce_response_limit(execution.request, &result)?;
    Ok((result, execution.publications))
}

impl<'a> Execution<'a> {
    fn new(
        request: &'a ReadSequenceRequest,
        plan: ReadSequencePlan,
        resume: Option<ReadSequenceToken>,
        request_digest: String,
    ) -> Self {
        let active = resume.as_ref().map_or_else(
            || {
                (0..plan.nodes.len())
                    .map(ReadSequenceNodeId::from_index)
                    .collect()
            },
            |token| active_nodes(&plan, token),
        );
        let completed = resume.as_ref().map_or_else(BTreeSet::new, |token| {
            token.completed_nodes.iter().copied().collect()
        });
        let replay_pages = resume
            .as_ref()
            .and_then(|token| token.replay_pages.clone())
            .unwrap_or_default();
        let outcomes = (0..plan.nodes.len()).map(|_| None).collect();
        Self {
            request,
            plan,
            resume,
            request_digest,
            active,
            outcomes,
            completed,
            replay_pages,
            query_continuations: Vec::new(),
            total_read_items: 0,
            publications: Vec::new(),
        }
    }

    async fn run<R: ReadSequenceStepReader>(
        &mut self,
        reader: &mut R,
    ) -> StorageResult<Option<String>> {
        for node_id in dependent_first_order(&self.plan)? {
            if !self.active.contains(&node_id) {
                continue;
            }
            if let Some(parent) = self.replay_pages.last()
                && node_id.index() != parent.node_ordinal
                && !depends_on(
                    &self.plan.graph.dependencies,
                    node_id.index(),
                    parent.node_ordinal,
                )
                && !depends_on(
                    &self.plan.graph.dependencies,
                    parent.node_ordinal,
                    node_id.index(),
                )
            {
                continue;
            }
            let Some((execution, replay)) = self.reserve_node(node_id)? else {
                if !self.replay_pages.is_empty() {
                    return Err(ReadSequenceValidationError::TotalReadLimitExceeded {
                        actual: self.total_read_items.saturating_add(1),
                        limit: self
                            .request
                            .max_total_read_items
                            .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS),
                    }
                    .into());
                }
                return self.frontier(node_id.index(), None);
            };
            let page_start = (
                execution.resume_ordinal.unwrap_or(0),
                execution.cursor.clone(),
            );
            let mut outcome = execution.execute(reader).await?;
            self.record_item_count(
                outcome.item_count,
                replay,
                outcome.continuation.is_some(),
                node_id,
            )?;
            let continuation = outcome.continuation.clone();
            self.publications.append(&mut outcome.publications);
            self.outcomes[node_id.index()] = Some(outcome);
            if let Some(cursor) = continuation {
                if !self.plan.graph.consumers[node_id.index()].is_empty() {
                    self.completed.insert(node_id.index());
                    self.remember_page(node_id, page_start, Some(cursor));
                    continue;
                }
                if self.independent_root_queries()
                    && let Some(query_cursor) = cursor.1.clone()
                {
                    self.query_continuations
                        .push(ReadSequenceQueryContinuation {
                            node_ordinal: node_id.index(),
                            invocation_ordinal: cursor.0,
                            query_cursor,
                        });
                    if self.response_for(None).is_ok() {
                        continue;
                    }
                    self.query_continuations.pop();
                    if !self.query_continuations.is_empty() {
                        let count = self.outcomes[node_id.index()]
                            .as_ref()
                            .map_or(0, |outcome| outcome.item_count);
                        self.outcomes[node_id.index()] = None;
                        self.total_read_items -= count;
                        return self.frontier(node_id.index(), None);
                    }
                }
                let token = self.frontier(node_id.index(), Some(cursor))?;
                return self.fit_response(node_id, token);
            }
            self.completed.insert(node_id.index());
            self.remember_page(node_id, page_start, None);
            if self.response_for(None).is_err() {
                if !self.replay_pages.is_empty() {
                    self.response_for(None)?;
                }
                return self.fit_response(node_id, None);
            }
        }
        self.finish_pending_pages()
    }

    fn remember_page(
        &mut self,
        node_id: ReadSequenceNodeId,
        start: (u32, Option<ExclusiveStartKey>),
        next: Option<(u32, Option<ExclusiveStartKey>)>,
    ) {
        let index = node_id.index();
        if !matches!(
            self.plan.nodes[index].operation,
            storage_types::ReadSequenceNodeOperation::Query(_)
        ) || self.plan.graph.consumers[index].is_empty()
            || next.is_none() && start.0 == 0 && start.1.is_none()
            || self
                .replay_pages
                .iter()
                .any(|page| page.node_ordinal == index)
        {
            return;
        }
        self.replay_pages.push(ReadSequenceReplayPage {
            node_ordinal: index,
            start_invocation_ordinal: start.0,
            start_cursor: start.1,
            next_invocation_ordinal: next.as_ref().map(|cursor| cursor.0),
            next_cursor: next.and_then(|cursor| cursor.1),
        });
    }

    fn finish_pending_pages(&mut self) -> StorageResult<Option<String>> {
        if let Some(first) = self.query_continuations.first() {
            let token = self.frontier(first.node_ordinal, None)?;
            self.response_for(token.clone())?;
            return Ok(token);
        }
        while let Some(page) = self.replay_pages.pop() {
            if let Some(ordinal) = page.next_invocation_ordinal {
                let token = self.frontier(page.node_ordinal, Some((ordinal, page.next_cursor)))?;
                self.response_for(token.clone())?;
                return Ok(token);
            }
        }
        if let Some(node) = dependent_first_order(&self.plan)?
            .into_iter()
            .find(|node| self.active.contains(node) && !self.completed.contains(&node.index()))
        {
            let token = self.frontier(node.index(), None)?;
            self.response_for(token.clone())?;
            return Ok(token);
        }
        Ok(None)
    }

    fn reserve_node(
        &self,
        node_id: ReadSequenceNodeId,
    ) -> StorageResult<Option<(NodeExecution<'_>, bool)>> {
        // Replayed ancestors bind pending children. Charging them a second
        // time can leave the child with no budget on every continuation page.
        let replay = self
            .resume
            .as_ref()
            .is_some_and(|token| token.completed_nodes.contains(&node_id.index()));
        let total_limit = self
            .request
            .max_total_read_items
            .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS);
        let remaining = if replay {
            total_limit
        } else {
            total_limit.saturating_sub(self.total_read_items)
        };
        if !replay && remaining == 0 && self.total_read_items > 0 {
            return Ok(None);
        }
        let execution = self.node_execution(node_id, remaining)?;
        let reservation = execution.reservation()?;
        if !replay && reservation > remaining {
            if self.total_read_items == 0 {
                return Err(ReadSequenceValidationError::TotalReadLimitExceeded {
                    actual: reservation,
                    limit: total_limit,
                }
                .into());
            }
            return Ok(None);
        }
        Ok(Some((execution, replay)))
    }

    fn record_item_count(
        &mut self,
        count: u32,
        replay: bool,
        continued: bool,
        node_id: ReadSequenceNodeId,
    ) -> StorageResult<()> {
        if replay {
            let expected_continuation = self
                .replay_pages
                .iter()
                .find(|page| page.node_ordinal == node_id.index())
                .is_some_and(|page| page.next_invocation_ordinal.is_some());
            if continued != expected_continuation {
                return Err(ReadSequenceValidationError::StaleToken.into());
            }
            return Ok(());
        }
        let limit = self
            .request
            .max_total_read_items
            .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS);
        self.total_read_items = self.total_read_items.checked_add(count).ok_or(
            ReadSequenceValidationError::TotalReadLimitExceeded {
                actual: u32::MAX,
                limit,
            },
        )?;
        if self.total_read_items > limit {
            return Err(ReadSequenceValidationError::TotalReadLimitExceeded {
                actual: self.total_read_items,
                limit,
            }
            .into());
        }
        Ok(())
    }

    fn response_for(&self, token: Option<String>) -> StorageResult<ReadSequenceResponse> {
        let prior_completed = self
            .resume
            .as_ref()
            .map_or(&[][..], |resume| resume.completed_nodes.as_slice());
        let result = response(
            self.request,
            &self.plan,
            &self.outcomes,
            prior_completed,
            self.total_read_items,
            token,
        );
        enforce_response_limit(self.request, &result)?;
        Ok(result)
    }

    fn fit_response(
        &mut self,
        node_id: ReadSequenceNodeId,
        token: Option<String>,
    ) -> StorageResult<Option<String>> {
        if self.response_for(token.clone()).is_ok() {
            return Ok(token);
        }
        // A completed ancestor is replayed only to bind pending children. It
        // was charged and returned on an earlier page, so trimming it here
        // would subtract uncharged items and could return it twice.
        if self
            .resume
            .as_ref()
            .is_some_and(|resume| resume.completed_nodes.contains(&node_id.index()))
        {
            return self.response_for(token).map(|_| None);
        }
        if self.query_continuations.is_empty()
            && let Some(token) = self.fit_query_prefix(node_id)?
        {
            return Ok(Some(token));
        }
        if self.has_prior_output(node_id) {
            let count = self.outcomes[node_id.index()]
                .as_ref()
                .map_or(0, |outcome| outcome.item_count);
            self.outcomes[node_id.index()] = None;
            self.completed.remove(&node_id.index());
            self.total_read_items -= count;
            let token = self.frontier(node_id.index(), None)?;
            self.response_for(token.clone())?;
            return Ok(token);
        }
        self.response_for(token).map(|_| None)
    }

    fn has_prior_output(&self, node_id: ReadSequenceNodeId) -> bool {
        self.plan.graph.outputs.iter().any(|output| {
            output.index() != node_id.index()
                && self.outcomes[output.index()].is_some()
                && !self
                    .resume
                    .as_ref()
                    .is_some_and(|token| token.completed_nodes.contains(&output.index()))
        })
    }

    fn fit_query_prefix(&mut self, node_id: ReadSequenceNodeId) -> StorageResult<Option<String>> {
        if !self.plan.graph.outputs.contains(&node_id)
            || !matches!(
                self.plan.nodes[node_id.index()].operation,
                storage_types::ReadSequenceNodeOperation::Query(_)
            )
        {
            return Ok(None);
        }
        let index = node_id.index();
        let Some(outcome) = self.outcomes[index].as_ref() else {
            return Ok(None);
        };
        let original = outcome.result.clone();
        let item_cursors = outcome.item_cursors.clone();
        let original_count = outcome.item_count;
        let original_total = self.total_read_items;
        for kept in (1..original.invocations.len()).rev() {
            let mut candidate = original.clone();
            candidate.invocations.truncate(kept);
            let reduced = candidate
                .invocations
                .iter()
                .map(|invocation| invocation.result.item_count())
                .sum::<u32>();
            self.outcomes[index]
                .as_mut()
                .ok_or_else(|| StorageError::internal("read sequence query outcome disappeared"))?
                .result = candidate;
            self.total_read_items = original_total - original_count + reduced;
            let next_ordinal = original.invocations[kept].ordinal;
            let token = self.frontier(index, Some((next_ordinal, None)))?;
            if self.response_for(token.clone()).is_ok() {
                self.completed.remove(&index);
                return Ok(token);
            }
        }
        for (invocation_index, (invocation, cursors)) in original
            .invocations
            .iter()
            .zip(&item_cursors)
            .enumerate()
            .rev()
        {
            let ReadSequenceInvocationPayload::Query(query) = &invocation.result else {
                continue;
            };
            let Some((items, cursors)) = query.items.as_ref().zip(cursors.as_ref()) else {
                continue;
            };
            if items.len() < 2
                || items.len() != cursors.len()
                || query.count as usize != items.len()
            {
                continue;
            }
            for prefix in (1..items.len()).rev() {
                let cursor = cursors[prefix - 1].clone();
                let mut candidate = original.clone();
                candidate.invocations.truncate(invocation_index + 1);
                let invocation = &mut candidate.invocations[invocation_index];
                let invocation_ordinal = invocation.ordinal;
                if let ReadSequenceInvocationPayload::Query(query) = &mut invocation.result {
                    query.items = Some(items[..prefix].to_vec());
                    query.count = prefix as u32;
                    query.last_evaluated_key = Some(cursor.clone());
                }
                let reduced = candidate
                    .invocations
                    .iter()
                    .map(|invocation| invocation.result.item_count())
                    .sum::<u32>();
                self.outcomes[index]
                    .as_mut()
                    .ok_or_else(|| {
                        StorageError::internal("read sequence query outcome disappeared")
                    })?
                    .result = candidate;
                self.total_read_items = original_total - original_count + reduced;
                let token = self.frontier(
                    index,
                    Some((invocation_ordinal, Some(ExclusiveStartKey::Key(cursor)))),
                )?;
                if self.response_for(token.clone()).is_ok() {
                    self.completed.remove(&index);
                    return Ok(token);
                }
            }
        }
        self.outcomes[index]
            .as_mut()
            .ok_or_else(|| StorageError::internal("read sequence query outcome disappeared"))?
            .result = original;
        self.total_read_items = original_total;
        Ok(None)
    }

    fn node_execution(
        &self,
        node_id: ReadSequenceNodeId,
        remaining_total: u32,
    ) -> StorageResult<NodeExecution<'_>> {
        let index = node_id.index();
        let node = &self.plan.nodes[index];
        let results = self
            .outcomes
            .iter()
            .map(|outcome| outcome.as_ref().map(|outcome| &outcome.result))
            .collect::<Vec<_>>();
        let resolved = resolve_inputs(node, &results, &self.plan.graph.node_names)?;
        let bindings = invocation_bindings(node, &resolved)?;
        let root = self.plan.graph.dependencies[index].is_empty();
        let item_limit = if root {
            self.request
                .max_root_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS)
        } else {
            self.request
                .max_intermediate_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS)
        };
        let item_limit = if matches!(
            node.operation,
            storage_types::ReadSequenceNodeOperation::Query(_)
        ) {
            item_limit.min(remaining_total)
        } else {
            item_limit
        };
        Ok(NodeExecution {
            node,
            bindings,
            consistency: self.request.read_consistency,
            resume_ordinal: self.resume_ordinal(index),
            cursor: self.resume_cursor(index),
            item_limit,
            fanout_limit: self
                .request
                .max_fanout_per_step
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP),
        })
    }

    fn resume_ordinal(&self, index: usize) -> Option<u32> {
        if let Some(page) = self
            .replay_pages
            .iter()
            .find(|page| page.node_ordinal == index)
        {
            return Some(page.start_invocation_ordinal);
        }
        let token = self.resume.as_ref()?;
        if let Some(continuation) = token
            .query_continuations
            .as_ref()
            .and_then(|continuations| {
                continuations
                    .iter()
                    .find(|continuation| continuation.node_ordinal == index)
            })
        {
            return Some(continuation.invocation_ordinal);
        }
        if token.next_node_ordinal != index {
            return None;
        }
        token.invocation_ordinal
    }

    fn resume_cursor(&self, index: usize) -> Option<storage_types::ExclusiveStartKey> {
        if let Some(page) = self
            .replay_pages
            .iter()
            .find(|page| page.node_ordinal == index)
        {
            return page.start_cursor.clone();
        }
        let token = self.resume.as_ref()?;
        if let Some(continuation) = token
            .query_continuations
            .as_ref()
            .and_then(|continuations| {
                continuations
                    .iter()
                    .find(|continuation| continuation.node_ordinal == index)
            })
        {
            return Some(continuation.query_cursor.clone());
        }
        (token.next_node_ordinal == index)
            .then(|| token.query_cursor.clone())
            .flatten()
    }

    fn frontier(
        &self,
        next: usize,
        cursor: Option<(u32, Option<storage_types::ExclusiveStartKey>)>,
    ) -> StorageResult<Option<String>> {
        let mut token = ReadSequenceToken::new(
            &self.request_digest,
            &self.plan.graph.structural_digest,
            self.request.read_consistency,
        );
        token.next_node_ordinal = self
            .query_continuations
            .first()
            .map_or(next, |continuation| continuation.node_ordinal);
        token.query_continuations =
            (!self.query_continuations.is_empty()).then(|| self.query_continuations.clone());
        token.replay_pages = (!self.replay_pages.is_empty()).then(|| self.replay_pages.clone());
        token.completed_nodes = self.completed.iter().copied().collect();
        if let Some((ordinal, query_cursor)) = cursor {
            token.invocation_ordinal = Some(ordinal);
            token.query_cursor = query_cursor;
            token.completed_nodes.retain(|index| {
                *index != next && !depends_on(&self.plan.graph.dependencies, *index, next)
            });
        }
        Ok(Some(encode_read_sequence_token(&token)?))
    }

    fn independent_root_queries(&self) -> bool {
        self.plan.nodes.iter().enumerate().all(|(index, node)| {
            self.plan.graph.dependencies[index].is_empty()
                && matches!(
                    node.operation,
                    storage_types::ReadSequenceNodeOperation::Query(_)
                )
        })
    }
}

fn depends_on(dependencies: &[Vec<ReadSequenceNodeId>], node: usize, ancestor: usize) -> bool {
    dependencies[node].iter().any(|dependency| {
        dependency.index() == ancestor || depends_on(dependencies, dependency.index(), ancestor)
    })
}

fn dependent_first_order(plan: &ReadSequencePlan) -> StorageResult<Vec<ReadSequenceNodeId>> {
    let mut depths = vec![0; plan.nodes.len()];
    for node in &plan.graph.topological_order {
        depths[node.index()] = plan.graph.dependencies[node.index()]
            .iter()
            .map(|dependency| depths[dependency.index()] + 1)
            .max()
            .unwrap_or(0);
    }
    let mut remaining = plan.graph.topological_order.clone();
    let mut ordered = Vec::with_capacity(remaining.len());
    let mut scheduled = BTreeSet::new();
    while !remaining.is_empty() {
        let next = remaining
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                plan.graph.dependencies[node.index()]
                    .iter()
                    .all(|dependency| scheduled.contains(dependency))
            })
            .max_by_key(|(_, node)| (depths[node.index()], Reverse(node.index())))
            .map(|(index, _)| index)
            .ok_or_else(|| StorageError::internal("read sequence graph has no ready node"))?;
        let node = remaining.remove(next);
        scheduled.insert(node);
        ordered.push(node);
    }
    Ok(ordered)
}

fn active_nodes(
    plan: &ReadSequencePlan,
    token: &ReadSequenceToken,
) -> BTreeSet<ReadSequenceNodeId> {
    let mut active = (0..plan.nodes.len())
        .filter(|index| !token.completed_nodes.contains(index))
        .map(ReadSequenceNodeId::from_index)
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
