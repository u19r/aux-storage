use std::collections::BTreeMap;

use http_error::HttpApiError;
use storage_provider::StorageProviderReadContext;
use storage_types::{
    AttributeMap, AttributeValue, BatchGetItemResponse, KeyAttributes, ParsedReadSequenceSelector,
    QueryResponse, READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS,
    READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS, ReadSequenceConsistency, ReadSequenceForEach,
    ReadSequenceItemResult, ReadSequenceJoinResult, ReadSequenceJoinType, ReadSequenceOnMissing,
    ReadSequenceRequest, ReadSequenceResponse, ReadSequenceRootResponse,
    ReadSequenceSelectedContext, ReadSequenceStep, ReadSequenceValidationError, StorageEnum,
    StorageError, bind_read_sequence_attribute_value, plan_non_covering_lookup,
    plan_read_sequence_with_capabilities,
};

use crate::{
    batch_get_wire_response::BatchGetWireResponse,
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_batch_get_item::{
            add_empty_batch_get_response_tables, batch_get_needs_decoded_response,
            project_batch_get_response,
        },
        storage_manager_impl_expression::project_attribute_map,
        storage_manager_impl_read_sequence_token::{
            ReadSequenceToken, encode_read_sequence_token, prepare_resume_token,
            read_sequence_request_digest, read_sequence_step_metadata_digest,
            read_sequence_token_expiration,
        },
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn read_sequence_internal(
        &self,
        mut request: ReadSequenceRequest,
    ) -> Result<Response, HttpApiError> {
        request
            .validate_with_capabilities(self.read_sequence_capabilities.validation_capabilities())
            .map_err(read_sequence_error)?;
        if request.read_consistency == storage_types::ReadSequenceConsistency::Transactional
            && !self.read_sequence_capabilities.transactional_snapshots
        {
            return Err(read_sequence_error(
                ReadSequenceValidationError::UnsupportedConsistency {
                    consistency: request.read_consistency,
                },
            ));
        }
        let resume = prepare_resume_token(&mut request)?;
        let plan = plan_read_sequence_with_capabilities(
            &request,
            &Default::default(),
            self.read_sequence_capabilities.validation_capabilities(),
        )
        .map_err(read_sequence_error)?;
        let request_digest = match &resume {
            Some(token) => token.request_digest.clone(),
            None => read_sequence_request_digest(&request)?,
        };
        let limits = ReadSequenceExecutionLimits::from_request(&request);
        let read_context =
            ReadSequenceApiReadContext::begin(self, request.read_consistency).await?;
        let mut responses = Vec::with_capacity(request.sequence.len());
        let mut executed_steps = BTreeMap::<String, ExecutedStep>::new();
        let mut total_read_items = 0u32;
        let mut next_sequence_token = None;

        for (step_index, step) in request.sequence.iter().enumerate() {
            if let Some(for_each) = &step.for_each {
                let child_resume = resume
                    .as_ref()
                    .filter(|token| token.step_index == step_index && token.parent_index.is_some());
                let continuation = ChildContinuationContext {
                    step,
                    step_index,
                    resume: child_resume,
                    request_digest: &request_digest,
                    consistency: request.read_consistency,
                };
                let child_execution = if for_each.get.is_some() {
                    self.execute_read_sequence_for_each_get(
                        &mut responses,
                        &executed_steps,
                        for_each,
                        &limits,
                        &mut total_read_items,
                        ChildExecutionContext {
                            continuation,
                            read_context: &read_context,
                        },
                    )
                    .await?
                } else if for_each.batch_get.is_some() {
                    self.execute_read_sequence_for_each_batch_get(
                        &mut responses,
                        &executed_steps,
                        for_each,
                        &limits,
                        &mut total_read_items,
                        ChildExecutionContext {
                            continuation,
                            read_context: &read_context,
                        },
                    )
                    .await?
                } else if for_each.query.is_some() {
                    self.execute_read_sequence_for_each_query(
                        &mut responses,
                        &executed_steps,
                        for_each,
                        &limits,
                        &mut total_read_items,
                        ChildExecutionContext {
                            continuation,
                            read_context: &read_context,
                        },
                    )
                    .await?
                } else {
                    return Err(read_sequence_error(
                        ReadSequenceValidationError::InvalidForEachOperation,
                    ));
                };
                next_sequence_token = child_execution.next_sequence_token;
                executed_steps.insert(
                    step.name.clone(),
                    ExecutedStep {
                        response_index: None,
                        items: child_execution.items,
                    },
                );
                if next_sequence_token.is_some() {
                    break;
                }
            } else {
                let root = self
                    .execute_read_sequence_root_step(
                        step,
                        &limits,
                        &mut total_read_items,
                        request.read_consistency,
                        &read_context,
                    )
                    .await?;
                if let Some(next_start_key) = root.next_start_key {
                    next_sequence_token = Some(encode_read_sequence_token(&ReadSequenceToken {
                        version: 1,
                        request_digest: request_digest.clone(),
                        metadata_digest: read_sequence_step_metadata_digest(step)?,
                        step_index,
                        parent_index: None,
                        expires_at_epoch_seconds: read_sequence_token_expiration(),
                        exclusive_start_key: Some(next_start_key),
                    })?);
                }
                let response = root.response;
                let executed_items = root.executed_items;
                enforce_intermediate_item_limit(&response, limits.max_intermediate_items)?;
                let response_index = responses.len();
                responses.push(response);
                executed_steps.insert(
                    step.name.clone(),
                    ExecutedStep {
                        response_index: Some(response_index),
                        items: executed_items,
                    },
                );
                self.maybe_run_read_sequence_after_root_step_hook_for_test()
                    .await?;
                if next_sequence_token.is_some() {
                    break;
                }
            }
        }

        let partial = next_sequence_token.is_some();
        Ok(Response::ReadSequence(ReadSequenceResponse {
            responses,
            warning: plan.warning,
            consumed_capacity: read_sequence_consumed_capacity(
                request.return_consumed_capacity.as_deref(),
                total_read_items,
            ),
            next_sequence_token,
            read_consistency: request.read_consistency,
            partial,
        }))
    }

    async fn execute_read_sequence_root_step(
        &self,
        step: &ReadSequenceStep,
        limits: &ReadSequenceExecutionLimits,
        total_read_items: &mut u32,
        consistency: ReadSequenceConsistency,
        read_context: &ReadSequenceApiReadContext<'_>,
    ) -> Result<RootStepExecution, HttpApiError> {
        if let Some(get) = &step.get {
            let item = read_context
                .execute_read_sequence_get(apply_get_consistency(get.clone(), consistency))
                .await?;
            add_read_items(
                total_read_items,
                usize::from(item.is_some()),
                limits.max_total_read_items,
            )?;
            let executed_items = item
                .as_ref()
                .map(|item| executed_item(step, item, Some(ResponseSlot::RootItem)))
                .transpose()?
                .into_iter()
                .collect();
            return Ok(RootStepExecution {
                response: read_sequence_get_response(&step.name, item),
                executed_items,
                next_start_key: None,
            });
        }

        if let Some(batch_get) = &step.batch_get {
            let items = read_context
                .execute_read_sequence_batch_get(apply_batch_get_consistency(
                    batch_get.clone(),
                    consistency,
                ))
                .await?;
            add_read_items(total_read_items, items.len(), limits.max_total_read_items)?;
            let executed_items = executed_items_for_step(step, &items, ResponseSlot::Items)?;
            return Ok(RootStepExecution {
                response: read_sequence_items_response(&step.name, items),
                executed_items,
                next_start_key: None,
            });
        }

        if let Some(query) = &step.query {
            let query_response = read_context
                .execute_read_sequence_query(apply_query_consistency(query.clone(), consistency))
                .await?;
            let next_start_key = query_response.last_evaluated_key.clone();
            let items = query_response.items.unwrap_or_default();
            add_read_items(total_read_items, items.len(), limits.max_total_read_items)?;
            enforce_root_item_limit(items.len(), limits.max_root_items)?;
            let executed_items = executed_items_for_step(step, &items, ResponseSlot::Items)?;
            return Ok(RootStepExecution {
                response: ReadSequenceRootResponse {
                    name: step.name.clone(),
                    item: None,
                    items: Some(read_sequence_item_results(items)),
                    joins: None,
                    count: Some(query_response.count),
                    scanned_count: Some(query_response.scanned_count),
                },
                executed_items,
                next_start_key,
            });
        }

        Err(HttpApiError::validation_error(
            "ReadSequence step has no executable root operation",
        ))
    }

    async fn execute_read_sequence_for_each_get(
        &self,
        responses: &mut [ReadSequenceRootResponse],
        executed_steps: &BTreeMap<String, ExecutedStep>,
        for_each: &ReadSequenceForEach,
        limits: &ReadSequenceExecutionLimits,
        total_read_items: &mut u32,
        execution: ChildExecutionContext<'_, '_>,
    ) -> Result<ChildStepExecution, HttpApiError> {
        let continuation = execution.continuation;
        let read_context = execution.read_context;
        if for_each.batch_get.is_some() || for_each.query.is_some() {
            return Err(HttpApiError::validation_error(
                "ReadSequence dependent BatchGet and Query execution is not yet supported",
            ));
        }
        let Some(get_request) = &for_each.get else {
            return Err(read_sequence_error(
                ReadSequenceValidationError::InvalidForEachOperation,
            ));
        };
        let parent_step = executed_steps.get(&for_each.join.to).ok_or_else(|| {
            read_sequence_error(ReadSequenceValidationError::UnknownDependency {
                step: for_each.join.as_name.clone(),
                dependency: for_each.join.to.clone(),
            })
        })?;
        let parent_response_index = parent_step.response_index.ok_or_else(|| {
            HttpApiError::validation_error(
                "ReadSequence nested dependent joins are not yet supported",
            )
        })?;
        let selector = ParsedReadSequenceSelector::parse(&parent_relative_selector(for_each))
            .map_err(read_sequence_error)?;
        let start_parent_index = continuation.start_parent_index();
        let mut candidate_contexts = Vec::new();
        let mut next_parent_index = None;
        for (parent_index, parent) in parent_step
            .items
            .iter()
            .enumerate()
            .skip(start_parent_index)
        {
            let Some(parent_slot) = parent.slot else {
                return Err(HttpApiError::validation_error(
                    "ReadSequence nested dependent joins are not yet supported",
                ));
            };
            let Some(contexts) = contexts_for_each_parent(for_each, &selector, parent)? else {
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    parent_slot,
                    &for_each.join.as_name,
                    join_result_for_item(None, for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    next_parent_index = Some(parent_index);
                    break;
                }
                continue;
            };
            if contexts.is_empty() {
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    parent_slot,
                    &for_each.join.as_name,
                    join_result_for_item(None, for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    next_parent_index = Some(parent_index);
                    break;
                }
                continue;
            }
            if contexts.len() > 1 {
                return Err(HttpApiError::validation_error(
                    "ReadSequence set fanout currently requires child BatchGet",
                ));
            }
            if candidate_contexts.len() + contexts.len() > limits.max_fanout_per_step as usize {
                if candidate_contexts.is_empty() {
                    return Err(read_sequence_error(
                        ReadSequenceValidationError::FanoutLimitExceeded {
                            actual: contexts.len() as u32,
                            limit: limits.max_fanout_per_step,
                        },
                    ));
                }
                next_parent_index = Some(parent_index);
                break;
            }
            for context in contexts {
                let child_request = apply_get_consistency(
                    bind_get_request(get_request, &context)?,
                    continuation.consistency,
                );
                candidate_contexts.push(ChildGetCandidate {
                    parent_index,
                    parent_slot,
                    request: child_request,
                    context,
                });
            }
        }

        let lookup_plan = plan_non_covering_lookup(
            candidate_contexts
                .iter()
                .map(|candidate| storage_types::NonCoveringLookupCandidate {
                    parent_index: candidate.parent_index,
                    key: candidate.request.key.clone(),
                }),
            limits.max_fanout_per_step,
        )
        .map_err(non_covering_lookup_error)?;
        let candidate_by_parent = candidate_contexts
            .iter()
            .map(|candidate| (candidate.parent_index, candidate))
            .collect::<BTreeMap<_, _>>();
        let mut fetched_items = Vec::with_capacity(lookup_plan.fetches.len());
        for fetch in &lookup_plan.fetches {
            let request = apply_get_consistency(
                get_request_with_key(get_request, fetch.key.clone()),
                continuation.consistency,
            );
            fetched_items.push(read_context.execute_read_sequence_get(request).await?);
        }
        add_read_items(
            total_read_items,
            fetched_items.iter().filter(|item| item.is_some()).count(),
            limits.max_total_read_items,
        )?;

        let mut child_items = Vec::new();
        for (fetch, child_item) in lookup_plan.fetches.iter().zip(fetched_items) {
            for parent_index in &fetch.parent_indexes {
                let Some(candidate) = candidate_by_parent.get(parent_index) else {
                    return Err(HttpApiError::internal_server_error(
                        "ReadSequence lookup parent attachment is missing",
                    ));
                };
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    candidate.parent_slot,
                    &for_each.join.as_name,
                    join_result_for_item(child_item.clone(), for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if child_items.is_empty() && *parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    return Ok(ChildStepExecution {
                        items: child_items,
                        next_sequence_token: Some(read_sequence_child_token(
                            continuation.step,
                            continuation.step_index,
                            *parent_index,
                            None,
                            continuation.request_digest,
                        )?),
                    });
                }
                if let Some(child_item) = child_item.clone() {
                    child_items.push(ExecutedItem {
                        context: candidate.context.clone(),
                        item: child_item,
                        slot: None,
                    });
                }
            }
        }

        Ok(ChildStepExecution {
            items: child_items,
            next_sequence_token: next_parent_index
                .map(|parent_index| {
                    read_sequence_child_token(
                        continuation.step,
                        continuation.step_index,
                        parent_index,
                        None,
                        continuation.request_digest,
                    )
                })
                .transpose()?,
        })
    }

    async fn execute_read_sequence_for_each_batch_get(
        &self,
        responses: &mut [ReadSequenceRootResponse],
        executed_steps: &BTreeMap<String, ExecutedStep>,
        for_each: &ReadSequenceForEach,
        limits: &ReadSequenceExecutionLimits,
        total_read_items: &mut u32,
        execution: ChildExecutionContext<'_, '_>,
    ) -> Result<ChildStepExecution, HttpApiError> {
        let continuation = execution.continuation;
        let read_context = execution.read_context;
        if for_each.get.is_some() || for_each.query.is_some() {
            return Err(HttpApiError::validation_error(
                "ReadSequence dependent Get and Query cannot execute through BatchGet path",
            ));
        }
        let Some(batch_get_request) = &for_each.batch_get else {
            return Err(read_sequence_error(
                ReadSequenceValidationError::InvalidForEachOperation,
            ));
        };
        let parent_step = executed_steps.get(&for_each.join.to).ok_or_else(|| {
            read_sequence_error(ReadSequenceValidationError::UnknownDependency {
                step: for_each.join.as_name.clone(),
                dependency: for_each.join.to.clone(),
            })
        })?;
        let parent_response_index = parent_step.response_index.ok_or_else(|| {
            HttpApiError::validation_error(
                "ReadSequence nested dependent joins are not yet supported",
            )
        })?;
        let selector = ParsedReadSequenceSelector::parse(&parent_relative_selector(for_each))
            .map_err(read_sequence_error)?;
        let mut child_items = Vec::new();
        let mut fanout_count = 0usize;
        let start_parent_index = continuation.start_parent_index();

        for (parent_index, parent) in parent_step
            .items
            .iter()
            .enumerate()
            .skip(start_parent_index)
        {
            let Some(parent_slot) = parent.slot else {
                return Err(HttpApiError::validation_error(
                    "ReadSequence nested dependent joins are not yet supported",
                ));
            };
            let Some(contexts) = contexts_for_each_parent(for_each, &selector, parent)? else {
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    parent_slot,
                    &for_each.join.as_name,
                    join_result_for_items(Vec::new(), for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if child_items.is_empty() && parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    return Ok(ChildStepExecution {
                        items: child_items,
                        next_sequence_token: Some(read_sequence_child_token(
                            continuation.step,
                            continuation.step_index,
                            parent_index,
                            None,
                            continuation.request_digest,
                        )?),
                    });
                }
                continue;
            };
            if contexts.is_empty() {
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    parent_slot,
                    &for_each.join.as_name,
                    join_result_for_items(Vec::new(), for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if child_items.is_empty() && parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    return Ok(ChildStepExecution {
                        items: child_items,
                        next_sequence_token: Some(read_sequence_child_token(
                            continuation.step,
                            continuation.step_index,
                            parent_index,
                            None,
                            continuation.request_digest,
                        )?),
                    });
                }
                continue;
            }
            if fanout_count + contexts.len() > limits.max_fanout_per_step as usize {
                if fanout_count == 0 {
                    return Err(read_sequence_error(
                        ReadSequenceValidationError::FanoutLimitExceeded {
                            actual: contexts.len() as u32,
                            limit: limits.max_fanout_per_step,
                        },
                    ));
                }
                return Ok(ChildStepExecution {
                    items: child_items,
                    next_sequence_token: Some(read_sequence_child_token(
                        continuation.step,
                        continuation.step_index,
                        parent_index,
                        None,
                        continuation.request_digest,
                    )?),
                });
            }
            fanout_count += contexts.len();
            let mut parent_items = Vec::new();
            for context in contexts {
                let child_request = apply_batch_get_consistency(
                    bind_batch_get_request(batch_get_request, &context)?,
                    continuation.consistency,
                );
                let items = read_context
                    .execute_read_sequence_batch_get(child_request)
                    .await?;
                add_read_items(total_read_items, items.len(), limits.max_total_read_items)?;
                child_items.extend(items.iter().cloned().map(|item| ExecutedItem {
                    context: context.clone(),
                    item,
                    slot: None,
                }));
                parent_items.extend(items);
            }
            if !attach_join_result_with_budget(
                responses,
                parent_response_index,
                parent_slot,
                &for_each.join.as_name,
                join_result_for_items(parent_items, for_each.join.join_type)?,
                limits.max_response_bytes,
            )? {
                if child_items.is_empty() && parent_index == start_parent_index {
                    return Err(response_byte_budget_too_small_error());
                }
                return Ok(ChildStepExecution {
                    items: child_items,
                    next_sequence_token: Some(read_sequence_child_token(
                        continuation.step,
                        continuation.step_index,
                        parent_index,
                        None,
                        continuation.request_digest,
                    )?),
                });
            }
        }

        Ok(ChildStepExecution {
            items: child_items,
            next_sequence_token: None,
        })
    }

    async fn execute_read_sequence_for_each_query(
        &self,
        responses: &mut [ReadSequenceRootResponse],
        executed_steps: &BTreeMap<String, ExecutedStep>,
        for_each: &ReadSequenceForEach,
        limits: &ReadSequenceExecutionLimits,
        total_read_items: &mut u32,
        execution: ChildExecutionContext<'_, '_>,
    ) -> Result<ChildStepExecution, HttpApiError> {
        let continuation = execution.continuation;
        let read_context = execution.read_context;
        if for_each.get.is_some() || for_each.batch_get.is_some() {
            return Err(HttpApiError::validation_error(
                "ReadSequence dependent Get and BatchGet cannot execute through Query path",
            ));
        }
        let Some(query_request) = &for_each.query else {
            return Err(read_sequence_error(
                ReadSequenceValidationError::InvalidForEachOperation,
            ));
        };
        let parent_step = executed_steps.get(&for_each.join.to).ok_or_else(|| {
            read_sequence_error(ReadSequenceValidationError::UnknownDependency {
                step: for_each.join.as_name.clone(),
                dependency: for_each.join.to.clone(),
            })
        })?;
        let parent_response_index = parent_step.response_index.ok_or_else(|| {
            HttpApiError::validation_error(
                "ReadSequence nested dependent joins are not yet supported",
            )
        })?;
        let selector = ParsedReadSequenceSelector::parse(&parent_relative_selector(for_each))
            .map_err(read_sequence_error)?;
        let mut child_items = Vec::new();
        let mut fanout_count = 0usize;
        let start_parent_index = continuation.start_parent_index();

        for (parent_index, parent) in parent_step
            .items
            .iter()
            .enumerate()
            .skip(start_parent_index)
        {
            let Some(parent_slot) = parent.slot else {
                return Err(HttpApiError::validation_error(
                    "ReadSequence nested dependent joins are not yet supported",
                ));
            };
            let Some(contexts) = contexts_for_each_parent(for_each, &selector, parent)? else {
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    parent_slot,
                    &for_each.join.as_name,
                    join_result_for_items(Vec::new(), for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if child_items.is_empty() && parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    return Ok(ChildStepExecution {
                        items: child_items,
                        next_sequence_token: Some(read_sequence_child_token(
                            continuation.step,
                            continuation.step_index,
                            parent_index,
                            None,
                            continuation.request_digest,
                        )?),
                    });
                }
                continue;
            };
            if contexts.is_empty() {
                if !attach_join_result_with_budget(
                    responses,
                    parent_response_index,
                    parent_slot,
                    &for_each.join.as_name,
                    join_result_for_items(Vec::new(), for_each.join.join_type)?,
                    limits.max_response_bytes,
                )? {
                    if child_items.is_empty() && parent_index == start_parent_index {
                        return Err(response_byte_budget_too_small_error());
                    }
                    return Ok(ChildStepExecution {
                        items: child_items,
                        next_sequence_token: Some(read_sequence_child_token(
                            continuation.step,
                            continuation.step_index,
                            parent_index,
                            None,
                            continuation.request_digest,
                        )?),
                    });
                }
                continue;
            }
            if contexts.len() > 1 {
                return Err(HttpApiError::validation_error(
                    "ReadSequence set fanout currently requires child BatchGet",
                ));
            }
            if fanout_count + contexts.len() > limits.max_fanout_per_step as usize {
                if fanout_count == 0 {
                    return Err(read_sequence_error(
                        ReadSequenceValidationError::FanoutLimitExceeded {
                            actual: contexts.len() as u32,
                            limit: limits.max_fanout_per_step,
                        },
                    ));
                }
                return Ok(ChildStepExecution {
                    items: child_items,
                    next_sequence_token: Some(read_sequence_child_token(
                        continuation.step,
                        continuation.step_index,
                        parent_index,
                        None,
                        continuation.request_digest,
                    )?),
                });
            }
            fanout_count += contexts.len();
            let mut parent_items = Vec::new();
            let mut parent_last_evaluated_key = None;
            for context in contexts {
                let mut child_request = apply_query_consistency(
                    bind_query_request(query_request, &context)?,
                    continuation.consistency,
                );
                if continuation.resume.and_then(|token| token.parent_index) == Some(parent_index) {
                    child_request.exclusive_start_key = continuation
                        .resume
                        .and_then(|token| token.exclusive_start_key.clone().map(Into::into));
                }
                let response = read_context
                    .execute_read_sequence_query(child_request)
                    .await?;
                let last_evaluated_key = response.last_evaluated_key.clone();
                let items = response.items.unwrap_or_default();
                add_read_items(total_read_items, items.len(), limits.max_total_read_items)?;
                child_items.extend(items.iter().cloned().map(|item| ExecutedItem {
                    context: context.clone(),
                    item,
                    slot: None,
                }));
                parent_items.extend(items);
                if last_evaluated_key.is_some() {
                    parent_last_evaluated_key = last_evaluated_key;
                    break;
                }
            }
            if !attach_join_result_with_budget(
                responses,
                parent_response_index,
                parent_slot,
                &for_each.join.as_name,
                join_result_for_items_with_partial(
                    parent_items,
                    for_each.join.join_type,
                    parent_last_evaluated_key.is_some(),
                )?,
                limits.max_response_bytes,
            )? {
                if child_items.is_empty() && parent_index == start_parent_index {
                    return Err(response_byte_budget_too_small_error());
                }
                return Ok(ChildStepExecution {
                    items: child_items,
                    next_sequence_token: Some(read_sequence_child_token(
                        continuation.step,
                        continuation.step_index,
                        parent_index,
                        None,
                        continuation.request_digest,
                    )?),
                });
            }
            if let Some(last_evaluated_key) = parent_last_evaluated_key {
                return Ok(ChildStepExecution {
                    items: child_items,
                    next_sequence_token: Some(read_sequence_child_token(
                        continuation.step,
                        continuation.step_index,
                        parent_index,
                        Some(last_evaluated_key),
                        continuation.request_digest,
                    )?),
                });
            }
        }

        Ok(ChildStepExecution {
            items: child_items,
            next_sequence_token: None,
        })
    }

    #[cfg(test)]
    async fn maybe_run_read_sequence_after_root_step_hook_for_test(
        &self,
    ) -> Result<(), HttpApiError> {
        if let Some(hook) = self.read_sequence_after_root_step_hook.as_ref() {
            hook.after_root_step().await?;
        }
        Ok(())
    }

    #[cfg(not(test))]
    async fn maybe_run_read_sequence_after_root_step_hook_for_test(
        &self,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

struct ReadSequenceApiReadContext<'a> {
    manager: &'a StorageApiManagerImpl,
    consistency: ReadSequenceConsistency,
    provider_context: Option<Box<dyn StorageProviderReadContext>>,
}

impl<'a> ReadSequenceApiReadContext<'a> {
    async fn begin(
        manager: &'a StorageApiManagerImpl,
        consistency: ReadSequenceConsistency,
    ) -> Result<Self, HttpApiError> {
        if consistency == ReadSequenceConsistency::Transactional
            && !manager.read_sequence_capabilities.transactional_snapshots
        {
            return Err(read_sequence_error(
                ReadSequenceValidationError::UnsupportedConsistency { consistency },
            ));
        }

        let provider_context = if consistency == ReadSequenceConsistency::Transactional {
            Some(
                manager
                    .db()
                    .begin_read_sequence_read_context(consistency)
                    .await?,
            )
        } else {
            None
        };

        Ok(Self {
            manager,
            consistency,
            provider_context,
        })
    }

    async fn execute_read_sequence_get(
        &self,
        request: storage_types::GetItemRequest,
    ) -> Result<Option<AttributeMap>, HttpApiError> {
        self.ensure_supported()?;
        if let Some(provider_context) = self.provider_context.as_ref() {
            let item = provider_context
                .get_item(
                    request.table_name,
                    request.key,
                    request.consistent_read.unwrap_or(false),
                )
                .await?
                .map(storage_types::WireItem::into_attribute_map)
                .transpose()?
                .map(AttributeMap::from)
                .map(|item| {
                    project_attribute_map(
                        item,
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

    async fn execute_read_sequence_batch_get(
        &self,
        request: storage_types::BatchGetItemRequest,
    ) -> Result<Vec<AttributeMap>, HttpApiError> {
        self.ensure_supported()?;
        if let Some(provider_context) = self.provider_context.as_ref() {
            let request_shape = request.clone();
            let wire_response = provider_context.batch_get_item(request).await?;
            let response = if batch_get_needs_decoded_response(&request_shape) {
                project_batch_get_response(wire_response, &request_shape)?
            } else {
                BatchGetWireResponse::from(add_empty_batch_get_response_tables(
                    wire_response,
                    &request_shape,
                ))
                .into_batch_get_response()?
            };
            return flatten_batch_get_items(response);
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
        flatten_batch_get_items(response)
    }

    async fn execute_read_sequence_query(
        &self,
        request: storage_types::QueryRequest,
    ) -> Result<QueryResponse, HttpApiError> {
        self.ensure_supported()?;
        if let Some(provider_context) = self.provider_context.as_ref() {
            return match self
                .manager
                .query_internal_with_read_context(request, provider_context.as_ref())
                .await?
            {
                Response::Query(response) => Ok(response),
                Response::QueryWire(response) => Ok(response.into_query_response()?),
                _ => Err(HttpApiError::internal_server_error(
                    "Query returned an unexpected response type",
                )),
            };
        }
        match self.manager.query_internal(request).await? {
            Response::Query(response) => Ok(response),
            Response::QueryWire(response) => Ok(response.into_query_response()?),
            _ => Err(HttpApiError::internal_server_error(
                "Query returned an unexpected response type",
            )),
        }
    }

    fn ensure_supported(&self) -> Result<(), HttpApiError> {
        if self.consistency == ReadSequenceConsistency::Transactional
            && !self
                .manager
                .read_sequence_capabilities
                .transactional_snapshots
        {
            return Err(read_sequence_error(
                ReadSequenceValidationError::UnsupportedConsistency {
                    consistency: self.consistency,
                },
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ReadSequenceExecutionLimits {
    max_root_items: u32,
    max_fanout_per_step: u32,
    max_intermediate_items: u32,
    max_total_read_items: u32,
    max_response_bytes: u32,
}

#[derive(Debug, Clone)]
struct RootStepExecution {
    response: ReadSequenceRootResponse,
    executed_items: Vec<ExecutedItem>,
    next_start_key: Option<KeyAttributes>,
}

#[derive(Debug, Clone)]
struct ChildStepExecution {
    items: Vec<ExecutedItem>,
    next_sequence_token: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct ChildContinuationContext<'a> {
    step: &'a ReadSequenceStep,
    step_index: usize,
    resume: Option<&'a ReadSequenceToken>,
    request_digest: &'a str,
    consistency: ReadSequenceConsistency,
}

struct ChildExecutionContext<'a, 'manager> {
    continuation: ChildContinuationContext<'a>,
    read_context: &'a ReadSequenceApiReadContext<'manager>,
}

impl ChildContinuationContext<'_> {
    fn start_parent_index(self) -> usize {
        self.resume
            .and_then(|token| token.parent_index)
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
struct ExecutedStep {
    response_index: Option<usize>,
    items: Vec<ExecutedItem>,
}

#[derive(Debug, Clone)]
struct ExecutedItem {
    item: AttributeMap,
    context: ReadSequenceSelectedContext,
    slot: Option<ResponseSlot>,
}

#[derive(Debug, Clone)]
struct ChildGetCandidate {
    parent_index: usize,
    parent_slot: ResponseSlot,
    request: storage_types::GetItemRequest,
    context: ReadSequenceSelectedContext,
}

#[derive(Debug, Clone, Copy)]
enum ResponseSlot {
    RootItem,
    Items(usize),
}

impl ReadSequenceExecutionLimits {
    fn from_request(request: &ReadSequenceRequest) -> Self {
        Self {
            max_root_items: request
                .max_root_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS),
            max_fanout_per_step: request
                .max_fanout_per_step
                .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP),
            max_intermediate_items: request
                .max_intermediate_items
                .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS),
            max_total_read_items: request
                .max_total_read_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS),
            max_response_bytes: request
                .max_response_bytes
                .unwrap_or(storage_types::READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES),
        }
    }
}

fn read_sequence_get_response(name: &str, item: Option<AttributeMap>) -> ReadSequenceRootResponse {
    ReadSequenceRootResponse {
        name: name.to_string(),
        item,
        items: None,
        joins: None,
        count: None,
        scanned_count: None,
    }
}

fn read_sequence_items_response(name: &str, items: Vec<AttributeMap>) -> ReadSequenceRootResponse {
    ReadSequenceRootResponse {
        name: name.to_string(),
        item: None,
        items: Some(read_sequence_item_results(items)),
        joins: None,
        count: None,
        scanned_count: None,
    }
}

fn read_sequence_item_results(items: Vec<AttributeMap>) -> Vec<ReadSequenceItemResult> {
    items
        .into_iter()
        .map(|item| ReadSequenceItemResult { item, joins: None })
        .collect()
}

fn executed_items_for_step(
    step: &ReadSequenceStep,
    items: &[AttributeMap],
    slot_kind: fn(usize) -> ResponseSlot,
) -> Result<Vec<ExecutedItem>, HttpApiError> {
    items
        .iter()
        .enumerate()
        .map(|(index, item)| executed_item(step, item, Some(slot_kind(index))))
        .collect()
}

fn executed_item(
    step: &ReadSequenceStep,
    item: &AttributeMap,
    slot: Option<ResponseSlot>,
) -> Result<ExecutedItem, HttpApiError> {
    Ok(ExecutedItem {
        item: item.clone(),
        context: selected_context(&step.select, item)?,
        slot,
    })
}

fn flatten_batch_get_items(
    response: BatchGetItemResponse,
) -> Result<Vec<AttributeMap>, HttpApiError> {
    let unprocessed_count = response.unprocessed_keys.as_ref().map_or(0usize, |tables| {
        tables.values().map(|keys| keys.keys.len()).sum()
    });
    if unprocessed_count > 0 {
        return Err(HttpApiError::from(StorageError::from(
            StorageEnum::Throttled {
                message: format!(
                    "ReadSequence BatchGetItem returned {unprocessed_count} unprocessed key(s)"
                ),
            },
        )));
    }

    Ok(response
        .responses
        .unwrap_or_default()
        .into_values()
        .flatten()
        .collect())
}

fn contexts_for_each_parent(
    for_each: &ReadSequenceForEach,
    selector: &ParsedReadSequenceSelector,
    parent: &ExecutedItem,
) -> Result<Option<Vec<ReadSequenceSelectedContext>>, HttpApiError> {
    let selected = selector
        .evaluate_item(&parent.item)
        .map_err(read_sequence_error)?;
    let Some(selected) = selected else {
        return match for_each.on_missing {
            ReadSequenceOnMissing::Skip => Ok(None),
            ReadSequenceOnMissing::Null => {
                let mut context = parent.context.clone();
                context.insert(for_each.as_name.clone(), AttributeValue::NULL(true));
                Ok(Some(vec![context]))
            }
            ReadSequenceOnMissing::Error => Err(read_sequence_error(
                ReadSequenceValidationError::SelectorFailure {
                    selector: for_each.from.0.clone(),
                },
            )),
        };
    };
    Ok(Some(expand_for_each_contexts(
        &parent.context,
        &for_each.as_name,
        selected,
    )))
}

fn expand_for_each_contexts(
    parent_context: &ReadSequenceSelectedContext,
    as_name: &str,
    selected: AttributeValue,
) -> Vec<ReadSequenceSelectedContext> {
    match selected {
        AttributeValue::SS(values) => values
            .into_iter()
            .map(AttributeValue::S)
            .map(|value| context_with_value(parent_context, as_name, value))
            .collect(),
        AttributeValue::NS(values) => values
            .into_iter()
            .map(AttributeValue::N)
            .map(|value| context_with_value(parent_context, as_name, value))
            .collect(),
        AttributeValue::BS(values) => values
            .into_iter()
            .map(AttributeValue::B)
            .map(|value| context_with_value(parent_context, as_name, value))
            .collect(),
        AttributeValue::L(values) => values
            .into_iter()
            .map(|value| context_with_value(parent_context, as_name, value))
            .collect(),
        value => vec![context_with_value(parent_context, as_name, value)],
    }
}

fn context_with_value(
    parent_context: &ReadSequenceSelectedContext,
    as_name: &str,
    value: AttributeValue,
) -> ReadSequenceSelectedContext {
    let mut context = parent_context.clone();
    context.insert(as_name.to_string(), value);
    context
}

fn parent_relative_selector(for_each: &ReadSequenceForEach) -> storage_types::ReadSequenceSelector {
    let parts = for_each.from.0.split('.').collect::<Vec<_>>();
    let relative = match parts.as_slice() {
        [_, "Item" | "Items", rest @ ..] => rest,
        [_, rest @ ..] => rest,
        [] => &[][..],
    };
    if relative.is_empty() {
        return storage_types::ReadSequenceSelector("$".to_string());
    }
    storage_types::ReadSequenceSelector(format!("$.{}", relative.join(".")))
}

fn bind_get_request(
    request: &storage_types::GetItemRequest,
    context: &ReadSequenceSelectedContext,
) -> Result<storage_types::GetItemRequest, HttpApiError> {
    let mut key = KeyAttributes::with_capacity(request.key.len());
    for (name, value) in request.key.iter() {
        key.insert(
            name.to_string(),
            bind_read_sequence_attribute_value(value, context).map_err(read_sequence_error)?,
        );
    }
    Ok(storage_types::GetItemRequest {
        table_name: request.table_name.clone(),
        key,
        attributes_to_get: request.attributes_to_get.clone(),
        consistent_read: request.consistent_read,
        projection_expression: request.projection_expression.clone(),
        expression_attribute_names: request.expression_attribute_names.clone(),
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    })
}

fn apply_get_consistency(
    mut request: storage_types::GetItemRequest,
    consistency: ReadSequenceConsistency,
) -> storage_types::GetItemRequest {
    if read_sequence_requires_consistent_base_read(consistency) {
        request.consistent_read = Some(true);
    }
    request
}

fn get_request_with_key(
    request: &storage_types::GetItemRequest,
    key: KeyAttributes,
) -> storage_types::GetItemRequest {
    storage_types::GetItemRequest {
        table_name: request.table_name.clone(),
        key,
        attributes_to_get: request.attributes_to_get.clone(),
        consistent_read: request.consistent_read,
        projection_expression: request.projection_expression.clone(),
        expression_attribute_names: request.expression_attribute_names.clone(),
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    }
}

fn apply_batch_get_consistency(
    mut request: storage_types::BatchGetItemRequest,
    consistency: ReadSequenceConsistency,
) -> storage_types::BatchGetItemRequest {
    if read_sequence_requires_consistent_base_read(consistency) {
        for keys_and_attributes in request.request_items.values_mut() {
            keys_and_attributes.consistent_read = Some(true);
        }
    }
    request
}

fn bind_batch_get_request(
    request: &storage_types::BatchGetItemRequest,
    context: &ReadSequenceSelectedContext,
) -> Result<storage_types::BatchGetItemRequest, HttpApiError> {
    let mut request_items = std::collections::HashMap::with_capacity(request.request_items.len());
    for (table_name, keys_and_attributes) in &request.request_items {
        let mut keys = keys_and_attributes.keys.clone();
        keys.clear();
        for key in &keys_and_attributes.keys {
            let mut bound_key = KeyAttributes::with_capacity(key.len());
            for (name, value) in key.iter() {
                bound_key.insert(
                    name.to_string(),
                    bind_read_sequence_attribute_value(value, context)
                        .map_err(read_sequence_error)?,
                );
            }
            keys.push(bound_key);
        }
        request_items.insert(
            table_name.clone(),
            storage_types::KeysAndAttributes {
                keys,
                attributes_to_get: keys_and_attributes.attributes_to_get.clone(),
                projection_expression: keys_and_attributes.projection_expression.clone(),
                expression_attribute_names: keys_and_attributes.expression_attribute_names.clone(),
                consistent_read: keys_and_attributes.consistent_read,
            },
        );
    }
    Ok(storage_types::BatchGetItemRequest {
        request_items,
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    })
}

fn apply_query_consistency(
    mut request: storage_types::QueryRequest,
    consistency: ReadSequenceConsistency,
) -> storage_types::QueryRequest {
    if read_sequence_requires_consistent_base_read(consistency) && request.index_name.is_none() {
        request.consistent_read = Some(true);
    }
    request
}

fn read_sequence_requires_consistent_base_read(consistency: ReadSequenceConsistency) -> bool {
    matches!(
        consistency,
        ReadSequenceConsistency::Strong | ReadSequenceConsistency::Transactional
    )
}

fn bind_query_request(
    request: &storage_types::QueryRequest,
    context: &ReadSequenceSelectedContext,
) -> Result<storage_types::QueryRequest, HttpApiError> {
    let expression_attribute_values = request
        .expression_attribute_values
        .as_ref()
        .map(|values| {
            values
                .iter()
                .map(|(name, value)| {
                    Ok((
                        name.clone(),
                        bind_read_sequence_attribute_value(value, context)
                            .map_err(read_sequence_error)?,
                    ))
                })
                .collect::<Result<std::collections::HashMap<_, _>, HttpApiError>>()
        })
        .transpose()?;
    Ok(storage_types::QueryRequest {
        table_name: request.table_name.clone(),
        index_name: request.index_name.clone(),
        key_condition_expression: request.key_condition_expression.clone(),
        attributes_to_get: request.attributes_to_get.clone(),
        conditional_operator: request.conditional_operator.clone(),
        filter_expression: request.filter_expression.clone(),
        query_filter: request.query_filter.clone(),
        projection_expression: request.projection_expression.clone(),
        expression_attribute_names: request.expression_attribute_names.clone(),
        expression_attribute_values,
        limit: request.limit,
        exclusive_start_key: request.exclusive_start_key.clone(),
        return_consumed_capacity: request.return_consumed_capacity.clone(),
        consistent_read: request.consistent_read,
        scan_index_forward: request.scan_index_forward,
        select: request.select.clone(),
    })
}

fn attach_join_result(
    response: &mut ReadSequenceRootResponse,
    slot: ResponseSlot,
    join_name: &str,
    join_result: ReadSequenceJoinResult,
) -> Result<(), HttpApiError> {
    match slot {
        ResponseSlot::RootItem => {
            response
                .joins
                .get_or_insert_with(BTreeMap::new)
                .insert(join_name.to_string(), join_result);
        }
        ResponseSlot::Items(index) => {
            let Some(items) = response.items.as_mut() else {
                return Err(HttpApiError::internal_server_error(
                    "ReadSequence response item slot is missing",
                ));
            };
            let Some(item) = items.get_mut(index) else {
                return Err(HttpApiError::internal_server_error(
                    "ReadSequence response item index is out of bounds",
                ));
            };
            item.joins
                .get_or_insert_with(BTreeMap::new)
                .insert(join_name.to_string(), join_result);
        }
    }
    Ok(())
}

fn attach_join_result_with_budget(
    responses: &mut [ReadSequenceRootResponse],
    response_index: usize,
    slot: ResponseSlot,
    join_name: &str,
    join_result: ReadSequenceJoinResult,
    max_response_bytes: u32,
) -> Result<bool, HttpApiError> {
    let mut candidate = responses.to_vec();
    attach_join_result(
        &mut candidate[response_index],
        slot,
        join_name,
        join_result.clone(),
    )?;
    if serialized_response_bytes(&candidate)? > max_response_bytes as usize {
        return Ok(false);
    }
    attach_join_result(&mut responses[response_index], slot, join_name, join_result)?;
    Ok(true)
}

fn join_result_for_item(
    item: Option<AttributeMap>,
    join_type: ReadSequenceJoinType,
) -> Result<ReadSequenceJoinResult, HttpApiError> {
    match (join_type, item) {
        (ReadSequenceJoinType::RequiredOne, None) => Err(HttpApiError::validation_error(
            "ReadSequence REQUIRED_ONE join item is missing",
        )),
        (ReadSequenceJoinType::Array, Some(item)) => Ok(ReadSequenceJoinResult {
            item: None,
            items: Some(vec![item]),
            partial: false,
        }),
        (ReadSequenceJoinType::Array, None) => Ok(ReadSequenceJoinResult {
            item: None,
            items: Some(Vec::new()),
            partial: false,
        }),
        (ReadSequenceJoinType::InnerOne, None) => Ok(ReadSequenceJoinResult {
            item: None,
            items: None,
            partial: false,
        }),
        (ReadSequenceJoinType::LeftOne | ReadSequenceJoinType::RequiredOne, item)
        | (ReadSequenceJoinType::InnerOne, item) => Ok(ReadSequenceJoinResult {
            item,
            items: None,
            partial: false,
        }),
    }
}

fn join_result_for_items(
    items: Vec<AttributeMap>,
    join_type: ReadSequenceJoinType,
) -> Result<ReadSequenceJoinResult, HttpApiError> {
    join_result_for_items_with_partial(items, join_type, false)
}

fn join_result_for_items_with_partial(
    items: Vec<AttributeMap>,
    join_type: ReadSequenceJoinType,
    partial: bool,
) -> Result<ReadSequenceJoinResult, HttpApiError> {
    match join_type {
        ReadSequenceJoinType::Array => Ok(ReadSequenceJoinResult {
            item: None,
            items: Some(items),
            partial,
        }),
        ReadSequenceJoinType::LeftOne | ReadSequenceJoinType::InnerOne => {
            Ok(ReadSequenceJoinResult {
                item: items.into_iter().next(),
                items: None,
                partial,
            })
        }
        ReadSequenceJoinType::RequiredOne => {
            let Some(item) = items.into_iter().next() else {
                return Err(HttpApiError::validation_error(
                    "ReadSequence REQUIRED_ONE join item is missing",
                ));
            };
            Ok(ReadSequenceJoinResult {
                item: Some(item),
                items: None,
                partial,
            })
        }
    }
}

fn read_sequence_child_token(
    step: &ReadSequenceStep,
    step_index: usize,
    parent_index: usize,
    exclusive_start_key: Option<KeyAttributes>,
    request_digest: &str,
) -> Result<String, HttpApiError> {
    encode_read_sequence_token(&ReadSequenceToken {
        version: 1,
        request_digest: request_digest.to_string(),
        metadata_digest: read_sequence_step_metadata_digest(step)?,
        step_index,
        parent_index: Some(parent_index),
        expires_at_epoch_seconds: read_sequence_token_expiration(),
        exclusive_start_key,
    })
}

fn serialized_response_bytes(
    responses: &[ReadSequenceRootResponse],
) -> Result<usize, HttpApiError> {
    serde_json::to_vec(responses)
        .map(|bytes| bytes.len())
        .map_err(|error| {
            HttpApiError::from(StorageError::internal(&format!(
                "serialize ReadSequence response budget estimate: {error}"
            )))
        })
}

fn response_byte_budget_too_small_error() -> HttpApiError {
    HttpApiError::validation_error(
        "ReadSequence response byte limit cannot fit the next child result",
    )
}

fn add_read_items(total: &mut u32, item_count: usize, limit: u32) -> Result<(), HttpApiError> {
    let item_count = u32::try_from(item_count).map_err(|_| {
        read_sequence_error(ReadSequenceValidationError::TotalReadLimitExceeded {
            actual: u32::MAX,
            limit,
        })
    })?;
    *total = total.checked_add(item_count).ok_or_else(|| {
        read_sequence_error(ReadSequenceValidationError::TotalReadLimitExceeded {
            actual: u32::MAX,
            limit,
        })
    })?;
    if *total > limit {
        return Err(read_sequence_error(
            ReadSequenceValidationError::TotalReadLimitExceeded {
                actual: *total,
                limit,
            },
        ));
    }
    Ok(())
}

fn enforce_root_item_limit(item_count: usize, limit: u32) -> Result<(), HttpApiError> {
    if item_count > limit as usize {
        return Err(read_sequence_error(
            ReadSequenceValidationError::FanoutLimitExceeded {
                actual: u32::try_from(item_count).unwrap_or(u32::MAX),
                limit,
            },
        ));
    }
    Ok(())
}

fn enforce_intermediate_item_limit(
    response: &ReadSequenceRootResponse,
    limit: u32,
) -> Result<(), HttpApiError> {
    let Some(items) = response.items.as_ref() else {
        return Ok(());
    };
    if items.len() > limit as usize {
        return Err(read_sequence_error(
            ReadSequenceValidationError::FanoutLimitExceeded {
                actual: u32::try_from(items.len()).unwrap_or(u32::MAX),
                limit,
            },
        ));
    }
    Ok(())
}

fn read_sequence_error(error: ReadSequenceValidationError) -> HttpApiError {
    HttpApiError::from(StorageError::from(error))
}

fn non_covering_lookup_error(error: storage_types::NonCoveringLookupError) -> HttpApiError {
    HttpApiError::from(StorageError::validation(error.to_string()))
}

fn read_sequence_consumed_capacity(
    return_consumed_capacity: Option<&str>,
    read_count: u32,
) -> Option<serde_json::Value> {
    match return_consumed_capacity? {
        "TOTAL" | "INDEXES" => {
            let capacity_units = f64::from(read_count).max(0.5);
            Some(serde_json::json!({
                "TableName": "ReadSequence",
                "CapacityUnits": capacity_units,
                "ReadCapacityUnits": capacity_units
            }))
        }
        "NONE" => None,
        _ => None,
    }
}

#[allow(dead_code)]
fn selected_context(
    selectors: &BTreeMap<String, storage_types::ReadSequenceSelector>,
    item: &AttributeMap,
) -> Result<ReadSequenceSelectedContext, HttpApiError> {
    let mut context = ReadSequenceSelectedContext::default();
    for (name, selector) in selectors {
        let selector = ParsedReadSequenceSelector::parse(selector).map_err(read_sequence_error)?;
        if let Some(value) = selector.evaluate_item(item).map_err(read_sequence_error)? {
            context.insert(name, value);
        }
    }
    Ok(context)
}
