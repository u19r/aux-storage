use std::collections::BTreeMap;

use crate::{
    ReadSequenceForEach, ReadSequenceJoinType, ReadSequenceRequest, ReadSequenceStep,
    ReadSequenceValidationCapabilities, ReadSequenceValidationError, ReadSequenceWarning,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadSequencePlannerInput {
    pub parent_counts: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSequencePlan {
    pub child_steps: Vec<ReadSequenceChildPlan>,
    pub warning: Option<ReadSequenceWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSequenceChildPlan {
    pub step_name: String,
    pub parent_step: String,
    pub operation: ReadSequencePlannedOperation,
    pub join_as: String,
    pub join_type: ReadSequenceJoinType,
    pub parent_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSequencePlannedOperation {
    Get,
    BatchGet,
    Query,
}

pub fn plan_read_sequence(
    request: &ReadSequenceRequest,
    input: &ReadSequencePlannerInput,
) -> Result<ReadSequencePlan, ReadSequenceValidationError> {
    plan_read_sequence_with_capabilities(
        request,
        input,
        ReadSequenceValidationCapabilities::default(),
    )
}

pub fn plan_read_sequence_with_capabilities(
    request: &ReadSequenceRequest,
    input: &ReadSequencePlannerInput,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<ReadSequencePlan, ReadSequenceValidationError> {
    request.validate_with_capabilities(capabilities)?;
    let max_fanout = request
        .max_fanout_per_step
        .unwrap_or(crate::READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP);
    let mut child_steps = Vec::new();
    let mut warning = None;
    for step in &request.sequence {
        let Some(for_each) = &step.for_each else {
            continue;
        };
        let parent_step = for_each.join.to.clone();
        let parent_count = input.parent_counts.get(&parent_step).copied().unwrap_or(0);
        if parent_count > max_fanout {
            return Err(ReadSequenceValidationError::FanoutLimitExceeded {
                actual: parent_count,
                limit: max_fanout,
            });
        }
        child_steps.push(ReadSequenceChildPlan {
            step_name: step.name.clone(),
            parent_step: parent_step.clone(),
            operation: planned_operation(for_each)?,
            join_as: for_each.join.as_name.clone(),
            join_type: for_each.join.join_type,
            parent_count,
        });
        warning = warning.or_else(|| modeling_warning_for_step(request, step, for_each));
    }
    Ok(ReadSequencePlan {
        child_steps,
        warning,
    })
}

fn planned_operation(
    for_each: &ReadSequenceForEach,
) -> Result<ReadSequencePlannedOperation, ReadSequenceValidationError> {
    if for_each.get.is_some() {
        Ok(ReadSequencePlannedOperation::Get)
    } else if for_each.batch_get.is_some() {
        Ok(ReadSequencePlannedOperation::BatchGet)
    } else if for_each.query.is_some() {
        Ok(ReadSequencePlannedOperation::Query)
    } else {
        Err(ReadSequenceValidationError::InvalidForEachOperation)
    }
}

fn modeling_warning_for_step(
    request: &ReadSequenceRequest,
    step: &ReadSequenceStep,
    for_each: &ReadSequenceForEach,
) -> Option<ReadSequenceWarning> {
    for_each.get.as_ref()?;
    let parent = request
        .sequence
        .iter()
        .find(|candidate| candidate.name == for_each.join.to)?;
    let parent_query = parent.query.as_ref()?;
    let child_get = for_each.get.as_ref()?;
    let parent_key = parent_query
        .key_condition_expression
        .split_once('=')
        .map(|(name, _)| name.trim())
        .filter(|name| !name.is_empty() && !name.contains(':'))?;
    let child_key = child_get.key.iter().next().map(|(name, _)| name)?;
    Some(ReadSequenceWarning {
        code: "BetterModeledAsGsi".to_string(),
        message: format!(
            "This ReadSequence can be better modeled as a GSI on {}.",
            child_get.table_name
        ),
        step_name: Some(step.name.clone()),
        suggested_gsi: Some(crate::ReadSequenceSuggestedGsi {
            table_name: child_get.table_name.clone(),
            partition_key: crate::ReadSequenceSuggestedGsiKey {
                attribute_name: parent_key.to_string(),
                source: format!("{}.Query.KeyConditionExpression", parent.name),
            },
            sort_key: Some(crate::ReadSequenceSuggestedGsiKey {
                attribute_name: child_key.to_string(),
                source: format!("{}.Items[].{}", parent.name, child_key),
            }),
        }),
    })
}
