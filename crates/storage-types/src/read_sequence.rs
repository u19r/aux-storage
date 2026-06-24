use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    BatchGetItemRequest, GetItemRequest, QueryRequest, ReadSequenceValidationError,
    read_sequence_selector::validate_selector_depth,
};

pub const READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS: u32 = 8;
pub const READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS: u32 = 16;
pub const READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS: u32 = 100;
pub const READ_SEQUENCE_HARD_MAX_ROOT_ITEMS: u32 = 100;
pub const READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP: u32 = 100;
pub const READ_SEQUENCE_HARD_MAX_FANOUT_PER_STEP: u32 = 100;
pub const READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS: u32 = 256;
pub const READ_SEQUENCE_HARD_MAX_INTERMEDIATE_ITEMS: u32 = 1024;
pub const READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS: u32 = 512;
pub const READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS: u32 = 2048;
pub const READ_SEQUENCE_DEFAULT_MAX_CHILD_QUERY_ITEMS_PER_PARENT: u32 = 25;
pub const READ_SEQUENCE_HARD_MAX_CHILD_QUERY_ITEMS_PER_PARENT: u32 = 100;
pub const READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES: u32 = 4 * 1024 * 1024;
pub const READ_SEQUENCE_HARD_MAX_RESPONSE_BYTES: u32 = 16 * 1024 * 1024;
pub const READ_SEQUENCE_DEFAULT_MAX_SELECTOR_BINDINGS_PER_STEP: u32 = 16;
pub const READ_SEQUENCE_HARD_MAX_SELECTOR_BINDINGS_PER_STEP: u32 = 64;
pub const READ_SEQUENCE_DEFAULT_MAX_SELECTOR_PATH_DEPTH: u32 = 8;
pub const READ_SEQUENCE_HARD_MAX_SELECTOR_PATH_DEPTH: u32 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[derive(Default)]
pub enum ReadSequenceConsistency {
    #[default]
    Eventual,
    Strong,
    Transactional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSequenceValidationCapabilities {
    pub eventual_reads: bool,
    pub strong_reads: bool,
    pub transactional_reads: bool,
    pub immediate_gsi_consistency: bool,
}

impl Default for ReadSequenceValidationCapabilities {
    fn default() -> Self {
        Self {
            eventual_reads: true,
            strong_reads: true,
            transactional_reads: true,
            immediate_gsi_consistency: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSequenceProviderCapabilities {
    pub eventual_reads: bool,
    pub strong_reads: bool,
    pub transactional_reads: bool,
    pub transactional_snapshots: bool,
    pub immediate_gsi_consistency: bool,
}

impl Default for ReadSequenceProviderCapabilities {
    fn default() -> Self {
        Self {
            eventual_reads: true,
            strong_reads: true,
            transactional_reads: true,
            transactional_snapshots: false,
            immediate_gsi_consistency: false,
        }
    }
}

impl ReadSequenceProviderCapabilities {
    #[must_use]
    pub fn validation_capabilities(self) -> ReadSequenceValidationCapabilities {
        ReadSequenceValidationCapabilities {
            eventual_reads: self.eventual_reads,
            strong_reads: self.strong_reads,
            transactional_reads: self.transactional_reads,
            immediate_gsi_consistency: self.immediate_gsi_consistency,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceRequest {
    #[serde(default)]
    pub read_consistency: ReadSequenceConsistency,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sequence_steps: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_root_items: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fanout_per_step: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_intermediate_items: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_total_read_items: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_child_query_items_per_parent: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_selector_bindings_per_step: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_selector_path_depth: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_sequence_token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_consumed_capacity: Option<String>,

    pub sequence: Vec<ReadSequenceStep>,
}

impl ReadSequenceRequest {
    pub fn validate(&self) -> Result<(), ReadSequenceValidationError> {
        self.validate_with_capabilities(ReadSequenceValidationCapabilities::default())
    }

    pub fn validate_with_capabilities(
        &self,
        capabilities: ReadSequenceValidationCapabilities,
    ) -> Result<(), ReadSequenceValidationError> {
        let limits = ReadSequenceEffectiveLimits::try_from(self)?;
        if self.sequence.is_empty() {
            return Err(ReadSequenceValidationError::EmptySequence);
        }
        if self.sequence.len() > limits.max_sequence_steps as usize {
            return Err(ReadSequenceValidationError::StepLimitExceeded {
                actual: self.sequence.len(),
                limit: limits.max_sequence_steps,
            });
        }
        validate_return_consumed_capacity(self.return_consumed_capacity.as_deref())?;

        let mut prior_names = HashSet::with_capacity(self.sequence.len());
        for step in &self.sequence {
            validate_step_name(&step.name)?;
            if !prior_names.insert(step.name.clone()) {
                return Err(ReadSequenceValidationError::DuplicateStepName {
                    name: step.name.clone(),
                });
            }

            if step.select.len() > limits.max_selector_bindings_per_step as usize {
                return Err(ReadSequenceValidationError::SelectorBindingLimitExceeded {
                    step: step.name.clone(),
                    actual: step.select.len(),
                    limit: limits.max_selector_bindings_per_step,
                });
            }
            for selector in step.select.values() {
                validate_selector_depth(selector, limits.max_selector_path_depth)?;
            }

            let operation = step.operation()?;
            validate_operation_consistency(
                self.read_consistency,
                operation.reads_gsi(),
                capabilities,
            )?;

            if let ReadSequenceStepOperation::ForEach(for_each) = operation {
                validate_for_each(
                    &step.name,
                    for_each,
                    &prior_names,
                    &limits,
                    self.read_consistency,
                    capabilities,
                )?;
            }
        }
        Ok(())
    }
}

impl TryFrom<serde_json::Value> for ReadSequenceRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        request.validate().map_err(|error| error.to_string())?;
        Ok(request)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceStep {
    pub name: String,

    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub select: std::collections::BTreeMap<String, ReadSequenceSelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<GetItemRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_get: Option<BatchGetItemRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_each: Option<ReadSequenceForEach>,
}

impl ReadSequenceStep {
    fn operation(&self) -> Result<ReadSequenceStepOperation<'_>, ReadSequenceValidationError> {
        let mut count = 0;
        count += usize::from(self.get.is_some());
        count += usize::from(self.batch_get.is_some());
        count += usize::from(self.query.is_some());
        count += usize::from(self.for_each.is_some());

        if count != 1 {
            return Err(ReadSequenceValidationError::InvalidStepOperation {
                step: self.name.clone(),
            });
        }

        if self.get.is_some() {
            Ok(ReadSequenceStepOperation::Get)
        } else if self.batch_get.is_some() {
            Ok(ReadSequenceStepOperation::BatchGet)
        } else if let Some(query) = &self.query {
            Ok(ReadSequenceStepOperation::Query(query))
        } else if let Some(for_each) = &self.for_each {
            Ok(ReadSequenceStepOperation::ForEach(for_each))
        } else {
            Err(ReadSequenceValidationError::InvalidStepOperation {
                step: self.name.clone(),
            })
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceForEach {
    pub from: ReadSequenceSelector,
    #[serde(rename = "As")]
    pub as_name: String,
    #[serde(default)]
    pub on_missing: ReadSequenceOnMissing,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<GetItemRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_get: Option<BatchGetItemRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryRequest>,
    pub join: ReadSequenceJoin,
}

impl ReadSequenceForEach {
    fn operation(&self) -> Result<ReadSequenceStepOperation<'_>, ReadSequenceValidationError> {
        let mut count = 0;
        count += usize::from(self.get.is_some());
        count += usize::from(self.batch_get.is_some());
        count += usize::from(self.query.is_some());

        if count != 1 {
            return Err(ReadSequenceValidationError::InvalidForEachOperation);
        }

        if self.get.is_some() {
            Ok(ReadSequenceStepOperation::Get)
        } else if self.batch_get.is_some() {
            Ok(ReadSequenceStepOperation::BatchGet)
        } else if let Some(query) = &self.query {
            Ok(ReadSequenceStepOperation::Query(query))
        } else {
            Err(ReadSequenceValidationError::InvalidForEachOperation)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[derive(Default)]
pub enum ReadSequenceOnMissing {
    #[default]
    Null,
    Skip,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
pub struct ReadSequenceJoin {
    pub to: String,
    #[serde(rename = "As")]
    pub as_name: String,
    #[serde(rename = "Type")]
    pub join_type: ReadSequenceJoinType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReadSequenceJoinType {
    LeftOne,
    RequiredOne,
    Array,
    InnerOne,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ReadSequenceSelector(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ReadSequenceTemplate(pub String);

enum ReadSequenceStepOperation<'a> {
    Get,
    BatchGet,
    Query(&'a QueryRequest),
    ForEach(&'a ReadSequenceForEach),
}

impl ReadSequenceStepOperation<'_> {
    fn reads_gsi(&self) -> bool {
        match self {
            Self::Get | Self::BatchGet => false,
            Self::Query(query) => query.index_name.is_some(),
            Self::ForEach(for_each) => for_each
                .operation()
                .map(|operation| operation.reads_gsi())
                .unwrap_or(false),
        }
    }
}

struct ReadSequenceEffectiveLimits {
    max_sequence_steps: u32,
    max_fanout_per_step: u32,
    max_selector_bindings_per_step: u32,
    max_selector_path_depth: u32,
}

impl TryFrom<&ReadSequenceRequest> for ReadSequenceEffectiveLimits {
    type Error = ReadSequenceValidationError;

    fn try_from(request: &ReadSequenceRequest) -> Result<Self, Self::Error> {
        validate_cap(
            request
                .max_root_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS),
            READ_SEQUENCE_HARD_MAX_ROOT_ITEMS,
            |actual, limit| ReadSequenceValidationError::HardLimitExceeded {
                limit_name: "MaxRootItems",
                actual,
                limit,
            },
        )?;
        let max_fanout_per_step = validate_cap(
            request
                .max_fanout_per_step
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP),
            READ_SEQUENCE_HARD_MAX_FANOUT_PER_STEP,
            |actual, limit| ReadSequenceValidationError::FanoutLimitExceeded { actual, limit },
        )?;
        validate_cap(
            request
                .max_intermediate_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS),
            READ_SEQUENCE_HARD_MAX_INTERMEDIATE_ITEMS,
            |actual, limit| ReadSequenceValidationError::HardLimitExceeded {
                limit_name: "MaxIntermediateItems",
                actual,
                limit,
            },
        )?;
        validate_cap(
            request
                .max_child_query_items_per_parent
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_CHILD_QUERY_ITEMS_PER_PARENT),
            READ_SEQUENCE_HARD_MAX_CHILD_QUERY_ITEMS_PER_PARENT,
            |actual, limit| ReadSequenceValidationError::HardLimitExceeded {
                limit_name: "MaxChildQueryItemsPerParent",
                actual,
                limit,
            },
        )?;
        validate_cap(
            request
                .max_response_bytes
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES),
            READ_SEQUENCE_HARD_MAX_RESPONSE_BYTES,
            |actual, limit| ReadSequenceValidationError::HardLimitExceeded {
                limit_name: "MaxResponseBytes",
                actual,
                limit,
            },
        )?;
        let max_sequence_steps = validate_cap(
            request
                .max_sequence_steps
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS),
            READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS,
            |actual, limit| ReadSequenceValidationError::StepLimitExceeded {
                actual: actual as usize,
                limit,
            },
        )?;
        validate_total_read_cap(
            request
                .max_total_read_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS),
        )?;
        let max_selector_bindings_per_step = validate_cap(
            request
                .max_selector_bindings_per_step
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SELECTOR_BINDINGS_PER_STEP),
            READ_SEQUENCE_HARD_MAX_SELECTOR_BINDINGS_PER_STEP,
            |actual, limit| ReadSequenceValidationError::HardLimitExceeded {
                limit_name: "MaxSelectorBindingsPerStep",
                actual,
                limit,
            },
        )?;
        let max_selector_path_depth = validate_cap(
            request
                .max_selector_path_depth
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SELECTOR_PATH_DEPTH),
            READ_SEQUENCE_HARD_MAX_SELECTOR_PATH_DEPTH,
            |actual, limit| ReadSequenceValidationError::HardLimitExceeded {
                limit_name: "MaxSelectorPathDepth",
                actual,
                limit,
            },
        )?;

        Ok(Self {
            max_sequence_steps,
            max_fanout_per_step,
            max_selector_bindings_per_step,
            max_selector_path_depth,
        })
    }
}

fn validate_cap<F>(actual: u32, limit: u32, error: F) -> Result<u32, ReadSequenceValidationError>
where F: FnOnce(u32, u32) -> ReadSequenceValidationError {
    if actual > limit {
        Err(error(actual, limit))
    } else {
        Ok(actual)
    }
}

fn validate_total_read_cap(actual: u32) -> Result<(), ReadSequenceValidationError> {
    if actual > READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS {
        Err(ReadSequenceValidationError::TotalReadLimitExceeded {
            actual,
            limit: READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS,
        })
    } else {
        Ok(())
    }
}

fn validate_step_name(name: &str) -> Result<(), ReadSequenceValidationError> {
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|character| character == '_' || character.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(ReadSequenceValidationError::InvalidStepName {
            name: name.to_string(),
        })
    }
}

fn validate_operation_consistency(
    consistency: ReadSequenceConsistency,
    reads_gsi: bool,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<(), ReadSequenceValidationError> {
    match consistency {
        ReadSequenceConsistency::Eventual if !capabilities.eventual_reads => {
            Err(ReadSequenceValidationError::UnsupportedConsistency { consistency })
        }
        ReadSequenceConsistency::Eventual => Ok(()),
        ReadSequenceConsistency::Strong if !capabilities.strong_reads => {
            Err(ReadSequenceValidationError::UnsupportedConsistency { consistency })
        }
        ReadSequenceConsistency::Strong if reads_gsi => {
            Err(ReadSequenceValidationError::StrongGsiRejected)
        }
        ReadSequenceConsistency::Strong => Ok(()),
        ReadSequenceConsistency::Transactional if !capabilities.transactional_reads => {
            Err(ReadSequenceValidationError::UnsupportedConsistency { consistency })
        }
        ReadSequenceConsistency::Transactional
            if reads_gsi && !capabilities.immediate_gsi_consistency =>
        {
            Err(ReadSequenceValidationError::TransactionalGsiRejected)
        }
        ReadSequenceConsistency::Transactional => Ok(()),
    }
}

fn validate_for_each(
    step_name: &str,
    for_each: &ReadSequenceForEach,
    prior_names: &HashSet<String>,
    limits: &ReadSequenceEffectiveLimits,
    consistency: ReadSequenceConsistency,
    capabilities: ReadSequenceValidationCapabilities,
) -> Result<(), ReadSequenceValidationError> {
    let parsed_from = validate_selector_depth(&for_each.from, limits.max_selector_path_depth)?;
    let dependency = parsed_from.dependency_root().ok_or_else(|| {
        ReadSequenceValidationError::SelectorFailure {
            selector: for_each.from.0.clone(),
        }
    })?;
    if !prior_names.contains(dependency) {
        return Err(ReadSequenceValidationError::UnknownDependency {
            step: step_name.to_string(),
            dependency: dependency.to_string(),
        });
    }
    if !prior_names.contains(&for_each.join.to) {
        return Err(ReadSequenceValidationError::UnknownDependency {
            step: step_name.to_string(),
            dependency: for_each.join.to.clone(),
        });
    }
    validate_step_name(&for_each.as_name)?;
    validate_step_name(&for_each.join.as_name)?;

    let operation = for_each.operation()?;
    validate_operation_consistency(consistency, operation.reads_gsi(), capabilities)?;
    if let ReadSequenceStepOperation::Query(query) = operation
        && query.limit.is_none()
    {
        return Err(ReadSequenceValidationError::ChildQueryLimitRequired {
            step: step_name.to_string(),
        });
    }
    if matches!(operation, ReadSequenceStepOperation::BatchGet) && limits.max_fanout_per_step == 0 {
        return Err(ReadSequenceValidationError::FanoutLimitExceeded {
            actual: 1,
            limit: limits.max_fanout_per_step,
        });
    }
    Ok(())
}

fn validate_return_consumed_capacity(
    value: Option<&str>,
) -> Result<(), ReadSequenceValidationError> {
    match value {
        None | Some("INDEXES" | "TOTAL" | "NONE") => Ok(()),
        Some(value) => Err(ReadSequenceValidationError::InvalidReturnConsumedCapacity {
            value: value.to_string(),
        }),
    }
}
