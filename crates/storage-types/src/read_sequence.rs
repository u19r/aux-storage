use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ReadSequenceNode, ReadSequenceValidationError, read_sequence_graph::build_graph_plan};

pub const READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS: u32 = 8;
pub const READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS: u32 = 16;
pub const READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS: u32 = 100;
pub const READ_SEQUENCE_HARD_MAX_ROOT_ITEMS: u32 = 100;
pub const READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP: u32 = 100;
pub const READ_SEQUENCE_HARD_MAX_FANOUT_PER_STEP: u32 = 1024;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
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
    pub nodes: Vec<ReadSequenceNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Vec<String>>,
}

impl ReadSequenceRequest {
    #[must_use]
    pub fn new(nodes: Vec<crate::ReadSequenceNode>) -> Self {
        Self {
            nodes,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<(), ReadSequenceValidationError> {
        self.validate_with_capabilities(ReadSequenceValidationCapabilities::default())
    }

    pub fn validate_with_capabilities(
        &self,
        capabilities: ReadSequenceValidationCapabilities,
    ) -> Result<(), ReadSequenceValidationError> {
        self.validate_limits_and_capacity()?;
        build_graph_plan(self, capabilities).map(|_| ())
    }

    pub(crate) fn validate_limits_and_capacity(&self) -> Result<(), ReadSequenceValidationError> {
        validate_request_item_limits(self)?;
        validate_sequence_node_limit(self)?;
        validate_request_selector_limits(self)?;
        validate_total_read_limit(self)?;
        validate_return_consumed_capacity(self.return_consumed_capacity.as_deref())?;
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

fn validate_request_item_limits(
    request: &ReadSequenceRequest,
) -> Result<(), ReadSequenceValidationError> {
    for (actual, limit, name) in [
        (
            request
                .max_root_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS),
            READ_SEQUENCE_HARD_MAX_ROOT_ITEMS,
            "MaxRootItems",
        ),
        (
            request
                .max_intermediate_items
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS),
            READ_SEQUENCE_HARD_MAX_INTERMEDIATE_ITEMS,
            "MaxIntermediateItems",
        ),
        (
            request
                .max_child_query_items_per_parent
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_CHILD_QUERY_ITEMS_PER_PARENT),
            READ_SEQUENCE_HARD_MAX_CHILD_QUERY_ITEMS_PER_PARENT,
            "MaxChildQueryItemsPerParent",
        ),
        (
            request
                .max_response_bytes
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES),
            READ_SEQUENCE_HARD_MAX_RESPONSE_BYTES,
            "MaxResponseBytes",
        ),
    ] {
        validate_hard_cap(actual, limit, name)?;
    }
    validate_request_fanout_limit(request)
}

fn validate_request_fanout_limit(
    request: &ReadSequenceRequest,
) -> Result<(), ReadSequenceValidationError> {
    validate_cap(
        request
            .max_fanout_per_step
            .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP),
        READ_SEQUENCE_HARD_MAX_FANOUT_PER_STEP,
        |actual, limit| ReadSequenceValidationError::FanoutLimitExceeded { actual, limit },
    )?;
    Ok(())
}

fn validate_request_selector_limits(
    request: &ReadSequenceRequest,
) -> Result<(), ReadSequenceValidationError> {
    for (actual, limit, name) in [
        (
            request
                .max_selector_bindings_per_step
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SELECTOR_BINDINGS_PER_STEP),
            READ_SEQUENCE_HARD_MAX_SELECTOR_BINDINGS_PER_STEP,
            "MaxSelectorBindingsPerStep",
        ),
        (
            request
                .max_selector_path_depth
                .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SELECTOR_PATH_DEPTH),
            READ_SEQUENCE_HARD_MAX_SELECTOR_PATH_DEPTH,
            "MaxSelectorPathDepth",
        ),
    ] {
        validate_hard_cap(actual, limit, name)?;
    }
    Ok(())
}

fn validate_hard_cap(
    actual: u32,
    limit: u32,
    limit_name: &'static str,
) -> Result<(), ReadSequenceValidationError> {
    validate_cap(actual, limit, |actual, limit| {
        ReadSequenceValidationError::HardLimitExceeded {
            limit_name,
            actual,
            limit,
        }
    })
}

fn validate_sequence_node_limit(
    request: &ReadSequenceRequest,
) -> Result<(), ReadSequenceValidationError> {
    let limit = request
        .max_sequence_steps
        .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_SEQUENCE_STEPS);
    if limit > READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS {
        return Err(ReadSequenceValidationError::NodeLimitExceeded {
            actual: request.nodes.len(),
            limit: READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS,
        });
    }
    if request.nodes.len() > limit as usize {
        return Err(ReadSequenceValidationError::NodeLimitExceeded {
            actual: request.nodes.len(),
            limit,
        });
    }
    Ok(())
}

fn validate_total_read_limit(
    request: &ReadSequenceRequest,
) -> Result<(), ReadSequenceValidationError> {
    let total = request
        .max_total_read_items
        .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS);
    if total > READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS {
        return Err(ReadSequenceValidationError::TotalReadLimitExceeded {
            actual: total,
            limit: READ_SEQUENCE_HARD_MAX_TOTAL_READ_ITEMS,
        });
    }
    Ok(())
}

fn validate_cap<F>(actual: u32, limit: u32, error: F) -> Result<(), ReadSequenceValidationError>
where F: FnOnce(u32, u32) -> ReadSequenceValidationError {
    if actual > limit {
        Err(error(actual, limit))
    } else {
        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ReadSequenceSelector(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReadSequenceOnMissing {
    #[default]
    Null,
    Skip,
    Error,
}
