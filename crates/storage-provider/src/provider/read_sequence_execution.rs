use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeMap, KeyAttributes, ReadSequenceInputReference, ReadSequenceNodeId, StorageResult,
    TableName,
};

/// A backend-neutral result row returned by an optional whole-plan lowering.
/// The API layer remains responsible for constructing the public response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadSequenceFlatRow {
    pub node: ReadSequenceNodeId,
    pub invocation_ordinal: u32,
    pub input_refs: BTreeMap<String, ReadSequenceInputReference>,
    pub result: ReadSequenceFlatResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadSequenceFlatResult {
    Get {
        item: Option<AttributeMap>,
    },
    BatchGet {
        responses: HashMap<TableName, Vec<AttributeMap>>,
    },
    Query {
        items: Vec<AttributeMap>,
        count: u32,
        scanned_count: u32,
        last_evaluated_key: Option<KeyAttributes>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadSequenceExecuted {
    pub rows: Vec<ReadSequenceFlatRow>,
    pub next_continuation: Option<String>,
}

/// A provider-neutral page budget for an optimized whole-plan execution.
///
/// The API manager still owns the public response-byte contract.  Providers
/// use this item frontier to avoid issuing a complete Query page when a
/// request has an explicit item/response budget; the continuation returned by
/// the provider is then the only state needed to resume that bounded page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSequenceExecutionBudget {
    pub max_items: Option<u32>,
}

impl ReadSequenceExecutionBudget {
    #[must_use]
    pub const fn unbounded() -> Self {
        Self { max_items: None }
    }

    #[must_use]
    pub const fn bounded_items(max_items: u32) -> Self {
        Self {
            max_items: Some(max_items),
        }
    }

    #[must_use]
    pub const fn is_unbounded(self) -> bool {
        self.max_items.is_none()
    }

    /// Build the only bounded shape currently supported by SQL providers:
    /// one independent Query with a provider-owned item frontier.
    pub fn bounded_query_plan(
        self,
        plan: &storage_types::ReadSequencePlan,
        default_query_limit: u32,
    ) -> Result<storage_types::ReadSequencePlan, ReadSequenceUnsupportedReason> {
        let Some(max_items) = self.max_items else {
            return Err(ReadSequenceUnsupportedReason::ParameterLimit);
        };
        if max_items == 0 {
            return Err(ReadSequenceUnsupportedReason::ParameterLimit);
        }
        let Some(node) = plan.nodes.first() else {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        };
        if plan.nodes.len() != 1
            || !node.inputs().is_empty()
            || node.iterate.is_some()
            || !node.after().is_empty()
        {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let storage_types::ReadSequenceNodeOperation::Query(request) = &node.operation else {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        };
        let mut bounded_plan = plan.clone();
        let Some(storage_types::ReadSequenceNodeOperation::Query(bounded_request)) = bounded_plan
            .nodes
            .first_mut()
            .map(|node| &mut node.operation)
        else {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        };
        bounded_request.limit = Some(request.limit.unwrap_or(default_query_limit).min(max_items));
        Ok(bounded_plan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadSequenceUnsupportedReason {
    BackendCapability,
    OperationShape,
    ParameterLimit,
    PhysicalLayout,
    Continuation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadSequenceExecution {
    Executed(ReadSequenceExecuted),
    Unsupported(ReadSequenceUnsupportedReason),
}

impl ReadSequenceExecution {
    #[must_use]
    pub const fn unsupported() -> Self {
        Self::Unsupported(ReadSequenceUnsupportedReason::BackendCapability)
    }
}

/// A provider-internal mapped-range request. The API request never carries
/// these bytes; a backend capability creates them only after physical
/// eligibility has been proved. The source range may represent either a
/// partition Query or an exact point Get. A mapper may then resolve each
/// source row to a point item or to a target partition range; the public
/// ReadSequence operation direction is therefore not coupled to this
/// backend primitive.
#[derive(Debug, PartialEq, Eq)]
pub struct ReadSequenceMappedRangeRequest {
    pub begin: Vec<u8>,
    pub end: Vec<u8>,
    /// `None` reads only the primary range. A mapper is present only when the
    /// child is a distinct physical item; GSI projections are never followed
    /// to their base item.
    pub mapper: Option<Vec<u8>>,
    pub exclusive_start: Option<Vec<u8>>,
    pub reverse: bool,
    pub target_bytes: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ReadSequenceMappedKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ReadSequenceMappedEntry {
    pub parent_key: Vec<u8>,
    pub parent_value: Vec<u8>,
    pub begin: Vec<u8>,
    pub end: Vec<u8>,
    pub key_values: Vec<ReadSequenceMappedKeyValue>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ReadSequenceMappedRangePage {
    pub entries: Vec<ReadSequenceMappedEntry>,
    pub more: bool,
}

impl ReadSequenceMappedRangePage {
    /// Validate the provider-neutral mapped envelope before it can be merged
    /// into a graph result.  A secondary continuation is not a complete
    /// logical read: publishing it would silently drop parents on retry.
    pub fn validate_complete(&self, reverse: bool) -> StorageResult<()> {
        if self.more {
            return Err(storage_types::StorageError::unsupported(
                "mapped range returned an incomplete secondary page",
            ));
        }
        let mut previous_parent: Option<&[u8]> = None;
        for entry in &self.entries {
            // A mapped point lookup has no exclusive secondary end selector
            // in the FoundationDB binding, so an empty end is valid only for
            // that envelope shape.  Range results must still prove begin < end.
            if entry.parent_key.is_empty()
                || entry.begin.is_empty()
                || (!entry.end.is_empty() && entry.begin >= entry.end)
            {
                return Err(storage_types::StorageError::internal(
                    "mapped range returned malformed parent or secondary bounds",
                ));
            }
            if previous_parent.is_some_and(|previous| {
                if reverse {
                    previous <= entry.parent_key.as_slice()
                } else {
                    previous >= entry.parent_key.as_slice()
                }
            }) {
                return Err(storage_types::StorageError::internal(
                    "mapped range returned out-of-order parent keys",
                ));
            }
            previous_parent = Some(entry.parent_key.as_slice());
            let mut previous_key: Option<&[u8]> = None;
            for key_value in &entry.key_values {
                if key_value.key.is_empty()
                    || previous_key.is_some_and(|previous| previous >= key_value.key.as_slice())
                {
                    return Err(storage_types::StorageError::internal(
                        "mapped range returned malformed or out-of-order secondary keys",
                    ));
                }
                previous_key = Some(key_value.key.as_slice());
            }
        }
        Ok(())
    }
}
