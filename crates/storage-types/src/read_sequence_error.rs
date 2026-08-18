use std::fmt;

use crate::{ReadSequenceConsistency, StorageError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSequenceValidationError {
    EmptySequence,
    DuplicateNodeName {
        name: String,
    },
    InvalidNodeName {
        name: String,
    },
    InvalidOperation {
        node: String,
        message: String,
    },
    UnknownNode {
        node: String,
        referenced: String,
    },
    UnknownInput {
        node: String,
        input: String,
    },
    SelfDependency {
        node: String,
    },
    MultipleIterationInputs {
        node: String,
    },
    InputCardinality {
        node: String,
        input: String,
    },
    InputResolution {
        node: String,
        input: String,
        expected: String,
        actual: String,
    },
    InputType {
        node: String,
        input: String,
        expected: String,
        actual: String,
    },
    InvalidStringTemplate {
        node: String,
    },
    UnreachableNode {
        node: String,
    },
    DependencyCycle {
        cycle: Vec<String>,
    },
    GraphResolutionInvariant {
        remaining: usize,
    },
    EmptyOutputs,
    NodeLimitExceeded {
        actual: usize,
        limit: u32,
    },
    FanoutLimitExceeded {
        actual: u32,
        limit: u32,
    },
    TotalReadLimitExceeded {
        actual: u32,
        limit: u32,
    },
    HardLimitExceeded {
        limit_name: &'static str,
        actual: u32,
        limit: u32,
    },
    SelectorBindingLimitExceeded {
        node: String,
        actual: usize,
        limit: u32,
    },
    SelectorPathTooDeep {
        selector: String,
        depth: u32,
        limit: u32,
    },
    SelectorFailure {
        selector: String,
    },
    SelectorTypeMismatch {
        selector: String,
        expected: &'static str,
        actual: &'static str,
    },
    UnsupportedConsistency {
        consistency: ReadSequenceConsistency,
    },
    TransactionalGsiRejected,
    StrongGsiRejected,
    ChildQueryLimitRequired {
        node: String,
    },
    StaleToken,
    SnapshotExpired,
    InvalidReturnConsumedCapacity {
        value: String,
    },
}

impl fmt::Display for ReadSequenceValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySequence => formatter.write_str("ReadSequence requires at least one node"),
            Self::DuplicateNodeName { name } => {
                write!(formatter, "ReadSequence node name '{name}' is duplicated")
            }
            Self::InvalidNodeName { name } => {
                write!(formatter, "ReadSequence node name '{name}' is invalid")
            }
            Self::InvalidOperation { node, message } => write!(
                formatter,
                "ReadSequence node '{node}' has an invalid operation: {message}"
            ),
            Self::UnknownNode { node, referenced } => write!(
                formatter,
                "ReadSequence node '{node}' references unknown node '{referenced}'"
            ),
            Self::UnknownInput { node, input } => write!(
                formatter,
                "ReadSequence node '{node}' references undeclared input '{input}'"
            ),
            Self::SelfDependency { node } => {
                write!(formatter, "ReadSequence node '{node}' depends on itself")
            }
            Self::MultipleIterationInputs { node } => write!(
                formatter,
                "ReadSequence node '{node}' has more than one iteration input"
            ),
            Self::InputCardinality { node, input } => write!(
                formatter,
                "ReadSequence node '{node}' input '{input}' has invalid cardinality"
            ),
            Self::InputResolution {
                node,
                input,
                expected,
                actual,
            } => write!(
                formatter,
                "ReadSequence node '{node}' input '{input}' expected {expected} value(s), got \
                 {actual}"
            ),
            Self::InputType {
                node,
                input,
                expected,
                actual,
            } => write!(
                formatter,
                "ReadSequence node '{node}' input '{input}' expected {expected}, got {actual}"
            ),
            Self::InvalidStringTemplate { node } => write!(
                formatter,
                "ReadSequence node '{node}' has an invalid string template"
            ),
            Self::UnreachableNode { node } => {
                write!(
                    formatter,
                    "ReadSequence node '{node}' is not required by Outputs"
                )
            }
            Self::DependencyCycle { cycle } => write!(
                formatter,
                "ReadSequence dependency cycle detected: {}",
                cycle.join(" -> ")
            ),
            Self::GraphResolutionInvariant { .. } => {
                formatter.write_str("ReadSequence graph resolution failed")
            }
            Self::EmptyOutputs => formatter.write_str("ReadSequence Outputs must not be empty"),
            Self::NodeLimitExceeded { actual, limit } => write!(
                formatter,
                "ReadSequence has {actual} nodes, exceeding the limit of {limit}"
            ),
            Self::FanoutLimitExceeded { actual, limit } => write!(
                formatter,
                "ReadSequence fanout {actual} exceeds the limit of {limit}"
            ),
            Self::TotalReadLimitExceeded { actual, limit } => write!(
                formatter,
                "ReadSequence total read limit {actual} exceeds the hard max of {limit}"
            ),
            Self::HardLimitExceeded {
                limit_name,
                actual,
                limit,
            } => write!(
                formatter,
                "ReadSequence {limit_name} {actual} exceeds the hard max of {limit}"
            ),
            Self::SelectorBindingLimitExceeded {
                node,
                actual,
                limit,
            } => write!(
                formatter,
                "ReadSequence node '{node}' has {actual} selector bindings, exceeding the limit \
                 of {limit}"
            ),
            Self::SelectorPathTooDeep {
                selector,
                depth,
                limit,
            } => write!(
                formatter,
                "ReadSequence selector '{selector}' depth {depth} exceeds the limit of {limit}"
            ),
            Self::SelectorFailure { selector } => {
                write!(formatter, "ReadSequence selector '{selector}' is invalid")
            }
            Self::SelectorTypeMismatch {
                selector,
                expected,
                actual,
            } => write!(
                formatter,
                "ReadSequence selector '{selector}' expected {expected}, got {actual}"
            ),
            Self::UnsupportedConsistency { consistency } => {
                write!(
                    formatter,
                    "ReadSequence consistency {consistency:?} is not supported"
                )
            }
            Self::TransactionalGsiRejected => formatter.write_str(
                "ReadSequence TRANSACTIONAL consistency cannot read GSIs unless immediate GSI \
                 consistency is enabled",
            ),
            Self::StrongGsiRejected => {
                formatter.write_str("ReadSequence STRONG consistency cannot read GSIs")
            }
            Self::ChildQueryLimitRequired { node } => write!(
                formatter,
                "ReadSequence child query in node '{node}' must specify Limit"
            ),
            Self::StaleToken => formatter.write_str("ReadSequence token is stale"),
            Self::SnapshotExpired => formatter.write_str("ReadSequence snapshot expired"),
            Self::InvalidReturnConsumedCapacity { value } => write!(
                formatter,
                "ReadSequence ReturnConsumedCapacity value '{value}' is invalid"
            ),
        }
    }
}

impl std::error::Error for ReadSequenceValidationError {}

impl From<ReadSequenceValidationError> for StorageError {
    fn from(error: ReadSequenceValidationError) -> Self {
        StorageError::validation(error.to_string())
    }
}
