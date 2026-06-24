use std::fmt;

use crate::{ReadSequenceConsistency, StorageError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSequenceValidationError {
    EmptySequence,
    DuplicateStepName {
        name: String,
    },
    InvalidStepName {
        name: String,
    },
    InvalidStepOperation {
        step: String,
    },
    InvalidForEachOperation,
    UnknownDependency {
        step: String,
        dependency: String,
    },
    StepLimitExceeded {
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
        step: String,
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
    TemplateFailure {
        template: String,
    },
    UnsupportedConsistency {
        consistency: ReadSequenceConsistency,
    },
    TransactionalGsiRejected,
    StrongGsiRejected,
    ChildQueryLimitRequired {
        step: String,
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
            Self::EmptySequence => formatter.write_str("ReadSequence requires at least one step"),
            Self::DuplicateStepName { name } => {
                write!(formatter, "ReadSequence step name '{name}' is duplicated")
            }
            Self::InvalidStepName { name } => {
                write!(formatter, "ReadSequence step name '{name}' is invalid")
            }
            Self::InvalidStepOperation { step } => {
                write!(
                    formatter,
                    "ReadSequence step '{step}' must specify exactly one operation"
                )
            }
            Self::InvalidForEachOperation => {
                formatter.write_str("ReadSequence ForEach must specify exactly one child operation")
            }
            Self::UnknownDependency { step, dependency } => write!(
                formatter,
                "ReadSequence step '{step}' references unknown or later step '{dependency}'"
            ),
            Self::StepLimitExceeded { actual, limit } => write!(
                formatter,
                "ReadSequence has {actual} steps, exceeding the limit of {limit}"
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
                step,
                actual,
                limit,
            } => write!(
                formatter,
                "ReadSequence step '{step}' has {actual} selector bindings, exceeding the limit \
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
            Self::TemplateFailure { template } => {
                write!(formatter, "ReadSequence template '{template}' is invalid")
            }
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
            Self::ChildQueryLimitRequired { step } => write!(
                formatter,
                "ReadSequence child query in step '{step}' must specify Limit"
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
