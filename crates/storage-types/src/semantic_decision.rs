use crate::{StorageEnum, StorageError, StorageResult};

/// A semantic decision made before any write effects are staged.
/// Observation, decoding, and native failures remain outside this decision.
#[must_use]
pub enum SemanticDecision<T> {
    Accepted(T),
    Rejected(StorageError),
}

impl<T> SemanticDecision<T> {
    pub fn map<U>(self, accepted: impl FnOnce(T) -> U) -> SemanticDecision<U> {
        match self {
            Self::Accepted(value) => SemanticDecision::Accepted(accepted(value)),
            Self::Rejected(error) => SemanticDecision::Rejected(error),
        }
    }

    /// Preserve ordinary operation errors inside the caller's native attempt.
    pub fn into_result(self) -> StorageResult<T> {
        match self {
            Self::Accepted(value) => Ok(value),
            Self::Rejected(error) => Err(error),
        }
    }
}

/// Convert only request-validation failures before staging effects. Native,
/// decoding and invariant failures remain errors and abort the attempt.
impl<T> TryFrom<StorageResult<T>> for SemanticDecision<T> {
    type Error = StorageError;

    fn try_from(result: StorageResult<T>) -> StorageResult<Self> {
        match result {
            Ok(value) => Ok(Self::Accepted(value)),
            Err(error)
                if matches!(
                    error.as_ref(),
                    StorageEnum::Validation { .. }
                        | StorageEnum::RawValidation { .. }
                        | StorageEnum::KeyValidation(_)
                ) =>
            {
                Ok(Self::Rejected(error))
            }
            Err(error) => Err(error),
        }
    }
}
