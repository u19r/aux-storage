use crate::{StorageError, StorageResult};

/// Vector with its non-emptiness proven at construction.
///
/// Batch-size upper bounds (where they exist) ride in `BoundedVec`; this
/// type carries only the non-empty proof downstream so callers never
/// re-check `is_empty`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonZeroVec<T>(Vec<T>);

impl<T> NonZeroVec<T> {
    pub fn try_new(items: Vec<T>) -> StorageResult<Self> {
        if items.is_empty() {
            return Err(StorageError::validation(
                "Batch must contain at least one item",
            ));
        }
        Ok(Self(items))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always false: construction proves non-emptiness. Provided for
    /// call-site parity with `BoundedVec`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}
