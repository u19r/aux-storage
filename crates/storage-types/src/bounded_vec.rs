use crate::{StorageError, StorageResult};

/// Vector with its length bounds proven at construction.
///
/// Batch-size limits are enforced once, here, instead of re-checked at
/// every layer. Content validation stays where its context lives (wire
/// shapes at the request boundary, schema-aware checks in provider
/// preparation); this type carries only the count proof downstream. Bounds
/// are const generics so each batch kind names its own limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedVec<T, const MIN: usize, const MAX: usize>(Vec<T>);

impl<T, const MIN: usize, const MAX: usize> BoundedVec<T, MIN, MAX> {
    pub fn try_new(items: Vec<T>) -> StorageResult<Self> {
        if items.len() < MIN || items.len() > MAX {
            return Err(StorageError::validation(format!(
                "Batch must contain between {MIN} and {MAX} items"
            )));
        }
        Ok(Self(items))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

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
