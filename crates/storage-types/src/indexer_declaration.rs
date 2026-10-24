use crate::{MAX_INDEXER_DECLARATION_BYTES, MaxIndexers, StorageError, StorageResult};

/// Ordered attribute names with at most 1,024 UTF-8 bytes in total, including
/// absent names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerDeclaration(Vec<String>);

impl IndexerDeclaration {
    pub fn try_new(names: Vec<String>, capacity: MaxIndexers) -> StorageResult<Self> {
        Self::validate(&names, capacity)?;
        Ok(Self(names))
    }

    pub fn validate(names: &[String], capacity: MaxIndexers) -> StorageResult<()> {
        if names.len() > capacity.as_usize() {
            return Err(StorageError::validation("Indexers:too_many"));
        }
        let mut bytes = IndexerDeclarationBytes::default();
        for (index, name) in names.iter().enumerate() {
            if name.is_empty() {
                return Err(StorageError::validation("Indexers:empty_attribute"));
            }
            if names[..index].iter().any(|seen| seen == name) {
                return Err(StorageError::validation("Indexers:duplicate_attribute"));
            }
            bytes.include_name(name)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub fn into_names(self) -> Vec<String> {
        self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Default)]
pub(crate) struct IndexerDeclarationBytes(usize);

impl IndexerDeclarationBytes {
    pub(crate) fn include_name(&mut self, name: &str) -> StorageResult<()> {
        let total = self
            .0
            .checked_add(name.len())
            .filter(|total| *total <= MAX_INDEXER_DECLARATION_BYTES)
            .ok_or_else(|| StorageError::validation("Indexers:declaration_too_large"))?;
        self.0 = total;
        Ok(())
    }
}
