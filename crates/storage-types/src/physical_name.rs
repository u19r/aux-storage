//! Physical storage naming shared by every backend.
//!
//! Logical name newtypes (e.g. [`TableName`](crate::TableName),
//! [`IndexName`](crate::IndexName)) live alongside these wrappers.

/// `AttributeName` newtype ensures we sanitize once and reuse.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttributeName(String);

impl AttributeName {
    #[must_use]
    pub fn new(raw: &str) -> Self {
        Self(raw.replace(['-', ' '], "_"))
    }
    #[must_use]
    pub fn sanitized(&self) -> &str {
        &self.0
    }
}
