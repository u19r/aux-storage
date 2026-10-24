//! Physical storage naming shared by every backend.
//!
//! Logical name newtypes (e.g. [`TableName`](crate::TableName),
//! [`IndexName`](crate::IndexName)) live alongside these wrappers. The full
//! `table_`/`gsi_`/`ttl_index_` compositions live as methods on
//! [`TableName`](crate::TableName); this module holds the sanitized physical
//! wrappers reused when formatting identifiers piecemeal.
use std::fmt::{Display, Formatter};

/// Wrapper for a sanitized physical GSI table name of form: gsi_<table>_<index>
pub struct GsiPhysicalName(String);

impl GsiPhysicalName {
    #[must_use]
    pub fn compose(sanitized_table: &str, sanitized_index: &str) -> Self {
        Self(format!("gsi_{sanitized_table}_{sanitized_index}"))
    }
}

impl Display for GsiPhysicalName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

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
