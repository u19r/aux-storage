//! Newtypes for physical `SQLite` table naming to ensure consistent
//! sanitization and avoid ad-hoc string formatting scattered across the
//! provider.
//!
//! Logical name newtypes (e.g. `TableName`, `IndexName`) live in
//! `storage_types`. These wrappers convert them into physical names safe for
//! `SQLite`.
use std::fmt::{Display, Formatter};

use storage_types::TableName;

/// Wrapper for a sanitized physical table name (main table) of form:
/// table_<sanitized>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalTableName(String);

impl PhysicalTableName {
    #[must_use]
    pub fn new(logical: &TableName) -> Self {
        Self(format!("table_{}", logical.sanitized_name()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for PhysicalTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Wrapper for a sanitized physical GSI table name of form: gsi_<table>_<index>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GsiPhysicalName(String);

impl GsiPhysicalName {
    #[must_use]
    pub fn compose(sanitized_table: &str, sanitized_index: &str) -> Self {
        Self(format!("gsi_{sanitized_table}_{sanitized_index}"))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
