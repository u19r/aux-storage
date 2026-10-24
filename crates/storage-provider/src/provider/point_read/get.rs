//! Provider-neutral ordinary GetItem plan (phase 6, §4).
//!
//! `GetItemPlan` owns one internal Get request. Binding resolves it against a
//! single metadata observation and borrows the owned key back as the
//! executor's one-element read slice.

use std::sync::Arc;

use storage_types::{
    KeyAttributes, StorageError, StorageResult, StoredTableInfo, TableName,
    validate_key_attributes_for_schema,
};

use super::table::PointReadTable;

/// Owned ordinary GetItem request: table identity plus one key.
pub struct GetItemPlan {
    pub table: TableName,
    pub key: KeyAttributes,
}

impl GetItemPlan {
    #[must_use]
    pub fn new(table: TableName, key: KeyAttributes) -> Self {
        Self { table, key }
    }

    /// Bind the owned request to its metadata observation. Checks table
    /// identity, validates the key against the table schema with the existing
    /// key validator, and borrows the key as a one-element read slice.
    pub fn bind(&self, table: Arc<StoredTableInfo>) -> StorageResult<PointReadTable<'_>> {
        if table.table_name != self.table {
            return Err(StorageError::internal(
                "point-read metadata identity must match the requested table",
            ));
        }
        validate_key_attributes_for_schema(&table.key_schema, &self.key)?;
        Ok(PointReadTable::bound(
            table,
            std::slice::from_ref(&self.key),
        ))
    }
}
