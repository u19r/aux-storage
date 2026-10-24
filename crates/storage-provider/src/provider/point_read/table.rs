//! Borrowed point-read observation for one table (phase 6, §4).
//!
//! `PointReadTable` pairs one metadata observation with the borrowed request
//! keys it authorizes. It performs no I/O and renders no SQL; the native
//! executor owns statement preparation and row reads.

use std::sync::Arc;

use storage_types::{KeyAttributes, StoredTableInfo};

/// One table bound to its borrowed request keys: the shared metadata
/// observation plus the exact key slice the executor must read.
#[derive(Debug)]
pub struct PointReadTable<'a> {
    pub table: Arc<StoredTableInfo>,
    pub keys: &'a [KeyAttributes],
}

impl<'a> PointReadTable<'a> {
    pub(super) fn bound(table: Arc<StoredTableInfo>, keys: &'a [KeyAttributes]) -> Self {
        Self { table, keys }
    }
}
