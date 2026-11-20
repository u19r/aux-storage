//! Node-local hints of queue catalog rows (W7 P7). In-process state is only a
//! hint: a backend that answers from it reads the row in the same attempt and
//! fails the attempt with a transaction conflict when the row differs.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

use storage_types::TimestampMillis;

use crate::QueueCatalogRecord;

/// How long a row read on this node answers later catalog reads.
pub const QUEUE_CATALOG_HINT_MILLIS: i64 = 5_000;
/// Hints kept per store; a full map is cleared, not evicted entry by entry.
const QUEUE_CATALOG_HINT_ENTRIES: usize = 4_096;

#[derive(Default)]
pub struct QueueCatalogHints {
    rows: Mutex<HashMap<String, (QueueCatalogRecord, TimestampMillis)>>,
}

impl QueueCatalogHints {
    /// The row read at most `QUEUE_CATALOG_HINT_MILLIS` before `now`.
    #[must_use]
    pub fn get(&self, name: &str, now: TimestampMillis) -> Option<QueueCatalogRecord> {
        let rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let (record, read_at) = rows.get(name)?;
        let age = now
            .timestamp_millis()
            .saturating_sub(read_at.timestamp_millis());
        (0..QUEUE_CATALOG_HINT_MILLIS)
            .contains(&age)
            .then(|| record.clone())
    }

    /// Records the row that an attempt read at `now`; an absent row removes
    /// the hint.
    pub fn observe(&self, name: &str, record: Option<&QueueCatalogRecord>, now: TimestampMillis) {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(record) = record else {
            rows.remove(name);
            return;
        };
        if rows.len() >= QUEUE_CATALOG_HINT_ENTRIES && !rows.contains_key(name) {
            rows.clear();
        }
        rows.insert(name.to_owned(), (record.clone(), now));
    }

    /// Drops the hint after a mismatch or a catalog write of this node.
    pub fn forget(&self, name: &str) {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name);
    }
}
