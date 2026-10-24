use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use super::{
    constants::{
        POINT_READ_IDENTITY_COPY_CHARGE, POINT_READ_KEY_COPY_CHARGE,
        POINT_READ_SOURCE_OVERHEAD_BYTES,
    },
    key_fingerprint,
};
use crate::types::{KeyAttributes, StorageError, TableName};

#[derive(Debug)]
pub(super) struct PointReadRequest {
    pub(super) table_name: TableName,
    pub(super) key: KeyAttributes,
    pub(super) fingerprint: String,
    pub(super) charge: NonZeroUsize,
}

#[derive(Debug, Default)]
pub struct PointReadPlan {
    pub(super) observer: super::observer::PointReadObserver,
    requests: Vec<Arc<PointReadRequest>>,
    dedupe: HashSet<String>,
    skipped_hot_total: u64,
    charged_bytes: usize,
}

impl PointReadPlan {
    pub fn with_observer(mut self, observer: fn(super::PointReadEvent)) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn skipped_hot_total(&self) -> u64 {
        self.skipped_hot_total
    }

    /// Adds a key request for prefetch. Duplicate `(table, key)` pairs are
    /// ignored.
    pub fn add_item(
        &mut self,
        table_name: TableName,
        key: KeyAttributes,
    ) -> crate::types::StorageResult<()> {
        let fingerprint = key_fingerprint(&table_name, &key)?;
        if self.dedupe.contains(&fingerprint) {
            return Ok(());
        }
        let encoded = serde_json::to_vec(&(&table_name, &key))
            .map_err(|error| StorageError::internal(&error))?
            .len();
        // Bound peak owned key/fingerprint copies, provider request and map
        // overhead.
        let bytes = encoded
            .saturating_mul(POINT_READ_KEY_COPY_CHARGE)
            .saturating_add(
                fingerprint
                    .len()
                    .saturating_mul(POINT_READ_IDENTITY_COPY_CHARGE),
            )
            .saturating_add(POINT_READ_SOURCE_OVERHEAD_BYTES);
        let charge =
            NonZeroUsize::new(bytes).ok_or_else(|| StorageError::service_unavailable(1))?;
        if self.requests.len() >= super::constants::POINT_READ_REQUEST_KEYS
            || bytes > super::constants::POINT_READ_BATCH_BYTES
            || self.charged_bytes.saturating_add(bytes) > super::constants::POINT_READ_REQUEST_BYTES
        {
            return Err(StorageError::service_unavailable(1));
        }
        self.charged_bytes += bytes;
        self.dedupe.insert(fingerprint.clone());
        self.requests.push(Arc::new(PointReadRequest {
            table_name,
            key,
            fingerprint,
            charge,
        }));
        Ok(())
    }

    /// Marks this contributor as hot-cache skipped for the current request.
    pub fn mark_skipped_hot(&mut self) {
        self.skipped_hot_total = self.skipped_hot_total.saturating_add(1);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub(super) fn into_parts(self) -> (Vec<Arc<PointReadRequest>>, u64) {
        (self.requests, self.skipped_hot_total)
    }
}
