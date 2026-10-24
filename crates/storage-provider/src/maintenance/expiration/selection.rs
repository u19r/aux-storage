//! Original bounded TTL rows and victim metadata, independent of execution
//! authority.

use serde::{Deserialize, Serialize};
use storage_common::ttl::TtlIndexKeyToken;
use storage_types::{ItemProvenance, KeyAttributes, StorageError, StorageResult};

use super::{
    AcceptedDirectExpirationPage, DirectExpirationScanRead, ExpirationIndexLocator, ExpirationPort,
};

/// Passive captured input. Binding table incarnation and authentic scheduler
/// scope belongs to the upper owner; preparation validates its bounded row
/// range.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpirationSelection {
    pub(super) rows: Vec<SelectedExpirationRow>,
    pub(super) has_more: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedExpirationRow {
    pub(super) index: ExpirationIndexLocator,
    pub(super) victim: Option<ItemProvenance>,
}

impl<P: ExpirationPort> AcceptedDirectExpirationPage<'_, P> {
    /// Capture the original ordered selection before applying or dropping its
    /// native borrow. Cleanup-only rows carry no victim; they cannot expire a
    /// newly discovered item during replay.
    pub fn capture_selection(&self) -> StorageResult<ExpirationSelection> {
        let mut victims = self.items.iter().peekable();
        let mut rows = Vec::with_capacity(self.write.deletions.index_entries.len());
        for index in &self.write.deletions.index_entries {
            let key = KeyAttributes::from(
                TtlIndexKeyToken::from(index.ttl_item_key.clone())
                    .parse_key_map(&self.write.table_info.info)?,
            );
            let victim = match victims.peek() {
                Some(item)
                    if item.key == key
                        && item
                            .old_item
                            .as_ref()
                            .map(|old| old.number_attribute_i64(&self.write.config.attribute_name))
                            .transpose()?
                            .flatten()
                            == Some(index.indexed_expiration_seconds) =>
                {
                    let metadata = item
                        .old_provenance
                        .clone()
                        .filter(|metadata| metadata.as_bytes() != ItemProvenance::LOCAL_BYTES)
                        .ok_or_else(invalid_selection)?;
                    victims.next();
                    Some(metadata)
                }
                _ => None,
            };
            rows.push(SelectedExpirationRow {
                index: index.clone(),
                victim,
            });
        }
        if victims.next().is_some() {
            return Err(invalid_selection());
        }
        Ok(ExpirationSelection {
            rows,
            has_more: self.write.result.next_cursor.is_some(),
        })
    }
}

impl ExpirationSelection {
    /// Borrow original victim metadata without exposing physical index rows.
    /// These facts grant no authority to expire a current item.
    pub fn victims(&self) -> impl Iterator<Item = &ItemProvenance> {
        self.rows.iter().filter_map(|row| row.victim.as_ref())
    }

    pub(super) fn validate_range(&self, scan: &DirectExpirationScanRead) -> StorageResult<()> {
        if self.rows.len() > scan.limit.get() || (self.has_more && self.rows.is_empty()) {
            return Err(invalid_selection());
        }
        let mut previous = scan
            .cursor
            .as_ref()
            .map(|cursor| (cursor.expiration_seconds, cursor.ttl_item_key.as_str()));
        for row in &self.rows {
            previous = Some(row.validate_after(scan.cutoff_seconds, previous)?);
        }
        Ok(())
    }
}

impl SelectedExpirationRow {
    fn validate_after(
        &self,
        cutoff: i64,
        previous: Option<(i64, &str)>,
    ) -> StorageResult<(i64, &str)> {
        let position = (
            self.index.indexed_expiration_seconds,
            self.index.ttl_item_key.as_str(),
        );
        if position.0 > cutoff
            || position.1.is_empty()
            || previous.is_some_and(|earlier| position <= earlier)
            || self
                .victim
                .as_ref()
                .is_some_and(|metadata| metadata.as_bytes() == ItemProvenance::LOCAL_BYTES)
        {
            return Err(invalid_selection());
        }
        Ok(position)
    }
}

pub(super) fn invalid_selection() -> StorageError {
    StorageError::validation("captured expiration selection is invalid")
}
