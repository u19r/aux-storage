use std::collections::HashSet;

use storage_common::TtlIndexWriteBatch;
use storage_types::{
    AttributeMap, IndexerDeclaration, KeyAttributes, ReturnValuesOldNewUpdated, TableKey,
    TableName, WireItem,
};

use crate::{
    provider::harmonised::{
        conditions::UncheckedCondition,
        models::{
            FinishedTableDelete, FinishedTablePut, FinishedTableUpdate, StreamWriteIntent,
            TableWriteError, TtlWriteIntent,
        },
        write_preparation::WriteObservationRequirements,
    },
    update_logic::apply_update_operations,
};

/// Prepared put: all local work done, old-item need declared.
#[derive(Clone)]
pub struct PreparedTablePut {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    /// Canonical primary-key identity retained during preparation. Native
    /// acquisition and SQL finish pairing compare this value directly rather
    /// than serializing the typed key again.
    pub key_json: String,
    pub table_key: TableKey,
    pub key_fingerprint: String,
    pub new_item: WireItem,
    pub new_indexers: IndexerDeclaration,
    pub observation: WriteObservationRequirements,
    /// Whether the successful response includes the previous item.
    pub return_old: bool,
    pub condition: Option<UncheckedCondition>,
    pub return_old_on_condition_failure: bool,
    pub stream: StreamWriteIntent,
    pub ttl: TtlWriteIntent,
}

impl PreparedTablePut {
    /// Small pure finish called inside the transaction after the old-item
    /// fetch. Evaluates the condition, diffs TTL writes, and resolves both
    /// stream images. No parsing, no I/O.
    pub fn finish_with_old(
        self,
        old: Option<WireItem>,
    ) -> Result<FinishedTablePut, TableWriteError> {
        let old = merge_request_keys(old, &self.key)?;
        if let Some(condition) = self.condition
            && !condition.check_borrowed(old.as_ref())?
        {
            return Err(TableWriteError::condition_failed(
                self.ordinal,
                old.filter(|_| self.return_old_on_condition_failure)
                    .map(Box::new),
                self.return_old_on_condition_failure,
            ));
        }
        let ttl_writes = TtlIndexWriteBatch::for_wire_items(
            &self.ttl.table,
            self.ttl.config.as_ref(),
            old.as_ref(),
            Some(&self.new_item),
        )?;
        let stream = self.stream.context;
        Ok(FinishedTablePut {
            ordinal: self.ordinal,
            table_name: self.table_name,
            key: self.key,
            table_key: self.table_key,
            new_item: self.new_item,
            old_item: old,
            new_indexers: self.new_indexers,
            ttl_writes,
            item_stream_ttl_hours: self.ttl.item_stream_ttl_hours,
            ttl_context: self.ttl.context.clone(),
            stream,
        })
    }
}

/// Prepared update: parsed update operations plus parsed condition.
/// Application onto the old (or key-seeded) image happens in
/// `finish_with_old`, inside the transaction.
#[derive(Clone)]
pub struct PreparedTableUpdate {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub key_json: String,
    pub table_key: TableKey,
    pub key_fingerprint: String,
    pub operations: Vec<crate::UpdateOperation>,
    pub observation: WriteObservationRequirements,
    /// Whether the successful response includes an old image.
    pub return_old: bool,
    pub condition: Option<UncheckedCondition>,
    pub return_old_on_condition_failure: bool,
    pub return_values: Option<ReturnValuesOldNewUpdated>,
    /// None inherits the stored declaration from the old item at stage
    /// time; Some replaces it (empty clears).
    pub new_indexers: Option<IndexerDeclaration>,
    pub stream: StreamWriteIntent,
    pub ttl: TtlWriteIntent,
}

impl PreparedTableUpdate {
    /// Small pure finish: evaluates the condition, applies update operations
    /// onto the old image (or a key-seeded image when absent), diffs TTL
    /// writes, and resolves both stream images. No parsing, no I/O.
    pub fn finish_with_old(
        self,
        old: Option<WireItem>,
    ) -> Result<FinishedTableUpdate, TableWriteError> {
        let old = merge_request_keys(old, &self.key)?;
        if let Some(condition) = self.condition
            && !condition.check_borrowed(old.as_ref())?
        {
            return Err(TableWriteError::condition_failed(
                self.ordinal,
                old.filter(|_| self.return_old_on_condition_failure)
                    .map(Box::new),
                self.return_old_on_condition_failure,
            ));
        }
        let base_map = match old.as_ref() {
            Some(item) => AttributeMap::try_from(item)?,
            None => self
                .key
                .iter()
                .map(|(name, value)| (name.to_owned(), value.clone()))
                .collect(),
        };
        let updated = apply_update_operations(base_map, &self.operations)?;
        let new_item = WireItem::try_from(updated)?.with_verified_keys(&self.key)?;
        let ttl_writes = TtlIndexWriteBatch::for_wire_items(
            &self.ttl.table,
            self.ttl.config.as_ref(),
            old.as_ref(),
            Some(&new_item),
        )?;
        let stream = self.stream.context;
        Ok(FinishedTableUpdate {
            ordinal: self.ordinal,
            table_name: self.table_name,
            key: self.key,
            table_key: self.table_key,
            new_item,
            old_item: old,
            new_indexers: self.new_indexers,
            ttl_writes,
            item_stream_ttl_hours: self.ttl.item_stream_ttl_hours,
            ttl_context: self.ttl.context.clone(),
            stream,
            return_values: self.return_values,
        })
    }
}

/// Prepared delete: key plus parsed condition.
#[derive(Clone)]
pub struct PreparedTableDelete {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub key_json: String,
    pub table_key: TableKey,
    pub key_fingerprint: String,
    pub observation: WriteObservationRequirements,
    /// Whether the successful response includes the previous item.
    pub return_old: bool,
    pub condition: Option<UncheckedCondition>,
    pub return_old_on_condition_failure: bool,
    pub stream: StreamWriteIntent,
    pub ttl: TtlWriteIntent,
}

impl PreparedTableDelete {
    /// Small pure finish: evaluates the condition, diffs TTL deletes, and
    /// captures the old image for stream and receipts. No parsing, no I/O.
    pub fn finish_with_old(
        self,
        old: Option<WireItem>,
    ) -> Result<FinishedTableDelete, TableWriteError> {
        let old = merge_request_keys(old, &self.key)?;
        if let Some(condition) = self.condition {
            // Deletes evaluate missing items against their key image,
            // mirroring `use_key_attributes_for_missing_item_condition`.
            let key_image = old
                .is_none()
                .then(|| {
                    AttributeMap::new()
                        .with_verified_keys(&self.key)
                        .and_then(|attributes| WireItem::try_from(&attributes))
                })
                .transpose()?;
            if !condition.check_borrowed(old.as_ref().or(key_image.as_ref()))? {
                return Err(TableWriteError::condition_failed(
                    self.ordinal,
                    old.filter(|_| self.return_old_on_condition_failure)
                        .map(Box::new),
                    self.return_old_on_condition_failure,
                ));
            }
        }
        let ttl_writes = TtlIndexWriteBatch::for_wire_items(
            &self.ttl.table,
            self.ttl.config.as_ref(),
            old.as_ref(),
            None,
        )?;
        let stream = self.stream.context;
        Ok(FinishedTableDelete {
            ordinal: self.ordinal,
            table_name: self.table_name,
            key: self.key,
            table_key: self.table_key,
            old_item: old,
            ttl_writes,
            item_stream_ttl_hours: self.ttl.item_stream_ttl_hours,
            ttl_context: self.ttl.context.clone(),
            stream,
        })
    }
}

/// Prepared condition check: key plus parsed condition.
#[derive(Clone)]
pub struct PreparedTableCheck {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub key_json: String,
    pub table_key: TableKey,
    pub key_fingerprint: String,
    pub condition: UncheckedCondition,
    pub return_old_on_condition_failure: bool,
}

impl PreparedTableCheck {
    pub fn check_with_old(self, old: Option<WireItem>) -> Result<(), TableWriteError> {
        let old = merge_request_keys(old, &self.key)?;
        if self.condition.check_borrowed(old.as_ref())? {
            Ok(())
        } else {
            Err(TableWriteError::condition_failed(
                self.ordinal,
                old.map(Box::new),
                self.return_old_on_condition_failure,
            ))
        }
    }
}

/// Prepared batch mirroring the input shape.
#[derive(Clone, Default)]
pub struct PreparedTableBatch {
    pub puts: Vec<PreparedTablePut>,
    pub updates: Vec<PreparedTableUpdate>,
    pub deletes: Vec<PreparedTableDelete>,
    pub checks: Vec<PreparedTableCheck>,
}

impl PreparedTableBatch {
    /// Derive the evidence contract from the prepared members that will
    /// consume the native acquisition. Keeping this on the prepared batch
    /// prevents provider adapters from rebuilding positional requirements.
    #[must_use]
    pub fn observation_requirements(&self) -> Vec<crate::WriteObservationRequirement> {
        let mut requirements = Vec::with_capacity(
            self.puts.len() + self.updates.len() + self.deletes.len() + self.checks.len(),
        );
        for put in &self.puts {
            requirements.push(crate::WriteObservationRequirement::new(
                crate::WriteObservationIdentity::new(
                    put.table_name.clone(),
                    put.key.clone(),
                    put.ordinal,
                    crate::WriteObservationRole::Write,
                ),
                put.observation,
            ));
        }
        for update in &self.updates {
            requirements.push(crate::WriteObservationRequirement::new(
                crate::WriteObservationIdentity::new(
                    update.table_name.clone(),
                    update.key.clone(),
                    update.ordinal,
                    crate::WriteObservationRole::Write,
                ),
                update.observation,
            ));
        }
        for delete in &self.deletes {
            requirements.push(crate::WriteObservationRequirement::new(
                crate::WriteObservationIdentity::new(
                    delete.table_name.clone(),
                    delete.key.clone(),
                    delete.ordinal,
                    crate::WriteObservationRole::Write,
                ),
                delete.observation,
            ));
        }
        for check in &self.checks {
            requirements.push(crate::WriteObservationRequirement::new(
                crate::WriteObservationIdentity::new(
                    check.table_name.clone(),
                    check.key.clone(),
                    check.ordinal,
                    crate::WriteObservationRole::Check,
                ),
                WriteObservationRequirements::OldItem,
            ));
        }
        // Native adapters execute the request in source order, while the
        // prepared batch stores members grouped by verb.  Keep this ordering
        // on the shared boundary so every adapter and the shared finish use
        // the same identity sequence.
        requirements.sort_unstable_by_key(|requirement| requirement.identity.ordinal);
        requirements
    }

    /// SQL acquisition loads application bytes only for writes that need an
    /// old image. Every write still loads its durable revision sidecar.
    #[must_use]
    pub fn old_item_ordinals(&self) -> HashSet<usize> {
        self.puts
            .iter()
            .filter(|put| put.observation.requests_old_item())
            .map(|put| put.ordinal)
            .chain(
                self.updates
                    .iter()
                    .filter(|update| update.observation.requests_old_item())
                    .map(|update| update.ordinal),
            )
            .chain(
                self.deletes
                    .iter()
                    .filter(|delete| delete.observation.requests_old_item())
                    .map(|delete| delete.ordinal),
            )
            .collect()
    }
}

/// Finished batch mirroring the prepared shape.
#[derive(Clone, Default)]
pub struct FinishedTableBatch {
    pub puts: Vec<FinishedTablePut>,
    pub updates: Vec<FinishedTableUpdate>,
    pub deletes: Vec<FinishedTableDelete>,
}

/// A finished native attempt retains the durable evidence that shared finish
/// accepted. Family mappers can lower its changes without choosing history
/// from a second, positional provider sidecar.
pub struct FinishedObservedBatch {
    pub(crate) finished: FinishedTableBatch,
    pub(crate) histories: Vec<super::super::write_preparation::ObservedWriteHistory>,
}

impl FinishedObservedBatch {
    #[must_use]
    pub fn histories(&self) -> &[super::super::write_preparation::ObservedWriteHistory] {
        &self.histories
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        FinishedTableBatch,
        Vec<super::super::write_preparation::ObservedWriteHistory>,
    ) {
        (self.finished, self.histories)
    }
}

impl std::ops::Deref for FinishedObservedBatch {
    type Target = FinishedTableBatch;

    fn deref(&self) -> &Self::Target {
        &self.finished
    }
}

/// Merge physically addressed request keys into a fetched image. Stored
/// rows predate key materialization: their bytes can omit key attributes
/// that condition checks, update application, stream records, and receipts
/// must see. Merging only fills missing keys and errors on mismatch, so
/// previously complete images pass through untouched.
fn merge_request_keys(
    old: Option<WireItem>,
    key: &KeyAttributes,
) -> Result<Option<WireItem>, TableWriteError> {
    old.map(|item| item.with_verified_keys(key))
        .transpose()
        .map_err(TableWriteError::from)
}
