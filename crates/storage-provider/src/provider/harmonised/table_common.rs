use std::collections::HashMap;

use storage_types::{
    AttributeValue, KeyAttributes, KeyType, StorageError, StorageResult, StoredTableInfo, TableKey,
    TableName, WireItem,
};

use super::{
    models::TableWriteError,
    table_finish::{FinishedObservedBatch, FinishedTableBatch, PreparedTableBatch},
};

/// Validate and consume identity-bound native evidence before shared finish.
/// Native adapters must use this entry point after their observation wave;
/// keeping the wire extraction here prevents a separate positional response
/// path from bypassing the coverage gate.
pub fn finish_batch_with_observations(
    batch: PreparedTableBatch,
    observations: crate::ValidatedWriteObservations,
) -> Result<FinishedObservedBatch, TableWriteError> {
    let super::write_preparation::ObservedWriteFinish { wires, histories } =
        observations.into_finish_inputs();
    let finished = finish_batch_by_ordinal(batch, wires, |_, error| Err(error))?;
    let histories = super::write_preparation::decode_write_histories(histories)?;
    Ok(FinishedObservedBatch {
        finished,
        histories,
    })
}

/// Finish identity-bound evidence by source ordinal.  Prepared batches keep
/// verbs in separate vectors for their physical owners, so a native result
/// ordered by request ordinal must never be consumed as a positional wire
/// page.
pub(super) fn finish_batch_by_ordinal<E>(
    batch: PreparedTableBatch,
    mut wires: Vec<(usize, Option<Option<WireItem>>)>,
    mut on_error: impl FnMut(usize, TableWriteError) -> Result<(), E>,
) -> Result<FinishedTableBatch, E>
where
    E: From<TableWriteError>,
{
    let mut puts = Vec::with_capacity(batch.puts.len());
    for put in batch.puts {
        let ordinal = put.ordinal;
        let old = take_wire(&mut wires, ordinal)?;
        match put.finish_with_old(old) {
            Ok(finished) => puts.push(finished),
            Err(error) => on_error(ordinal, error)?,
        }
    }
    let mut updates = Vec::with_capacity(batch.updates.len());
    for update in batch.updates {
        let ordinal = update.ordinal;
        let old = take_wire(&mut wires, ordinal)?;
        match update.finish_with_old(old) {
            Ok(finished) => updates.push(finished),
            Err(error) => on_error(ordinal, error)?,
        }
    }
    let mut deletes = Vec::with_capacity(batch.deletes.len());
    for delete in batch.deletes {
        let ordinal = delete.ordinal;
        let old = take_wire(&mut wires, ordinal)?;
        match delete.finish_with_old(old) {
            Ok(finished) => deletes.push(finished),
            Err(error) => on_error(ordinal, error)?,
        }
    }
    for check in batch.checks {
        let ordinal = check.ordinal;
        let old = take_wire(&mut wires, ordinal)?;
        if let Err(error) = check.check_with_old(old) {
            on_error(ordinal, error)?;
        }
    }
    if wires.iter().all(|(_, wire)| wire.is_none()) {
        Ok(FinishedTableBatch {
            puts,
            updates,
            deletes,
        })
    } else {
        Err(misaligned_batch().into())
    }
}

fn take_wire(
    wires: &mut [(usize, Option<Option<WireItem>>)],
    ordinal: usize,
) -> Result<Option<WireItem>, TableWriteError> {
    wires
        .binary_search_by_key(&ordinal, |(slot_ordinal, _)| *slot_ordinal)
        .ok()
        .and_then(|index| wires.get_mut(index))
        .and_then(|(_, wire)| wire.take())
        .ok_or_else(misaligned_batch)
}

pub fn misaligned_batch() -> TableWriteError {
    TableWriteError::Failed(StorageError::internal(
        "Table write batch carried a misaligned old-item page",
    ))
}

/// Expand a typed table key into request key attributes via the table key
/// schema. Shared by KV and SQL fetches so key-shape errors stay identical.
pub fn key_attributes_for(
    table_info: &StoredTableInfo,
    key: &TableKey,
) -> StorageResult<KeyAttributes> {
    let mut attributes = KeyAttributes::default();
    for element in &table_info.key_schema {
        let value = match element.key_type {
            KeyType::Hash => &key.hash_key,
            KeyType::Range => key.range_key.as_ref().ok_or_else(|| {
                StorageError::validation("Table key does not match the table key schema")
            })?,
        };
        attributes.insert(element.attribute_name.clone(), value.clone());
    }
    Ok(attributes)
}

/// Deduplicate table names while preserving first-seen order. Shared so
/// metadata fan-out stays a single batched lookup per table.
pub fn distinct_table_names<'a>(names: impl Iterator<Item = &'a TableName>) -> Vec<TableName> {
    let mut distinct = Vec::new();
    for name in names {
        if !distinct.contains(name) {
            distinct.push(name.clone());
        }
    }
    distinct
}

/// Distinct tables in a prepared batch (puts, updates, deletes, checks),
/// for stage-time metadata and lifecycle fan-out. Shared so every backend
/// loads the same table set in the same order.
pub fn prepared_table_names(batch: &PreparedTableBatch) -> Vec<TableName> {
    distinct_table_names(
        batch
            .puts
            .iter()
            .map(|put| &put.table_name)
            .chain(batch.updates.iter().map(|update| &update.table_name))
            .chain(batch.deletes.iter().map(|delete| &delete.table_name))
            .chain(batch.checks.iter().map(|check| &check.table_name)),
    )
}

pub fn key_attributes_to_map(key: &KeyAttributes) -> HashMap<String, AttributeValue> {
    key.iter()
        .map(|(name, value)| (name.to_owned(), value.clone()))
        .collect()
}
