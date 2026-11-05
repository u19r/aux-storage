//! Match native ordinals and reject malformed or ambiguous physical results.

use storage_provider::{ItemWriteObservation, ItemWriteObservationRead};
use storage_types::{StorageError, StorageResult, StoredWireItem};

use crate::{SqlRow, SqlValue, item_tombstone::decode::decode_deleted, item_write::decode_live};

pub(crate) fn match_observation_rows(
    count: usize,
    rows: Vec<SqlRow>,
) -> StorageResult<Vec<Option<SqlRow>>> {
    let mut matched = (0..count).map(|_| None).collect::<Vec<_>>();
    for row in rows {
        let mut values = row.into_values();
        let Some(SqlValue::Integer(ordinal)) = values.first() else {
            return Err(StorageError::internal(
                "SQL observation ordinal has invalid type",
            ));
        };
        let position = usize::try_from(*ordinal)
            .ok()
            .and_then(|ordinal| matched.get_mut(ordinal))
            .ok_or_else(|| StorageError::internal("SQL observation row has no requested key"))?;
        if position.replace(SqlRow::new(values.split_off(1))).is_some() {
            return Err(StorageError::internal(
                "SQL observation returned a duplicate key",
            ));
        }
    }
    Ok(matched)
}

pub(crate) fn decode_write_observations(
    reads: &[ItemWriteObservationRead<'_>],
    live_rows: Vec<Option<SqlRow>>,
    deleted_rows: Vec<Option<SqlRow>>,
    hydrated: Vec<Option<StoredWireItem>>,
) -> StorageResult<Vec<ItemWriteObservation>> {
    if live_rows.len() != reads.len()
        || deleted_rows.len() != reads.len()
        || hydrated.len() != reads.len()
    {
        return Err(StorageError::internal(
            "SQL observation result count differs",
        ));
    }
    live_rows
        .into_iter()
        .zip(deleted_rows)
        .zip(hydrated)
        .map(|((live, deleted), stored)| {
            if live.is_some() && deleted.is_some() {
                return Err(StorageError::internal(
                    "SQL key has both a live item and a tombstone",
                ));
            }
            match live {
                Some(row) => decode_live(
                    row,
                    stored
                        .ok_or_else(|| StorageError::internal("Missing SQL observation content"))?,
                ),
                None => match deleted {
                    Some(row) => decode_deleted(row),
                    None => Ok(ItemWriteObservation::Absent),
                },
            }
        })
        .collect()
}
