//! Native key selectors skip whole namespaces without loading values or chunks.

use storage_provider::maintenance::TableHistoryNamespace;
use storage_types::{ReadProtection, StorageError, StorageResult, StreamName, TableHistoryId};

use crate::{KvAttempt, range::prefix_upper_bound, stream_write::key_layout};

pub(super) async fn load_namespace(
    attempt: &impl KvAttempt,
    history: TableHistoryId,
    after: Option<&[u8]>,
) -> StorageResult<Option<TableHistoryNamespace>> {
    let prefix = key_layout::table_history_row_prefix(history);
    if after.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
        return Err(StorageError::internal(
            "History cursor belongs to another namespace",
        ));
    }
    let begin = after
        .map_or_else(|| Some(prefix.clone()), prefix_upper_bound)
        .ok_or_else(|| StorageError::internal("Invalid history namespace cursor"))?;
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("Invalid history namespace prefix"))?;
    let Some(key) = attempt
        .first_key_in_range(&begin, &end, ReadProtection::Conflict)
        .await?
    else {
        return Ok(None);
    };
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| StorageError::internal("History key has another table"))?;
    let (name, version) = key_layout::read_segment(suffix)?;
    if version.len() != 12 {
        return Err(StorageError::internal(
            "History namespace key has an invalid version",
        ));
    }
    let mut physical = history.stream_prefix();
    physical.extend_from_slice(name);
    let cursor = key[..key.len() - version.len()].to_vec();
    Ok(Some(TableHistoryNamespace {
        stream_name: StreamName::new(&physical),
        cursor,
    }))
}
