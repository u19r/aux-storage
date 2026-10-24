//! One ordered cursor key layout shared by RocksDB and FoundationDB.

use std::num::NonZeroU32;

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, storage_serde};
use stream_provider::{CursorName, MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS, StreamCursor};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::{STREAM_CURSOR_PREFIX, STREAM_READ_BYTES},
    range::prefix_upper_bound,
    stream_write::key_layout::append_segment,
};

pub(crate) async fn read_cursor<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    stream_name: &StreamName,
    cursor_name: &CursorName,
) -> StorageResult<Option<StreamCursor>> {
    native
        .get(&cursor_key(stream_name, cursor_name)?, protection)
        .await?
        .map(|bytes| {
            let cursor: StreamCursor = storage_serde::from_bytes(&bytes)?;
            if cursor.stream_name != *stream_name || cursor.name != *cursor_name {
                return Err(StorageError::internal("KV stream cursor identity differs"));
            }
            Ok(cursor)
        })
        .transpose()
}

pub(crate) async fn list_system_cursors<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
) -> StorageResult<Vec<StreamCursor>> {
    let stream_name = StreamName::system_table_stream();
    let prefix = cursor_stream_prefix(&stream_name)?;
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV System cursor prefix has no upper bound"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin: prefix,
                end,
                reverse: false,
                limit: NonZeroU32::new(MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS + 1)
                    .ok_or_else(|| StorageError::internal("System cursor scan bound is zero"))?,
                max_bytes: STREAM_READ_BYTES,
            },
            protection,
        )
        .await?;
    if page.more || page.entries.len() > MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS as usize {
        return Err(StorageError::unsupported(
            "System cursor scan exceeds its bound",
        ));
    }
    page.entries
        .into_iter()
        .map(|entry| {
            let cursor: StreamCursor = storage_serde::from_bytes(&entry.value)?;
            if cursor.stream_name != stream_name
                || cursor_key(&cursor.stream_name, &cursor.name)? != entry.key
            {
                return Err(StorageError::internal("KV System cursor identity differs"));
            }
            Ok(cursor)
        })
        .collect()
}

pub(crate) async fn write_cursor<A: KvAttempt>(
    native: &mut A,
    cursor: StreamCursor,
    // The shared decision read this key with conflict protection in the same
    // native attempt; commit checks that read before publishing the mutation.
    _expected_position: Option<StreamItemId>,
) -> StorageResult<()> {
    let key = cursor_key(&cursor.stream_name, &cursor.name)?;
    native.stage(vec![KvMutation::Put {
        key,
        value: storage_serde::to_bytes(&cursor)?,
    }])
}

pub(crate) async fn delete_cursor<A: KvAttempt>(
    native: &mut A,
    stream_name: &StreamName,
    cursor_name: &CursorName,
) -> StorageResult<()> {
    native.stage(vec![KvMutation::Delete {
        key: cursor_key(stream_name, cursor_name)?,
    }])
}

fn cursor_key(stream_name: &StreamName, cursor_name: &CursorName) -> StorageResult<Vec<u8>> {
    let mut key = cursor_stream_prefix(stream_name)?;
    append_segment(&mut key, cursor_name.as_str().as_bytes())?;
    Ok(key)
}

fn cursor_stream_prefix(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    let mut key = STREAM_CURSOR_PREFIX.to_vec();
    append_segment(&mut key, stream_name.as_ref())?;
    Ok(key)
}
