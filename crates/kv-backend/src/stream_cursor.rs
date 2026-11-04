//! One ordered cursor key layout shared by RocksDB and FoundationDB.

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, storage_serde};
use stream_provider::{CursorName, StreamCursor};

use crate::{
    KvAttempt, KvMutation, ReadProtection, constants::STREAM_CURSOR_PREFIX,
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

pub(crate) fn cursor_stream_prefix(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    let mut key = STREAM_CURSOR_PREFIX.to_vec();
    append_segment(&mut key, stream_name.as_ref())?;
    Ok(key)
}
