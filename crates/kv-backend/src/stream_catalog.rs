//! One ordered-key user-stream catalog layout for RocksDB and FoundationDB.

use std::borrow::Cow;

use storage_types::{StorageError, StorageResult, StreamName, storage_serde};
use stream_provider::{StreamCatalogLookup, StreamCatalogRecord, StreamPartitioningMode};

use crate::{
    KvAttempt, KvMutation, ReadProtection,
    constants::{STREAM_CATALOG_PREFIX, STREAM_ID_PREFIX},
    stream_partition::{key_layout, state},
    stream_write::counter::user_stream_sequence_key,
};

pub(crate) async fn read_stream_record<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    lookup: StreamCatalogLookup<'_>,
) -> StorageResult<Option<StreamCatalogRecord>> {
    // The ID index is a KV layout detail. Both families share lookup identity
    // checks through StreamCatalogLookup after decoding their physical rows.
    let name =
        match lookup {
            StreamCatalogLookup::Name(name) => Cow::Borrowed(name),
            StreamCatalogLookup::Id(id) => {
                let Some(name) = native.get(&id_key(id), protection).await? else {
                    return Ok(None);
                };
                Cow::Owned(String::from_utf8(name).map_err(|_| {
                    StorageError::internal("KV stream ID index has an invalid name")
                })?)
            }
        };
    let record = native
        .get(&catalog_key(&name), protection)
        .await?
        .map(|bytes| {
            let record: StreamCatalogRecord = storage_serde::from_bytes(&bytes)?;
            if !StreamCatalogLookup::Name(name.as_ref()).matches(&record) {
                return Err(StorageError::internal("KV stream catalog identity differs"));
            }
            Ok(record)
        })
        .transpose()?;
    Ok(record.filter(|record| lookup.matches(record)))
}

pub(crate) async fn write_stream_record<A: KvAttempt>(
    native: &mut A,
    name: &str,
    record: StreamCatalogRecord,
) -> StorageResult<()> {
    if record.stream.name.as_str() != name {
        return Err(StorageError::internal(
            "KV stream catalog write has another name",
        ));
    }
    let mut changes = vec![
        KvMutation::Put {
            key: catalog_key(name),
            value: storage_serde::to_bytes(&record)?,
        },
        KvMutation::Put {
            key: id_key(&record.stream.internal_id),
            value: name.as_bytes().to_vec(),
        },
    ];
    let sequence_key = user_stream_sequence_key(&record.stream.internal_id)?;
    let keyed = record.stream.partitioning_mode == StreamPartitioningMode::KeyOrdered;
    if record.deleted {
        changes.push(KvMutation::Delete { key: sequence_key });
        if keyed {
            changes.push(state::delete_state(&record.stream.internal_id)?);
        }
    } else {
        let mut keys = vec![sequence_key.clone()];
        if keyed {
            keys.push(key_layout::state_key(&record.stream.internal_id)?);
        }
        let existing = native.get_batch(keys, ReadProtection::Conflict).await?;
        if existing.len() != usize::from(keyed) + 1 {
            return Err(StorageError::internal(
                "KV stream catalog returned incomplete state",
            ));
        }
        if existing[0].is_none() {
            changes.push(KvMutation::Put {
                key: sequence_key,
                value: 1u64.to_be_bytes().to_vec(),
            });
        }
        if keyed && existing[1].is_none() {
            changes.push(state::initial_state(&record.stream.internal_id)?);
        }
    }
    native.stage(changes)
}

fn id_key(id: &StreamName) -> Vec<u8> {
    let mut key = Vec::with_capacity(STREAM_ID_PREFIX.len() + id.as_ref().len());
    key.extend_from_slice(STREAM_ID_PREFIX);
    key.extend_from_slice(id.as_ref());
    key
}

fn catalog_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(STREAM_CATALOG_PREFIX.len() + name.len());
    key.extend_from_slice(STREAM_CATALOG_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}
