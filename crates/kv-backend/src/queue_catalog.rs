//! One ordered queue catalog layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use queue_provider::{QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueIncarnation};
use storage_types::{StorageError, StorageResult, storage_serde};

use crate::{
    KvAttempt, KvEntry, KvMutation, KvRange, ReadProtection,
    constants::{CATALOG_BYTES, QUEUE_NAME_PREFIX},
    range::prefix_upper_bound,
};

pub(crate) async fn read_queue_by_name<A: KvAttempt>(
    native: &A,
    protection: ReadProtection,
    name: &str,
) -> StorageResult<Option<QueueCatalogRecord>> {
    native
        .get(&name_key(name), protection)
        .await?
        .map(|bytes| decode_named_value(&bytes, name))
        .transpose()
}

pub(crate) fn decode_named_value(bytes: &[u8], name: &str) -> StorageResult<QueueCatalogRecord> {
    let record: QueueCatalogRecord = storage_serde::from_bytes(bytes)?;
    if record.queue.queue_name != name {
        return Err(StorageError::internal("KV queue catalog name differs"));
    }
    Ok(record)
}

/// The single catalog key of one queue name, as a range for a scan wave. The
/// range holds at most that key; the limit of 2 keeps a native `more` flag at
/// a full page from meaning anything here.
pub(crate) fn queue_name_range(name: &str) -> KvRange {
    let begin = name_key(name);
    KvRange {
        entry_bounds: None,
        end: after_key(begin.clone()),
        begin,
        reverse: false,
        limit: NonZeroU32::MIN.saturating_add(1),
        max_bytes: CATALOG_BYTES,
    }
}

pub(crate) fn decode_named_page(
    page: crate::KvRangePage,
    name: &str,
) -> StorageResult<Option<QueueCatalogRecord>> {
    if page.entries.len() > 1 {
        return Err(StorageError::internal(
            "KV queue catalog read is not unique",
        ));
    }
    let record = page
        .entries
        .into_iter()
        .next()
        .map(decode_entry)
        .transpose()?;
    if record
        .as_ref()
        .is_some_and(|record| record.queue.queue_name != name)
    {
        return Err(StorageError::internal("KV queue catalog name differs"));
    }
    Ok(record)
}

/// The catalog rows of `names` in input order, in one native wave.
pub(crate) async fn read_queues_by_name<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    names: Vec<String>,
) -> StorageResult<storage_types::Exact<Option<QueueCatalogRecord>>> {
    let values = native
        .get_batch_exact(
            names.iter().map(|name| name_key(name)).collect(),
            protection,
        )
        .await?;
    values.try_zip_map(names, |name, value| {
        value
            .map(|bytes| decode_named_value(&bytes, &name))
            .transpose()
    })
}

pub(crate) async fn read_queue_page<A: KvAttempt>(
    native: &mut A,
    request: &QueueCatalogRequest,
) -> StorageResult<QueueCatalogPage> {
    let mut prefix = QUEUE_NAME_PREFIX.to_vec();
    if let Some(name_prefix) = &request.prefix {
        prefix.extend_from_slice(name_prefix.as_bytes());
    }
    let mut begin = prefix.clone();
    if let Some(start) = &request.exclusive_start {
        begin = begin.max(after_key(name_key(start)));
    }
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV queue prefix has no upper bound"))?;
    if begin >= end {
        return Ok(QueueCatalogPage {
            rows: Vec::new(),
            more: false,
        });
    }
    let limit = NonZeroU32::new(request.limit.get() + 1)
        .ok_or_else(|| StorageError::internal("KV queue page limit overflowed"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin,
                end,
                reverse: false,
                limit,
                max_bytes: CATALOG_BYTES,
            },
            ReadProtection::Snapshot,
        )
        .await?;
    let rows = page
        .entries
        .into_iter()
        .map(decode_entry)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(QueueCatalogPage {
        rows,
        more: page.more,
    })
}

pub(crate) async fn write_queue_record<A: KvAttempt>(
    native: &mut A,
    record: QueueCatalogRecord,
    _expected: Option<QueueIncarnation>,
) -> StorageResult<()> {
    // The shared decision read the name key under conflict protection; the
    // native commit validates that observation before publishing the key.
    native.stage(vec![KvMutation::Put {
        key: name_key(&record.queue.queue_name),
        value: storage_serde::to_bytes(&record)?,
    }])
}

pub(crate) async fn delete_queue_record<A: KvAttempt>(
    native: &mut A,
    record: QueueCatalogRecord,
) -> StorageResult<()> {
    native.stage(vec![KvMutation::Delete {
        key: name_key(&record.queue.queue_name),
    }])
}

fn decode_entry(entry: KvEntry) -> StorageResult<QueueCatalogRecord> {
    let name = entry
        .key
        .strip_prefix(QUEUE_NAME_PREFIX)
        .ok_or_else(|| StorageError::internal("KV queue catalog key has another prefix"))?;
    let record: QueueCatalogRecord = storage_serde::from_bytes(&entry.value)?;
    if record.queue.queue_name.as_bytes() != name {
        return Err(StorageError::internal(
            "KV queue catalog key differs from row",
        ));
    }
    Ok(record)
}

pub(crate) fn name_key(name: &str) -> Vec<u8> {
    [QUEUE_NAME_PREFIX, name.as_bytes()].concat()
}

fn after_key(mut key: Vec<u8>) -> Vec<u8> {
    key.push(0);
    key
}
