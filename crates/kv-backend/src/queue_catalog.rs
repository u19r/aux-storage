//! One ordered queue catalog layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use queue_provider::{QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueIncarnation};
use storage_types::{StorageError, StorageResult, storage_serde};

use crate::{
    KvAttempt, KvEntry, KvMutation, KvRange, ReadProtection,
    constants::{CATALOG_BYTES, QUEUE_NAME_PREFIX, QUEUE_URL_PREFIX},
    range::prefix_upper_bound,
};

pub(crate) async fn read_queue_by_name<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    name: &str,
) -> StorageResult<Option<QueueCatalogRecord>> {
    native
        .get(&name_key(name), protection)
        .await?
        .map(|bytes| {
            let record: QueueCatalogRecord = storage_serde::from_bytes(&bytes)?;
            if record.queue.queue_name != name {
                return Err(StorageError::internal("KV queue catalog name differs"));
            }
            Ok(record)
        })
        .transpose()
}

pub(crate) async fn read_queues_by_url<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    urls: Vec<String>,
) -> StorageResult<Vec<Option<QueueCatalogRecord>>> {
    let values = native
        .get_batch(urls.iter().map(|url| url_key(url)).collect(), protection)
        .await?;
    let names = values
        .into_iter()
        .map(|value| {
            value
                .map(|bytes| {
                    String::from_utf8(bytes).map_err(|_| {
                        StorageError::internal("KV queue URL index has an invalid name")
                    })
                })
                .transpose()
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let keys = names.iter().flatten().map(|name| name_key(name)).collect();
    let records = native.get_batch(keys, protection).await?;
    let mut records = records.into_iter();
    names
        .into_iter()
        .zip(urls)
        .map(|(name, url)| {
            let Some(name) = name else {
                return Ok(None);
            };
            let bytes = records
                .next()
                .flatten()
                .ok_or_else(|| StorageError::internal("KV queue URL index has no catalog row"))?;
            let record: QueueCatalogRecord = storage_serde::from_bytes(&bytes)?;
            if record.queue.queue_name != name || record.queue.queue_url != url {
                return Err(StorageError::internal(
                    "KV queue URL index differs from catalog row",
                ));
            }
            Ok(Some(record))
        })
        .collect()
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
    // The shared decision read name and URL under conflict protection; the
    // native commit validates those observations before publishing both keys.
    native.stage(vec![
        KvMutation::Put {
            key: name_key(&record.queue.queue_name),
            value: storage_serde::to_bytes(&record)?,
        },
        KvMutation::Put {
            key: url_key(&record.queue.queue_url),
            value: record.queue.queue_name.as_bytes().to_vec(),
        },
    ])
}

pub(crate) async fn delete_queue_record<A: KvAttempt>(
    native: &mut A,
    record: QueueCatalogRecord,
) -> StorageResult<()> {
    native.stage(vec![
        KvMutation::Delete {
            key: name_key(&record.queue.queue_name),
        },
        KvMutation::Delete {
            key: url_key(&record.queue.queue_url),
        },
    ])
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

fn name_key(name: &str) -> Vec<u8> {
    [QUEUE_NAME_PREFIX, name.as_bytes()].concat()
}

fn url_key(url: &str) -> Vec<u8> {
    [QUEUE_URL_PREFIX, url.as_bytes()].concat()
}

fn after_key(mut key: Vec<u8>) -> Vec<u8> {
    key.push(0);
    key
}
