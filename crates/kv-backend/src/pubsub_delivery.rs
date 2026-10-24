//! One delivery record layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use pubsub_provider::{DeliveryRecord, DeliveryRecordId};
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{CATALOG_BYTES, PUBSUB_DELIVERY_DUE_PREFIX, PUBSUB_DELIVERY_PREFIX},
    range::prefix_upper_bound,
};

pub(crate) async fn write_delivery_records<A: KvAttempt>(
    native: &mut A,
    records: Vec<DeliveryRecord>,
) -> StorageResult<()> {
    let keys = records
        .iter()
        .map(|record| delivery_key(&record.id.0))
        .collect();
    if native
        .get_batch(keys, ReadProtection::Conflict)
        .await?
        .into_iter()
        .any(|value| value.is_some())
    {
        return Err(StorageEnum::TransactionConflict {
            message: "publish delivery already exists".to_owned(),
        }
        .into());
    }
    let mut changes = Vec::with_capacity(records.len() * 2);
    for record in records {
        if let Some(due) = record.due_at() {
            changes.push(KvMutation::Put {
                key: due_key(due.timestamp_millis(), &record.id.0),
                value: Vec::new(),
            });
        }
        changes.push(KvMutation::Put {
            key: delivery_key(&record.id.0),
            value: storage_serde::to_bytes(&record)?,
        });
    }
    native.stage(changes)
}

pub(crate) async fn read_deliveries<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    ids: Vec<DeliveryRecordId>,
) -> StorageResult<Vec<Option<DeliveryRecord>>> {
    let values = native
        .get_batch(
            ids.iter().map(|id| delivery_key(&id.0)).collect(),
            protection,
        )
        .await?;
    values
        .into_iter()
        .zip(ids)
        .map(|(bytes, id)| bytes.map(|bytes| decode_record(&bytes, &id.0)).transpose())
        .collect()
}

pub(crate) async fn read_due_deliveries<A: KvAttempt>(
    native: &mut A,
    now: TimestampMillis,
    limit: NonZeroU32,
) -> StorageResult<Vec<DeliveryRecord>> {
    let end_prefix = due_time_prefix(now.timestamp_millis());
    let end = prefix_upper_bound(&end_prefix)
        .ok_or_else(|| StorageError::internal("KV delivery due bound is invalid"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin: PUBSUB_DELIVERY_DUE_PREFIX.to_vec(),
                end,
                reverse: false,
                limit,
                max_bytes: CATALOG_BYTES,
            },
            ReadProtection::Conflict,
        )
        .await?;
    let ids = page
        .entries
        .iter()
        .map(|entry| {
            let suffix = entry
                .key
                .strip_prefix(PUBSUB_DELIVERY_DUE_PREFIX)
                .ok_or_else(|| StorageError::internal("KV delivery due key has another prefix"))?;
            let id = suffix
                .get(8..)
                .ok_or_else(|| StorageError::internal("KV delivery due key is short"))?;
            std::str::from_utf8(id)
                .map(ToOwned::to_owned)
                .map_err(|_| StorageError::internal("KV delivery ID is not UTF-8"))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let values = native
        .get_batch(
            ids.iter().map(|id| delivery_key(id)).collect(),
            ReadProtection::Conflict,
        )
        .await?;
    values
        .into_iter()
        .zip(ids)
        .zip(page.entries)
        .map(|((bytes, id), entry)| {
            let bytes =
                bytes.ok_or_else(|| StorageError::internal("KV delivery due index is dangling"))?;
            let record = decode_record(&bytes, &id)?;
            let Some(due) = record.due_at() else {
                return Err(StorageError::internal(
                    "KV terminal delivery has a due index",
                ));
            };
            if due_key(due.timestamp_millis(), &id) != entry.key {
                return Err(StorageError::internal("KV delivery due index differs"));
            }
            Ok(record)
        })
        .collect()
}

pub(crate) async fn replace_deliveries<A: KvAttempt>(
    native: &mut A,
    replacements: Vec<(DeliveryRecord, DeliveryRecord)>,
) -> StorageResult<()> {
    let mut changes = Vec::with_capacity(replacements.len() * 3);
    for (expected, next) in replacements {
        if expected.id != next.id {
            return Err(StorageError::internal("delivery identity changed"));
        }
        if let Some(due) = expected.due_at() {
            changes.push(KvMutation::Delete {
                key: due_key(due.timestamp_millis(), &expected.id.0),
            });
        }
        if let Some(due) = next.due_at() {
            changes.push(KvMutation::Put {
                key: due_key(due.timestamp_millis(), &next.id.0),
                value: Vec::new(),
            });
        }
        changes.push(KvMutation::Put {
            key: delivery_key(&next.id.0),
            value: storage_serde::to_bytes(&next)?,
        });
    }
    if changes.is_empty() {
        return Ok(());
    }
    native.stage(changes)
}

fn delivery_key(id: &str) -> Vec<u8> {
    [PUBSUB_DELIVERY_PREFIX, id.as_bytes()].concat()
}

fn due_key(due: i64, id: &str) -> Vec<u8> {
    [due_time_prefix(due), id.as_bytes().to_vec()].concat()
}

fn due_time_prefix(due: i64) -> Vec<u8> {
    let ordered = (due as u64) ^ (1_u64 << 63);
    [PUBSUB_DELIVERY_DUE_PREFIX, &ordered.to_be_bytes()].concat()
}

fn decode_record(bytes: &[u8], id: &str) -> StorageResult<DeliveryRecord> {
    let record: DeliveryRecord = storage_serde::from_bytes(bytes)?;
    if record.id.0 != id {
        return Err(StorageError::internal("KV delivery key differs from row"));
    }
    Ok(record)
}
