//! One topic key layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use pubsub_provider::{TopicArn, TopicIncarnation, TopicPage, TopicRecord};
use storage_types::{ReadProtection, StorageError, StorageResult, storage_serde};

use crate::{
    KvAttempt, KvEntry, KvMutation, KvRange,
    constants::{CATALOG_BYTES, PUBSUB_TOPIC_PREFIX},
    range::prefix_upper_bound,
};

pub(crate) async fn read_topics<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    arns: Vec<TopicArn>,
) -> StorageResult<Vec<Option<TopicRecord>>> {
    let values = native
        .get_batch(arns.iter().map(topic_key).collect(), protection)
        .await?;
    if values.len() != arns.len() {
        return Err(StorageError::internal("KV topic batch count differs"));
    }
    values
        .into_iter()
        .zip(arns)
        .map(|(value, arn)| {
            value
                .map(|bytes| decode_record(&bytes, arn.as_str().as_bytes()))
                .transpose()
        })
        .collect()
}

pub(crate) async fn write_topic<A: KvAttempt>(
    native: &mut A,
    record: TopicRecord,
    _expected: Option<TopicIncarnation>,
) -> StorageResult<()> {
    // The shared decision made a conflict-protected read of this key.
    native.stage(vec![KvMutation::Put {
        key: topic_key(&record.topic.topic_arn),
        value: storage_serde::to_bytes(&record)?,
    }])
}

pub(crate) async fn delete_topic<A: KvAttempt>(
    native: &mut A,
    record: TopicRecord,
) -> StorageResult<()> {
    native.stage(vec![KvMutation::Delete {
        key: topic_key(&record.topic.topic_arn),
    }])
}

pub(crate) async fn read_topic_page<A: KvAttempt>(
    native: &mut A,
    after: Option<&TopicArn>,
    limit: NonZeroU32,
) -> StorageResult<TopicPage> {
    let begin = after
        .map(|arn| {
            let mut key = topic_key(arn);
            key.push(0);
            key
        })
        .unwrap_or_else(|| PUBSUB_TOPIC_PREFIX.to_vec());
    let end = prefix_upper_bound(PUBSUB_TOPIC_PREFIX)
        .ok_or_else(|| StorageError::internal("KV topic prefix has no upper bound"))?;
    let scan_limit = NonZeroU32::new(limit.get() + 1)
        .ok_or_else(|| StorageError::internal("KV topic page limit overflowed"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin,
                end,
                reverse: false,
                limit: scan_limit,
                max_bytes: CATALOG_BYTES,
            },
            ReadProtection::Snapshot,
        )
        .await?;
    Ok(TopicPage {
        rows: page
            .entries
            .into_iter()
            .map(decode_entry)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

fn topic_key(arn: &TopicArn) -> Vec<u8> {
    [PUBSUB_TOPIC_PREFIX, arn.as_str().as_bytes()].concat()
}

fn decode_entry(entry: KvEntry) -> StorageResult<TopicRecord> {
    let arn = entry
        .key
        .strip_prefix(PUBSUB_TOPIC_PREFIX)
        .ok_or_else(|| StorageError::internal("KV topic row has another prefix"))?;
    decode_record(&entry.value, arn)
}

fn decode_record(bytes: &[u8], arn: &[u8]) -> StorageResult<TopicRecord> {
    let record: TopicRecord = storage_serde::from_bytes(bytes)?;
    if record.topic.topic_arn.as_str().as_bytes() != arn {
        return Err(StorageError::internal("KV topic key differs from row"));
    }
    Ok(record)
}
