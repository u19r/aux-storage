//! One publication intent and immutable snapshot layout for both KV drivers.

use std::num::NonZeroU32;

use pubsub_provider::{
    AcceptedPublication, PublishCandidate, PublishIntent, PubsubMessageId, Subscription,
    SubscriptionSnapshotGeneration,
};
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{
        CATALOG_BYTES, PUBSUB_PUBLISH_CHUNK_PREFIX, PUBSUB_PUBLISH_DUE_PREFIX,
        PUBSUB_PUBLISH_INTENT_PREFIX,
    },
    range::prefix_upper_bound,
};

pub(crate) async fn read_publish_intent<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    id: &PubsubMessageId,
) -> StorageResult<Option<PublishIntent>> {
    native
        .get(&intent_key(id), protection)
        .await?
        .map(|bytes| {
            let intent: PublishIntent = storage_serde::from_bytes(&bytes)?;
            if &intent.acceptance.message_id != id {
                return Err(StorageError::internal("KV publish intent identity differs"));
            }
            Ok(intent)
        })
        .transpose()
}

pub(crate) async fn read_publish_candidate<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    id: &PubsubMessageId,
) -> StorageResult<Option<PublishCandidate>> {
    let Some(intent) = read_publish_intent(native, protection, id).await? else {
        return Ok(None);
    };
    let subscriptions = if intent.snapshot.chunk_count == 0 {
        Vec::new()
    } else {
        let bytes = native
            .get(
                &chunk_key(intent.snapshot.generation, intent.next_chunk),
                protection,
            )
            .await?
            .ok_or_else(|| StorageError::internal("KV publish chunk is missing"))?;
        storage_serde::from_bytes(&bytes)?
    };
    Ok(Some(PublishCandidate {
        intent,
        subscriptions,
    }))
}

pub(crate) async fn write_publication<A: KvAttempt>(
    native: &mut A,
    publication: AcceptedPublication,
) -> StorageResult<()> {
    let generation = publication.snapshot.root.generation;
    let keys = (0..publication.snapshot.chunks.len())
        .map(|index| {
            let index = u32::try_from(index)
                .map_err(|_| StorageError::internal("publish chunk index exceeds KV range"))?;
            Ok(chunk_key(generation, index))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    if native
        .get_batch(keys.clone(), ReadProtection::Conflict)
        .await?
        .into_iter()
        .any(|value| value.is_some())
    {
        return Err(conflict());
    }
    let mut mutations = keys
        .into_iter()
        .zip(publication.snapshot.chunks)
        .map(|(key, chunk)| {
            Ok(KvMutation::Put {
                key,
                value: storage_serde::to_bytes(&chunk)?,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    mutations.push(KvMutation::Put {
        key: due_key(&publication.intent),
        value: Vec::new(),
    });
    mutations.push(KvMutation::Put {
        key: intent_key(&publication.intent.acceptance.message_id),
        value: storage_serde::to_bytes(&publication.intent)?,
    });
    native.stage(mutations)
}

pub(crate) async fn read_due_publish_chunks<A: KvAttempt>(
    native: &mut A,
    now: TimestampMillis,
    limit: NonZeroU32,
) -> StorageResult<Vec<PublishCandidate>> {
    let due_prefix = due_time_prefix(now.timestamp_millis());
    let end = prefix_upper_bound(&due_prefix)
        .ok_or_else(|| StorageError::internal("KV publish due bound is invalid"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin: PUBSUB_PUBLISH_DUE_PREFIX.to_vec(),
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
                .strip_prefix(PUBSUB_PUBLISH_DUE_PREFIX)
                .ok_or_else(|| StorageError::internal("KV publish due key has another prefix"))?;
            let id = suffix
                .get(8..)
                .ok_or_else(|| StorageError::internal("KV publish due key is short"))?;
            decode_message_id(id)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let values = native
        .get_batch(
            ids.iter().map(intent_key).collect(),
            ReadProtection::Conflict,
        )
        .await?;
    let intents = values
        .into_iter()
        .zip(&ids)
        .zip(&page.entries)
        .map(|((bytes, id), entry)| {
            let bytes =
                bytes.ok_or_else(|| StorageError::internal("KV publish due index is dangling"))?;
            let intent: PublishIntent = storage_serde::from_bytes(&bytes)?;
            if intent.acceptance.message_id != *id || due_key(&intent) != entry.key {
                return Err(StorageError::internal("KV publish due index differs"));
            }
            Ok(intent)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let chunk_keys = intents
        .iter()
        .filter(|intent| intent.snapshot.chunk_count != 0)
        .map(|intent| chunk_key(intent.snapshot.generation, intent.next_chunk))
        .collect::<Vec<_>>();
    let chunks = native
        .get_batch(chunk_keys, ReadProtection::Conflict)
        .await?;
    let mut chunks = chunks.into_iter();
    intents
        .into_iter()
        .map(|intent| {
            let subscriptions = if intent.snapshot.chunk_count == 0 {
                Vec::new()
            } else {
                let bytes = chunks
                    .next()
                    .flatten()
                    .ok_or_else(|| StorageError::internal("KV publish chunk is missing"))?;
                storage_serde::from_bytes::<Vec<Subscription>>(&bytes)?
            };
            Ok(PublishCandidate {
                intent,
                subscriptions,
            })
        })
        .collect()
}

pub(crate) async fn replace_publish_intent<A: KvAttempt>(
    native: &mut A,
    expected: PublishIntent,
    next: Option<PublishIntent>,
) -> StorageResult<()> {
    native.stage(intent_mutations(expected, next)?)
}

pub(crate) async fn replace_publish_intents<A: KvAttempt>(
    native: &mut A,
    replacements: Vec<(PublishIntent, PublishIntent)>,
) -> StorageResult<()> {
    let mut changes = Vec::with_capacity(replacements.len() * 3);
    for (expected, next) in replacements {
        changes.extend(intent_mutations(expected, Some(next))?);
    }
    if changes.is_empty() {
        return Ok(());
    }
    native.stage(changes)
}

fn intent_mutations(
    expected: PublishIntent,
    next: Option<PublishIntent>,
) -> StorageResult<Vec<KvMutation>> {
    let mut changes = vec![KvMutation::Delete {
        key: due_key(&expected),
    }];
    let key = intent_key(&expected.acceptance.message_id);
    if let Some(next) = next {
        changes.push(KvMutation::Put {
            key: due_key(&next),
            value: Vec::new(),
        });
        changes.push(KvMutation::Put {
            key,
            value: storage_serde::to_bytes(&next)?,
        });
    } else {
        changes.push(KvMutation::Delete { key });
        for index in 0..expected.snapshot.chunk_count {
            changes.push(KvMutation::Delete {
                key: chunk_key(expected.snapshot.generation, index),
            });
        }
    }
    Ok(changes)
}

fn intent_key(id: &PubsubMessageId) -> Vec<u8> {
    [PUBSUB_PUBLISH_INTENT_PREFIX, id.as_str().as_bytes()].concat()
}

pub fn chunk_key(generation: SubscriptionSnapshotGeneration, index: u32) -> Vec<u8> {
    [
        PUBSUB_PUBLISH_CHUNK_PREFIX,
        generation.to_string().as_bytes(),
        &index.to_be_bytes(),
    ]
    .concat()
}

fn due_key(intent: &PublishIntent) -> Vec<u8> {
    let due = intent
        .lease_expires_at
        .map(|time| time.timestamp_millis())
        .unwrap_or(i64::MIN);
    [
        due_time_prefix(due),
        intent.acceptance.message_id.as_str().as_bytes().to_vec(),
    ]
    .concat()
}

fn due_time_prefix(due: i64) -> Vec<u8> {
    let ordered = (due as u64) ^ (1_u64 << 63);
    [PUBSUB_PUBLISH_DUE_PREFIX, &ordered.to_be_bytes()].concat()
}

fn decode_message_id(bytes: &[u8]) -> StorageResult<PubsubMessageId> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| StorageError::internal("KV publish message ID is not UTF-8"))?;
    PubsubMessageId::new_from_string(value.to_owned())
        .map_err(|_| StorageError::internal("KV publish message ID is invalid"))
}

fn conflict() -> StorageError {
    StorageEnum::TransactionConflict {
        message: "publish snapshot generation already exists".to_owned(),
    }
    .into()
}
