//! One publication intent and immutable snapshot layout for both KV drivers.

use std::num::NonZeroU32;

use pubsub_provider::{
    AcceptedPublication, DueRow, PublishCandidate, PublishDueRequest, PublishIntent,
    PublishIntentClaim, PubsubMessageId, Subscription, SubscriptionSnapshotGeneration,
    UndecodableRow,
};
use storage_types::{
    MissExpectation, ReadProtection, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{
        CATALOG_BYTES, PUBSUB_PUBLISH_CHUNK_PREFIX, PUBSUB_PUBLISH_DUE_PREFIX,
        PUBSUB_PUBLISH_INTENT_PREFIX,
    },
    range::prefix_upper_bound,
};

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
    // No read of the chunk keys (W3 C8): the generation is new for this
    // acceptance (`SubscriptionSnapshotGeneration::new`), as the message id
    // of the intent key is (review C7); a retry follows only a proven abort.
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

/// A snapshot scan of `request.window` due entries, the slot choice, then
/// a conflict read of the chosen intents and their current chunks only
/// (design 7.2, review D8): concurrent claims conflict only on a shared
/// intent. An intent that changed after the scan is left out.
pub(crate) async fn read_due_publish_chunks<A: KvAttempt>(
    native: &mut A,
    request: &PublishDueRequest,
) -> StorageResult<Vec<DueRow<PublishCandidate>>> {
    let due_prefix = due_time_prefix(request.now.timestamp_millis());
    let end = prefix_upper_bound(&due_prefix)
        .ok_or_else(|| StorageError::internal("KV publish due bound is invalid"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin: PUBSUB_PUBLISH_DUE_PREFIX.to_vec(),
                end,
                reverse: false,
                limit: request.window,
                max_bytes: CATALOG_BYTES,
            },
            ReadProtection::Snapshot,
        )
        .await?;
    let window_full = page.entries.len() >= request.window.get() as usize || page.more;
    let chosen = request
        .slot
        .positions(page.entries.len(), window_full)
        .into_iter()
        .filter_map(|position| page.entries.get(position))
        .map(|entry| {
            let suffix = entry
                .key
                .strip_prefix(PUBSUB_PUBLISH_DUE_PREFIX)
                .ok_or_else(|| StorageError::internal("KV publish due key has another prefix"))?;
            let id = suffix
                .get(8..)
                .ok_or_else(|| StorageError::internal("KV publish due key is short"))?;
            let id = std::str::from_utf8(id)
                .map_err(|_| StorageError::internal("KV publish message ID is not UTF-8"))?;
            let due_at = decode_due_time(&entry.key, PUBSUB_PUBLISH_DUE_PREFIX)?;
            Ok((id.to_owned(), due_at))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let values = native
        .get_batch(
            chosen
                .iter()
                .map(|(id, _)| [PUBSUB_PUBLISH_INTENT_PREFIX, id.as_bytes()].concat())
                .collect(),
            ReadProtection::Conflict,
        )
        .await?;
    let mut rows = Vec::with_capacity(chosen.len());
    let mut intents = Vec::with_capacity(chosen.len());
    for (bytes, (id, due_at)) in values.into_iter().zip(chosen) {
        let Some(bytes) = bytes else {
            continue;
        };
        let undecodable = |reason| {
            DueRow::Undecodable(UndecodableRow {
                key: id.clone(),
                due_at,
                reason,
            })
        };
        // A row that does not decode is a typed observation (review D4).
        // The scanned key is the only due source (finding 2): after a poison
        // move it differs from the intent's own times, and the claim
        // replaces that scanned key (W3S review finding 1).
        match storage_serde::from_bytes::<PublishIntent>(&bytes) {
            Ok(intent) if intent.acceptance.message_id.as_str() == id => {
                intents.push((intent, due_at));
            }
            _ => rows.push(undecodable("publish intent does not decode")),
        }
    }
    let chunk_keys = intents
        .iter()
        .map(|(intent, _)| intent)
        .filter(|intent| intent.snapshot.chunk_count != 0)
        .map(|intent| chunk_key(intent.snapshot.generation, intent.next_chunk))
        .collect::<Vec<_>>();
    let chunks = native
        .get_batch(chunk_keys, ReadProtection::Conflict)
        .await?;
    let mut chunks = chunks.into_iter();
    for (intent, due_at) in intents {
        let subscriptions = if intent.snapshot.chunk_count == 0 {
            Some(Vec::new())
        } else {
            chunks
                .next()
                .flatten()
                .and_then(|bytes| storage_serde::from_bytes::<Vec<Subscription>>(&bytes).ok())
        };
        rows.push(match subscriptions {
            Some(subscriptions) => DueRow::Decoded(PublishCandidate {
                intent,
                subscriptions,
                due_at,
            }),
            None => DueRow::Undecodable(UndecodableRow {
                key: intent.acceptance.message_id.to_string(),
                due_at,
                reason: "publish chunk is missing or does not decode",
            }),
        });
    }
    Ok(rows)
}

/// Move the due entry of one undecodable intent (review D4).
pub(crate) async fn move_publish_due<A: KvAttempt>(
    native: &mut A,
    row: &UndecodableRow,
    until: TimestampMillis,
) -> StorageResult<()> {
    let id = row.key.as_bytes();
    native.stage(vec![
        KvMutation::Delete {
            key: [due_time_prefix(row.due_at.timestamp_millis()), id.to_vec()].concat(),
        },
        KvMutation::Put {
            key: [due_time_prefix(until.timestamp_millis()), id.to_vec()].concat(),
            value: Vec::new(),
        },
    ])
}

/// The least due time of an intent, read at `Snapshot` with a limit of one.
pub(crate) async fn read_next_publish_due<A: KvAttempt>(
    native: &mut A,
) -> StorageResult<Option<TimestampMillis>> {
    let end = prefix_upper_bound(PUBSUB_PUBLISH_DUE_PREFIX)
        .ok_or_else(|| StorageError::internal("KV publish due bound is invalid"))?;
    let page = native
        .scan(
            KvRange {
                entry_bounds: None,
                begin: PUBSUB_PUBLISH_DUE_PREFIX.to_vec(),
                end,
                reverse: false,
                limit: NonZeroU32::MIN,
                max_bytes: CATALOG_BYTES,
            },
            ReadProtection::Snapshot,
        )
        .await?;
    page.entries
        .first()
        .map(|entry| decode_due_time(&entry.key, PUBSUB_PUBLISH_DUE_PREFIX))
        .transpose()
}

/// The time of a due key: 8 bytes after the prefix, sign bit flipped.
fn decode_due_time(key: &[u8], prefix: &[u8]) -> StorageResult<TimestampMillis> {
    let bytes: [u8; 8] = key
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.get(..8))
        .and_then(|time| time.try_into().ok())
        .ok_or_else(|| StorageError::internal("KV due key is short"))?;
    let ordered = u64::from_be_bytes(bytes) ^ (1_u64 << 63);
    Ok(TimestampMillis::from_timestamp(ordered as i64))
}

/// The stored intent must equal `expected`: KV has no conditional write, so
/// the key is read with conflict protection and compared (a physical check).
pub(crate) async fn replace_publish_intent<A: KvAttempt>(
    native: &mut A,
    expected: PublishIntent,
    next: Option<PublishIntent>,
) -> StorageResult<()> {
    let stored = native
        .get_batch(
            vec![intent_key(&expected.acceptance.message_id)],
            ReadProtection::Conflict,
        )
        .await?
        .pop()
        .flatten();
    if stored != Some(storage_serde::to_bytes(&expected)?) {
        return Err(conflict());
    }
    native.stage(intent_mutations(expected, next)?)
}

pub(crate) async fn replace_publish_intents<A: KvAttempt>(
    native: &mut A,
    replacements: Vec<PublishIntentClaim>,
) -> StorageResult<()> {
    let mut changes = Vec::with_capacity(replacements.len() * 3);
    for claim in replacements {
        let id = claim.previous.acceptance.message_id.as_str().as_bytes();
        changes.push(KvMutation::Delete {
            key: [
                due_time_prefix(claim.scanned_due.timestamp_millis()),
                id.to_vec(),
            ]
            .concat(),
        });
        changes.push(KvMutation::Put {
            key: due_key(&claim.claimed),
            value: Vec::new(),
        });
        changes.push(KvMutation::Put {
            key: intent_key(&claim.claimed.acceptance.message_id),
            value: storage_serde::to_bytes(&claim.claimed)?,
        });
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

pub(crate) fn intent_key(id: &PubsubMessageId) -> Vec<u8> {
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

/// Unleased intents are due at their acceptance (review D2).
fn due_key(intent: &PublishIntent) -> Vec<u8> {
    let due = intent.due_at().timestamp_millis();
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

fn conflict() -> StorageError {
    StorageError::write_miss("pubsub_publish_generation", MissExpectation::Absent)
}
