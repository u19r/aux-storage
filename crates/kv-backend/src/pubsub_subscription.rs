//! One subscription key layout for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use pubsub_provider::{
    PendingSubscription, PendingSubscriptionCursor, PendingSubscriptionPage, SubscriptionArn,
    SubscriptionIdentity, SubscriptionPage, SubscriptionPageRequest, SubscriptionRecord,
    TopicIncarnation, digest_bytes,
};
use storage_types::{
    MissExpectation, ReadProtection, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange,
    constants::{
        CATALOG_BYTES, PUBSUB_SUBSCRIPTION_IDENTITY_PREFIX, PUBSUB_SUBSCRIPTION_PREFIX,
        PUBSUB_SUBSCRIPTION_TOKEN_PREFIX, PUBSUB_SUBSCRIPTION_TOPIC_PREFIX,
    },
    range::prefix_upper_bound,
};

pub(crate) async fn read_subscription<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    arn: &SubscriptionArn,
) -> StorageResult<Option<SubscriptionRecord>> {
    native
        .get(&subscription_key(arn), protection)
        .await?
        .map(|bytes| decode_record(&bytes, arn))
        .transpose()
}

pub(crate) async fn find_subscription_by_identity<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    identity: SubscriptionIdentity<'_>,
) -> StorageResult<Option<SubscriptionRecord>> {
    let Some(arn) = native.get(&identity_key(&identity), protection).await? else {
        return Ok(None);
    };
    let arn = decode_arn(&arn)?;
    let record = read_index_target(native, protection, &arn).await?;
    if record.topic_incarnation != identity.topic_incarnation
        || record.subscription.protocol != identity.protocol
        || record.subscription.endpoint != identity.endpoint
    {
        return Err(StorageError::internal(
            "KV subscription identity index differs",
        ));
    }
    Ok(Some(record))
}

pub(crate) async fn find_subscription_by_token<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    incarnation: TopicIncarnation,
    token: &str,
) -> StorageResult<Option<SubscriptionRecord>> {
    let Some(value) = native
        .get(&token_key(incarnation, token), protection)
        .await?
    else {
        return Ok(None);
    };
    let (created_at, arn) = decode_token_value(&value)?;
    let record = read_index_target(native, protection, &arn).await?;
    if record.topic_incarnation != incarnation
        || record.subscription.confirmation.token() != Some(token)
        || record.subscription.created_at != created_at
    {
        return Err(StorageError::internal(
            "KV subscription token index differs",
        ));
    }
    Ok(Some(record))
}

pub(crate) async fn write_subscription<A: KvAttempt>(
    native: &mut A,
    record: SubscriptionRecord,
    expected: Option<SubscriptionRecord>,
) -> StorageResult<()> {
    let arn = &record.subscription.subscription_arn;
    if expected
        .as_ref()
        .is_some_and(|previous| previous.topic_incarnation != record.topic_incarnation)
    {
        return Err(StorageError::internal(
            "KV subscription changed its topic incarnation",
        ));
    }
    let mut mutations = Vec::new();
    let previous_token = expected
        .as_ref()
        .and_then(|row| row.subscription.confirmation.token());
    if let Some(previous) = &expected {
        if let Some(token) =
            previous_token.filter(|token| Some(*token) != record.subscription.confirmation.token())
        {
            mutations.push(KvMutation::Delete {
                key: token_key(previous.topic_incarnation, token),
            });
        }
    } else {
        mutations.push(KvMutation::Put {
            key: identity_key(&SubscriptionIdentity {
                topic_incarnation: record.topic_incarnation,
                protocol: record.subscription.protocol,
                endpoint: &record.subscription.endpoint,
            }),
            value: arn.as_str().as_bytes().to_vec(),
        });
    }
    if let Some(token) = record
        .subscription
        .confirmation
        .token()
        .filter(|token| Some(*token) != previous_token)
    {
        let key = token_key(record.topic_incarnation, token);
        if native.get(&key, ReadProtection::Conflict).await?.is_some() {
            return Err(conflict());
        }
        mutations.push(KvMutation::Put {
            key,
            value: token_value(record.subscription.created_at, arn),
        });
    }
    // The topic index carries the whole record, so a topic page is one
    // scan (W8-E-f). Both copies are staged together.
    let value = storage_serde::to_bytes(&record)?;
    mutations.push(KvMutation::Put {
        key: topic_key(record.topic_incarnation, arn),
        value: value.clone(),
    });
    mutations.push(KvMutation::Put {
        key: subscription_key(arn),
        value,
    });
    native.stage(mutations)
}

pub(crate) async fn delete_subscription<A: KvAttempt>(
    native: &mut A,
    record: SubscriptionRecord,
) -> StorageResult<()> {
    let arn = &record.subscription.subscription_arn;
    let mut mutations = vec![
        KvMutation::Delete {
            key: subscription_key(arn),
        },
        KvMutation::Delete {
            key: topic_key(record.topic_incarnation, arn),
        },
        KvMutation::Delete {
            key: identity_key(&SubscriptionIdentity {
                topic_incarnation: record.topic_incarnation,
                protocol: record.subscription.protocol,
                endpoint: &record.subscription.endpoint,
            }),
        },
    ];
    if let Some(token) = record.subscription.confirmation.token() {
        mutations.push(KvMutation::Delete {
            key: token_key(record.topic_incarnation, token),
        });
    }
    native.stage(mutations)
}

pub(crate) async fn read_subscription_page<A: KvAttempt>(
    native: &mut A,
    request: SubscriptionPageRequest<'_>,
) -> StorageResult<SubscriptionPage> {
    let prefix = request
        .topic_incarnation
        .map(topic_prefix)
        .unwrap_or_else(|| PUBSUB_SUBSCRIPTION_PREFIX.to_vec());
    let mut begin = prefix.clone();
    if let Some(after) = request.after {
        begin.extend_from_slice(after.as_str().as_bytes());
        begin.push(0);
    }
    let end = prefix_upper_bound(&prefix)
        .ok_or_else(|| StorageError::internal("KV subscription prefix has no upper bound"))?;
    let limit = request
        .limit
        .get()
        .checked_add(1)
        .and_then(std::num::NonZeroU32::new)
        .ok_or_else(|| StorageError::internal("KV subscription page limit overflowed"))?;
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
    let rows =
        page.entries
            .iter()
            .map(|entry| {
                let suffix = entry.key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                    StorageError::internal("KV subscription row has another prefix")
                })?;
                let arn = decode_arn(suffix)?;
                // `~p/s` rows and `~p/x` index entries hold the same record.
                let record = decode_record(&entry.value, &arn)?;
                if request
                    .topic_incarnation
                    .is_some_and(|incarnation| record.topic_incarnation != incarnation)
                {
                    return Err(StorageError::internal(
                        "KV subscription topic index differs",
                    ));
                }
                Ok(record)
            })
            .collect::<StorageResult<Vec<_>>>()?;
    Ok(SubscriptionPage {
        rows,
        more: page.more,
    })
}

/// Pending subscriptions after `after` in token-index key order (topic
/// incarnation, then token digest), read at `Snapshot`.
pub(crate) async fn read_pending_subscription_page<A: KvAttempt>(
    native: &mut A,
    after: Option<&PendingSubscriptionCursor>,
    limit: NonZeroU32,
) -> StorageResult<PendingSubscriptionPage> {
    let prefix = PUBSUB_SUBSCRIPTION_TOKEN_PREFIX;
    let mut begin = prefix.to_vec();
    if let Some(after) = after {
        begin.extend_from_slice(&after.0);
        begin.push(0);
    }
    let end = prefix_upper_bound(prefix)
        .ok_or_else(|| StorageError::internal("KV token prefix has no upper bound"))?;
    let scan_limit = limit
        .get()
        .checked_add(1)
        .and_then(NonZeroU32::new)
        .ok_or_else(|| StorageError::internal("KV pending subscription limit overflowed"))?;
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
    let more = page.more || page.entries.len() > limit.get() as usize;
    let rows =
        page.entries
            .iter()
            .take(limit.get() as usize)
            .map(|entry| {
                let suffix = entry.key.strip_prefix(prefix).ok_or_else(|| {
                    StorageError::internal("KV token index row has another prefix")
                })?;
                let (created_at, subscription_arn) = decode_token_value(&entry.value)?;
                Ok(PendingSubscription {
                    cursor: PendingSubscriptionCursor(suffix.to_vec()),
                    subscription_arn,
                    created_at,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
    Ok(PendingSubscriptionPage { rows, more })
}

async fn read_index_target<A: KvAttempt>(
    native: &mut A,
    protection: ReadProtection,
    arn: &SubscriptionArn,
) -> StorageResult<SubscriptionRecord> {
    read_subscription(native, protection, arn)
        .await?
        .ok_or_else(|| StorageError::internal("KV subscription index is dangling"))
}

fn subscription_key(arn: &SubscriptionArn) -> Vec<u8> {
    [PUBSUB_SUBSCRIPTION_PREFIX, arn.as_str().as_bytes()].concat()
}

fn topic_prefix(incarnation: TopicIncarnation) -> Vec<u8> {
    [PUBSUB_SUBSCRIPTION_TOPIC_PREFIX, incarnation.as_bytes()].concat()
}

fn topic_key(incarnation: TopicIncarnation, arn: &SubscriptionArn) -> Vec<u8> {
    [topic_prefix(incarnation), arn.as_str().as_bytes().to_vec()].concat()
}

fn identity_key(identity: &SubscriptionIdentity<'_>) -> Vec<u8> {
    [
        PUBSUB_SUBSCRIPTION_IDENTITY_PREFIX,
        identity.topic_incarnation.as_bytes(),
        identity.protocol.as_str().as_bytes(),
        b"/",
        &identity.endpoint_digest(),
    ]
    .concat()
}

fn token_key(incarnation: TopicIncarnation, token: &str) -> Vec<u8> {
    [
        PUBSUB_SUBSCRIPTION_TOKEN_PREFIX,
        incarnation.as_bytes(),
        &digest_bytes(token.as_bytes()),
    ]
    .concat()
}

/// The token-index value: the creation time of the subscription (big-endian
/// milliseconds), then its ARN. The stale sweep reads the time from the index
/// alone.
pub(crate) fn token_value(created_at: TimestampMillis, arn: &SubscriptionArn) -> Vec<u8> {
    [
        &created_at.timestamp_millis().to_be_bytes()[..],
        arn.as_str().as_bytes(),
    ]
    .concat()
}

pub(crate) fn decode_token_value(
    bytes: &[u8],
) -> StorageResult<(TimestampMillis, SubscriptionArn)> {
    let (created_at, arn) = bytes
        .split_first_chunk::<8>()
        .ok_or_else(|| StorageError::internal("KV subscription token value is too short"))?;
    Ok((
        TimestampMillis::from_timestamp(i64::from_be_bytes(*created_at)),
        decode_arn(arn)?,
    ))
}

fn decode_arn(bytes: &[u8]) -> StorageResult<SubscriptionArn> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| StorageError::internal("KV subscription ARN is not UTF-8"))?;
    SubscriptionArn::new(value.to_owned())
        .map_err(|_| StorageError::internal("KV subscription ARN is invalid"))
}

fn decode_record(bytes: &[u8], arn: &SubscriptionArn) -> StorageResult<SubscriptionRecord> {
    let record: SubscriptionRecord = storage_serde::from_bytes(bytes)?;
    if &record.subscription.subscription_arn != arn {
        return Err(StorageError::internal(
            "KV subscription key differs from row",
        ));
    }
    Ok(record)
}

fn conflict() -> StorageError {
    StorageError::write_miss("pubsub_subscription_token", MissExpectation::Absent)
}
