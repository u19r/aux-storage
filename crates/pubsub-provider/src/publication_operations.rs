//! Shared publication persistence, claim, and materialization decisions.

use std::sync::Arc;

use storage_types::{
    AttemptKind, AttemptTry, ClaimSlot, StorageError, StorageResult, TimestampMillis,
};

use crate::{
    ClaimedPublishChunk, DueRow, PUBLISH_INTENT_LEASE_MILLIS, PublishChunkClaimRequest,
    PublishChunkCommit, PublishDueRequest, PublishIntentClaim, PubsubAttempt, PubsubError,
    PubsubPort, PubsubResult, UndecodableRow,
    constants::{CHUNK_ITEMS, PUBLISH_DUE_PROBE, PUBSUB_CLAIM_WINDOW},
};

pub(crate) async fn claim_publish_chunks<P: PubsubPort>(
    port: &P,
    request: PublishChunkClaimRequest,
) -> PubsubResult<Vec<ClaimedPublishChunk>> {
    // The snapshot window and slot rule of the delivery claim (design
    // 7.2, review D8); SQL ignores them.
    let claimed = port
        .run(
            CHUNK_ITEMS,
            Arc::new(request),
            |attempt, input, attempt_try| {
                Box::pin(claim_chunks_in_attempt(attempt, input, attempt_try))
            },
        )
        .await;
    match claimed {
        Ok(claims) => Ok(claims),
        // Every retry lost a conflict: nothing committed, claim nothing.
        Err(error) if error.attempt_kind() == Some(AttemptKind::Conflict) => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn commit_publish_chunk<P: PubsubPort>(
    port: &P,
    commit: PublishChunkCommit,
) -> PubsubResult<()> {
    validate_commit(&commit)?;
    // A known abort is retried with the same commit (review D8): the
    // conditional replace of the claimed intent is the claim check, so a
    // lost claim stays a conflict and the record writes roll back.
    port.run(CHUNK_ITEMS, Arc::new(commit), |attempt, input, _| {
        Box::pin(commit_chunk_in_attempt(attempt, input))
    })
    .await
    .map_err(PubsubError::from)
}

pub(crate) async fn next_publish_due<P: PubsubPort>(
    port: &P,
) -> PubsubResult<Option<TimestampMillis>> {
    port.run(PUBLISH_DUE_PROBE, Arc::new(()), |attempt, _, _| {
        Box::pin(async move { attempt.read_next_publish_due().await })
    })
    .await
    .map_err(PubsubError::from)
}

/// One try of the chunk claim: the slot rule of the delivery claim (design
/// 7.2, review D8); SQL ignores the window and the slot.
async fn claim_chunks_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    request: &PublishChunkClaimRequest,
    attempt_try: AttemptTry,
) -> StorageResult<Vec<ClaimedPublishChunk>> {
    let limit = request.limit.get();
    let due = PublishDueRequest {
        now: request.now,
        limit,
        window: PUBSUB_CLAIM_WINDOW,
        slot: ClaimSlot {
            limit,
            first_attempt: attempt_try.is_first(),
            rotation: storage_types::ambient::random_u64() as u32,
        },
    };
    claim_in_attempt(attempt, &due).await
}

async fn commit_chunk_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    commit: &PublishChunkCommit,
) -> StorageResult<()> {
    attempt.store_new_deliveries(commit.records.clone()).await?;
    attempt
        .replace_publish_intent(commit.claimed_intent.clone(), commit.next_intent.clone())
        .await
}

async fn claim_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    due: &PublishDueRequest,
) -> StorageResult<Vec<ClaimedPublishChunk>> {
    let now = due.now;
    let rows = attempt.read_due_publish_chunks(due).await?;
    if rows.len() > due.limit.get() as usize {
        return Err(StorageError::internal(
            "publish claim page exceeded its limit",
        ));
    }
    let mut claims = Vec::with_capacity(rows.len());
    let mut replacements = Vec::with_capacity(rows.len());
    for row in rows {
        let candidate = match row {
            DueRow::Decoded(candidate) => candidate,
            DueRow::Undecodable(row) => {
                set_intent_aside(attempt, &row, now).await?;
                continue;
            }
        };
        // A decoded intent that fails its own checks is set aside the same
        // way: it can never be materialized.
        if candidate.validate().is_err()
            || candidate
                .intent
                .lease_expires_at
                .is_some_and(|until| until > now)
        {
            let row = UndecodableRow {
                key: candidate.intent.acceptance.message_id.to_string(),
                due_at: candidate.due_at,
                reason: "publish candidate is invalid",
            };
            set_intent_aside(attempt, &row, now).await?;
            continue;
        }
        let previous = candidate.intent;
        let mut claimed = previous.clone();
        claimed.lease_expires_at = Some(TimestampMillis::from_timestamp(
            now.timestamp_millis()
                .saturating_add(PUBLISH_INTENT_LEASE_MILLIS),
        ));
        replacements.push(PublishIntentClaim {
            scanned_due: candidate.due_at,
            previous,
            claimed: claimed.clone(),
        });
        claims.push(ClaimedPublishChunk {
            intent: claimed,
            subscriptions: candidate.subscriptions,
        });
    }
    attempt.replace_publish_intents(replacements).await?;
    Ok(claims)
}

/// An undecodable intent at the head of the due index would stop every
/// chunk claim on every node (review D4). Count it, log its key without
/// payload, and move its due entry one hour ahead in the caller's attempt.
async fn set_intent_aside<A: PubsubAttempt>(
    attempt: &mut A,
    row: &UndecodableRow,
    now: TimestampMillis,
) -> StorageResult<()> {
    let until = crate::after(now.max(row.due_at), crate::constants::POISON_DEFERRAL);
    attempt.move_publish_due(row, until).await?;
    metrics::counter!("pubsub.publish.poison").increment(1);
    tracing::error!(
        key = row.key.as_str(),
        reason = row.reason,
        "pubsub due row cannot be decoded; moved one hour ahead"
    );
    Ok(())
}

fn validate_commit(commit: &PublishChunkCommit) -> StorageResult<()> {
    let current = &commit.claimed_intent;
    if current.lease_expires_at.is_none() {
        return Err(StorageError::internal("publish chunk was never claimed"));
    }
    let expected_next = current.next_chunk()?;
    for (index, (payload, state)) in commit.records.iter().enumerate() {
        if payload.message_id != current.acceptance.message_id
            || payload.id != state.id
            || payload
                .subscription
                .as_ref()
                .is_none_or(|subscription| subscription.topic_arn != current.snapshot.topic_arn)
            || commit.records[..index]
                .iter()
                .any(|(previous, _)| previous.id == payload.id)
        {
            return Err(StorageError::internal(
                "publish delivery record differs from claim",
            ));
        }
    }
    if commit.next_intent != expected_next {
        return Err(StorageError::internal("publish chunk continuation differs"));
    }
    Ok(())
}
