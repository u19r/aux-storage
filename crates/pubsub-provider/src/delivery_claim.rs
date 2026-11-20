//! The destination-aware delivery claim, the sweep of destination heads and
//! the audit read (W3S plan 5.6). The runner asks each destination only for
//! the records it can attempt at once, so a claim never defers.

use std::{num::NonZeroU32, sync::Arc};

use storage_common::telemetry::record_provider_count;
use storage_types::{AttemptKind, AttemptTry, StorageError, StorageResult, TimestampMillis};

use crate::{
    ClaimRows, ClaimScan, ClaimTerms, ClaimedDelivery, DeliveryClaimRequest, DeliveryFence,
    DeliveryLocator, DeliveryPayload, DeliveryRecordId, DeliveryRetryPolicy, DeliveryState,
    DestinationClaim, DestinationKind, PayloadRow, PubsubAttempt, PubsubPort, PubsubResult,
    StateRow, SweepCursor, SweepPage, after,
    constants::{DELIVERY_CLAIM, DELIVERY_READ, DESTINATION_HEADS_READ, POISON_DEFERRAL},
};

/// Claim the due records of each ask in one native attempt per try: one
/// bounded read per ask, one batched state write: one claim per ask. A claim
/// that loses every conflict retry returns no claim at all (a conflict
/// proves nothing committed, and says nothing about the destinations).
pub async fn claim_deliveries<P: PubsubPort>(
    port: &P,
    request: DeliveryClaimRequest,
) -> PubsubResult<Vec<DestinationClaim>> {
    request.validate()?;
    let claimed = port
        .run(
            DELIVERY_CLAIM,
            Arc::new(request),
            |attempt, input, attempt_try| {
                Box::pin(claim_deliveries_in_attempt(attempt, input, attempt_try))
            },
        )
        .await;
    match claimed {
        Ok(claims) => Ok(claims),
        Err(error) if error.attempt_kind() == Some(AttemptKind::Conflict) => {
            record_provider_count("pubsub", "delivery_claim_contention_empty", 1);
            Ok(Vec::new())
        }
        Err(error) => Err(error.into()),
    }
}

async fn claim_deliveries_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    request: &DeliveryClaimRequest,
    attempt_try: AttemptTry,
) -> StorageResult<Vec<DestinationClaim>> {
    let scan = ClaimScan {
        now: request.terms.now,
        asks: request.asks.clone(),
        first_attempt: attempt_try.is_first(),
        rotation: storage_types::ambient::random_u64() as u32,
    };
    claim_in_attempt(attempt, &scan, &request.terms).await
}

async fn claim_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    scan: &ClaimScan,
    terms: &ClaimTerms,
) -> StorageResult<Vec<DestinationClaim>> {
    let pages = attempt.scan_claimable(scan).await?;
    let mut writes = Vec::new();
    let mut claims = Vec::with_capacity(pages.len());
    for (ask, page) in scan.asks.iter().zip(pages) {
        let ClaimRows { rows, next_due, .. } = page;
        if rows.len() > ask.take.get() as usize {
            return Err(StorageError::internal("delivery claim exceeded its ask"));
        }
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let Some((state, payload)) = claimable(attempt, row, scan.now).await? else {
                continue;
            };
            let next = state.claim(terms, storage_types::ambient::uuid_v4())?;
            let fence = DeliveryFence::of(&next).map_err(|_| StorageError::internal("no fence"))?;
            writes.push((state, next.clone()));
            claimed.push(ClaimedDelivery {
                fence,
                state: next,
                payload,
            });
        }
        claims.push(DestinationClaim {
            destination: ask.destination.clone(),
            claimed,
            next_due,
        });
    }
    attempt.store_claimed_states(writes).await?;
    Ok(claims)
}

/// A decoded, due state with its payload; a row that does not decode, or
/// whose payload does not, is set aside one hour in this attempt.
async fn claimable<A: PubsubAttempt>(
    attempt: &mut A,
    row: (StateRow, PayloadRow),
    now: TimestampMillis,
) -> StorageResult<Option<(DeliveryState, DeliveryPayload)>> {
    // W3S review finding 6: a row that cannot be delivered is deleted and
    // counted as a drop once its retry deadline has passed, not moved one
    // hour ahead forever. A state that does not decode takes the deadline
    // of its payload's creation; with no payload it can never be delivered.
    let (locator, deadline, reason) = match row {
        (StateRow::Decoded(state), PayloadRow::Decoded(payload)) if state.due_at <= now => {
            return Ok(Some((state, *payload)));
        }
        (StateRow::Decoded(_), PayloadRow::Decoded(_)) => return Ok(None),
        (StateRow::Decoded(state), PayloadRow::Undecodable) => (
            state.locator(),
            Some(state.retry_deadline),
            "delivery payload does not decode",
        ),
        (StateRow::Decoded(state), PayloadRow::Missing) => (
            state.locator(),
            Some(state.retry_deadline),
            "delivery payload is missing",
        ),
        (StateRow::Undecodable(locator), PayloadRow::Decoded(payload)) => {
            let policy = match payload.destination.kind() {
                DestinationKind::LocalQueue => DeliveryRetryPolicy::SQS,
                DestinationKind::Http | DestinationKind::Confirmation => DeliveryRetryPolicy::HTTP,
            };
            let deadline = policy.deadline(payload.created_at);
            (locator, Some(deadline), "delivery state does not decode")
        }
        (StateRow::Undecodable(locator), PayloadRow::Missing) => (
            locator,
            Some(TimestampMillis::from_timestamp(i64::MIN)),
            "delivery state does not decode and its payload is missing",
        ),
        (StateRow::Undecodable(locator), PayloadRow::Undecodable) => {
            (locator, None, "delivery state and payload do not decode")
        }
    };
    if deadline.is_some_and(|deadline| deadline <= now) {
        attempt.delete_state_at(&locator).await?;
        record_provider_count("pubsub_delivery_dropped", "undeliverable_row", 1);
        tracing::error!(
            key = locator.id.0.as_str(),
            reason,
            "pubsub delivery row cannot be delivered after its deadline; dropped"
        );
    } else {
        set_state_aside(attempt, &locator, now, reason).await?;
    }
    Ok(None)
}

/// An undecodable row at the head of a partition would stop every claim of
/// that destination (review D4). Count it, log its id without payload, and
/// move it one hour ahead with its bytes unchanged. A node that decodes it
/// later processes it as an ordinary row.
async fn set_state_aside<A: PubsubAttempt>(
    attempt: &mut A,
    locator: &DeliveryLocator,
    now: TimestampMillis,
    reason: &'static str,
) -> StorageResult<()> {
    let until = after(now.max(locator.due_at), POISON_DEFERRAL);
    attempt.move_undecodable_state(locator, until).await?;
    metrics::counter!("pubsub.delivery.poison").increment(1);
    tracing::error!(
        key = locator.id.0.as_str(),
        reason,
        "pubsub delivery row cannot be decoded; moved one hour ahead"
    );
    Ok(())
}

struct SweepInput {
    after: SweepCursor,
    limit: NonZeroU32,
}

/// One page of destination heads after `after`, read-only. A hint: a wrong
/// answer costs one claim or one sweep period (rule DUR-F6).
pub async fn sweep_destination_heads<P: PubsubPort>(
    port: &P,
    after: SweepCursor,
    limit: NonZeroU32,
) -> PubsubResult<SweepPage> {
    let input = Arc::new(SweepInput { after, limit });
    Ok(port
        .run(DESTINATION_HEADS_READ, input, |attempt, input, _| {
            Box::pin(async move {
                attempt
                    .scan_destination_heads(&input.after, input.limit)
                    .await
            })
        })
        .await?)
}

/// The state and payload of one delivery (tests and workloads only).
pub async fn get_delivery<P: PubsubPort>(
    port: &P,
    id: &DeliveryRecordId,
) -> PubsubResult<Option<(DeliveryState, DeliveryPayload)>> {
    let row = port
        .run(DELIVERY_READ, Arc::new(id.clone()), |attempt, input, _| {
            Box::pin(async move { attempt.load_delivery(input).await })
        })
        .await?;
    match row {
        None => Ok(None),
        Some((StateRow::Decoded(state), PayloadRow::Decoded(payload))) => {
            Ok(Some((state, *payload)))
        }
        Some(_) => Err(StorageError::internal("stored delivery does not decode").into()),
    }
}
