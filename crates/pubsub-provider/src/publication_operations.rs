//! Shared publication persistence, claim, and materialization decisions.

use async_trait::async_trait;
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult, TimestampMillis};

use crate::{
    AcceptedPublication, ClaimedPublishChunk, PUBLISH_INTENT_LEASE_MILLIS,
    PublishChunkClaimRequest, PublishChunkCommit, PubsubAttempt, PubsubError, PubsubPort,
    PubsubPublicationRepository, PubsubResult,
    constants::{PUBSUB_CLAIM_BATCH_SIZE, PUBSUB_WRITE_RETRY},
};

#[async_trait]
impl<P: PubsubPort> PubsubPublicationRepository for P {
    async fn persist_accepted_publication(
        &self,
        publication: AcceptedPublication,
    ) -> PubsubResult<()> {
        publication.validate().map_err(PubsubError::from)?;
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let publication = publication.clone();
            async move {
                self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let id = &publication.intent.acceptance.message_id;
                        if let Some(existing) = attempt.read_publish_intent(id).await? {
                            if existing == publication.intent {
                                return Ok(());
                            }
                            return Err(StorageError::internal(
                                "publish message identity conflicts",
                            ));
                        }
                        attempt.write_publication(publication).await
                    })
                })
                .await
            }
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn claim_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<Vec<ClaimedPublishChunk>> {
        let limit = PUBSUB_CLAIM_BATCH_SIZE.get().min(u32::from(request.limit));
        let limit = std::num::NonZeroU32::new(limit)
            .ok_or_else(|| PubsubError::storage("publish claim limit is zero"))?;
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { claim_in_attempt(attempt, request.now, limit).await })
            })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn commit_publish_chunk(&self, commit: PublishChunkCommit) -> PubsubResult<()> {
        self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                validate_commit(&commit)?;
                let id = &commit.claimed_intent.acceptance.message_id;
                if attempt.read_publish_intent(id).await? != Some(commit.claimed_intent.clone()) {
                    return Err(StorageError::internal("publish intent claim was lost"));
                }
                attempt.write_delivery_records(commit.records).await?;
                attempt
                    .replace_publish_intent(commit.claimed_intent, commit.next_intent)
                    .await
            })
        })
        .await
        .map_err(PubsubError::from)
    }
}

async fn claim_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    now: TimestampMillis,
    limit: std::num::NonZeroU32,
) -> StorageResult<Vec<ClaimedPublishChunk>> {
    let candidates = attempt.read_due_publish_chunks(now, limit).await?;
    if candidates.len() > limit.get() as usize {
        return Err(StorageError::internal(
            "publish claim page exceeded its limit",
        ));
    }
    let mut claims = Vec::with_capacity(candidates.len());
    let mut replacements = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        candidate.validate()?;
        let previous = candidate.intent;
        if previous.lease_expires_at.is_some_and(|until| until > now) {
            return Err(StorageError::internal(
                "publish due index contains leased intent",
            ));
        }
        let mut claimed = previous.clone();
        claimed.lease_expires_at = Some(TimestampMillis::from_timestamp(
            now.timestamp_millis()
                .saturating_add(PUBLISH_INTENT_LEASE_MILLIS),
        ));
        replacements.push((previous, claimed.clone()));
        claims.push(ClaimedPublishChunk {
            intent: claimed,
            subscriptions: candidate.subscriptions,
        });
    }
    attempt.replace_publish_intents(replacements).await?;
    Ok(claims)
}

fn validate_commit(commit: &PublishChunkCommit) -> StorageResult<()> {
    let current = &commit.claimed_intent;
    if current.lease_expires_at.is_none() {
        return Err(StorageError::internal("publish chunk was never claimed"));
    }
    let expected_next = current.next_chunk()?;
    for (index, record) in commit.records.iter().enumerate() {
        if record.message_id != current.acceptance.message_id
            || record
                .subscription
                .as_ref()
                .is_none_or(|subscription| subscription.topic_arn != current.snapshot.topic_arn)
            || commit.records[..index]
                .iter()
                .any(|previous| previous.id == record.id)
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
