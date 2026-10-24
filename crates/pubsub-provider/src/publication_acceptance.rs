//! Accept one publication against a coherent topic and subscription snapshot.

use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult, TimestampMillis};

use crate::{
    AcceptedPublication, PublishIntent, PublishRequest, PubsubAttempt, PubsubError,
    PubsubMessageId, PubsubPort, PubsubPublishAcceptanceRepository, PubsubResult, Subscription,
    SubscriptionArn, SubscriptionPageRequest, SubscriptionSnapshot, SubscriptionSnapshotGeneration,
    TopicIncarnation,
    constants::{PUBSUB_PAGE_LIMIT, PUBSUB_SNAPSHOT_CHUNK_SIZE, PUBSUB_WRITE_RETRY},
    pubsub_port::read_topic,
    subscription_operations::validate_page,
};

/// Captured input for durable publish acceptance.
///
/// Protected subscription selection happens in the caller's native attempt;
/// these inputs supply its message, time, and snapshot identities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishAcceptanceInput {
    pub request: PublishRequest,
    pub message_id: PubsubMessageId,
    pub accepted_at: TimestampMillis,
    pub custom_sender_enabled: bool,
    pub snapshot_generation: SubscriptionSnapshotGeneration,
}

impl PublishAcceptanceInput {
    #[must_use]
    pub fn capture(
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> Self {
        Self {
            request,
            message_id,
            accepted_at: TimestampMillis::now(),
            custom_sender_enabled,
            snapshot_generation: SubscriptionSnapshotGeneration::new(),
        }
    }

    #[must_use]
    pub fn new(
        request: PublishRequest,
        message_id: PubsubMessageId,
        accepted_at: TimestampMillis,
        custom_sender_enabled: bool,
    ) -> Self {
        Self {
            request,
            message_id,
            accepted_at,
            custom_sender_enabled,
            snapshot_generation: SubscriptionSnapshotGeneration::new(),
        }
    }
}

#[async_trait::async_trait]
impl<P: PubsubPort> PubsubPublishAcceptanceRepository for P {
    async fn persist_publish_generation(
        &self,
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        accept_publish_on_port(self, request, message_id, custom_sender_enabled).await
    }
}

/// The protected facts selected before a publication is written.
pub enum PublicationAcceptance {
    Ready {
        topic_incarnation: TopicIncarnation,
        publication: AcceptedPublication,
    },
    AlreadyAccepted(PublishIntent),
    MissingTopic,
    ConflictingIdentity,
}

pub async fn accept_publish_on_port<P: PubsubPort>(
    port: &P,
    request: PublishRequest,
    message_id: PubsubMessageId,
    custom_sender_enabled: bool,
) -> PubsubResult<()> {
    let acceptance = PublishAcceptanceInput::capture(request, message_id, custom_sender_enabled);
    retry_native_conflicts(port, PUBSUB_WRITE_RETRY, || {
        let acceptance = acceptance.clone();
        async move {
            port.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    let topic_arn = acceptance.request.topic_arn.clone();
                    let prepared = prepare_publish_in_attempt(attempt, acceptance).await?;
                    Ok(match prepared {
                        PublicationAcceptance::Ready { publication, .. } => {
                            attempt.write_publication(publication).await?;
                            Ok(())
                        }
                        PublicationAcceptance::AlreadyAccepted(_) => Ok(()),
                        PublicationAcceptance::MissingTopic => {
                            Err(PubsubError::topic_not_found(topic_arn.to_string()))
                        }
                        PublicationAcceptance::ConflictingIdentity => Err(PubsubError::storage(
                            "publish message identity already exists with another request",
                        )),
                    })
                })
            })
            .await
        }
    })
    .await
    .map_err(PubsubError::from)?
}

pub async fn prepare_publish_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    acceptance: PublishAcceptanceInput,
) -> StorageResult<PublicationAcceptance> {
    if let Some(intent) = attempt.read_publish_intent(&acceptance.message_id).await? {
        return Ok(
            if intent.acceptance.request == acceptance.request
                && intent.acceptance.custom_sender_enabled == acceptance.custom_sender_enabled
            {
                PublicationAcceptance::AlreadyAccepted(intent)
            } else {
                PublicationAcceptance::ConflictingIdentity
            },
        );
    }
    let Some(topic) = read_topic(attempt, &acceptance.request.topic_arn).await? else {
        return Ok(PublicationAcceptance::MissingTopic);
    };
    let subscriptions = collect_subscriptions(attempt, topic.incarnation).await?;
    let snapshot = SubscriptionSnapshot::with_generation(
        topic.topic.topic_arn,
        subscriptions,
        PUBSUB_SNAPSHOT_CHUNK_SIZE,
        acceptance.snapshot_generation,
    )
    .map_err(|_| StorageError::internal("publish snapshot exceeds supported size"))?;
    let publication = AcceptedPublication::new(snapshot, acceptance);
    publication.validate()?;
    Ok(PublicationAcceptance::Ready {
        topic_incarnation: topic.incarnation,
        publication,
    })
}

async fn collect_subscriptions<A: PubsubAttempt>(
    attempt: &mut A,
    incarnation: TopicIncarnation,
) -> StorageResult<Vec<Subscription>> {
    let mut subscriptions = Vec::new();
    let mut after: Option<SubscriptionArn> = None;
    loop {
        let page = attempt
            .read_subscription_page(SubscriptionPageRequest {
                topic_incarnation: Some(incarnation),
                after: after.as_ref(),
                limit: PUBSUB_PAGE_LIMIT,
            })
            .await?;
        validate_page(&page, after.as_ref())?;
        let more = page.more || page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize;
        let selected = page
            .rows
            .into_iter()
            .take(PUBSUB_PAGE_LIMIT.get() as usize)
            .map(|row| row.subscription)
            .collect::<Vec<_>>();
        if more && selected.is_empty() {
            return Err(StorageError::internal(
                "publish subscription page cannot advance",
            ));
        }
        after = selected.last().map(|row| row.subscription_arn.clone());
        subscriptions.extend(selected);
        if !more {
            return Ok(subscriptions);
        }
    }
}
