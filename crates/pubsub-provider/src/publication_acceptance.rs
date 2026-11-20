//! Accept one publication against a coherent topic and subscription snapshot.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    AcceptedPublication, PublishRequest, PubsubAttempt, PubsubError, PubsubMessageId, PubsubPort,
    PubsubResult, Subscription, SubscriptionArn, SubscriptionPageRequest, SubscriptionSnapshot,
    SubscriptionSnapshotGeneration, TopicIncarnation,
    constants::{PUBLISH_WRITE, PUBSUB_PAGE_LIMIT, PUBSUB_SNAPSHOT_CHUNK_SIZE},
    pubsub_port::read_topic,
    subscription_operations::validate_page,
};

/// Captured input for durable publish acceptance.
///
/// Protected subscription selection happens in the caller's native attempt;
/// these inputs supply its message, time, and snapshot identities.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
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

/// The protected facts selected before a publication is written.
pub enum PublicationAcceptance {
    Ready {
        topic_incarnation: TopicIncarnation,
        publication: Box<AcceptedPublication>,
    },
    MissingTopic,
}

pub async fn accept_publish_on_port<P: PubsubPort>(
    port: &P,
    request: PublishRequest,
    message_id: PubsubMessageId,
    custom_sender_enabled: bool,
) -> PubsubResult<()> {
    let acceptance =
        PublishAcceptanceInput::new(request, message_id, port.now(), custom_sender_enabled);
    port.run(PUBLISH_WRITE, Arc::new(acceptance), |attempt, input, _| {
        Box::pin(accept_in_attempt(attempt, input))
    })
    .await
    .map_err(PubsubError::from)?
}

async fn accept_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    acceptance: &PublishAcceptanceInput,
) -> StorageResult<PubsubResult<()>> {
    let topic_arn = acceptance.request.topic_arn.clone();
    let prepared = prepare_publish_in_attempt(attempt, acceptance.clone()).await?;
    Ok(match prepared {
        PublicationAcceptance::Ready { publication, .. } => {
            attempt.write_publication(*publication).await?;
            Ok(())
        }
        PublicationAcceptance::MissingTopic => {
            Err(PubsubError::topic_not_found(topic_arn.to_string()))
        }
    })
}

pub async fn prepare_publish_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    acceptance: PublishAcceptanceInput,
) -> StorageResult<PublicationAcceptance> {
    // The message ID is generated for this request and a retry follows only
    // a proven abort, so no intent with this ID can exist (review C7). The
    // insert of the intent still fails on a duplicate key.
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
        publication: Box::new(publication),
    })
}

pub(crate) async fn collect_subscriptions<A: PubsubAttempt>(
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
