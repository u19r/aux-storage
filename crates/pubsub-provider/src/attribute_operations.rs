//! Topic attribute counts use every visible subscription page in one snapshot.

use async_trait::async_trait;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{
    GetTopicAttributesRequest, GetTopicAttributesResponse, PubsubAttempt,
    PubsubAttributeRepository, PubsubError, PubsubPort, PubsubResult, SubscriptionArn,
    SubscriptionPage, SubscriptionPageRequest, TopicRecord, constants::PUBSUB_PAGE_LIMIT,
    pubsub_port::read_topic, subscription_operations::validate_page,
};

#[async_trait]
impl<P: PubsubPort> PubsubAttributeRepository for P {
    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        let arn = request.topic_arn;
        let requested = arn.clone();
        let response = self
            .with_pubsub_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let Some(topic) = read_topic(attempt, &requested).await? else {
                        return Ok(None);
                    };
                    get_topic_attributes_from_topic(attempt, &topic)
                        .await
                        .map(Some)
                })
            })
            .await
            .map_err(PubsubError::from)?;
        response.ok_or_else(|| PubsubError::topic_not_found(arn.to_string()))
    }
}

/// The caller binds this catalog incarnation in the same native snapshot.
/// Replication serving authority can join the read without opening a second
/// pubsub transaction or reloading the topic.
pub async fn get_topic_attributes_from_topic<A: PubsubAttempt>(
    attempt: &mut A,
    topic: &TopicRecord,
) -> StorageResult<GetTopicAttributesResponse> {
    let mut after: Option<SubscriptionArn> = None;
    let mut counts = (0usize, 0usize);
    loop {
        let page = attempt
            .read_subscription_page(SubscriptionPageRequest {
                topic_incarnation: Some(topic.incarnation),
                after: after.as_ref(),
                limit: PUBSUB_PAGE_LIMIT,
            })
            .await?;
        after = count_subscription_page(page, after.as_ref(), &mut counts)?;
        if after.is_none() {
            return Ok(GetTopicAttributesResponse {
                attributes: topic.topic.attributes(counts.0, counts.1),
            });
        }
    }
}

fn count_subscription_page(
    page: SubscriptionPage,
    after: Option<&SubscriptionArn>,
    counts: &mut (usize, usize),
) -> StorageResult<Option<SubscriptionArn>> {
    validate_page(&page, after)?;
    let more = page.more || page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize;
    let mut last = None;
    for row in page.rows.into_iter().take(PUBSUB_PAGE_LIMIT.get() as usize) {
        let counter = if row.subscription.confirmation.pending_confirmation() {
            &mut counts.1
        } else {
            &mut counts.0
        };
        *counter = counter
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("subscription count overflowed"))?;
        last = Some(row.subscription.subscription_arn);
    }
    if more {
        return last
            .map(Some)
            .ok_or_else(|| StorageError::internal("subscription count page cannot advance"));
    }
    Ok(None)
}
