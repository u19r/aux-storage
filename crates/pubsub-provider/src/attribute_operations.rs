//! Topic attribute counts use every visible subscription page in one snapshot.

use std::sync::Arc;

use storage_types::{StorageError, StorageResult};

use crate::{
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, PubsubAttempt, PubsubError, PubsubPort, PubsubResult,
    SubscriptionArn, SubscriptionPage, SubscriptionPageRequest, TopicArn, TopicRecord,
    constants::{PUBSUB_PAGE_LIMIT, TOPIC_ATTRIBUTES_READ},
    pubsub_port::read_topic,
    subscription_operations::validate_page,
};

pub(crate) async fn get_topic_attributes<P: PubsubPort>(
    port: &P,
    request: GetTopicAttributesRequest,
) -> PubsubResult<GetTopicAttributesResponse> {
    let arn = Arc::new(request.topic_arn);
    let response = port
        .run(
            TOPIC_ATTRIBUTES_READ,
            Arc::clone(&arn),
            |attempt, arn, _| Box::pin(topic_attributes_in_attempt(attempt, arn)),
        )
        .await
        .map_err(PubsubError::from)?;
    response.ok_or_else(|| PubsubError::topic_not_found(arn.to_string()))
}

/// One subscription point read (`CATALOG_POINT_READ`); the attributes are
/// computed from the subscription it returns.
pub(crate) async fn get_subscription_attributes<P: PubsubPort>(
    port: &P,
    request: GetSubscriptionAttributesRequest,
) -> PubsubResult<GetSubscriptionAttributesResponse> {
    let Some(subscription) =
        crate::subscription_operations::get_subscription(port, &request.subscription_arn).await?
    else {
        return Err(PubsubError::subscription_not_found(
            request.subscription_arn.to_string(),
        ));
    };
    Ok(GetSubscriptionAttributesResponse {
        attributes: subscription.attributes(),
    })
}

async fn topic_attributes_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &TopicArn,
) -> StorageResult<Option<GetTopicAttributesResponse>> {
    let Some(topic) = read_topic(attempt, arn).await? else {
        return Ok(None);
    };
    get_topic_attributes_from_topic(attempt, &topic)
        .await
        .map(Some)
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
