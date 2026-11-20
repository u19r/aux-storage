//! Shared topic operations over one pubsub attempt.

use std::{collections::BTreeMap, sync::Arc};

use storage_types::{StorageError, StorageResult};

use crate::{
    ListTopicsRequest, ListTopicsResponse, PubsubAttempt, PubsubError, PubsubPort, PubsubResult,
    PubsubValidationKind, SetTopicAttributesRequest, Topic, TopicArn, TopicRecord,
    constants::{CATALOG_POINT_READ, CATALOG_RANGE_READ, CATALOG_WRITE, PUBSUB_PAGE_LIMIT},
    pubsub_port::read_topic,
};

pub(crate) async fn create_topic<P: PubsubPort>(port: &P, topic: Topic) -> PubsubResult<Topic> {
    let proposed = TopicRecord::new(topic);
    port.run(CATALOG_WRITE, Arc::new(proposed), |attempt, input, _| {
        Box::pin(async move {
            Ok(create_topic_in_attempt(attempt, input.clone())
                .await?
                .map(Topic::from))
        })
    })
    .await
    .map_err(PubsubError::from)?
}

pub(crate) async fn delete_topic<P: PubsubPort>(port: &P, arn: &TopicArn) -> PubsubResult<()> {
    port.run(CATALOG_WRITE, Arc::new(arn.clone()), |attempt, input, _| {
        Box::pin(delete_topic_in_attempt(attempt, input))
    })
    .await
    .map_err(PubsubError::from)
}

pub(crate) async fn get_topic<P: PubsubPort>(
    port: &P,
    arn: &TopicArn,
) -> PubsubResult<Option<Topic>> {
    port.run(
        CATALOG_POINT_READ,
        Arc::new(arn.clone()),
        |attempt, input, _| {
            Box::pin(async move { Ok(read_topic(attempt, input).await?.map(|row| row.topic)) })
        },
    )
    .await
    .map_err(PubsubError::from)
}

pub(crate) async fn set_topic_attributes<P: PubsubPort>(
    port: &P,
    request: SetTopicAttributesRequest,
) -> PubsubResult<Topic> {
    port.run(CATALOG_WRITE, Arc::new(request), |attempt, input, _| {
        Box::pin(set_topic_attributes_in_attempt(attempt, input.clone()))
    })
    .await
    .map_err(PubsubError::from)?
}

pub(crate) async fn list_topics<P: PubsubPort>(
    port: &P,
    request: ListTopicsRequest,
) -> PubsubResult<ListTopicsResponse> {
    let after = request
        .next_token
        .map(|token| {
            queue_provider::decode_page_token(&token)
                .and_then(|arn| TopicArn::new(arn).ok())
                .ok_or_else(|| {
                    PubsubError::validation_with_detail(
                        PubsubValidationKind::InvalidNextToken,
                        &token,
                    )
                })
        })
        .transpose()?;
    port.run(CATALOG_RANGE_READ, Arc::new(after), |attempt, input, _| {
        Box::pin(list_topics_in_attempt(attempt, input))
    })
    .await
    .map_err(PubsubError::from)
}

async fn list_topics_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    after: &Option<TopicArn>,
) -> StorageResult<ListTopicsResponse> {
    let page = attempt
        .read_topic_page(after.as_ref(), PUBSUB_PAGE_LIMIT)
        .await?;
    if page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize + 1 || page.more && page.rows.is_empty() {
        return Err(StorageError::internal(
            "topic catalog page has invalid bounds",
        ));
    }
    let mut previous = after.as_ref().map(TopicArn::as_str);
    for record in &page.rows {
        if previous.is_some_and(|prev| record.topic.topic_arn.as_str() <= prev) {
            return Err(StorageError::internal("topic catalog page is unordered"));
        }
        previous = Some(record.topic.topic_arn.as_str());
    }
    let more = page.more || page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize;
    let topics = page
        .rows
        .into_iter()
        .take(PUBSUB_PAGE_LIMIT.get() as usize)
        .map(|record| record.topic)
        .collect::<Vec<_>>();
    let next_token = more
        .then(|| {
            topics
                .last()
                .map(|topic| queue_provider::encode_page_token(topic.topic_arn.as_str()))
        })
        .flatten();
    Ok(ListTopicsResponse { topics, next_token })
}

/// Creation distinguishes a fresh catalog write from an idempotent existing
/// topic, so replication cannot implicitly adopt an unassigned topic.
pub enum TopicCreation {
    Created(TopicRecord),
    Existing(TopicRecord),
}

impl From<TopicCreation> for Topic {
    fn from(value: TopicCreation) -> Self {
        match value {
            TopicCreation::Created(record) | TopicCreation::Existing(record) => record.topic,
        }
    }
}

/// A topic with the same name and another `DisplayName` is
/// `InvalidParameter` (design 8.3; the text is not probed).
pub async fn create_topic_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    proposed: TopicRecord,
) -> StorageResult<PubsubResult<TopicCreation>> {
    if let Some(existing) = read_topic(attempt, &proposed.topic.topic_arn).await? {
        if existing.topic.display_name != proposed.topic.display_name {
            return Ok(Err(PubsubError::validation(
                PubsubValidationKind::TopicExists,
            )));
        }
        // Tags are compared only when the request carries some (suite script
        // `create_topic_with_different_tags_rejected`).
        if !proposed.topic.tags.is_empty() && existing.topic.tags != proposed.topic.tags {
            return Ok(Err(PubsubError::validation_with_detail(
                PubsubValidationKind::InvalidAttributeValue,
                "Tags Reason: Topic already exists with different tags",
            )));
        }
        return Ok(Ok(TopicCreation::Existing(existing)));
    }
    attempt.write_topic(proposed.clone(), None).await?;
    Ok(Ok(TopicCreation::Created(proposed)))
}

pub async fn delete_topic_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &TopicArn,
) -> StorageResult<()> {
    if let Some(record) = read_topic(attempt, arn).await? {
        attempt.delete_topic(record).await?;
    }
    Ok(())
}

pub async fn set_topic_attributes_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    request: SetTopicAttributesRequest,
) -> StorageResult<PubsubResult<Topic>> {
    let Some(mut record) = read_topic(attempt, &request.topic_arn).await? else {
        return Ok(Err(PubsubError::topic_not_found(
            request.topic_arn.to_string(),
        )));
    };
    if let Some(display_name) = request.attributes.get("DisplayName") {
        record.topic.display_name = Some(display_name.clone());
    }
    let topic = record.topic.clone();
    let incarnation = record.incarnation;
    attempt.write_topic(record, Some(incarnation)).await?;
    Ok(Ok(topic))
}

struct TagChangeInput {
    arn: TopicArn,
    added: BTreeMap<String, String>,
    removed: Vec<String>,
}

/// `TagResource` and `UntagResource` on one topic, in one catalog attempt.
pub(crate) async fn change_topic_tags<P: PubsubPort>(
    port: &P,
    arn: &TopicArn,
    added: BTreeMap<String, String>,
    removed: Vec<String>,
) -> PubsubResult<Topic> {
    let input = Arc::new(TagChangeInput {
        arn: arn.clone(),
        added,
        removed,
    });
    port.run(CATALOG_WRITE, input, |attempt, input, _| {
        Box::pin(change_tags_in_attempt(attempt, input))
    })
    .await
    .map_err(PubsubError::from)?
}

async fn change_tags_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    input: &TagChangeInput,
) -> StorageResult<PubsubResult<Topic>> {
    let Some(mut record) = read_topic(attempt, &input.arn).await? else {
        return Ok(Err(PubsubError::tagged_resource_not_found(
            input.arn.to_string(),
        )));
    };
    record.topic.untag(&input.removed);
    if let Err(error) = record.topic.tag(input.added.clone()) {
        return Ok(Err(error));
    }
    let topic = record.topic.clone();
    let incarnation = record.incarnation;
    attempt.write_topic(record, Some(incarnation)).await?;
    Ok(Ok(topic))
}
