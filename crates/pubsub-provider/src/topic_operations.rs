//! Shared topic operations over one pubsub attempt.

use async_trait::async_trait;
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{
    ListTopicsRequest, ListTopicsResponse, PubsubAttempt, PubsubError, PubsubPort, PubsubResult,
    PubsubTopicRepository, SetTopicAttributesRequest, Topic, TopicArn, TopicRecord,
    constants::{PUBSUB_PAGE_LIMIT, PUBSUB_WRITE_RETRY},
    pubsub_port::read_topic,
};

#[async_trait]
impl<P: PubsubPort> PubsubTopicRepository for P {
    async fn create_topic(&self, topic: Topic) -> PubsubResult<Topic> {
        let proposed = TopicRecord::new(topic);
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let proposed = proposed.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    create_topic_in_attempt(attempt, proposed)
                        .await
                        .map(Topic::from)
                })
            })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn delete_topic(&self, arn: &TopicArn) -> PubsubResult<()> {
        let arn = arn.clone();
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let arn = arn.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { delete_topic_in_attempt(attempt, &arn).await })
            })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn get_topic(&self, arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        let arn = arn.clone();
        self.with_pubsub_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { Ok(read_topic(attempt, &arn).await?.map(|row| row.topic)) })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<Topic> {
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let request = request.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { set_topic_attributes_in_attempt(attempt, request).await })
            })
        })
        .await
        .map_err(PubsubError::from)?
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        let after = request.next_token.map(TopicArn::new).transpose()?;
        self.with_pubsub_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let page = attempt
                    .read_topic_page(after.as_ref(), PUBSUB_PAGE_LIMIT)
                    .await?;
                if page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize + 1
                    || page.more && page.rows.is_empty()
                {
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
                    .then(|| topics.last().map(|topic| topic.topic_arn.to_string()))
                    .flatten();
                Ok(ListTopicsResponse { topics, next_token })
            })
        })
        .await
        .map_err(PubsubError::from)
    }
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

pub async fn create_topic_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    proposed: TopicRecord,
) -> StorageResult<TopicCreation> {
    if let Some(existing) = read_topic(attempt, &proposed.topic.topic_arn).await? {
        return Ok(TopicCreation::Existing(existing));
    }
    attempt.write_topic(proposed.clone(), None).await?;
    Ok(TopicCreation::Created(proposed))
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
