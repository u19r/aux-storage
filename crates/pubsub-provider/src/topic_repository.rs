use async_trait::async_trait;

use crate::{
    ListTopicsRequest, ListTopicsResponse, PubsubResult, SetTopicAttributesRequest, Topic, TopicArn,
};

#[async_trait]
pub trait PubsubTopicRepository: Send + Sync {
    async fn create_topic(&self, topic: Topic) -> PubsubResult<Topic>;

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()>;

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>>;

    async fn set_topic_attributes(&self, request: SetTopicAttributesRequest)
    -> PubsubResult<Topic>;

    async fn list_topics(&self, request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse>;
}
