mod errors;
mod memory;
mod newtypes;
mod provider;
mod types;

pub use errors::{PubsubError, PubsubInternalKind, PubsubResult, PubsubValidationKind};
pub use memory::InMemoryPubsubProvider;
pub use newtypes::{PubsubMessageId, SubscriptionArn, TopicArn, TopicName};
pub use provider::PubsubProvider;
pub use types::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, CreateTopicResponse, DeliveryRecord,
    DeliveryRecordId, DeliveryRecordKind, DeliveryStatus, DeliveryTarget,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PublishRequest, PublishResponse, PubsubArnParts,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscribeRequest,
    SubscribeResponse, Subscription, SubscriptionConfirmation, SubscriptionProtocol, Topic,
};

#[cfg(test)]
mod errors_tests;
#[cfg(test)]
mod memory_tests;
