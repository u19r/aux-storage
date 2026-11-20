use storage_types::MessagingIdentity;

/// The messaging services a process runs. The host builds it from its file
/// configuration (storage-api `service_host::messaging_config`).
#[derive(Clone, Debug, Default)]
pub struct MessagingConfig {
    /// The public base of queue URLs; `Some` runs the queue service.
    pub queue: Option<queue::QueueEndpoint>,
    /// `Some` runs the pubsub service and its delivery runner.
    pub pubsub: Option<PubsubServiceConfig>,
    /// Partition, region and account of queue ARNs and topic ARNs (review C2).
    pub identity: MessagingIdentity,
}

/// Delivery timing and URLs of both pubsub managers, and the runner limits
/// (review D12).
#[derive(Clone, Debug)]
pub struct PubsubServiceConfig {
    pub delivery: pubsub::PubsubDeliveryConfig,
    pub runner: pubsub::DeliveryRunnerConfig,
}
