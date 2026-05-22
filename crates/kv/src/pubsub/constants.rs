/// Primary topic records keyed by topic ARN.
pub(crate) const TOPIC_PREFIX: &[u8] = b"sys/pubsub/topics/by-arn/";

/// Topic lookup records keyed by topic name.
pub(crate) const TOPIC_NAME_PREFIX: &[u8] = b"sys/pubsub/topics/by-name/";

/// Primary subscription records keyed by subscription ARN.
pub(crate) const SUBSCRIPTION_PREFIX: &[u8] = b"sys/pubsub/subscriptions/by-arn/";

/// Subscription index records grouped by topic ARN.
pub(crate) const SUBSCRIPTION_TOPIC_PREFIX: &[u8] = b"sys/pubsub/subscriptions/by-topic/";

/// Deduplication records used to keep subscription creation idempotent.
pub(crate) const SUBSCRIPTION_DEDUPE_PREFIX: &[u8] = b"sys/pubsub/subscriptions/by-dedupe/";

/// Primary delivery records keyed by delivery id.
pub(crate) const DELIVERY_PREFIX: &[u8] = b"sys/pubsub/deliveries/by-id/";

/// Delivery index records grouped by subscription ARN.
pub(crate) const DELIVERY_SUBSCRIPTION_PREFIX: &[u8] = b"sys/pubsub/deliveries/by-subscription/";

/// Claimable delivery index records ordered by visibility timestamp.
pub(crate) const DELIVERY_CLAIM_PREFIX: &[u8] = b"sys/pubsub/deliveries/claimable/";

/// Scan overfetch factor because expired or already-claimed deliveries can
/// remain in the claim index until cleanup.
pub(crate) const CLAIM_SCAN_MULTIPLIER: usize = 32;

/// Hard cap for one delivery claim scan to bound memory and transaction size.
pub(crate) const CLAIM_SCAN_MAX_LIMIT: u32 = 1024;
