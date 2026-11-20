mod delivery_config;
mod delivery_runner;
/// The pure scheduling core of the delivery runner. Public only so that the
/// conformance suite can drive it against each real store (W3S plan 6 (i)).
#[doc(hidden)]
pub mod delivery_scheduler;
mod delivery_transport;
mod manager;
mod notification;
mod protocol;

pub use delivery_config::{
    BreakerConfig, DeliveryRunnerConfig, DeliveryTiming, PubsubDeliveryConfig, SETTLE_MARGIN,
};
pub use delivery_runner::{DeliveryRunner, DeliveryRunnerStats, DeliveryWake};
pub use manager::{PubsubManager, PubsubManagerBuilder};
pub use protocol::{decode_query_request, render_query_api_error, render_query_success};
pub use pubsub_provider::*;

#[cfg(all(test, feature = "sqlite"))]
mod attribute_rule_tests;

#[cfg(all(test, feature = "sqlite"))]
mod durability_class_tests;

#[cfg(all(test, feature = "sqlite"))]
mod provider_attempt_tests;

#[cfg(all(test, feature = "sqlite"))]
mod manager_tests;

#[cfg(all(
    test,
    feature = "postgres",
    not(feature = "sqlite"),
    not(feature = "turso"),
    not(feature = "rocksdb"),
    not(feature = "foundationdb")
))]
mod queue_initialization_tests;

#[cfg(test)]
mod protocol_tests;

#[cfg(test)]
mod notification_tests;

#[cfg(all(test, feature = "sqlite"))]
mod delivery_runner_tests;

#[cfg(test)]
mod delivery_scheduler_tests;

#[cfg(all(test, feature = "sqlite"))]
mod delivery_perf_tests;

#[cfg(test)]
mod quint_pubsub_delivery_tests;

#[cfg(all(test, feature = "sqlite"))]
mod stale_subscription_tests;
