//! Standalone DynamoDB-compatible storage API service assembly.
//!
//! Downstream service hosts should prefer the curated re-exports from this
//! crate root instead of depending on implementation modules directly.

mod config_watch;
mod constants;
#[cfg(test)]
mod constants_tests;
mod errors;
mod get_item_response;
#[cfg(test)]
mod get_item_response_tests;
mod health;
mod raw_dynamodb_response;
mod router;
mod routes;
mod runtime_config;
mod service;
mod service_host;
mod storage_operations;
mod types;

#[cfg(test)]
mod cli_tests;
#[cfg(test)]
mod errors_tests;
#[cfg(test)]
mod main_tests;
#[cfg(all(test, feature = "postgres"))]
mod postgres_deadline_tests;
#[cfg(test)]
mod pubsub_delivery_tests;
#[cfg(test)]
mod router_tests;
#[cfg(all(test, feature = "sqlite"))]
mod service_host_tests;
#[cfg(test)]
mod storage_api_test_support_tests;

pub use router::{
    MetricsEndpointConfig, PrometheusMetricsEndpointConfig, openapi, server_router,
    server_router_with_metrics,
};
pub use service::run;
pub use storage_operations::{describe_system_stream, dynamo_admitted, get_stream_records};
pub use types::{
    AppState, PointInTimeRecoverySpecification, Response, UpdateContinuousBackupsRequest,
};
