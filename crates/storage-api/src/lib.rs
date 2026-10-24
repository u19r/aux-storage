//! Standalone DynamoDB-compatible storage API service assembly.
//!
//! Downstream service hosts should prefer the curated re-exports from this
//! crate root instead of depending on implementation modules directly.

mod config_watch;
mod constants;
#[cfg(test)]
mod constants_tests;
mod dynamodb_assembly;
mod dynamodb_streams;
mod errors;
mod get_item_response;
#[cfg(test)]
mod get_item_response_tests;
mod health;
pub(crate) mod manager;
mod raw_dynamodb_response;
mod router;
mod routes;
mod runtime_config;
mod service;
mod service_host;
mod types;

#[cfg(test)]
mod cli_tests;
#[cfg(test)]
mod main_tests;
#[cfg(test)]
mod raw_dynamodb_response_tests;
#[cfg(test)]
mod router_tests;
#[cfg(test)]
mod storage_api_test_support_tests;

pub use dynamodb_assembly::dynamodb_router;
pub use manager::{StorageApiManager, StorageApiManagerImpl};
pub use router::{
    MetricsEndpointConfig, PrometheusMetricsEndpointConfig, openapi, server_router,
    server_router_with_metrics,
};
pub use service::run;
pub use types::{
    AppState, PointInTimeRecoverySpecification, Response, UpdateContinuousBackupsRequest,
};

mod history_response;
