//! Public protocol assembly for a concrete operation manager. Ordinary service
//! assembly retains its own health, admission, and internal-route ownership.

use std::sync::Arc;

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::HeaderMap,
    response::Response,
    routing::post,
};

use crate::{
    constants::DYNAMODB_JSON_BODY_LIMIT_BYTES,
    manager::StorageApiManager,
    routes::dynamodb::{DynamoError, execute_dynamodb_request},
};

/// Mount the DynamoDB and DynamoDB Streams protocol at `/`. The host supplies
/// readiness and ingress limits around this router; the manager owns operation
/// admission. Providers and background workers are initialized by the host.
pub fn dynamodb_router<M: StorageApiManager + 'static>(manager: Arc<M>) -> Router {
    Router::new()
        .route("/", post(execute_manager_request::<M>))
        .with_state(manager)
        .layer(DefaultBodyLimit::max(DYNAMODB_JSON_BODY_LIMIT_BYTES))
}

async fn execute_manager_request<M: StorageApiManager + 'static>(
    State(manager): State<Arc<M>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, DynamoError> {
    execute_dynamodb_request(manager, headers, body).await
}
