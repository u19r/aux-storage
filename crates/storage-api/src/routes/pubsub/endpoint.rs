use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use http_error::HttpApiError;
use pubsub::{decode_query_request, render_query_api_error, render_query_success};
use tracing::{Instrument as _, instrument::WithSubscriber as _};
use uuid::Uuid;

use crate::types::AppState;

#[utoipa::path(
    post,
    path = "/pubsub",
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses((status = 200, description = "SNS-compatible response"))
)]
pub async fn pubsub_endpoint(State(app_state): State<Arc<AppState>>, body: Bytes) -> Response {
    run_query_request(&app_state, &body).await
}

/// The Query protocol over GET: the query string carries the pairs of a POST
/// form. `SubscribeURL` and `UnsubscribeURL` are requested this way.
pub async fn pubsub_query_endpoint(
    State(app_state): State<Arc<AppState>>,
    RawQuery(query): RawQuery,
) -> Response {
    run_query_request(&app_state, query.as_deref().unwrap_or_default().as_bytes()).await
}

/// One SNS Query request: form pairs from a POST body or a GET query string.
async fn run_query_request(app_state: &AppState, body: &[u8]) -> Response {
    let request_id = Uuid::now_v7().to_string();
    let Some(manager) = app_state.pubsub_manager.as_ref() else {
        let error =
            HttpApiError::aws_query_error("InvalidAction", "pubsub route is not enabled", 404);
        return (
            StatusCode::NOT_FOUND,
            render_query_api_error(&error, &request_id),
        )
            .into_response();
    };
    let action = match decode_query_request(body) {
        Ok(action) => action,
        Err(error) => return pubsub_error_response(&error, &request_id),
    };
    // The store takes one admission permit for each native attempt, as for
    // SQS. The action runs to completion after a client disconnect, so a
    // commit always reaches its reply path and its delivery wake.
    let manager = Arc::clone(manager);
    let task = tokio::spawn(
        metrics_facade::inherit_request_cost(
            async move { manager.execute_query_action(action).await },
        )
        .instrument(tracing::Span::current())
        .with_current_subscriber(),
    );
    let result = match task.await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(pubsub::PubsubError::storage("pubsub request task stopped")),
    };
    match result {
        Ok(success) => {
            (StatusCode::OK, render_query_success(&success, &request_id)).into_response()
        }
        Err(error) => pubsub_error_response(&error, &request_id),
    }
}

fn add_error_headers(headers: &mut HeaderMap, error: &HttpApiError) {
    for (name, value) in &error.response_headers {
        let Ok(name) = name.parse::<header::HeaderName>() else {
            continue;
        };
        let Ok(value) = HeaderValue::from_str(value) else {
            continue;
        };
        headers.insert(name, value);
    }
}

fn pubsub_error_response(error: &pubsub::PubsubError, request_id: &str) -> Response {
    if let Some(storage) = error.storage_error() {
        crate::errors::log_storage_failure("sns", storage);
    }
    let error = HttpApiError::from(error);
    let status =
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, render_query_api_error(&error, request_id)).into_response();
    add_error_headers(response.headers_mut(), &error);
    response
}
