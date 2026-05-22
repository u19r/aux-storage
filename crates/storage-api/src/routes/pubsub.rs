#![cfg_attr(not(feature = "pubsub"), allow(dead_code, unused_variables))]

use std::sync::Arc;

#[cfg(feature = "pubsub")]
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
#[cfg(not(feature = "pubsub"))]
use axum::{body::Bytes, extract::State, response::Response};
#[cfg(feature = "pubsub")]
use http_error::HttpApiError;
#[cfg(feature = "pubsub")]
use pubsub::{decode_query_request, render_query_api_error, render_query_success};
#[cfg(feature = "pubsub")]
use uuid::Uuid;

use crate::types::AppState;

#[utoipa::path(
    post,
    path = "/pubsub",
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses((status = 200, description = "SNS-compatible response"))
)]
#[cfg(feature = "pubsub")]
pub async fn pubsub_endpoint(State(app_state): State<Arc<AppState>>, body: Bytes) -> Response {
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
    let action = match decode_query_request(&body) {
        Ok(action) => action,
        Err(error) => return pubsub_error_response(&error, &request_id),
    };
    match manager.execute_query_action(action).await {
        Ok(success) => {
            (StatusCode::OK, render_query_success(&success, &request_id)).into_response()
        }
        Err(error) => pubsub_error_response(&error, &request_id),
    }
}

#[utoipa::path(
    post,
    path = "/pubsub",
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses((status = 404, description = "Pubsub route is not enabled in this binary"))
)]
#[cfg(not(feature = "pubsub"))]
pub async fn pubsub_endpoint(State(_app_state): State<Arc<AppState>>, _body: Bytes) -> Response {
    axum::response::IntoResponse::into_response((
        axum::http::StatusCode::NOT_FOUND,
        "pubsub route is not enabled in this binary",
    ))
}

#[cfg(feature = "pubsub")]
fn pubsub_error_response(error: &pubsub::PubsubError, request_id: &str) -> Response {
    let error = HttpApiError::from(error);
    let status =
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, render_query_api_error(&error, request_id)).into_response()
}
