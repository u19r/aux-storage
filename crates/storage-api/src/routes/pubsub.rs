#![cfg_attr(not(feature = "pubsub"), allow(dead_code, unused_variables))]

use std::sync::Arc;
#[cfg(feature = "pubsub")]
use std::time::Instant;

#[cfg(feature = "pubsub")]
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
#[cfg(not(feature = "pubsub"))]
use axum::{body::Bytes, extract::State, response::Response};
#[cfg(feature = "pubsub")]
use http_error::HttpApiError;
#[cfg(feature = "pubsub")]
use pubsub::{decode_query_request, render_query_api_error, render_query_success};
#[cfg(feature = "pubsub")]
use storage::{AdmissionClass, AdmissionOutcome};
#[cfg(feature = "pubsub")]
use uuid::Uuid;

use crate::types::AppState;

#[cfg(feature = "pubsub")]
#[derive(Debug, Clone, Copy)]
struct ProviderPressureResponse;

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
    let started = Instant::now();
    let permit = match app_state
        .db_manager
        .default_admission_controller()
        .acquire(AdmissionClass::Write)
        .await
    {
        Ok(permit) => permit,
        Err(rejection) => {
            let error = HttpApiError::service_unavailable(rejection.retry_after_seconds);
            let mut response = (
                StatusCode::SERVICE_UNAVAILABLE,
                render_query_api_error(&error, &request_id),
            )
                .into_response();
            add_error_headers(response.headers_mut(), &error);
            return response;
        }
    };
    let result = manager.execute_query_action(action).await;
    let response = match result {
        Ok(success) => {
            (StatusCode::OK, render_query_success(&success, &request_id)).into_response()
        }
        Err(error) => pubsub_error_response(&error, &request_id),
    };
    let provider_pressure = manager.take_admission_pressure_signal()
        || response
            .extensions()
            .get::<ProviderPressureResponse>()
            .is_some();
    let outcome = if provider_pressure {
        AdmissionOutcome::RetryablePressure(started.elapsed())
    } else if response.status().is_server_error() {
        AdmissionOutcome::Failure(started.elapsed())
    } else {
        AdmissionOutcome::Success(started.elapsed())
    };
    permit.complete(outcome);
    response
}

#[cfg(feature = "pubsub")]
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
    let mut response = (status, render_query_api_error(&error, request_id)).into_response();
    add_error_headers(response.headers_mut(), &error);
    if is_provider_pressure_error(&error) {
        response.extensions_mut().insert(ProviderPressureResponse);
    }
    response
}

#[cfg(feature = "pubsub")]
fn is_provider_pressure_error(error: &HttpApiError) -> bool {
    let code = error
        .error_type
        .rsplit('#')
        .next()
        .unwrap_or(&error.error_type);
    matches!(
        code,
        "ServiceUnavailableException"
            | "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "LimitExceededException"
            | "RequestLimitExceeded"
            | "RequestTimeout"
            | "RequestTimeoutException"
    )
}

#[cfg(all(test, feature = "pubsub"))]
mod pubsub_tests;
