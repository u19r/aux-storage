use std::sync::Arc;

use axum::{
    Extension,
    body::{Body, Bytes},
    extract::{Path, State, rejection::BytesRejection},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::Response,
};
use http_error::HttpApiError;
use queue_provider::{
    QueueError, QueueRequest, QueueResponse, QueueResult, SQS_INVALID_ACTION_ERROR_TYPE,
    wire::{
        QueueProtocol, WireRequestParts, WireResponse, decode_wire_request, encode_error,
        encode_success,
    },
};
use tracing::{Instrument as _, instrument::WithSubscriber as _};
use uuid::Uuid;

use crate::{router::IngressSlot, types::AppState};

#[utoipa::path(
    post,
    path = "/queue",
    summary = "Dispatch queue operations.",
    description = "Dispatches SQS-compatible operations and the native JSON AmazonSQS.PublishMessage extension. PublishMessage accepts MessageId (a hyphenated UUID, excluding the nil UUID) and Message (a SendMessage request). Repeating an existing identity preserves its pending delivery, receipt, visibility, expiry and checkpoint. Different body or attributes conflict. A missing or expired delivery can be recovered from its retained source intent.",
    request_body(content = String, content_type = "application/x-amz-json-1.0"),
    responses(
        (status = 200, description = "SQS-compatible response; PublishMessage returns the retained message identity and message checksums"),
        (status = 400, description = "Invalid queue request or publication identity reused with different immutable content"),
        (status = 503, description = "Storage or admission unavailable; follow Retry-After")
    )
)]
pub async fn queue_endpoint(
    State(app_state): State<Arc<AppState>>,
    ingress: Option<Extension<IngressSlot>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    run_queue_request(app_state, ingress, &headers, body, None).await
}

#[utoipa::path(
    post,
    path = "/queue/{account}/{queue_name}",
    summary = "Dispatch queue operations posted to a queue URL.",
    description = "The same operations as `/queue`. A request with no QueueUrl member acts on the queue of the path. A QueueUrl member wins over the path. A path account other than the configured account answers AWS.SimpleQueueService.NonExistentQueue.",
    params(
        ("account" = String, Path, description = "The configured 12-digit account", pattern = "^[0-9]{12}$"),
        ("queue_name" = String, Path, description = "The queue name", pattern = "^[a-zA-Z0-9_-]{1,80}$")
    ),
    request_body(content = String, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "SQS-compatible response"),
        (status = 400, description = "Invalid queue request, or no such queue"),
        (status = 503, description = "Storage or admission unavailable; follow Retry-After")
    )
)]
pub async fn queue_url_endpoint(
    State(app_state): State<Arc<AppState>>,
    Path((account, queue_name)): Path<(String, String)>,
    ingress: Option<Extension<IngressSlot>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let path_queue_url = format!("/{account}/{queue_name}");
    run_queue_request(app_state, ingress, &headers, body, Some(path_queue_url)).await
}

/// The HTTP lifecycle of one SQS request: decode, admission of the ingress
/// slot, dispatch, and encode.
async fn run_queue_request(
    app_state: Arc<AppState>,
    ingress: Option<Extension<IngressSlot>>,
    headers: &HeaderMap,
    body: Result<Bytes, BytesRejection>,
    path_queue_url: Option<String>,
) -> Response {
    let request_id = Uuid::now_v7().to_string();
    let content_type = header_text(headers, header::CONTENT_TYPE.as_str());
    // A body over the route limit is answered in the SQS shape (review L10).
    let body = match body {
        Ok(body) => body,
        Err(rejection) => {
            let error = HttpApiError::aws_query_error(
                queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
                "The request body is too large.",
                rejection.status().as_u16(),
            );
            return into_response(encode_error(
                QueueProtocol::of(content_type),
                &request_id,
                &error,
            ));
        }
    };
    let Some(manager) = app_state.queue_manager.clone() else {
        let error = HttpApiError::aws_query_error(
            SQS_INVALID_ACTION_ERROR_TYPE,
            "queue route is not enabled",
            404,
        );
        return into_response(encode_error(
            QueueProtocol::of(content_type),
            &request_id,
            &error,
        ));
    };
    let wire = match decode_wire_request(WireRequestParts {
        content_type,
        target: header_text(headers, "x-amz-target"),
        body: &body,
        queue_url: path_queue_url.as_deref(),
    }) {
        Ok(wire) => wire,
        Err(rejection) => {
            return into_response(encode_error(
                rejection.protocol,
                &request_id,
                &rejection.error,
            ));
        }
    };
    let protocol = wire.protocol;
    // The queue provider takes one admission permit for each storage attempt.
    if matches!(wire.request, QueueRequest::ReceiveMessage(_)) {
        // A waiting receive holds no ingress slot; the manager bounds waiting
        // receives. A dropped receive leaves claimed messages to expire.
        let release = move || {
            if let Some(Extension(slot)) = ingress {
                slot.release();
            }
        };
        let result = manager.dispatch(wire.request, release).await;
        return respond(protocol, &request_id, result);
    }
    // Other actions run to completion after a client disconnect, so a commit
    // always reaches its reply path and wakes local waiting receives.
    let task = tokio::spawn(
        async move { manager.dispatch(wire.request, || {}).await }
            .instrument(tracing::Span::current())
            .with_current_subscriber(),
    );
    let result = match task.await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(QueueError::from(storage_types::StorageError::internal(
            "queue request task stopped",
        ))),
    };
    respond(protocol, &request_id, result)
}

/// 503 `ServiceUnavailable` in the protocol of the request, with
/// `Retry-After`, when the ingress ceiling refuses a slot.
pub(crate) fn unavailable_response(headers: &HeaderMap) -> Response {
    let error = HttpApiError::aws_query_error(
        queue_provider::SQS_SERVICE_UNAVAILABLE_ERROR_TYPE,
        "The service is temporarily unavailable.",
        503,
    )
    .with_response_header("Retry-After", "1".to_owned());
    into_response(encode_error(
        QueueProtocol::of(header_text(headers, header::CONTENT_TYPE.as_str())),
        &Uuid::now_v7().to_string(),
        &error,
    ))
}

fn respond(
    protocol: QueueProtocol,
    request_id: &str,
    result: QueueResult<QueueResponse>,
) -> Response {
    into_response(match result {
        Ok(response) => encode_success(protocol, request_id, &response),
        Err(error) => {
            if let Some(storage) = error.storage_error() {
                crate::errors::log_storage_failure("sqs", storage);
            }
            encode_error(protocol, request_id, &HttpApiError::from(error))
        }
    })
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// Copies one encoded SQS response to HTTP.
fn into_response(wire: WireResponse) -> Response {
    let mut response = Response::new(Body::from(wire.body));
    *response.status_mut() =
        StatusCode::from_u16(wire.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(wire.content_type),
    );
    for (name, value) in wire.headers {
        if let Ok(value) = HeaderValue::from_str(&value) {
            headers.insert(HeaderName::from_static(name), value);
        }
    }
    response
}
