#![cfg_attr(not(feature = "queue"), allow(dead_code, unused_variables))]

#[cfg(not(feature = "queue"))]
use std::sync::Arc;
#[cfg(feature = "queue")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "queue")]
use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
#[cfg(not(feature = "queue"))]
use axum::{body::Bytes, extract::State, http::HeaderMap, response::Response};
#[cfg(feature = "queue")]
use http_error::HttpApiError;
#[cfg(feature = "queue")]
use queue::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityRequest, CreateQueueRequest,
    DeleteMessageBatchRequest, DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest,
    GetQueueUrlRequest, ListQueuesRequest, PurgeQueueRequest, QueueManager, ReceiveMessageRequest,
    SendMessageBatchRequest, SendMessageRequest, SetQueueAttributesRequest,
};
#[cfg(feature = "queue")]
use queue_provider::{
    SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE, SQS_INVALID_ACTION_ERROR_TYPE,
    SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_MISSING_PARAMETER_ERROR_TYPE, sqs_json_error_type,
};
#[cfg(feature = "queue")]
use serde::Serialize;
#[cfg(feature = "queue")]
use serde_json::{Value, json};
#[cfg(feature = "queue")]
use uuid::Uuid;

use crate::types::AppState;

#[cfg(feature = "queue")]
const QUEUE_JSON_TARGET_PREFIX: &str = concat!("Amazon", "SQ", "S.");

#[cfg(feature = "queue")]
#[derive(Debug, Clone, Copy)]
enum QueueProtocol {
    Json,
    Query,
}

#[cfg(feature = "queue")]
#[derive(Debug, Clone, Copy)]
enum QueueAction {
    CreateQueue,
    DeleteQueue,
    ListQueues,
    GetQueueUrl,
    GetQueueAttributes,
    SetQueueAttributes,
    PurgeQueue,
    SendMessage,
    SendMessageBatch,
    ReceiveMessage,
    DeleteMessage,
    DeleteMessageBatch,
    ChangeMessageVisibility,
    ChangeMessageVisibilityBatch,
}

#[cfg(feature = "queue")]
impl QueueAction {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "CreateQueue" => Some(Self::CreateQueue),
            "DeleteQueue" => Some(Self::DeleteQueue),
            "ListQueues" => Some(Self::ListQueues),
            "GetQueueUrl" => Some(Self::GetQueueUrl),
            "GetQueueAttributes" => Some(Self::GetQueueAttributes),
            "SetQueueAttributes" => Some(Self::SetQueueAttributes),
            "PurgeQueue" => Some(Self::PurgeQueue),
            "SendMessage" => Some(Self::SendMessage),
            "SendMessageBatch" => Some(Self::SendMessageBatch),
            "ReceiveMessage" => Some(Self::ReceiveMessage),
            "DeleteMessage" => Some(Self::DeleteMessage),
            "DeleteMessageBatch" => Some(Self::DeleteMessageBatch),
            "ChangeMessageVisibility" => Some(Self::ChangeMessageVisibility),
            "ChangeMessageVisibilityBatch" => Some(Self::ChangeMessageVisibilityBatch),
            _ => None,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::CreateQueue => "CreateQueue",
            Self::DeleteQueue => "DeleteQueue",
            Self::ListQueues => "ListQueues",
            Self::GetQueueUrl => "GetQueueUrl",
            Self::GetQueueAttributes => "GetQueueAttributes",
            Self::SetQueueAttributes => "SetQueueAttributes",
            Self::PurgeQueue => "PurgeQueue",
            Self::SendMessage => "SendMessage",
            Self::SendMessageBatch => "SendMessageBatch",
            Self::ReceiveMessage => "ReceiveMessage",
            Self::DeleteMessage => "DeleteMessage",
            Self::DeleteMessageBatch => "DeleteMessageBatch",
            Self::ChangeMessageVisibility => "ChangeMessageVisibility",
            Self::ChangeMessageVisibilityBatch => "ChangeMessageVisibilityBatch",
        }
    }
}

#[cfg(feature = "queue")]
struct QueueWireRequest {
    protocol: QueueProtocol,
    action: QueueAction,
    payload: Value,
}

#[utoipa::path(
    post,
    path = "/queue",
    request_body(content = String, content_type = "application/x-amz-json-1.0"),
    responses((status = 200, description = "SQS-compatible response"))
)]
#[cfg(feature = "queue")]
pub async fn queue_endpoint(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = Uuid::now_v7().to_string();
    let Some(manager) = app_state.queue_manager.as_ref() else {
        return error_response(
            &request_id,
            QueueProtocol::Json,
            StatusCode::NOT_FOUND,
            SQS_INVALID_ACTION_ERROR_TYPE,
            "queue route is not enabled",
        );
    };
    let wire_request = match decode_request(&headers, body) {
        Ok(request) => request,
        Err(message) => {
            return error_response(
                &request_id,
                protocol_from_headers(&headers),
                StatusCode::BAD_REQUEST,
                SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
                &message,
            );
        }
    };
    dispatch_queue_request(
        manager,
        app_state.as_ref(),
        &request_id,
        wire_request.protocol,
        wire_request.action,
        wire_request.payload,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/queue",
    request_body(content = String, content_type = "application/x-amz-json-1.0"),
    responses((status = 404, description = "Queue route is not enabled in this binary"))
)]
#[cfg(not(feature = "queue"))]
pub async fn queue_endpoint(
    State(_app_state): State<Arc<AppState>>,
    _headers: HeaderMap,
    _body: Bytes,
) -> Response {
    axum::response::IntoResponse::into_response((
        axum::http::StatusCode::NOT_FOUND,
        "queue route is not enabled in this binary",
    ))
}

#[cfg(feature = "queue")]
async fn dispatch_queue_request(
    manager: &Arc<QueueManager>,
    app_state: &AppState,
    request_id: &str,
    protocol: QueueProtocol,
    action: QueueAction,
    payload: Value,
) -> Response {
    match action {
        QueueAction::CreateQueue => {
            let request = match CreateQueueRequest::from_json(payload) {
                Ok(request) => request,
                Err(err) => return validation_response(request_id, protocol, &err.message),
            };
            let queue_url = format!(
                "{}/{}/{}",
                app_state.queue_public_base_url.trim_end_matches('/'),
                app_state.queue_account_id,
                request.queue_name
            );
            match manager.create_queue_with_url(request, queue_url).await {
                Ok(response) => ok_response(request_id, protocol, action, &response),
                Err(err) => api_error_response(request_id, protocol, HttpApiError::from(err)),
            }
        }
        QueueAction::DeleteQueue => {
            handle_manager(
                request_id,
                protocol,
                action,
                DeleteQueueRequest::from_json(payload),
                |req| async move { manager.delete_queue(req).await },
            )
            .await
        }
        QueueAction::ListQueues => {
            handle_manager(
                request_id,
                protocol,
                action,
                ListQueuesRequest::from_json(payload),
                |req| async move { manager.list_queues(req).await },
            )
            .await
        }
        QueueAction::GetQueueUrl => {
            handle_manager(
                request_id,
                protocol,
                action,
                GetQueueUrlRequest::from_json(payload),
                |req| async move { manager.get_queue_url(req).await },
            )
            .await
        }
        QueueAction::GetQueueAttributes => {
            handle_manager(
                request_id,
                protocol,
                action,
                GetQueueAttributesRequest::from_json(payload),
                |req| async move { manager.get_queue_attributes(req).await },
            )
            .await
        }
        QueueAction::SetQueueAttributes => {
            handle_manager(
                request_id,
                protocol,
                action,
                SetQueueAttributesRequest::from_json(payload),
                |req| async move { manager.set_queue_attributes(req).await },
            )
            .await
        }
        QueueAction::PurgeQueue => {
            handle_manager(
                request_id,
                protocol,
                action,
                PurgeQueueRequest::from_json(payload),
                |req| async move { manager.purge_queue(req).await },
            )
            .await
        }
        QueueAction::SendMessage => {
            handle_manager(
                request_id,
                protocol,
                action,
                SendMessageRequest::from_json(payload),
                |req| async move { manager.send_message(req).await },
            )
            .await
        }
        QueueAction::SendMessageBatch => {
            handle_manager(
                request_id,
                protocol,
                action,
                SendMessageBatchRequest::from_json(payload),
                |req| async move { manager.send_message_batch(req).await },
            )
            .await
        }
        QueueAction::ReceiveMessage => {
            handle_manager(
                request_id,
                protocol,
                action,
                ReceiveMessageRequest::from_json(payload),
                |req| async move { manager.receive_message(req).await },
            )
            .await
        }
        QueueAction::DeleteMessage => {
            handle_manager(
                request_id,
                protocol,
                action,
                DeleteMessageRequest::from_json(payload),
                |req| async move {
                    manager.delete_message(req).await?;
                    Ok(json!({}))
                },
            )
            .await
        }
        QueueAction::DeleteMessageBatch => {
            handle_manager(
                request_id,
                protocol,
                action,
                DeleteMessageBatchRequest::from_json(payload),
                |req| async move { manager.delete_message_batch(req).await },
            )
            .await
        }
        QueueAction::ChangeMessageVisibility => {
            handle_manager(
                request_id,
                protocol,
                action,
                ChangeMessageVisibilityRequest::from_json(payload),
                |req| async move {
                    manager.change_message_visibility(req).await?;
                    Ok(json!({}))
                },
            )
            .await
        }
        QueueAction::ChangeMessageVisibilityBatch => {
            handle_manager(
                request_id,
                protocol,
                action,
                ChangeMessageVisibilityBatchRequest::from_json(payload),
                |req| async move { manager.change_message_visibility_batch(req).await },
            )
            .await
        }
    }
}

#[cfg(feature = "queue")]
async fn handle_manager<Request, Handler, Fut, T>(
    request_id: &str,
    protocol: QueueProtocol,
    action: QueueAction,
    request: Result<Request, HttpApiError>,
    handler: Handler,
) -> Response
where
    Handler: FnOnce(Request) -> Fut,
    Fut: std::future::Future<Output = queue::QueueResult<T>>,
    T: Serialize,
{
    let request = match request {
        Ok(request) => request,
        Err(err) => return validation_response(request_id, protocol, &err.message),
    };
    match handler(request).await {
        Ok(value) => ok_response(request_id, protocol, action, &value),
        Err(err) => api_error_response(request_id, protocol, HttpApiError::from(err)),
    }
}

#[cfg(feature = "queue")]
fn decode_request(headers: &HeaderMap, body: Bytes) -> Result<QueueWireRequest, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/x-amz-json-1.0") {
        let target = headers
            .get("x-amz-target")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| "missing_x_amz_target".to_string())?;
        let action_name = target
            .strip_prefix(QUEUE_JSON_TARGET_PREFIX)
            .ok_or_else(|| "invalid_x_amz_target".to_string())?;
        let action =
            QueueAction::from_name(action_name).ok_or_else(|| "unsupported_action".to_string())?;
        let payload =
            serde_json::from_slice::<Value>(&body).map_err(|err| format!("invalid_json:{err}"))?;
        return Ok(QueueWireRequest {
            protocol: QueueProtocol::Json,
            action,
            payload,
        });
    }
    if content_type.starts_with("application/x-www-form-urlencoded") {
        let fields: HashMap<String, String> = url::form_urlencoded::parse(&body)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let action_name = fields
            .get("Action")
            .ok_or_else(|| "missing_action".to_string())?;
        let action =
            QueueAction::from_name(action_name).ok_or_else(|| "unsupported_action".to_string())?;
        return Ok(QueueWireRequest {
            protocol: QueueProtocol::Query,
            action,
            payload: query_fields_to_json(fields),
        });
    }
    Err("unsupported_content_type".to_string())
}

#[cfg(feature = "queue")]
fn query_fields_to_json(fields: HashMap<String, String>) -> Value {
    let mut payload = serde_json::Map::new();
    for (key, value) in fields {
        if key != "Action" && key != "Version" {
            payload.insert(key, Value::String(value));
        }
    }
    Value::Object(payload)
}

#[cfg(feature = "queue")]
fn protocol_from_headers(headers: &HeaderMap) -> QueueProtocol {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/x-www-form-urlencoded") {
        QueueProtocol::Query
    } else {
        QueueProtocol::Json
    }
}

#[cfg(feature = "queue")]
fn validation_response(request_id: &str, protocol: QueueProtocol, message: &str) -> Response {
    let code = if message.starts_with("Id ") && message.ends_with(" repeated.") {
        SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE
    } else if message == "The request must contain the parameter MessageBody." {
        SQS_MISSING_PARAMETER_ERROR_TYPE
    } else {
        SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    };
    error_response(request_id, protocol, StatusCode::BAD_REQUEST, code, message)
}

#[cfg(feature = "queue")]
fn ok_response<T: Serialize>(
    request_id: &str,
    protocol: QueueProtocol,
    action: QueueAction,
    value: &T,
) -> Response {
    match protocol {
        QueueProtocol::Json => {
            let mut response = (StatusCode::OK, Json(value)).into_response();
            add_json_headers(response.headers_mut(), request_id, None);
            response
        }
        QueueProtocol::Query => {
            let value = serde_json::to_value(value).unwrap_or(Value::Null);
            let action_name = action.name();
            let body = format!(
                "<?xml version=\"1.0\"?><{action_name}Response><{action_name}Result>{}</\
                 {action_name}Result><ResponseMetadata><RequestId>{}</RequestId></\
                 ResponseMetadata></{action_name}Response>",
                escape_xml(&value.to_string()),
                escape_xml(request_id)
            );
            let mut response = (StatusCode::OK, body).into_response();
            add_query_headers(response.headers_mut(), request_id, None);
            response
        }
    }
}

#[cfg(feature = "queue")]
fn api_error_response(request_id: &str, protocol: QueueProtocol, error: HttpApiError) -> Response {
    let status =
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    error_response(
        request_id,
        protocol,
        status,
        &error.error_type,
        &error.message,
    )
}

#[cfg(feature = "queue")]
fn error_response(
    request_id: &str,
    protocol: QueueProtocol,
    status: StatusCode,
    error_type: &str,
    message: &str,
) -> Response {
    match protocol {
        QueueProtocol::Json => {
            let mut response = (
                status,
                Json(json!({ "__type": sqs_json_error_type(error_type), "message": message })),
            )
                .into_response();
            add_json_headers(response.headers_mut(), request_id, Some(error_type));
            response
        }
        QueueProtocol::Query => {
            let body = format!(
                "<?xml version=\"1.0\"?><ErrorResponse><Error><Type>Sender</Type><Code>{}</\
                 Code><Message>{}</Message></Error><RequestId>{}</RequestId></ErrorResponse>",
                escape_xml(error_type),
                escape_xml(message),
                escape_xml(request_id)
            );
            let mut response = (status, body).into_response();
            add_query_headers(response.headers_mut(), request_id, Some(error_type));
            response
        }
    }
}

#[cfg(feature = "queue")]
fn add_json_headers(headers: &mut HeaderMap, request_id: &str, error_type: Option<&str>) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    add_common_headers(headers, request_id, error_type);
}

#[cfg(feature = "queue")]
fn add_query_headers(headers: &mut HeaderMap, request_id: &str, error_type: Option<&str>) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    add_common_headers(headers, request_id, error_type);
}

#[cfg(feature = "queue")]
fn add_common_headers(headers: &mut HeaderMap, request_id: &str, error_type: Option<&str>) {
    headers.insert(
        "x-amzn-requestid",
        request_id
            .parse()
            .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
    );
    if let Some(error_type) = error_type {
        let query_error = format!("{error_type};Sender");
        headers.insert(
            "x-amzn-query-error",
            query_error
                .parse()
                .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
        );
    }
}

#[cfg(feature = "queue")]
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
