#![cfg_attr(not(feature = "queue"), allow(dead_code, unused_variables))]

#[cfg(not(feature = "queue"))]
use std::sync::Arc;
#[cfg(feature = "queue")]
use std::sync::Arc;

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
use queue::QueueManager;
#[cfg(feature = "queue")]
use queue_provider::{
    QueueAction, QueueRequest, SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE,
    SQS_INVALID_ACTION_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
    SQS_MISSING_PARAMETER_ERROR_TYPE, decode_json_request, decode_value_request,
    query_fields_to_json, sqs_json_error_type,
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
struct QueueWireRequest {
    protocol: QueueProtocol,
    request: QueueRequest,
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
            return validation_response(&request_id, protocol_from_headers(&headers), &message);
        }
    };
    dispatch_queue_request(
        manager,
        app_state.as_ref(),
        &request_id,
        wire_request.protocol,
        wire_request.request,
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
    request: QueueRequest,
) -> Response {
    let action = request.action();
    match request {
        QueueRequest::CreateQueue(request) => {
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
        QueueRequest::DeleteQueue(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.delete_queue(req).await
            })
            .await
        }
        QueueRequest::ListQueues(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.list_queues(req).await
            })
            .await
        }
        QueueRequest::GetQueueUrl(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.get_queue_url(req).await
            })
            .await
        }
        QueueRequest::GetQueueAttributes(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.get_queue_attributes(req).await
            })
            .await
        }
        QueueRequest::SetQueueAttributes(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.set_queue_attributes(req).await
            })
            .await
        }
        QueueRequest::PurgeQueue(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.purge_queue(req).await
            })
            .await
        }
        QueueRequest::SendMessage(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.send_message(req).await
            })
            .await
        }
        QueueRequest::SendMessageBatch(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.send_message_batch(req).await
            })
            .await
        }
        QueueRequest::ReceiveMessage(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.receive_message(req).await
            })
            .await
        }
        QueueRequest::DeleteMessage(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.delete_message(req).await?;
                Ok(json!({}))
            })
            .await
        }
        QueueRequest::DeleteMessageBatch(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.delete_message_batch(req).await
            })
            .await
        }
        QueueRequest::ChangeMessageVisibility(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.change_message_visibility(req).await?;
                Ok(json!({}))
            })
            .await
        }
        QueueRequest::ChangeMessageVisibilityBatch(request) => {
            handle_manager(request_id, protocol, action, request, |req| async move {
                manager.change_message_visibility_batch(req).await
            })
            .await
        }
    }
}
#[cfg(feature = "queue")]
async fn handle_manager<Request, Handler, Fut, T>(
    request_id: &str,
    protocol: QueueProtocol,
    action: QueueAction,
    request: Request,
    handler: Handler,
) -> Response
where
    Handler: FnOnce(Request) -> Fut,
    Fut: std::future::Future<Output = queue::QueueResult<T>>,
    T: Serialize,
{
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
        let request = decode_json_request(action, &body).map_err(|error| error.message)?;
        return Ok(QueueWireRequest {
            protocol: QueueProtocol::Json,
            request,
        });
    }
    if content_type.starts_with("application/x-www-form-urlencoded") {
        let fields = url::form_urlencoded::parse(&body)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        let action_name = fields
            .iter()
            .rev()
            .find_map(|(key, value)| (key == "Action").then_some(value))
            .ok_or_else(|| "missing_action".to_string())?;
        let action =
            QueueAction::from_name(action_name).ok_or_else(|| "unsupported_action".to_string())?;
        let request = decode_value_request(action, query_fields_to_json(fields))
            .map_err(|error| error.message)?;
        return Ok(QueueWireRequest {
            protocol: QueueProtocol::Query,
            request,
        });
    }
    Err("unsupported_content_type".to_string())
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

#[cfg(all(test, feature = "queue"))]
mod tests {
    use super::*;

    fn decode_query(body: &'static [u8]) -> QueueWireRequest {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        decode_request(&headers, Bytes::from_static(body)).expect("query request decodes")
    }

    #[test]
    fn query_protocol_decodes_numbered_attribute_maps_and_lists() {
        let request = decode_query(
            b"Action=ReceiveMessage&QueueUrl=https%3A%2F%2Fqueue.example%2Fjobs&AttributeName.2=SentTimestamp&AttributeName.1=All&MessageAttributeName.1=TraceId",
        );

        let QueueRequest::ReceiveMessage(request) = request.request else {
            panic!("expected receive request");
        };
        assert_eq!(
            request.attribute_names,
            Some(vec!["All".to_string(), "SentTimestamp".to_string()])
        );
        assert_eq!(
            request.message_attribute_names,
            Some(vec!["TraceId".to_string()])
        );
    }

    #[test]
    fn query_protocol_decodes_message_attributes() {
        let request = decode_query(
            b"Action=SendMessage&QueueUrl=https%3A%2F%2Fqueue.example%2Fjobs&MessageBody=body&MessageAttribute.1.Name=Trace&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringValue=abc",
        );

        let QueueRequest::SendMessage(request) = request.request else {
            panic!("expected send request");
        };
        let attributes = request
            .message_attributes
            .as_ref()
            .expect("message attributes");
        let trace = attributes.get("Trace").expect("trace attribute");
        assert_eq!(trace.data_type, "String");
        assert_eq!(trace.string_value.as_deref(), Some("abc"));
    }

    #[test]
    fn query_protocol_decodes_batch_entries_in_index_order() {
        let request = decode_query(
            b"Action=SendMessageBatch&QueueUrl=https%3A%2F%2Fqueue.example%2Fjobs&SendMessageBatchRequestEntry.2.Id=second&SendMessageBatchRequestEntry.2.MessageBody=two&SendMessageBatchRequestEntry.1.Id=first&SendMessageBatchRequestEntry.1.MessageBody=one&SendMessageBatchRequestEntry.1.DelaySeconds=5",
        );

        let QueueRequest::SendMessageBatch(request) = request.request else {
            panic!("expected send batch request");
        };
        assert_eq!(request.entries[0].id, "first");
        assert_eq!(request.entries[0].delay_seconds, Some(5));
        assert_eq!(request.entries[1].id, "second");
    }
}
