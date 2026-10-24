use std::fmt::Write as _;

use axum::{
    Json,
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use http_error::HttpApiError;
pub(crate) use queue_provider::{QueueAction, QueueRequest};
use queue_provider::{
    SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE, SQS_INVALID_ACTION_ERROR_TYPE,
    SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_MISSING_PARAMETER_ERROR_TYPE,
    decode_json_request as decode_json_body, decode_value_request, query_fields_to_json,
    sqs_json_error_type,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::constants::QUEUE_JSON_TARGET_PREFIX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueProtocol {
    Json,
    Query,
}

#[derive(Debug)]
pub(crate) struct QueueWireRequest {
    pub(crate) protocol: QueueProtocol,
    pub(crate) request: QueueRequest,
}

#[derive(Debug)]
pub(crate) struct QueueWireError {
    pub(crate) status: StatusCode,
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl QueueWireError {
    pub(crate) fn invalid_action(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: SQS_INVALID_ACTION_ERROR_TYPE,
            message: message.into(),
        }
    }

    pub(crate) fn invalid_parameter(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            message: message.into(),
        }
    }

    fn validation(message: String) -> Self {
        let code = if message.starts_with("Id ") && message.ends_with(" repeated.") {
            SQS_BATCH_ENTRY_IDS_NOT_DISTINCT_ERROR_TYPE
        } else if message == "The request must contain the parameter MessageBody." {
            SQS_MISSING_PARAMETER_ERROR_TYPE
        } else {
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
        };
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message,
        }
    }
}

pub(crate) fn decode_request(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<QueueWireRequest, QueueWireError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if content_type.starts_with("application/x-amz-json-1.0") {
        return decode_json_request(headers, body);
    }

    if content_type.starts_with("application/x-www-form-urlencoded") {
        return decode_query_request(body);
    }

    Err(QueueWireError::invalid_parameter(
        "unsupported_content_type",
    ))
}

fn decode_json_request(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<QueueWireRequest, QueueWireError> {
    let target = headers
        .get("x-amz-target")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| QueueWireError::invalid_action("missing_x_amz_target"))?;
    let action_name = target
        .strip_prefix(QUEUE_JSON_TARGET_PREFIX)
        .ok_or_else(|| QueueWireError::invalid_action("invalid_x_amz_target"))?;
    let action = QueueAction::from_name(action_name)
        .ok_or_else(|| QueueWireError::invalid_action("unsupported_action"))?;
    let request = decode_json_body(action, &body)
        .map_err(|error| QueueWireError::validation(error.message))?;

    Ok(QueueWireRequest {
        protocol: QueueProtocol::Json,
        request,
    })
}

fn decode_query_request(body: Bytes) -> Result<QueueWireRequest, QueueWireError> {
    let fields = url::form_urlencoded::parse(&body)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let action_name = fields
        .iter()
        .rev()
        .find_map(|(key, value)| (key == "Action").then_some(value))
        .ok_or_else(|| QueueWireError::invalid_action("missing_action"))?;
    let action = QueueAction::from_name(action_name)
        .ok_or_else(|| QueueWireError::invalid_action("unsupported_action"))?;

    let request = decode_value_request(action, query_fields_to_json(fields))
        .map_err(|error| QueueWireError::validation(error.message))?;
    Ok(QueueWireRequest {
        protocol: QueueProtocol::Query,
        request,
    })
}

pub(crate) fn ok_response<T: Serialize>(
    request_id: &str,
    protocol: QueueProtocol,
    action: QueueAction,
    value: &T,
) -> Response {
    match protocol {
        QueueProtocol::Json => json_ok_response(request_id, value),
        QueueProtocol::Query => query_ok_response(request_id, action, value),
    }
}

fn json_ok_response<T: Serialize>(request_id: &str, value: &T) -> Response {
    let mut response = (StatusCode::OK, Json(value)).into_response();
    add_common_headers(response.headers_mut(), request_id, None);
    response
}

pub(crate) fn error_response(
    request_id: &str,
    protocol: QueueProtocol,
    status: StatusCode,
    error_type: &str,
    message: &str,
) -> Response {
    match protocol {
        QueueProtocol::Json => json_error_response(request_id, status, error_type, message),
        QueueProtocol::Query => query_error_response(request_id, status, error_type, message),
    }
}

pub(crate) fn wire_error_response(
    request_id: &str,
    protocol: QueueProtocol,
    error: QueueWireError,
) -> Response {
    error_response(
        request_id,
        protocol,
        error.status,
        error.code,
        &error.message,
    )
}

pub(crate) fn api_error_response(
    request_id: &str,
    protocol: QueueProtocol,
    error: HttpApiError,
) -> Response {
    let status = status_code_from_u16(error.status_code);
    let mut response = error_response(
        request_id,
        protocol,
        status,
        &error.error_type,
        &error.message,
    );
    add_error_headers(response.headers_mut(), &error);
    response
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

fn status_code_from_u16(status_code: u16) -> StatusCode {
    StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn json_error_response(
    request_id: &str,
    status: StatusCode,
    error_type: &str,
    message: &str,
) -> Response {
    let body = Json(json!({
        "__type": sqs_json_error_type(error_type),
        "message": message,
    }));
    let mut response = (status, body).into_response();
    add_common_headers(response.headers_mut(), request_id, Some(error_type));
    response
}

fn query_ok_response<T: Serialize>(request_id: &str, action: QueueAction, value: &T) -> Response {
    let value = serde_json::to_value(value).unwrap_or(Value::Null);
    let action_name = action.name();
    let mut body = String::new();
    body.push_str("<?xml version=\"1.0\"?>");
    push_query_open_tag(&mut body, action_name, "Response");
    body.push_str(" xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">");
    push_query_tag(&mut body, action_name, "Result");
    write_query_value(&mut body, None, &value);
    push_query_close_tag(&mut body, action_name, "Result");
    body.push_str("<ResponseMetadata><RequestId>");
    append_escaped_xml(&mut body, request_id);
    body.push_str("</RequestId></ResponseMetadata>");
    push_query_close_tag(&mut body, action_name, "Response");

    let mut response = (StatusCode::OK, body).into_response();
    add_query_headers(response.headers_mut(), request_id, None);
    response
}

fn query_error_response(
    request_id: &str,
    status: StatusCode,
    error_type: &str,
    message: &str,
) -> Response {
    let code = error_type.rsplit('.').next().unwrap_or(error_type);
    let mut body =
        String::from("<?xml version=\"1.0\"?><ErrorResponse><Error><Type>Sender</Type><Code>");
    append_escaped_xml(&mut body, code);
    body.push_str("</Code><Message>");
    append_escaped_xml(&mut body, message);
    body.push_str("</Message></Error><RequestId>");
    append_escaped_xml(&mut body, request_id);
    body.push_str("</RequestId></ErrorResponse>");
    let mut response = (status, body).into_response();
    add_query_headers(response.headers_mut(), request_id, Some(error_type));
    response
}

fn write_query_value(output: &mut String, name: Option<&str>, value: &Value) {
    match value {
        Value::Object(map) => {
            if is_attribute_map(name, map) {
                write_attribute_entries(output, map);
                return;
            }
            for (key, value) in map {
                write_query_value(output, Some(key), value);
            }
        }
        Value::Array(values) => {
            let member_name = array_member_name(name);
            for value in values {
                write_named_query_value(output, member_name, value);
            }
        }
        Value::Null => {}
        scalar => {
            if let Some(name) = name {
                write_named_query_value(output, name, scalar);
            }
        }
    }
}

fn write_named_query_value(output: &mut String, name: &str, value: &Value) {
    output.push('<');
    output.push_str(name);
    output.push('>');
    match value {
        Value::Object(_) | Value::Array(_) => write_query_value(output, Some(name), value),
        Value::String(value) => append_escaped_xml(output, value),
        Value::Number(value) => {
            let _ = write!(output, "{value}");
        }
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Null => {}
    }
    output.push_str("</");
    output.push_str(name);
    output.push('>');
}

fn is_attribute_map(name: Option<&str>, map: &serde_json::Map<String, Value>) -> bool {
    name.is_some_and(|name| name == "Attributes") && map.values().all(Value::is_string)
}

fn write_attribute_entries(output: &mut String, map: &serde_json::Map<String, Value>) {
    let mut keys: Vec<_> = map.keys().collect();
    keys.sort();
    for key in keys {
        let value = map.get(key).and_then(Value::as_str).unwrap_or_default();
        output.push_str("<Attribute><Name>");
        append_escaped_xml(output, key);
        output.push_str("</Name><Value>");
        append_escaped_xml(output, value);
        output.push_str("</Value></Attribute>");
    }
}

fn array_member_name(name: Option<&str>) -> &'static str {
    match name {
        Some("QueueUrls") => "QueueUrl",
        Some("Messages") => "Message",
        Some("Successful") | Some("Failed") => "member",
        _ => "member",
    }
}

fn push_query_open_tag(output: &mut String, action_name: &str, suffix: &str) {
    output.push('<');
    output.push_str(action_name);
    output.push_str(suffix);
}

fn push_query_tag(output: &mut String, action_name: &str, suffix: &str) {
    push_query_open_tag(output, action_name, suffix);
    output.push('>');
}

fn push_query_close_tag(output: &mut String, action_name: &str, suffix: &str) {
    output.push_str("</");
    output.push_str(action_name);
    output.push_str(suffix);
    output.push('>');
}

fn append_escaped_xml(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(character),
        }
    }
}

pub(crate) fn add_common_headers(
    headers: &mut HeaderMap,
    request_id: &str,
    error_type: Option<&str>,
) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amzn-requestid",
        HeaderValue::from_str(request_id).unwrap_or_else(|_| HeaderValue::from_static("invalid")),
    );
    if let Some(error_type) = error_type {
        let query_error = format!("{error_type};Sender");
        headers.insert(
            "x-amzn-query-error",
            HeaderValue::from_str(&query_error)
                .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
        );
    }
}

fn add_query_headers(headers: &mut HeaderMap, request_id: &str, error_type: Option<&str>) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    headers.insert(
        "x-amzn-requestid",
        HeaderValue::from_str(request_id).unwrap_or_else(|_| HeaderValue::from_static("invalid")),
    );
    if let Some(error_type) = error_type {
        let query_error = format!("{error_type};Sender");
        headers.insert(
            "x-amzn-query-error",
            HeaderValue::from_str(&query_error)
                .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
        );
    }
}
