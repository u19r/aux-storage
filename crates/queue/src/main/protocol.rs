use std::collections::HashMap;

use axum::{
    Json,
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use http_error::HttpApiError;
use queue_provider::{
    SQS_INVALID_ACTION_ERROR_TYPE, SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, sqs_json_error_type,
};
use serde::Serialize;
use serde_json::{Value, json};

const QUEUE_JSON_TARGET_PREFIX: &str = concat!("Amazon", "SQ", "S.");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueProtocol {
    Json,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueAction {
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

    pub(crate) const fn name(self) -> &'static str {
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

#[derive(Debug)]
pub(crate) struct QueueWireRequest {
    pub(crate) protocol: QueueProtocol,
    pub(crate) action: QueueAction,
    pub(crate) payload: Value,
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
    let payload = serde_json::from_slice::<Value>(&body)
        .map_err(|err| QueueWireError::invalid_parameter(format!("invalid_json:{err}")))?;

    Ok(QueueWireRequest {
        protocol: QueueProtocol::Json,
        action,
        payload,
    })
}

fn decode_query_request(body: Bytes) -> Result<QueueWireRequest, QueueWireError> {
    let fields: HashMap<String, String> = url::form_urlencoded::parse(&body)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let action_name = fields
        .get("Action")
        .ok_or_else(|| QueueWireError::invalid_action("missing_action"))?;
    let action = QueueAction::from_name(action_name)
        .ok_or_else(|| QueueWireError::invalid_action("unsupported_action"))?;

    Ok(QueueWireRequest {
        protocol: QueueProtocol::Query,
        action,
        payload: query_fields_to_json(fields),
    })
}

fn query_fields_to_json(fields: HashMap<String, String>) -> Value {
    let mut payload = serde_json::Map::new();
    let mut attributes = serde_json::Map::new();
    let mut attribute_names = Vec::new();
    let mut message_attribute_names = Vec::new();
    let mut send_entries = Vec::new();
    let mut delete_entries = Vec::new();
    let mut visibility_entries = Vec::new();
    let mut message_attributes = serde_json::Map::new();

    for (key, value) in &fields {
        if key == "Action" || key == "Version" {
            continue;
        }
        if collect_map_entry(&mut attributes, key, value, "Attribute") {
            continue;
        }
        if collect_list_member(&mut attribute_names, key, value, "AttributeName") {
            continue;
        }
        if collect_list_member(
            &mut message_attribute_names,
            key,
            value,
            "MessageAttributeName",
        ) {
            continue;
        }
        if collect_batch_entry(
            &mut send_entries,
            key,
            value,
            "SendMessageBatchRequestEntry",
        ) {
            continue;
        }
        if collect_batch_entry(
            &mut delete_entries,
            key,
            value,
            "DeleteMessageBatchRequestEntry",
        ) {
            continue;
        }
        if collect_batch_entry(
            &mut visibility_entries,
            key,
            value,
            "ChangeMessageVisibilityBatchRequestEntry",
        ) {
            continue;
        }
        if collect_message_attribute(&mut message_attributes, key, value, "MessageAttribute") {
            continue;
        }
        let json_value = query_value_to_json(key, value.clone());
        payload.insert(key.clone(), json_value);
    }

    if !attributes.is_empty() {
        payload.insert("Attributes".to_string(), Value::Object(attributes));
    }
    if !attribute_names.is_empty() {
        payload.insert("AttributeNames".to_string(), Value::Array(attribute_names));
    }
    if !message_attribute_names.is_empty() {
        payload.insert(
            "MessageAttributeNames".to_string(),
            Value::Array(message_attribute_names),
        );
    }
    message_attributes.retain(|key, _| !key.starts_with("__message_attribute_"));
    if !message_attributes.is_empty() {
        payload.insert(
            "MessageAttributes".to_string(),
            Value::Object(message_attributes),
        );
    }
    if !send_entries.is_empty() {
        payload.insert("Entries".to_string(), Value::Array(send_entries));
    } else if !delete_entries.is_empty() {
        payload.insert("Entries".to_string(), Value::Array(delete_entries));
    } else if !visibility_entries.is_empty() {
        payload.insert("Entries".to_string(), Value::Array(visibility_entries));
    }
    Value::Object(payload)
}

fn collect_map_entry(
    output: &mut serde_json::Map<String, Value>,
    key: &str,
    value: &str,
    prefix: &str,
) -> bool {
    let Some(rest) = key
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    let parts: Vec<_> = rest.split('.').collect();
    if parts.len() != 2 {
        return false;
    }
    let Some(field) = map_entry_field(parts[1]) else {
        return false;
    };
    let entry_key = format!("__entry_{}_{}", prefix, parts[0]);
    let entry = output
        .entry(entry_key)
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    let Some(entry) = entry.as_object_mut() else {
        return false;
    };
    entry.insert(field.to_string(), Value::String(value.to_string()));

    let completed = entry
        .get("Name")
        .and_then(Value::as_str)
        .zip(entry.get("Value").and_then(Value::as_str))
        .map(|(name, value)| (name.to_string(), value.to_string()));
    if let Some((name, value)) = completed {
        output.remove(&format!("__entry_{}_{}", prefix, parts[0]));
        output.insert(name, Value::String(value));
    }
    true
}

fn map_entry_field(field: &str) -> Option<&'static str> {
    match field {
        "Name" | "key" => Some("Name"),
        "Value" | "value" => Some("Value"),
        _ => None,
    }
}

fn collect_list_member(output: &mut Vec<Value>, key: &str, value: &str, prefix: &str) -> bool {
    let Some(index) = key
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    if index.parse::<usize>().is_err() {
        return false;
    }
    output.push(Value::String(value.to_string()));
    true
}

fn collect_batch_entry(output: &mut Vec<Value>, key: &str, value: &str, prefix: &str) -> bool {
    let Some(rest) = key
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    let parts: Vec<_> = rest.split('.').collect();
    if parts.len() != 2 {
        return false;
    }
    let Ok(index) = parts[0].parse::<usize>() else {
        return false;
    };
    ensure_array_len(output, index);
    if let Some(entry) = output.get_mut(index - 1).and_then(Value::as_object_mut) {
        entry.insert(
            parts[1].to_string(),
            query_value_to_json(parts[1], value.to_string()),
        );
    }
    true
}

fn collect_message_attribute(
    output: &mut serde_json::Map<String, Value>,
    key: &str,
    value: &str,
    prefix: &str,
) -> bool {
    let Some(rest) = key
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    let parts: Vec<_> = rest.split('.').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return false;
    }
    let entry_key = format!("__message_attribute_{}", parts[0]);
    let entry = output
        .entry(entry_key.clone())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    let Some(entry) = entry.as_object_mut() else {
        return false;
    };
    let field = if parts.len() == 3 && parts[1] == "Value" {
        parts[2]
    } else {
        parts[1]
    };
    entry.insert(field.to_string(), Value::String(value.to_string()));
    let completed = entry
        .get("Name")
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(name) = completed {
        let mut attr = entry.clone();
        attr.remove("Name");
        output.insert(name, Value::Object(attr));
    }
    true
}

fn ensure_array_len(output: &mut Vec<Value>, index: usize) {
    while output.len() < index {
        output.push(Value::Object(serde_json::Map::new()));
    }
}

fn query_value_to_json(key: &str, value: String) -> Value {
    match key {
        "DelaySeconds" | "MaxNumberOfMessages" | "VisibilityTimeout" | "WaitTimeSeconds" => value
            .parse::<u32>()
            .map_or_else(|_| Value::String(value), |number| json!(number)),
        _ => Value::String(value),
    }
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
    error_response(
        request_id,
        protocol,
        status,
        &error.error_type,
        &error.message,
    )
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
    body.push_str(&format!(
        "<{action_name}Response xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">"
    ));
    body.push_str(&format!("<{action_name}Result>"));
    write_query_value(&mut body, None, &value);
    body.push_str(&format!("</{action_name}Result>"));
    body.push_str("<ResponseMetadata><RequestId>");
    body.push_str(&escape_xml(request_id));
    body.push_str("</RequestId></ResponseMetadata>");
    body.push_str(&format!("</{action_name}Response>"));

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
    let body = format!(
        "<?xml version=\"1.0\"?><ErrorResponse><Error><Type>Sender</Type><Code>{}</\
         Code><Message>{}</Message></Error><RequestId>{}</RequestId></ErrorResponse>",
        escape_xml(code),
        escape_xml(message),
        escape_xml(request_id)
    );
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
        Value::String(value) => output.push_str(&escape_xml(value)),
        Value::Number(value) => output.push_str(&escape_xml(&value.to_string())),
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
        output.push_str(&escape_xml(key));
        output.push_str("</Name><Value>");
        output.push_str(&escape_xml(value));
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

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
