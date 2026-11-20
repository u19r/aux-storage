use std::io::Write as _;

use axum::{
    body::Body,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Json, Response as AxumResponse},
};
use http_error::ErrorResponse;
use storage_types::WireItem;

pub(crate) fn json_response_bytes(bytes: Vec<u8>) -> AxumResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/x-amz-json-1.0")],
        Body::from(bytes),
    )
        .into_response()
}

pub(crate) fn serialization_error_response(
    operation: &str,
    error: serde_json::Error,
) -> AxumResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error_type: "InternalFailure".to_string(),
            message: format!("serialize {operation} response failed: {error}"),
            transaction_message: None,
            cancellation_reasons: None,
            item: None,
            request_id: None,
            documentation_url: None,
            retry_after_seconds: None,
        }),
    )
        .into_response()
}

pub(crate) fn write_field_name(
    out: &mut Vec<u8>,
    first: &mut bool,
    name: &str,
) -> serde_json::Result<()> {
    if *first {
        *first = false;
    } else {
        out.push(b',');
    }
    serde_json::to_writer(&mut *out, name)?;
    out.write_all(b":").map_err(serde_json::Error::io)
}

pub(crate) fn write_wire_item(out: &mut Vec<u8>, item: WireItem) -> serde_json::Result<()> {
    write_wire_item_ref(out, &item)
}

fn write_wire_item_ref(out: &mut Vec<u8>, item: &WireItem) -> serde_json::Result<()> {
    match item {
        WireItem::DynamoJson { data } => out
            .write_all(data.as_slice())
            .map_err(serde_json::Error::io),
    }
}
