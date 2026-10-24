use axum::response::Response as AxumResponse;
use storage_types::GetItemResponse;

use crate::raw_dynamodb_response::{
    json_response_bytes, serialization_error_response, write_field_name, write_wire_item,
};

pub fn into_http_response(response: GetItemResponse) -> AxumResponse {
    match into_json_bytes(response) {
        Ok(bytes) => json_response_bytes(bytes),
        Err(error) => serialization_error_response("GetItem", error),
    }
}

pub(crate) fn into_json_bytes(response: GetItemResponse) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'{');
    let mut first = true;
    if let Some(item) = response.item {
        write_field_name(&mut out, &mut first, "Item")?;
        write_wire_item(&mut out, item)?;
    }
    out.push(b'}');
    Ok(out)
}
