use axum::response::Response as AxumResponse;
use storage_types::{GetItemResponse, StorageError, WireItem};

use crate::raw_dynamodb_response::{
    json_response_bytes, serialization_error_response, write_field_name, write_wire_item,
};

#[derive(Debug, Clone)]
pub struct GetWireResponse {
    pub item: Option<WireItem>,
}

impl GetWireResponse {
    pub fn into_get_item_response(self) -> Result<GetItemResponse, StorageError> {
        let item = self
            .item
            .map(WireItem::into_attribute_map)
            .transpose()?
            .map(Into::into);
        Ok(GetItemResponse { item })
    }

    pub fn into_http_response(self) -> AxumResponse {
        match self.into_json_bytes() {
            Ok(bytes) => json_response_bytes(bytes),
            Err(error) => serialization_error_response("GetItem", error),
        }
    }

    fn into_json_bytes(self) -> serde_json::Result<Vec<u8>> {
        let mut out = Vec::new();
        out.push(b'{');
        let mut first = true;

        if let Some(item) = self.item {
            write_field_name(&mut out, &mut first, "Item")?;
            write_wire_item(&mut out, item)?;
        }

        out.push(b'}');
        Ok(out)
    }
}

impl TryFrom<GetWireResponse> for GetItemResponse {
    type Error = StorageError;

    fn try_from(response: GetWireResponse) -> Result<Self, Self::Error> {
        response.into_get_item_response()
    }
}

impl From<Option<WireItem>> for GetWireResponse {
    fn from(item: Option<WireItem>) -> Self {
        Self { item }
    }
}
