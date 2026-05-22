use axum::response::Response as AxumResponse;
use storage_types::{
    AttributeMap, ConsumedCapacity, KeyAttributes, QueryResponse, StorageError, WireItem,
};

use crate::raw_dynamodb_response::{
    json_response_bytes, serialization_error_response, wire_item_array_json_capacity,
    write_field_name, write_wire_item_array,
};

#[derive(Debug, Clone)]
pub struct QueryWireResponse {
    pub items: Option<Vec<WireItem>>,
    pub count: u32,
    pub scanned_count: u32,
    pub last_evaluated_key: Option<KeyAttributes>,
    pub consumed_capacity: Option<ConsumedCapacity>,
}

impl QueryWireResponse {
    pub fn into_query_response(self) -> Result<QueryResponse, StorageError> {
        let items = self
            .items
            .map(|items| {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(item.into_attribute_map()?.into());
                }
                Ok::<Vec<AttributeMap>, StorageError>(out)
            })
            .transpose()?;
        Ok(QueryResponse {
            items,
            count: self.count,
            scanned_count: self.scanned_count,
            last_evaluated_key: self.last_evaluated_key,
            consumed_capacity: self.consumed_capacity,
        })
    }

    pub fn into_http_response(self) -> AxumResponse {
        match self.into_json_bytes() {
            Ok(bytes) => json_response_bytes(bytes),
            Err(error) => serialization_error_response("query", error),
        }
    }

    pub(crate) fn into_json_bytes(self) -> serde_json::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.json_capacity_hint());
        out.push(b'{');
        let mut first = true;

        if let Some(items) = self.items {
            write_field_name(&mut out, &mut first, "Items")?;
            write_wire_item_array(&mut out, items)?;
        }

        write_field_name(&mut out, &mut first, "Count")?;
        serde_json::to_writer(&mut out, &self.count)?;

        write_field_name(&mut out, &mut first, "ScannedCount")?;
        serde_json::to_writer(&mut out, &self.scanned_count)?;

        if let Some(last_evaluated_key) = self.last_evaluated_key {
            write_field_name(&mut out, &mut first, "LastEvaluatedKey")?;
            serde_json::to_writer(&mut out, &last_evaluated_key)?;
        }

        if let Some(consumed_capacity) = self.consumed_capacity {
            write_field_name(&mut out, &mut first, "ConsumedCapacity")?;
            serde_json::to_writer(&mut out, &consumed_capacity)?;
        }

        out.push(b'}');
        Ok(out)
    }

    fn json_capacity_hint(&self) -> usize {
        let mut capacity = 64;
        if let Some(items) = &self.items {
            capacity += "Items".len() + wire_item_array_json_capacity(items);
        }
        if let Some(last_evaluated_key) = &self.last_evaluated_key {
            capacity += "LastEvaluatedKey".len() + last_evaluated_key.len();
        }
        if self.consumed_capacity.is_some() {
            capacity += 128;
        }
        capacity
    }
}
