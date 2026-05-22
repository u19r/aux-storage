use std::collections::HashMap;

use axum::response::Response as AxumResponse;
use storage_types::{
    AttributeMap, BatchGetItemResponse, BatchGetWireItemResponse, KeysAndAttributes, StorageError,
    TableName, WireItem,
};

use crate::raw_dynamodb_response::{
    json_response_bytes, serialization_error_response, wire_item_array_json_capacity,
    write_field_name, write_wire_item_array,
};

#[derive(Debug, Clone)]
pub struct BatchGetWireResponse {
    pub responses: Option<HashMap<TableName, Vec<WireItem>>>,
    pub unprocessed_keys: Option<HashMap<TableName, KeysAndAttributes>>,
    pub consumed_capacity: Option<serde_json::Value>,
}

impl BatchGetWireResponse {
    pub fn into_batch_get_response(self) -> Result<BatchGetItemResponse, StorageError> {
        let responses = self
            .responses
            .map(|table_items| {
                let mut decoded_tables = HashMap::with_capacity(table_items.len());
                for (table, items) in table_items {
                    let mut decoded_items = Vec::with_capacity(items.len());
                    for item in items {
                        decoded_items.push(item.into_attribute_map()?.into());
                    }
                    decoded_tables.insert(table, decoded_items);
                }
                Ok::<HashMap<TableName, Vec<AttributeMap>>, StorageError>(decoded_tables)
            })
            .transpose()?;

        Ok(BatchGetItemResponse {
            responses,
            unprocessed_keys: self.unprocessed_keys,
            consumed_capacity: self.consumed_capacity,
        })
    }

    pub fn into_http_response(self) -> AxumResponse {
        match self.into_json_bytes() {
            Ok(bytes) => json_response_bytes(bytes),
            Err(error) => serialization_error_response("batch get item", error),
        }
    }

    pub(crate) fn into_json_bytes(self) -> serde_json::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.json_capacity_hint());
        out.push(b'{');
        let mut first = true;

        if let Some(responses) = self.responses {
            write_field_name(&mut out, &mut first, "Responses")?;
            out.push(b'{');
            let mut first_table = true;
            for (table_name, items) in responses {
                write_field_name(&mut out, &mut first_table, table_name.as_ref())?;
                write_wire_item_array(&mut out, items)?;
            }
            out.push(b'}');
        }

        if let Some(unprocessed_keys) = self.unprocessed_keys {
            write_field_name(&mut out, &mut first, "UnprocessedKeys")?;
            serde_json::to_writer(&mut out, &unprocessed_keys)?;
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
        if let Some(responses) = &self.responses {
            capacity += responses
                .iter()
                .map(|(table_name, items)| {
                    table_name.as_ref().len() + wire_item_array_json_capacity(items)
                })
                .sum::<usize>();
        }
        if self.unprocessed_keys.is_some() {
            capacity += 128;
        }
        if self.consumed_capacity.is_some() {
            capacity += 128;
        }
        capacity
    }
}

impl From<BatchGetWireItemResponse> for BatchGetWireResponse {
    fn from(response: BatchGetWireItemResponse) -> Self {
        Self {
            responses: response.responses,
            unprocessed_keys: response.unprocessed_keys,
            consumed_capacity: response.consumed_capacity,
        }
    }
}
