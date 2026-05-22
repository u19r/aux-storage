use std::collections::HashMap;

use http_error::HttpApiError;

use crate::{
    ReceiptHandle,
    types::{
        ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry,
        MessageAttributeValue, SendMessageBatchRequestEntry,
    },
};

type JsonMap = serde_json::Map<String, serde_json::Value>;

pub(crate) struct RequestDecoder {
    fields: JsonMap,
    allowed: &'static [&'static str],
}

impl RequestDecoder {
    pub(crate) fn new(
        value: serde_json::Value,
        allowed: &'static [&'static str],
    ) -> Result<Self, HttpApiError> {
        let fields = match value {
            serde_json::Value::Object(fields) => fields,
            _ => {
                return Err(HttpApiError::validation_error(
                    "Request body must be a JSON object",
                ));
            }
        };
        if let Some(key) = fields.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(HttpApiError::validation_error(format!(
                "Unknown field: {key}"
            )));
        }
        Ok(Self { fields, allowed })
    }

    pub(crate) fn finish(self) -> Result<(), HttpApiError> {
        if let Some(key) = self
            .fields
            .keys()
            .find(|key| !self.allowed.contains(&key.as_str()))
        {
            return Err(HttpApiError::validation_error(format!(
                "Unknown field: {key}"
            )));
        }
        Ok(())
    }

    pub(crate) fn required_string(&mut self, field: &'static str) -> Result<String, HttpApiError> {
        match self.fields.remove(field) {
            Some(serde_json::Value::String(value)) => Ok(value),
            Some(_) => Err(wrong_type(field, "string")),
            None => Err(missing_field(field)),
        }
    }

    pub(crate) fn optional_string(
        &mut self,
        field: &'static str,
    ) -> Result<Option<String>, HttpApiError> {
        match self.fields.remove(field) {
            Some(serde_json::Value::String(value)) => Ok(Some(value)),
            Some(serde_json::Value::Null) | None => Ok(None),
            Some(_) => Err(wrong_type(field, "string")),
        }
    }

    pub(crate) fn required_u32(&mut self, field: &'static str) -> Result<u32, HttpApiError> {
        match self.fields.remove(field) {
            Some(value) => json_u32(field, value)?.ok_or_else(|| missing_field(field)),
            None => Err(missing_field(field)),
        }
    }

    pub(crate) fn optional_u32(
        &mut self,
        field: &'static str,
    ) -> Result<Option<u32>, HttpApiError> {
        match self.fields.remove(field) {
            Some(value) => json_u32(field, value),
            None => Ok(None),
        }
    }

    pub(crate) fn required_string_map(
        &mut self,
        field: &'static str,
    ) -> Result<HashMap<String, String>, HttpApiError> {
        match self.optional_string_map(field)? {
            Some(value) => Ok(value),
            None => Err(missing_field(field)),
        }
    }

    pub(crate) fn optional_string_map(
        &mut self,
        field: &'static str,
    ) -> Result<Option<HashMap<String, String>>, HttpApiError> {
        let Some(value) = self.fields.remove(field) else {
            return Ok(None);
        };
        let serde_json::Value::Object(map) = value else {
            return Err(wrong_type(field, "object"));
        };
        let mut output = HashMap::with_capacity(map.len());
        for (key, value) in map {
            match value {
                serde_json::Value::String(value) => {
                    output.insert(key, value);
                }
                _ => return Err(wrong_type(&format!("{field}.{key}"), "string")),
            }
        }
        Ok(Some(output))
    }

    pub(crate) fn optional_string_list(
        &mut self,
        field: &'static str,
    ) -> Result<Option<Vec<String>>, HttpApiError> {
        let Some(value) = self.fields.remove(field) else {
            return Ok(None);
        };
        let serde_json::Value::Array(values) = value else {
            return Err(wrong_type(field, "array"));
        };
        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| match value {
                serde_json::Value::String(value) => Ok(value),
                _ => Err(wrong_type(&format!("{field}[{index}]"), "string")),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(crate) fn required_array<T>(
        &mut self,
        field: &'static str,
        decode: fn(serde_json::Value, String) -> Result<T, HttpApiError>,
    ) -> Result<Vec<T>, HttpApiError> {
        let value = self
            .fields
            .remove(field)
            .ok_or_else(|| missing_field(field))?;
        let serde_json::Value::Array(values) = value else {
            return Err(wrong_type(field, "array"));
        };
        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| decode(value, format!("{field}[{index}]")))
            .collect()
    }

    pub(crate) fn optional_message_attributes(
        &mut self,
        field: &'static str,
    ) -> Result<Option<HashMap<String, MessageAttributeValue>>, HttpApiError> {
        let Some(value) = self.fields.remove(field) else {
            return Ok(None);
        };
        let serde_json::Value::Object(map) = value else {
            return Err(wrong_type(field, "object"));
        };
        let mut output = HashMap::with_capacity(map.len());
        for (name, value) in map {
            output.insert(
                name.clone(),
                decode_message_attribute_value(value, format!("{field}.{name}"))?,
            );
        }
        Ok(Some(output))
    }
}

pub(crate) fn decode_send_message_batch_entry(
    value: serde_json::Value,
    path: String,
) -> Result<SendMessageBatchRequestEntry, HttpApiError> {
    let mut decoder = NestedDecoder::new(
        value,
        path,
        &["Id", "MessageBody", "DelaySeconds", "MessageAttributes"],
    )?;
    let entry = SendMessageBatchRequestEntry {
        id: decoder.required_string("Id")?,
        message_body: decoder.required_string("MessageBody")?,
        delay_seconds: decoder.optional_u32("DelaySeconds")?,
        message_attributes: decoder.optional_message_attributes("MessageAttributes")?,
    };
    decoder.finish()?;
    Ok(entry)
}

pub(crate) fn decode_delete_message_batch_entry(
    value: serde_json::Value,
    path: String,
) -> Result<DeleteMessageBatchRequestEntry, HttpApiError> {
    let mut decoder = NestedDecoder::new(value, path, &["Id", "ReceiptHandle"])?;
    let entry = DeleteMessageBatchRequestEntry {
        id: decoder.required_string("Id")?,
        receipt_handle: ReceiptHandle::from(decoder.required_string("ReceiptHandle")?.as_str()),
    };
    decoder.finish()?;
    Ok(entry)
}

pub(crate) fn decode_change_message_visibility_batch_entry(
    value: serde_json::Value,
    path: String,
) -> Result<ChangeMessageVisibilityBatchRequestEntry, HttpApiError> {
    let mut decoder =
        NestedDecoder::new(value, path, &["Id", "ReceiptHandle", "VisibilityTimeout"])?;
    let entry = ChangeMessageVisibilityBatchRequestEntry {
        id: decoder.required_string("Id")?,
        receipt_handle: ReceiptHandle::from(decoder.required_string("ReceiptHandle")?.as_str()),
        visibility_timeout: decoder.required_u32("VisibilityTimeout")?,
    };
    decoder.finish()?;
    Ok(entry)
}

fn decode_message_attribute_value(
    value: serde_json::Value,
    path: String,
) -> Result<MessageAttributeValue, HttpApiError> {
    let mut decoder = NestedDecoder::new(value, path, &["StringValue", "BinaryValue", "DataType"])?;
    let attribute = MessageAttributeValue {
        string_value: decoder.optional_string("StringValue")?,
        binary_value: decoder.optional_string("BinaryValue")?,
        data_type: decoder.required_string("DataType")?,
    };
    decoder.finish()?;
    Ok(attribute)
}

struct NestedDecoder {
    path: String,
    fields: JsonMap,
    allowed: &'static [&'static str],
}

impl NestedDecoder {
    fn new(
        value: serde_json::Value,
        path: String,
        allowed: &'static [&'static str],
    ) -> Result<Self, HttpApiError> {
        let fields = match value {
            serde_json::Value::Object(fields) => fields,
            _ => return Err(wrong_type(&path, "object")),
        };
        if let Some(key) = fields.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(HttpApiError::validation_error(format!(
                "Unknown field: {path}.{key}"
            )));
        }
        Ok(Self {
            path,
            fields,
            allowed,
        })
    }

    fn finish(self) -> Result<(), HttpApiError> {
        if let Some(key) = self
            .fields
            .keys()
            .find(|key| !self.allowed.contains(&key.as_str()))
        {
            return Err(HttpApiError::validation_error(format!(
                "Unknown field: {}.{key}",
                self.path
            )));
        }
        Ok(())
    }

    fn field_path(&self, field: &str) -> String {
        format!("{}.{}", self.path, field)
    }

    fn required_string(&mut self, field: &'static str) -> Result<String, HttpApiError> {
        match self.fields.remove(field) {
            Some(serde_json::Value::String(value)) => Ok(value),
            Some(_) => Err(wrong_type(&self.field_path(field), "string")),
            None => Err(missing_field(&self.field_path(field))),
        }
    }

    fn optional_string(&mut self, field: &'static str) -> Result<Option<String>, HttpApiError> {
        match self.fields.remove(field) {
            Some(serde_json::Value::String(value)) => Ok(Some(value)),
            Some(serde_json::Value::Null) | None => Ok(None),
            Some(_) => Err(wrong_type(&self.field_path(field), "string")),
        }
    }

    fn required_u32(&mut self, field: &'static str) -> Result<u32, HttpApiError> {
        match self.fields.remove(field) {
            Some(value) => json_u32(&self.field_path(field), value)?
                .ok_or_else(|| missing_field(&self.field_path(field))),
            None => Err(missing_field(&self.field_path(field))),
        }
    }

    fn optional_u32(&mut self, field: &'static str) -> Result<Option<u32>, HttpApiError> {
        match self.fields.remove(field) {
            Some(value) => json_u32(&self.field_path(field), value),
            None => Ok(None),
        }
    }

    fn optional_message_attributes(
        &mut self,
        field: &'static str,
    ) -> Result<Option<HashMap<String, MessageAttributeValue>>, HttpApiError> {
        let Some(value) = self.fields.remove(field) else {
            return Ok(None);
        };
        let path = self.field_path(field);
        let serde_json::Value::Object(map) = value else {
            return Err(wrong_type(&path, "object"));
        };
        let mut output = HashMap::with_capacity(map.len());
        for (name, value) in map {
            output.insert(
                name.clone(),
                decode_message_attribute_value(value, format!("{path}.{name}"))?,
            );
        }
        Ok(Some(output))
    }
}

fn json_u32(field: &str, value: serde_json::Value) -> Result<Option<u32>, HttpApiError> {
    match value {
        serde_json::Value::Number(value) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| wrong_type(field, "unsigned 32-bit integer")),
        serde_json::Value::String(value) => value
            .parse::<u32>()
            .map(Some)
            .map_err(|_| wrong_type(field, "unsigned 32-bit integer")),
        serde_json::Value::Null => Ok(None),
        _ => Err(wrong_type(field, "unsigned 32-bit integer")),
    }
}

fn missing_field(field: &str) -> HttpApiError {
    HttpApiError::validation_error(format!("Missing required field: {field}"))
}

fn wrong_type(field: &str, expected: &str) -> HttpApiError {
    HttpApiError::validation_error(format!("{field} must be a {expected}"))
}
