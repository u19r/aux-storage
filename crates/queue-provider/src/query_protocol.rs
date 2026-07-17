use std::collections::BTreeMap;

use serde_json::{Value, json};

pub fn query_fields_to_json(fields: impl IntoIterator<Item = (String, String)>) -> Value {
    let mut payload = serde_json::Map::new();
    let mut attributes = BTreeMap::<usize, (Option<String>, Option<String>)>::new();
    let mut attribute_names = BTreeMap::new();
    let mut message_attribute_names = BTreeMap::new();
    let mut message_attributes = BTreeMap::<usize, serde_json::Map<String, Value>>::new();
    let mut send_entries = BTreeMap::<usize, serde_json::Map<String, Value>>::new();
    let mut delete_entries = BTreeMap::<usize, serde_json::Map<String, Value>>::new();
    let mut visibility_entries = BTreeMap::<usize, serde_json::Map<String, Value>>::new();

    for (key, value) in fields {
        if key == "Action" || key == "Version" {
            continue;
        }
        if let Some((index, field)) = indexed_field(&key, "Attribute") {
            let entry = attributes.entry(index).or_default();
            match field {
                "Name" | "key" => entry.0 = Some(value),
                "Value" | "value" => entry.1 = Some(value),
                _ => {
                    payload.insert(key, Value::String(value));
                }
            }
            continue;
        }
        if let Some(index) = indexed_member(&key, "AttributeName") {
            attribute_names.insert(index, Value::String(value));
            continue;
        }
        if let Some(index) = indexed_member(&key, "MessageAttributeName") {
            message_attribute_names.insert(index, Value::String(value));
            continue;
        }
        if collect_message_attribute(&mut message_attributes, &key, &value) {
            continue;
        }
        if collect_batch_entry(
            &mut send_entries,
            &key,
            &value,
            "SendMessageBatchRequestEntry",
        ) || collect_batch_entry(
            &mut delete_entries,
            &key,
            &value,
            "DeleteMessageBatchRequestEntry",
        ) || collect_batch_entry(
            &mut visibility_entries,
            &key,
            &value,
            "ChangeMessageVisibilityBatchRequestEntry",
        ) {
            continue;
        }
        payload.insert(key.clone(), query_scalar(&key, value));
    }

    let attributes = attributes
        .into_values()
        .filter_map(|(name, value)| name.zip(value))
        .map(|(name, value)| (name, Value::String(value)))
        .collect::<serde_json::Map<_, _>>();
    if !attributes.is_empty() {
        payload.insert("Attributes".to_string(), Value::Object(attributes));
    }
    if !attribute_names.is_empty() {
        payload.insert(
            "AttributeNames".to_string(),
            Value::Array(attribute_names.into_values().collect()),
        );
    }
    if !message_attribute_names.is_empty() {
        payload.insert(
            "MessageAttributeNames".to_string(),
            Value::Array(message_attribute_names.into_values().collect()),
        );
    }
    let message_attributes = message_attributes
        .into_values()
        .filter_map(|mut entry| {
            let name = entry.remove("Name")?.as_str()?.to_string();
            Some((name, Value::Object(entry)))
        })
        .collect::<serde_json::Map<_, _>>();
    if !message_attributes.is_empty() {
        payload.insert(
            "MessageAttributes".to_string(),
            Value::Object(message_attributes),
        );
    }
    let entries = if !send_entries.is_empty() {
        send_entries
    } else if !delete_entries.is_empty() {
        delete_entries
    } else {
        visibility_entries
    };
    if !entries.is_empty() {
        payload.insert(
            "Entries".to_string(),
            Value::Array(entries.into_values().map(Value::Object).collect()),
        );
    }
    Value::Object(payload)
}

fn indexed_field<'a>(key: &'a str, prefix: &str) -> Option<(usize, &'a str)> {
    let rest = key.strip_prefix(prefix)?.strip_prefix('.')?;
    let (index, field) = rest.split_once('.')?;
    let index = index.parse().ok()?;
    (index > 0).then_some((index, field))
}

fn indexed_member(key: &str, prefix: &str) -> Option<usize> {
    let index = key.strip_prefix(prefix)?.strip_prefix('.')?.parse().ok()?;
    (index > 0).then_some(index)
}

fn collect_batch_entry(
    output: &mut BTreeMap<usize, serde_json::Map<String, Value>>,
    key: &str,
    value: &str,
    prefix: &str,
) -> bool {
    let Some((index, field)) = indexed_field(key, prefix) else {
        return false;
    };
    output
        .entry(index)
        .or_default()
        .insert(field.to_string(), query_scalar(field, value.to_string()));
    true
}

fn collect_message_attribute(
    output: &mut BTreeMap<usize, serde_json::Map<String, Value>>,
    key: &str,
    value: &str,
) -> bool {
    let Some((index, field)) = indexed_field(key, "MessageAttribute") else {
        return false;
    };
    let field = field.strip_prefix("Value.").unwrap_or(field);
    if !matches!(field, "Name" | "DataType" | "StringValue" | "BinaryValue") {
        return false;
    }
    output
        .entry(index)
        .or_default()
        .insert(field.to_string(), Value::String(value.to_string()));
    true
}

fn query_scalar(key: &str, value: String) -> Value {
    match key {
        "DelaySeconds" | "MaxNumberOfMessages" | "VisibilityTimeout" | "WaitTimeSeconds" => value
            .parse::<u32>()
            .map_or_else(|_| Value::String(value), |number| json!(number)),
        _ => Value::String(value),
    }
}
