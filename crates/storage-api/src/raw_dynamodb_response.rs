use std::{borrow::Cow, io::Write as _};

use axum::{
    body::Body,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Json, Response as AxumResponse},
};
use http_error::ErrorResponse;
use serde::de::{IgnoredAny, MapAccess, Visitor};
use storage_types::{AttributeValue, StorageError, WireItem, WireItemKeyAttributes};

pub(crate) type AttributeMap = std::collections::HashMap<String, AttributeValue>;

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

pub(crate) fn write_wire_item_array(
    out: &mut Vec<u8>,
    items: Vec<WireItem>,
) -> serde_json::Result<()> {
    out.push(b'[');
    for (idx, item) in items.into_iter().enumerate() {
        if idx > 0 {
            out.push(b',');
        }
        write_wire_item(out, item)?;
    }
    out.push(b']');
    Ok(())
}

pub(crate) fn wire_item_array_json_capacity(items: &[WireItem]) -> usize {
    let separators = items.len().saturating_sub(1);
    2 + separators + items.iter().map(WireItem::payload_len).sum::<usize>()
}

pub(crate) fn wire_item_json_len_upper_bound(item: &WireItem) -> serde_json::Result<usize> {
    match item {
        WireItem::DynamoJson { data } => Ok(data.len()),
        WireItem::LocalSplit {
            primary_key,
            secondary_key,
            non_key_attributes_blob,
        } => local_split_wire_item_json_len_upper_bound(
            primary_key,
            secondary_key.as_ref(),
            non_key_attributes_blob.as_deref(),
        ),
    }
}

pub(crate) fn write_wire_item(out: &mut Vec<u8>, item: WireItem) -> serde_json::Result<()> {
    write_wire_item_ref(out, &item)
}

fn write_wire_item_ref(out: &mut Vec<u8>, item: &WireItem) -> serde_json::Result<()> {
    match item {
        WireItem::DynamoJson { data } => out
            .write_all(data.as_slice())
            .map_err(serde_json::Error::io),
        WireItem::LocalSplit {
            primary_key,
            secondary_key,
            non_key_attributes_blob,
        } => write_local_split_wire_item(
            out,
            primary_key,
            secondary_key.as_ref(),
            non_key_attributes_blob.as_deref(),
        ),
    }
}

pub(crate) fn storage_error_to_json_error(error: StorageError) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::other(error.to_string()))
}

fn write_local_split_wire_item(
    out: &mut Vec<u8>,
    primary_key: &WireItemKeyAttributes,
    secondary_key: Option<&WireItemKeyAttributes>,
    non_key_attributes_blob: Option<&[u8]>,
) -> serde_json::Result<()> {
    if can_compose_local_split(primary_key, secondary_key, non_key_attributes_blob)? {
        return write_composed_local_split(
            out,
            primary_key,
            secondary_key,
            non_key_attributes_blob,
        );
    }

    let map: AttributeMap = WireItem::LocalSplit {
        primary_key: primary_key.clone(),
        secondary_key: secondary_key.cloned(),
        non_key_attributes_blob: non_key_attributes_blob.map(Vec::from),
    }
    .into_attribute_map()
    .map_err(storage_error_to_json_error)?;
    serde_json::to_writer(out, &map)
}

fn local_split_wire_item_json_len_upper_bound(
    primary_key: &WireItemKeyAttributes,
    secondary_key: Option<&WireItemKeyAttributes>,
    non_key_attributes_blob: Option<&[u8]>,
) -> serde_json::Result<usize> {
    let Some(blob) = non_key_attributes_blob else {
        return composed_local_split_json_len(primary_key, secondary_key, None);
    };
    let trimmed = trim_ascii_whitespace(blob);
    if trimmed.is_empty() || trimmed == b"{}" {
        return composed_local_split_json_len(primary_key, secondary_key, None);
    }
    if trimmed.starts_with(b"{") && trimmed.ends_with(b"}") {
        return composed_local_split_json_len(primary_key, secondary_key, Some(trimmed));
    }

    let map: AttributeMap = WireItem::LocalSplit {
        primary_key: primary_key.clone(),
        secondary_key: secondary_key.cloned(),
        non_key_attributes_blob: non_key_attributes_blob.map(Vec::from),
    }
    .into_attribute_map()
    .map_err(storage_error_to_json_error)?;
    Ok(serde_json::to_vec(&map)?.len())
}

fn can_compose_local_split(
    primary_key: &WireItemKeyAttributes,
    secondary_key: Option<&WireItemKeyAttributes>,
    non_key_attributes_blob: Option<&[u8]>,
) -> serde_json::Result<bool> {
    if let Some(secondary_key) = secondary_key
        && key_attributes_overlap(primary_key, secondary_key)
    {
        return Ok(false);
    }

    let Some(blob) = non_key_attributes_blob else {
        return Ok(true);
    };
    let trimmed = trim_ascii_whitespace(blob);
    if trimmed.is_empty() || trimmed == b"{}" {
        return Ok(true);
    }
    if !trimmed.starts_with(b"{") || !trimmed.ends_with(b"}") {
        return Ok(false);
    }

    object_keys_are_disjoint_from_local_split_keys(trimmed, primary_key, secondary_key)
}

fn composed_local_split_json_len(
    primary_key: &WireItemKeyAttributes,
    secondary_key: Option<&WireItemKeyAttributes>,
    non_key_attributes_blob: Option<&[u8]>,
) -> serde_json::Result<usize> {
    let mut len = 2;
    let mut first = true;
    add_key_attributes_json_len(&mut len, &mut first, primary_key)?;
    if let Some(secondary_key) = secondary_key {
        add_key_attributes_json_len(&mut len, &mut first, secondary_key)?;
    }
    if let Some(blob) = non_key_attributes_blob {
        let trimmed = trim_ascii_whitespace(blob);
        if trimmed.len() > 2 {
            let body = trim_ascii_whitespace(&trimmed[1..trimmed.len() - 1]);
            if !body.is_empty() {
                len += usize::from(!first) + body.len();
            }
        }
    }
    Ok(len)
}

fn add_key_attributes_json_len(
    len: &mut usize,
    first: &mut bool,
    key_attributes: &WireItemKeyAttributes,
) -> serde_json::Result<()> {
    add_key_attribute_json_len(
        len,
        first,
        key_attributes.hash_key_name.as_ref(),
        &key_attributes.hash_key,
    )?;
    if let (Some(name), Some(value)) = (
        key_attributes.sort_key_name.as_ref(),
        key_attributes.sort_key.as_ref(),
    ) {
        add_key_attribute_json_len(len, first, name.as_ref(), value)?;
    }
    Ok(())
}

fn add_key_attribute_json_len(
    len: &mut usize,
    first: &mut bool,
    name: &str,
    value: &AttributeValue,
) -> serde_json::Result<()> {
    *len += usize::from(!*first);
    *first = false;
    *len += serde_json::to_vec(name)?.len() + 1 + serde_json::to_vec(value)?.len();
    Ok(())
}

fn write_composed_local_split(
    out: &mut Vec<u8>,
    primary_key: &WireItemKeyAttributes,
    secondary_key: Option<&WireItemKeyAttributes>,
    non_key_attributes_blob: Option<&[u8]>,
) -> serde_json::Result<()> {
    out.push(b'{');
    let mut first = true;
    write_key_attributes(out, &mut first, primary_key)?;
    if let Some(secondary_key) = secondary_key {
        write_key_attributes(out, &mut first, secondary_key)?;
    }
    if let Some(blob) = non_key_attributes_blob {
        let trimmed = trim_ascii_whitespace(blob);
        if trimmed.len() > 2 {
            let body = trim_ascii_whitespace(&trimmed[1..trimmed.len() - 1]);
            if !body.is_empty() {
                if !first {
                    out.push(b',');
                }
                out.write_all(body).map_err(serde_json::Error::io)?;
            }
        }
    }
    out.push(b'}');
    Ok(())
}

fn write_key_attributes(
    out: &mut Vec<u8>,
    first: &mut bool,
    key_attributes: &WireItemKeyAttributes,
) -> serde_json::Result<()> {
    write_key_attribute(
        out,
        first,
        key_attributes.hash_key_name.as_ref(),
        &key_attributes.hash_key,
    )?;
    if let (Some(name), Some(value)) = (
        key_attributes.sort_key_name.as_ref(),
        key_attributes.sort_key.as_ref(),
    ) {
        write_key_attribute(out, first, name.as_ref(), value)?;
    }
    Ok(())
}

fn write_key_attribute(
    out: &mut Vec<u8>,
    first: &mut bool,
    name: &str,
    value: &AttributeValue,
) -> serde_json::Result<()> {
    write_field_name(out, first, name)?;
    serde_json::to_writer(out, value)
}

fn object_keys_are_disjoint_from_local_split_keys(
    raw_object: &[u8],
    primary_key: &WireItemKeyAttributes,
    secondary_key: Option<&WireItemKeyAttributes>,
) -> serde_json::Result<bool> {
    let mut deserializer = serde_json::Deserializer::from_slice(raw_object);
    let disjoint = serde::Deserializer::deserialize_map(
        &mut deserializer,
        LocalSplitKeyDisjointVisitor {
            primary_key,
            secondary_key,
        },
    )?;
    deserializer.end()?;
    Ok(disjoint)
}

struct LocalSplitKeyDisjointVisitor<'a> {
    primary_key: &'a WireItemKeyAttributes,
    secondary_key: Option<&'a WireItemKeyAttributes>,
}

impl<'de> Visitor<'de> for LocalSplitKeyDisjointVisitor<'_> {
    type Value = bool;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a DynamoDB item attribute object")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut disjoint = true;
        while let Some(key) = access.next_key::<Cow<'de, str>>()? {
            if local_split_key_name_matches(self.primary_key, key.as_ref())
                || self
                    .secondary_key
                    .is_some_and(|secondary| local_split_key_name_matches(secondary, key.as_ref()))
            {
                disjoint = false;
            }
            access.next_value::<IgnoredAny>()?;
        }
        Ok(disjoint)
    }
}

fn key_attributes_overlap(
    primary_key: &WireItemKeyAttributes,
    secondary_key: &WireItemKeyAttributes,
) -> bool {
    local_split_key_name_matches(secondary_key, primary_key.hash_key_name.as_ref())
        || primary_key
            .sort_key_name
            .as_ref()
            .is_some_and(|name| local_split_key_name_matches(secondary_key, name.as_ref()))
}

fn local_split_key_name_matches(key_attributes: &WireItemKeyAttributes, name: &str) -> bool {
    key_attributes.hash_key_name == name
        || key_attributes
            .sort_key_name
            .as_ref()
            .is_some_and(|sort_key_name| sort_key_name == name)
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}
