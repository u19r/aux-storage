use std::collections::HashMap;

use storage_types::{AttributeValue, StorageResult, TableNamespace, TryFromWireItem, WireItem};

use crate::{
    namespace_routing::NamespaceRequestRewriter,
    updated_at_apply::{
        refresh_existing_item_map_timestamp_now, refresh_existing_wire_item_timestamp_now,
        stamp_item_map_now, stamp_wire_item_now,
    },
};

#[derive(Debug, Clone)]
pub enum PutItemPayload {
    AttributeMap(HashMap<String, AttributeValue>),
    WireItem(Box<WireItem>),
}

impl PutItemPayload {
    pub fn into_attribute_map(self) -> StorageResult<HashMap<String, AttributeValue>> {
        match self {
            Self::AttributeMap(map) => Ok(map),
            Self::WireItem(item) => (*item).into_attribute_map(),
        }
    }
}

impl From<HashMap<String, AttributeValue>> for PutItemPayload {
    fn from(value: HashMap<String, AttributeValue>) -> Self {
        Self::AttributeMap(value)
    }
}

impl From<WireItem> for PutItemPayload {
    fn from(value: WireItem) -> Self {
        Self::WireItem(Box::new(value))
    }
}

pub(crate) fn stamp_updated_at_on_put_payload(item: &mut PutItemPayload) -> StorageResult<()> {
    match item {
        PutItemPayload::AttributeMap(map) => stamp_item_map_now(map),
        PutItemPayload::WireItem(wire_item) => stamp_wire_item_now(wire_item.as_mut()),
    }
}

pub(crate) fn refresh_existing_updated_at_on_put_payload(
    item: &mut PutItemPayload,
) -> StorageResult<()> {
    match item {
        PutItemPayload::AttributeMap(map) => refresh_existing_item_map_timestamp_now(map),
        PutItemPayload::WireItem(wire_item) => {
            refresh_existing_wire_item_timestamp_now(wire_item.as_mut())
        }
    }
}

pub(crate) fn decode_wire_items_to_maps(
    items: Vec<WireItem>,
) -> StorageResult<Vec<HashMap<String, AttributeValue>>> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(item.into_attribute_map()?);
    }
    Ok(out)
}

pub(crate) fn decode_wire_items_to_decoded<T>(items: Vec<WireItem>) -> StorageResult<Vec<T>>
where T: TryFromWireItem {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(T::try_from_wire_item(&item)?);
    }
    Ok(out)
}

pub(crate) fn normalize_wire_items_for_shared_table(
    rewriter: &NamespaceRequestRewriter,
    namespace: &TableNamespace,
    items: &mut [WireItem],
) -> StorageResult<()> {
    for item in items {
        rewriter.normalize_wire_item_from_shared_table(namespace, item)?;
    }
    Ok(())
}
