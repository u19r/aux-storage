use std::collections::HashMap;

use storage_types::{AttributeMap, AttributeValue, StorageResult, WireItem};

#[derive(Debug, Clone)]
pub enum PutItemPayload {
    AttributeMap(AttributeMap),
    WireItem(Box<WireItem>),
}

impl From<HashMap<String, AttributeValue>> for PutItemPayload {
    fn from(value: HashMap<String, AttributeValue>) -> Self {
        Self::AttributeMap(value.into())
    }
}

impl From<AttributeMap> for PutItemPayload {
    fn from(value: AttributeMap) -> Self {
        Self::AttributeMap(value)
    }
}

impl From<WireItem> for PutItemPayload {
    fn from(value: WireItem) -> Self {
        Self::WireItem(Box::new(value))
    }
}

impl TryFrom<PutItemPayload> for WireItem {
    type Error = storage_types::StorageError;

    fn try_from(payload: PutItemPayload) -> StorageResult<Self> {
        match payload {
            PutItemPayload::AttributeMap(attributes) => Self::try_from(attributes),
            PutItemPayload::WireItem(item) => Ok(*item),
        }
    }
}
