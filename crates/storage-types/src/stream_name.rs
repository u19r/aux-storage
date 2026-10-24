use std::ops::{Add, Deref};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ItemKey, StreamItemId, StreamKey, TableName,
    constants::{STREAM_ITEM_HASH_PREFIX, STREAM_ITEM_KEY_MAX_BYTES},
    item_key::ItemKeyError,
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamName(pub(crate) Vec<u8>);

impl StreamName {
    // Legacy SQL/KV providers still parse these logical addresses. Shorten
    // them when those providers are deleted at the single-owner cutover.
    pub const TABLE_STREAM_SUFFIX: &'static [u8] = b"/stream-table";
    pub const ITEM_STREAM_SEGMENT: &'static [u8] = b"/stream-item/";

    #[must_use]
    pub fn new(name: &[u8]) -> Self {
        Self(name.to_vec())
    }

    #[must_use]
    pub fn system_table_stream() -> Self {
        // This address changes with the table/item markers at cutover.
        Self(b"system-streams/tables".to_vec())
    }

    #[must_use]
    pub fn table_stream(table_name: &TableName) -> Self {
        let mut name = table_name.sanitized_name().as_bytes().to_vec();
        name.extend_from_slice(Self::TABLE_STREAM_SUFFIX);
        Self(name)
    }

    /// The logical item address has one table-name segment before the item
    /// marker.
    pub fn item_table_name(&self) -> Option<TableName> {
        let separator = self.0.iter().position(|byte| *byte == b'/')?;
        if !self.0[separator..].starts_with(Self::ITEM_STREAM_SEGMENT) {
            return None;
        }
        std::str::from_utf8(&self.0[..separator])
            .ok()
            .map(TableName::new)
    }

    pub fn table_item_stream(
        table_name: &TableName,
        item_key: &ItemKey,
    ) -> Result<Self, ItemKeyError> {
        let mut name = table_name.sanitized_name().as_bytes().to_vec();
        name.extend_from_slice(Self::ITEM_STREAM_SEGMENT);
        let key_part = item_key.hash_range_key_part()?;
        if key_part.len() > STREAM_ITEM_KEY_MAX_BYTES {
            let digest = Uuid::new_v5(&Uuid::NAMESPACE_OID, &key_part)
                .as_hyphenated()
                .to_string();
            name.extend(STREAM_ITEM_HASH_PREFIX);
            name.extend(digest.as_bytes());
        } else {
            name.extend(key_part);
        }
        Ok(Self(name))
    }
}

impl Deref for StreamName {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for StreamName {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<String> for StreamName {
    fn from(value: String) -> Self {
        StreamName(value.into_bytes())
    }
}

impl From<&str> for StreamName {
    fn from(value: &str) -> Self {
        StreamName(value.bytes().collect())
    }
}

impl From<&[u8]> for StreamName {
    fn from(value: &[u8]) -> Self {
        StreamName(value.to_vec())
    }
}
impl From<Vec<u8>> for StreamName {
    fn from(value: Vec<u8>) -> Self {
        StreamName(value)
    }
}

impl From<StreamName> for Vec<u8> {
    fn from(value: StreamName) -> Self {
        value.0
    }
}

impl From<&StreamName> for Vec<u8> {
    fn from(value: &StreamName) -> Self {
        value.0.clone()
    }
}
impl From<StreamName> for String {
    fn from(value: StreamName) -> Self {
        String::from_utf8_lossy(&value.0).to_string()
    }
}

impl From<&StreamName> for String {
    fn from(value: &StreamName) -> Self {
        String::from_utf8_lossy(&value.0).to_string()
    }
}

impl Add<&StreamItemId> for &StreamName {
    type Output = StreamKey;

    fn add(self, rhs: &StreamItemId) -> Self::Output {
        let mut v = Vec::with_capacity(self.0.len() + 1 + rhs.as_bytes().len());
        v.extend_from_slice(&self.0);
        v.extend(b"/");
        v.extend(rhs.as_bytes());
        v.into()
    }
}

impl std::fmt::Debug for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamName({})", String::from_utf8_lossy(&self.0))
    }
}
