use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{ItemKeyError, SerializesToKey, StreamItemId, StreamName, TableName};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamKey(Vec<u8>);

impl StreamKey {
    #[must_use]
    pub fn new(name: &[u8]) -> Self {
        Self(name.to_vec())
    }

    #[must_use]
    pub fn for_system_stream(stream_item_id: &StreamItemId) -> Self {
        &StreamName::system_table_stream() + stream_item_id
    }

    #[must_use]
    pub fn for_table_stream(table_name: &TableName, stream_item_id: &StreamItemId) -> Self {
        &StreamName::table_stream(table_name) + stream_item_id
    }
}

impl Deref for StreamKey {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for StreamKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for StreamKey {
    fn from(value: &[u8]) -> Self {
        StreamKey(value.to_vec())
    }
}

impl std::fmt::Debug for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamKey({})", String::from_utf8_lossy(&self.0))
    }
}

impl SerializesToKey for StreamKey {
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError> {
        Ok(self.0.clone())
    }
}
