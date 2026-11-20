use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{ItemKeyError, SerializesToKey};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamKey(Vec<u8>);

impl StreamKey {
    #[must_use]
    pub fn new(name: &[u8]) -> Self {
        Self(name.to_vec())
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

impl From<Vec<u8>> for StreamKey {
    fn from(value: Vec<u8>) -> Self {
        StreamKey(value)
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
