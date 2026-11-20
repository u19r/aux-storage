use std::ops::{Add, Deref};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StreamItemId, StreamKey};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamName(pub(crate) Vec<u8>);

impl StreamName {
    pub fn table_history_id(&self) -> crate::StorageResult<Option<crate::TableHistoryId>> {
        let Some(rest) = self.0.strip_prefix(b"history/") else {
            return Ok(None);
        };
        let invalid = || crate::StorageError::raw_validation("Invalid table history namespace");
        let (identity, suffix) = rest.split_at_checked(32).ok_or_else(invalid)?;
        if !suffix
            .strip_prefix(b"/")
            .is_some_and(|suffix| !suffix.is_empty())
        {
            return Err(invalid());
        }
        let identity_text = std::str::from_utf8(identity).map_err(|_| invalid())?;
        let id = Uuid::parse_str(identity_text).map_err(|_| invalid())?;
        if id.is_nil() || id.simple().to_string().as_bytes() != identity {
            return Err(invalid());
        }
        Ok(Some(id.into()))
    }

    #[must_use]
    pub fn new(name: &[u8]) -> Self {
        Self(name.to_vec())
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
