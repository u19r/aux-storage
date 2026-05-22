use std::{fmt, ops::Deref, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::constants::MESSAGE_ID_VERSIONSTAMP_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId([u8; MESSAGE_ID_VERSIONSTAMP_LEN]);

impl MessageId {
    #[must_use]
    pub fn from_bytes(bytes: [u8; MESSAGE_ID_VERSIONSTAMP_LEN]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; MESSAGE_ID_VERSIONSTAMP_LEN] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    #[must_use]
    pub fn random() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        let bytes = uuid.into_bytes();
        let mut out = [0u8; MESSAGE_ID_VERSIONSTAMP_LEN];
        out.copy_from_slice(&bytes[..MESSAGE_ID_VERSIONSTAMP_LEN]);
        Self(out)
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self([0u8; MESSAGE_ID_VERSIONSTAMP_LEN])
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for MessageId {
    type Err = hex::FromHexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != MESSAGE_ID_VERSIONSTAMP_LEN * 2 {
            return Err(hex::FromHexError::InvalidStringLength);
        }
        let mut bytes = [0u8; MESSAGE_ID_VERSIONSTAMP_LEN];
        hex::decode_to_slice(value, &mut bytes)?;
        Ok(Self(bytes))
    }
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        MessageId::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Deref for MessageId {
    type Target = [u8; MESSAGE_ID_VERSIONSTAMP_LEN];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&MessageId> for String {
    fn from(value: &MessageId) -> Self {
        value.to_string()
    }
}

impl From<String> for MessageId {
    fn from(value: String) -> Self {
        MessageId::from_str(&value).unwrap_or_default()
    }
}

impl From<&str> for MessageId {
    fn from(value: &str) -> Self {
        MessageId::from_str(value).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReceiptHandle(pub String);

impl ReceiptHandle {
    #[must_use]
    pub fn new(timestamp: i64, message_id: Uuid) -> Self {
        let key = format!("{timestamp:010}:{message_id}");
        let b64 = URL_SAFE.encode(key);
        ReceiptHandle(b64)
    }
}

impl From<&str> for ReceiptHandle {
    fn from(value: &str) -> Self {
        ReceiptHandle(value.to_string())
    }
}

impl Deref for ReceiptHandle {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ReceiptHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
