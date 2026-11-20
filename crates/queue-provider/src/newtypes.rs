use std::{fmt, ops::Deref, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::constants::{MESSAGE_ID_LEN, MESSAGE_ID_TEXT_LEN};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId([u8; MESSAGE_ID_LEN]);

impl MessageId {
    #[must_use]
    pub fn from_bytes(bytes: [u8; MESSAGE_ID_LEN]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; MESSAGE_ID_LEN] {
        &self.0
    }

    #[must_use]
    pub fn random() -> Self {
        Self::from_uuid(storage_types::ambient::uuid_now_v7())
    }

    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid.into_bytes())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self([0u8; MESSAGE_ID_LEN])
    }
}

/// A message id text that is not a 36-character hyphenated UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidMessageId;

impl fmt::Display for InvalidMessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("message id is not a hyphenated UUID")
    }
}

impl std::error::Error for InvalidMessageId {}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&Uuid::from_bytes(self.0).hyphenated(), f)
    }
}

impl FromStr for MessageId {
    type Err = InvalidMessageId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Only the hyphenated form has 36 characters; simple (32), braced (38)
        // and URN (45) forms fail here.
        if value.len() != MESSAGE_ID_TEXT_LEN {
            return Err(InvalidMessageId);
        }
        Uuid::try_parse(value)
            .map(|uuid| Self(uuid.into_bytes()))
            .map_err(|_| InvalidMessageId)
    }
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut buffer = Uuid::encode_buffer();
        serializer.serialize_str(
            Uuid::from_bytes(self.0)
                .hyphenated()
                .encode_lower(&mut buffer),
        )
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
    type Target = [u8; MESSAGE_ID_LEN];

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

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ReceiptHandle(pub String);

/// What a receipt handle text decodes to. One classification owns both the
/// refusal decision and the AWS text of a malformed handle.
pub(crate) enum DecodedReceipt {
    /// A handle this service minted: it names the queue it was issued for.
    Minted {
        queue_id: crate::QueueId,
        queue_name: String,
    },
    /// Text of the standard base64 alphabet that is not a handle (probe 4).
    Unrecognised,
    /// Text with a character outside that alphabet (suite scripts).
    NotBase64,
}

impl ReceiptHandle {
    /// A handle for a lease: `<next-visible>:<entropy>:<queue id>:<queue name>`
    /// where the scope is `<queue id>:<queue name>` of the issuing queue.
    #[must_use]
    pub fn new(timestamp: i64, message_id: Uuid, scope: &str) -> Self {
        let key = format!("{timestamp:010}:{message_id}:{scope}");
        ReceiptHandle(URL_SAFE.encode(key))
    }

    pub(crate) fn decode(&self) -> DecodedReceipt {
        let unrecognised = || {
            if self
                .0
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '/' | '='))
            {
                DecodedReceipt::Unrecognised
            } else {
                DecodedReceipt::NotBase64
            }
        };
        let Ok(bytes) = URL_SAFE.decode(&self.0) else {
            return unrecognised();
        };
        let Ok(decoded) = std::str::from_utf8(&bytes) else {
            return unrecognised();
        };
        let mut parts = decoded.splitn(4, ':');
        let (Some(timestamp), Some(message_id), Some(queue_id), Some(queue_name)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return unrecognised();
        };
        match (
            timestamp.parse::<i64>(),
            Uuid::parse_str(message_id),
            crate::QueueId::parse(queue_id),
        ) {
            (Ok(_), Ok(_), Some(queue_id)) if !queue_name.is_empty() => DecodedReceipt::Minted {
                queue_id,
                queue_name: queue_name.to_owned(),
            },
            _ => unrecognised(),
        }
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
