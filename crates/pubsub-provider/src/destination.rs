//! The destination of a delivery (W3S plan 5.2): the unit of the
//! per-destination limit, the breaker and the stored due partition.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_types::{StorageError, StorageResult};

use crate::{SubscriptionArn, SubscriptionProtocol};

/// SQS: the queue ARN. HTTP: lower-case `scheme://host:port`. A subscription
/// confirmation: its subscription, so it never shares a breaker with
/// notifications. The stored text has a kind prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DestinationKey(String);

impl TryFrom<String> for DestinationKey {
    type Error = StorageError;

    fn try_from(value: String) -> StorageResult<Self> {
        value.parse()
    }
}

impl From<DestinationKey> for String {
    fn from(key: DestinationKey) -> Self {
        key.0
    }
}

/// How a destination is delivered to; it selects the retry policy and the
/// slot rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DestinationKind {
    LocalQueue,
    Http,
    Confirmation,
}

const QUEUE_PREFIX: &str = "sqs:";
const HTTP_PREFIX: &str = "http:";
const CONFIRMATION_PREFIX: &str = "confirmation:";

impl DestinationKey {
    #[must_use]
    pub fn of_endpoint(protocol: SubscriptionProtocol, endpoint: &str) -> Self {
        let origin = match protocol {
            SubscriptionProtocol::Queue => return Self(format!("{QUEUE_PREFIX}{endpoint}")),
            SubscriptionProtocol::Http | SubscriptionProtocol::Https => url::Url::parse(endpoint)
                .ok()
                .and_then(|url| {
                    let host = url.host_str()?.to_ascii_lowercase();
                    let port = url.port_or_known_default()?;
                    Some(format!("{}://{host}:{port}", url.scheme()))
                })
                .unwrap_or_else(|| protocol.as_str().to_owned()),
        };
        Self(format!("{HTTP_PREFIX}{origin}"))
    }

    #[must_use]
    pub fn of_confirmation(subscription: &SubscriptionArn) -> Self {
        Self(format!("{CONFIRMATION_PREFIX}{subscription}"))
    }

    /// The first 16 bytes of the SHA-256 of the stored text: the KV key part
    /// and the SQL partition column.
    #[must_use]
    pub fn id(&self) -> DestinationId {
        let digest: [u8; 32] = Sha256::digest(self.0.as_bytes()).into();
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        DestinationId(id)
    }

    #[must_use]
    pub fn kind(&self) -> DestinationKind {
        if self.0.starts_with(QUEUE_PREFIX) {
            DestinationKind::LocalQueue
        } else if self.0.starts_with(CONFIRMATION_PREFIX) {
            DestinationKind::Confirmation
        } else {
            DestinationKind::Http
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DestinationKey {
    type Err = StorageError;

    fn from_str(value: &str) -> StorageResult<Self> {
        if [QUEUE_PREFIX, HTTP_PREFIX, CONFIRMATION_PREFIX]
            .iter()
            .any(|prefix| value.starts_with(prefix))
        {
            return Ok(Self(value.to_owned()));
        }
        Err(StorageError::internal("stored destination has no kind"))
    }
}

impl fmt::Display for DestinationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The fixed-width identity of a destination. Its byte order is the order of
/// the stored partitions and of the sweep cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DestinationId([u8; 16]);

impl DestinationId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Lower-case hexadecimal: the SQL partition column, ordered as the bytes.
impl fmt::Display for DestinationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

impl FromStr for DestinationId {
    type Err = StorageError;

    fn from_str(value: &str) -> StorageResult<Self> {
        let invalid = || StorageError::internal("stored destination id is invalid");
        if value.len() != 32 {
            return Err(invalid());
        }
        let mut id = [0_u8; 16];
        for (index, byte) in id.iter_mut().enumerate() {
            let pair = value.get(index * 2..index * 2 + 2).ok_or_else(invalid)?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
        }
        Ok(Self(id))
    }
}
