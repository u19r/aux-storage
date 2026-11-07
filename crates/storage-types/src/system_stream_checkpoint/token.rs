use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use utoipa::ToSchema;

use super::{
    CHECKPOINT_TOKEN_PREFIX, SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES, SystemStreamCheckpoint,
    SystemStreamContractError,
};
use crate::TimestampMillis;

const CUTOFF_BYTES: usize = size_of::<i64>();

/// Versioned SystemStream resume token.
///
/// The token is a pointer, not a lookup key: it encodes the per-shard frontier
/// of the last evaluated records plus the visibility cutoff fixed by the first
/// page. Resuming reads no stored receipt, so issuing a token writes nothing
/// and needs no retention job. The token is not secret and not tamper-proof;
/// the provider validates scope, generation and lineage membership against its
/// catalog before reading from the decoded frontier.
#[derive(Clone, PartialEq, Eq, ToSchema)]
#[schema(value_type = String)]
pub struct SystemStreamCheckpointToken(String);

impl fmt::Debug for SystemStreamCheckpointToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemStreamCheckpointToken(redacted)")
    }
}

impl SystemStreamCheckpointToken {
    /// Encode a frontier and its visibility cutoff. Equal inputs produce equal
    /// tokens, so a retried page read returns the same token.
    pub fn encode(
        checkpoint: &SystemStreamCheckpoint,
        initial_visibility_cutoff: TimestampMillis,
    ) -> Result<Self, SystemStreamContractError> {
        let mut payload = checkpoint.encode_state()?;
        payload.extend(initial_visibility_cutoff.timestamp_millis().to_be_bytes());
        let mut encoded =
            String::with_capacity(CHECKPOINT_TOKEN_PREFIX.len() + payload.len().div_ceil(3) * 4);
        encoded.push_str(CHECKPOINT_TOKEN_PREFIX);
        URL_SAFE_NO_PAD.encode_string(payload, &mut encoded);
        Ok(Self(encoded))
    }

    /// Decode the frontier and visibility cutoff carried by the token.
    pub fn decode(
        &self,
    ) -> Result<(SystemStreamCheckpoint, TimestampMillis), SystemStreamContractError> {
        decode_payload(&self.0)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn decode_payload(
    value: &str,
) -> Result<(SystemStreamCheckpoint, TimestampMillis), SystemStreamContractError> {
    if value.is_empty() || value.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES {
        return Err(SystemStreamContractError::MalformedCheckpoint);
    }
    let Some(encoded) = value.strip_prefix(CHECKPOINT_TOKEN_PREFIX) else {
        if value.starts_with('v') {
            return Err(SystemStreamContractError::UnsupportedCheckpointVersion);
        }
        return Err(SystemStreamContractError::MalformedCheckpoint);
    };
    let payload = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| SystemStreamContractError::MalformedCheckpoint)?;
    if URL_SAFE_NO_PAD.encode(&payload) != encoded {
        return Err(SystemStreamContractError::NonCanonicalCheckpoint);
    }
    let split = payload
        .len()
        .checked_sub(CUTOFF_BYTES)
        .ok_or(SystemStreamContractError::MalformedCheckpoint)?;
    let (state, cutoff) = payload.split_at(split);
    let cutoff = <[u8; CUTOFF_BYTES]>::try_from(cutoff)
        .map_err(|_| SystemStreamContractError::MalformedCheckpoint)?;
    Ok((
        SystemStreamCheckpoint::decode_state(state)?,
        TimestampMillis::from_timestamp(i64::from_be_bytes(cutoff)),
    ))
}

impl TryFrom<String> for SystemStreamCheckpointToken {
    type Error = SystemStreamContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        decode_payload(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for SystemStreamCheckpointToken {
    type Error = SystemStreamContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        decode_payload(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl From<SystemStreamCheckpointToken> for String {
    fn from(value: SystemStreamCheckpointToken) -> Self {
        value.0
    }
}

impl Serialize for SystemStreamCheckpointToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SystemStreamCheckpointToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(D::Error::custom)
    }
}
