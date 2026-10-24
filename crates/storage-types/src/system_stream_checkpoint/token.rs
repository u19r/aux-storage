use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use utoipa::ToSchema;

use super::{
    CHECKPOINT_TOKEN_PREFIX, SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES,
    SYSTEM_STREAM_CHECKPOINT_TOKEN_ID_BYTES, SystemStreamContractError,
};

/// Opaque, versioned, server-issued SystemStream checkpoint identifier.
///
/// The identifier intentionally carries no scope, lineage, offset, or
/// physical-key material.  A backend must persist the identifier together
/// with the validated [`super::SystemStreamCheckpoint`] in one durable
/// operation and resolve it before reading records.  A syntactically valid but
/// unknown ID is therefore rejected by the backend as
/// [`SystemStreamContractError::UnknownCheckpoint`].
#[derive(Clone, PartialEq, Eq, ToSchema)]
#[schema(value_type = String)]
pub struct SystemStreamCheckpointToken(String);

impl fmt::Debug for SystemStreamCheckpointToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemStreamCheckpointToken(redacted)")
    }
}

impl SystemStreamCheckpointToken {
    /// Verify that a token is present in the backend's issued-token catalog.
    /// The catalog lookup is intentionally supplied by the persistence owner;
    /// this helper never treats token shape as authority.
    pub fn ensure_issued<'a, I>(&self, issued_tokens: I) -> Result<(), SystemStreamContractError>
    where I: IntoIterator<Item = &'a SystemStreamCheckpointToken> {
        if issued_tokens.into_iter().any(|token| token == self) {
            Ok(())
        } else {
            Err(SystemStreamContractError::UnknownCheckpoint)
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SystemStreamCheckpointToken {
    type Error = SystemStreamContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_token_shape(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for SystemStreamCheckpointToken {
    type Error = SystemStreamContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_token_shape(value)?;
        Ok(Self(value.to_owned()))
    }
}

fn validate_token_shape(value: &str) -> Result<(), SystemStreamContractError> {
    if value.is_empty() || value.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES {
        return Err(SystemStreamContractError::MalformedCheckpoint);
    }
    let Some(encoded_id) = value.strip_prefix(CHECKPOINT_TOKEN_PREFIX) else {
        if value.starts_with('v') {
            return Err(SystemStreamContractError::UnsupportedCheckpointVersion);
        }
        return Err(SystemStreamContractError::MalformedCheckpoint);
    };
    if encoded_id.len() != SYSTEM_STREAM_CHECKPOINT_TOKEN_ID_BYTES * 2 {
        return Err(SystemStreamContractError::MalformedCheckpoint);
    }

    let mut uppercase = false;
    for byte in encoded_id.bytes() {
        match byte {
            b'0'..=b'9' | b'a'..=b'f' => {}
            b'A'..=b'F' => uppercase = true,
            _ => return Err(SystemStreamContractError::MalformedCheckpoint),
        }
    }
    if uppercase {
        return Err(SystemStreamContractError::NonCanonicalCheckpoint);
    }
    Ok(())
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
