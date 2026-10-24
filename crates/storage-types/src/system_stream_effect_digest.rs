use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::{StorageError, constants::SYSTEM_STREAM_EFFECT_DIGEST_HEX_BYTES};

/// Lowercase SHA-256 digest of canonical SystemStream effect evidence.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct SystemStreamEffectDigest(String);

impl SystemStreamEffectDigest {
    pub fn try_new(value: impl Into<String>) -> Result<Self, StorageError> {
        let value = value.into();
        if value.len() != SYSTEM_STREAM_EFFECT_DIGEST_HEX_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(StorageError::internal(
                "SystemStream effect digest must be lowercase hexadecimal",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SystemStreamEffectDigest {
    type Error = StorageError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<SystemStreamEffectDigest> for String {
    fn from(value: SystemStreamEffectDigest) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for SystemStreamEffectDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}
