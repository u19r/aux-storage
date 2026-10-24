use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Content identity only. A digest never certifies native existence or custody.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ImmutableImageId([u8; 32]);

impl ImmutableImageId {
    pub fn from_content(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

impl From<[u8; 32]> for ImmutableImageId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for ImmutableImageId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for ImmutableImageId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ImmutableImageId(..)")
    }
}
