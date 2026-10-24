use std::{num::NonZeroUsize, sync::Arc};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::ReplicationProtocolError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProtocolVersion(u16);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SemanticVersion(u32);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerCapabilities {
    pub wire: Vec<ProtocolVersion>,
    pub semantics: Vec<SemanticVersion>,
}
impl PeerCapabilities {
    pub fn require(
        &self,
        wire: ProtocolVersion,
        semantics: SemanticVersion,
    ) -> Result<(), ReplicationProtocolError> {
        if !self.wire.contains(&wire) || !self.semantics.contains(&semantics) {
            return Err(ReplicationProtocolError::ProtocolIncompatible {
                context: "protocol.require",
            });
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadBounds {
    pub event_bytes: NonZeroUsize,
    pub manifest_bytes: NonZeroUsize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CanonicalDigest([u8; 32]);
impl CanonicalDigest {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
    /// Hash the existing JSON spelling without retaining a serialized payload.
    pub fn of_json(value: &impl Serialize) -> Result<Self, serde_json::Error> {
        Self::of_json_bounded(value, NonZeroUsize::MAX)
    }
    /// Enforce the caller's serialized JSON allowance before updating the
    /// digest.
    pub fn of_json_bounded(
        value: &impl Serialize,
        limit: NonZeroUsize,
    ) -> Result<Self, serde_json::Error> {
        let mut writer = JsonDigestWriter {
            digest: Sha256::new(),
            bytes: 0,
            limit: limit.get(),
        };
        serde_json::to_writer(&mut writer, value)?;
        Ok(Self(writer.digest.finalize().into()))
    }
    pub fn verify(self, bytes: &[u8]) -> Result<(), ReplicationProtocolError> {
        if self != Self::of(bytes) {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "protocol.verify",
            });
        }
        Ok(())
    }
}

struct JsonDigestWriter {
    digest: Sha256,
    bytes: usize,
    limit: usize,
}

impl std::io::Write for JsonDigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or(std::io::ErrorKind::FileTooLarge)?;
        self.digest.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl AsRef<[u8]> for CanonicalDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl From<[u8; 32]> for CanonicalDigest {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}
/// Canonical bytes are immutable; Debug omits their content.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalBytes(Arc<[u8]>);
impl CanonicalBytes {
    pub fn try_new(
        bytes: Arc<[u8]>,
        limit: NonZeroUsize,
    ) -> Result<Self, ReplicationProtocolError> {
        validate_payload_bound(bytes.as_ref(), limit)?;
        Ok(Self(bytes))
    }
}
impl From<CanonicalBytes> for Arc<[u8]> {
    fn from(value: CanonicalBytes) -> Self {
        value.0
    }
}
impl AsRef<[u8]> for CanonicalBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Debug for CanonicalBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalBytes")
            .field("byte_count", &self.0.len())
            .finish()
    }
}

impl From<u16> for ProtocolVersion {
    fn from(value: u16) -> Self {
        Self(value)
    }
}
impl From<ProtocolVersion> for u16 {
    fn from(value: ProtocolVersion) -> Self {
        value.0
    }
}
impl From<u32> for SemanticVersion {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<SemanticVersion> for u32 {
    fn from(value: SemanticVersion) -> Self {
        value.0
    }
}
impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::fmt::Display for SemanticVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

fn validate_payload_bound(
    bytes: &[u8],
    limit: NonZeroUsize,
) -> Result<(), ReplicationProtocolError> {
    if bytes.len() > limit.get() {
        return Err(ReplicationProtocolError::PayloadTooLarge {
            context: "protocol.validate_payload_bound",
            actual: bytes.len(),
            limit: limit.get(),
        });
    }
    Ok(())
}
