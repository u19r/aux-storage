use replication_types::protocol::{CanonicalBytes, CanonicalDigest};
use serde::{Deserialize, Serialize};

use super::{
    codec::EncodedFrame,
    error::{TransportError, TransportResult},
    message::WireIdentity,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChunkDeclaration {
    pub kind: u8,
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub content: CanonicalDigest,
    pub raw_bytes: u32,
    pub packed_bytes: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ByteChunk {
    pub content: CanonicalDigest,
    pub offset: u32,
    pub bytes: CanonicalBytes,
}

#[derive(Serialize)]
pub(super) struct OutgoingChunk<'a> {
    pub content: CanonicalDigest,
    pub offset: u32,
    pub bytes: &'a [u8],
}

impl ChunkDeclaration {
    pub(super) fn try_new(
        input: (u8, &EncodedFrame),
        identities: (WireIdentity, WireIdentity),
    ) -> TransportResult<Self> {
        let (kind, frame) = input;
        Self::require_message_kind(kind)?;
        Ok(Self {
            kind,
            from: identities.0,
            to: identities.1,
            content: CanonicalDigest::of(&frame.packed),
            raw_bytes: frame.raw_bytes,
            packed_bytes: u32::try_from(frame.packed.len())
                .map_err(|_| TransportError::FrameTooLarge)?,
        })
    }

    pub(super) fn require_wire_capacity(
        identities: (WireIdentity, WireIdentity),
        limit: usize,
    ) -> TransportResult<()> {
        let declaration = Self {
            kind: u8::MAX,
            from: identities.0,
            to: identities.1,
            content: [u8::MAX; 32].into(),
            raw_bytes: u32::MAX,
            packed_bytes: u32::MAX,
        };
        super::codec::encode(&declaration, limit).map_err(|error| match error {
            TransportError::FrameTooLarge => TransportError::Configuration,
            error => error,
        })?;
        Ok(())
    }

    pub(super) fn require_bound(&self, limit: usize) -> TransportResult<()> {
        Self::require_message_kind(self.kind)?;
        if self.raw_bytes == 0
            || self.packed_bytes == 0
            || self.raw_bytes as usize > limit
            || self.packed_bytes as usize > limit
        {
            return Err(TransportError::Frame);
        }
        Ok(())
    }

    pub(super) fn require_message_kind(kind: u8) -> TransportResult<()> {
        if !matches!(kind, 1..=6 | 9) {
            return Err(TransportError::Frame);
        }
        Ok(())
    }

    pub(super) fn require_request_kind(&self) -> TransportResult<()> {
        Self::require_message_kind(self.kind)?;
        if self.kind == crate::constants::TRANSPORT_REPLY_KIND {
            return Err(TransportError::Frame);
        }
        Ok(())
    }
}
