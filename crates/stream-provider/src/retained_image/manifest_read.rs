//! Exact original and body identities are verified by one streaming read.
use std::io::{self, Cursor, Read};

use infrastructure_provider::record::DurableRecord;
use sha2::{Digest as _, Sha256};
use storage_types::{ImmutableImageId, StorageResult};

use super::{
    error::invalid_image,
    manifest::{ImageSegment, RetainedImageManifest},
    payload_read::RetainedPayloadRead,
};

pub(super) struct RetainedManifestRead<'a> {
    segments: Vec<SegmentRead<'a>>,
    next: usize,
    digest: Sha256,
    expected: &'a RetainedImageManifest,
    length: usize,
    complete: bool,
}

enum SegmentRead<'a> {
    Inline(Cursor<&'a [u8]>),
    Body {
        reader: RetainedPayloadRead<'a>,
        digest: Sha256,
        identity: ImmutableImageId,
    },
}

impl<'a> RetainedManifestRead<'a> {
    pub(super) fn try_new(
        manifest: &'a RetainedImageManifest,
        rows: &'a [DurableRecord],
        slots: &[usize],
        causal: &'a super::causal_hydration::VerifiedCausalBodies,
    ) -> StorageResult<Self> {
        let mut slots = slots.iter();
        let mut segments = Vec::with_capacity(manifest.segments.len());
        for segment in &manifest.segments {
            segments.push(match segment {
                ImageSegment::Inline(bytes) => SegmentRead::Inline(Cursor::new(bytes)),
                ImageSegment::Image { identity, length } => {
                    let row = rows
                        .get(*slots.next().ok_or_else(invalid_image)?)
                        .ok_or_else(invalid_image)?;
                    SegmentRead::Body {
                        reader: RetainedPayloadRead::try_new(&row.bytes, *length)?,
                        digest: Sha256::new(),
                        identity: *identity,
                    }
                }
                ImageSegment::Causal { length, .. } => {
                    let slot = *slots.next().ok_or_else(invalid_image)?;
                    SegmentRead::Inline(Cursor::new(causal.selection(slot, (0, *length))?))
                }
                ImageSegment::CausalSlice { offset, length, .. } => {
                    let slot = *slots.next().ok_or_else(invalid_image)?;
                    SegmentRead::Inline(Cursor::new(causal.selection(slot, (*offset, *length))?))
                }
            });
        }
        if slots.next().is_some() {
            return Err(invalid_image());
        }
        Ok(Self {
            segments,
            next: 0,
            digest: Sha256::new(),
            expected: manifest,
            length: 0,
            complete: false,
        })
    }
    pub(super) fn require_complete(&self) -> StorageResult<()> {
        if !self.complete {
            return Err(invalid_image());
        }
        Ok(())
    }
}

impl Read for RetainedManifestRead<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while let Some(segment) = self.segments.get_mut(self.next) {
            let emitted = segment.read(output)?;
            if emitted == 0 {
                self.next += 1;
                continue;
            }
            self.length = self
                .length
                .checked_add(emitted)
                .ok_or_else(invalid_stream)?;
            if self.length > self.expected.length {
                return Err(invalid_stream());
            }
            self.digest.update(&output[..emitted]);
            return Ok(emitted);
        }
        if self.length != self.expected.length
            || <[u8; 32]>::from(self.digest.clone().finalize()) != self.expected.digest
        {
            return Err(invalid_stream());
        }
        self.complete = true;
        Ok(0)
    }
}

impl Read for SegmentRead<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        match self {
            Self::Inline(reader) => reader.read(output),
            Self::Body {
                reader,
                digest,
                identity,
            } => {
                let emitted = reader.read(output)?;
                digest.update(&output[..emitted]);
                if emitted == 0 && <[u8; 32]>::from(digest.clone().finalize()) != *identity.as_ref()
                {
                    return Err(invalid_stream());
                }
                Ok(emitted)
            }
        }
    }
}

fn invalid_stream() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid retained image identity",
    )
}
