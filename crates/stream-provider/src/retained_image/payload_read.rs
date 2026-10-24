//! Pull-based retained decoding verifies termination before returning EOF.
use std::io::{self, Read};

use storage_types::StorageResult;

use super::{error::invalid_image, payload::RetainedPayload};
use crate::constants::{
    RETAINED_PAYLOAD_DECODE_CHUNK_BYTES, RETAINED_PAYLOAD_HEADER_BYTES, RETAINED_RAW_PAYLOAD_FORMAT,
};

pub(super) enum RetainedPayloadRead<'a> {
    Compressed(CompressedPayloadRead<'a>),
    Raw(std::io::Cursor<&'a [u8]>),
}

pub(super) struct CompressedPayloadRead<'a> {
    compressed: &'a [u8],
    length: u64,
    decoder: flate2::Decompress,
    finished: bool,
}

impl<'a> RetainedPayloadRead<'a> {
    pub(super) fn try_new(bytes: &'a [u8], maximum: usize) -> StorageResult<Self> {
        let length = RetainedPayload::decoded_length(bytes)?;
        if length > maximum || bytes.len() > RetainedPayload::encoded_length_bound(length)? {
            return Err(invalid_image());
        }
        if bytes.first() == Some(&RETAINED_RAW_PAYLOAD_FORMAT) {
            return Ok(Self::Raw(std::io::Cursor::new(RetainedPayload::raw_bytes(
                bytes, length,
            )?)));
        }
        Ok(Self::Compressed(CompressedPayloadRead {
            compressed: bytes
                .get(RETAINED_PAYLOAD_HEADER_BYTES..)
                .ok_or_else(invalid_image)?,
            length: u64::try_from(length).map_err(|_| invalid_image())?,
            decoder: flate2::Decompress::new(true),
            finished: false,
        }))
    }
}

impl CompressedPayloadRead<'_> {
    fn require_termination(&self) -> io::Result<()> {
        if self.decoder.total_out() != self.length
            || self.decoder.total_in()
                != u64::try_from(self.compressed.len()).map_err(|_| invalid_stream())?
        {
            return Err(invalid_stream());
        }
        Ok(())
    }
}

impl Read for CompressedPayloadRead<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.finished {
            return Ok(0);
        }
        loop {
            let before = (self.decoder.total_in(), self.decoder.total_out());
            let offset = usize::try_from(before.0).map_err(|_| invalid_stream())?;
            let input = self.compressed.get(offset..).ok_or_else(invalid_stream)?;
            let count = output.len().min(RETAINED_PAYLOAD_DECODE_CHUNK_BYTES);
            let status = self
                .decoder
                .decompress(input, &mut output[..count], flate2::FlushDecompress::None)
                .map_err(|_| invalid_stream())?;
            let after = (self.decoder.total_in(), self.decoder.total_out());
            if after.1 > self.length {
                return Err(invalid_stream());
            }
            let emitted =
                usize::try_from(after.1.checked_sub(before.1).ok_or_else(invalid_stream)?)
                    .map_err(|_| invalid_stream())?;
            if status == flate2::Status::StreamEnd {
                self.require_termination()?;
                self.finished = true;
            } else if after == before {
                return Err(invalid_stream());
            }
            if emitted != 0 || self.finished {
                return Ok(emitted);
            }
        }
    }
}

impl Read for RetainedPayloadRead<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Compressed(reader) => reader.read(output),
            Self::Raw(reader) => reader.read(output),
        }
    }
}

fn invalid_stream() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid retained compressed stream",
    )
}
