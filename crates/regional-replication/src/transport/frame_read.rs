//! Exact decoded length and compressed consumption without a raw JSON buffer.
use std::io::{self, Read};

use flate2::{Decompress, FlushDecompress, Status};

use super::error::{TransportError, TransportResult};

pub(super) struct FrameReader<'a> {
    decoder: Decompress,
    decoded: usize,
    expected: usize,
    packed: &'a [u8],
    finished: bool,
}

impl<'a> FrameReader<'a> {
    pub(super) fn new(packed: &'a [u8], expected: u32) -> Self {
        Self {
            decoder: Decompress::new(true),
            decoded: 0,
            expected: expected as usize,
            packed,
            finished: false,
        }
    }

    pub(super) fn require_complete(&self) -> TransportResult<()> {
        if !self.finished
            || self.decoded != self.expected
            || self.decoder.total_in() != self.packed.len() as u64
        {
            return Err(TransportError::Frame);
        }
        Ok(())
    }
}

impl Read for FrameReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() || self.finished {
            return Ok(0);
        }
        let remaining = self.expected - self.decoded;
        let count = bytes.len().min(remaining.saturating_add(1));
        loop {
            let consumed = self.decoder.total_in();
            let produced = self.decoder.total_out();
            let status = self
                .decoder
                .decompress(
                    &self.packed[consumed as usize..],
                    &mut bytes[..count],
                    FlushDecompress::None,
                )
                .map_err(|_| io::ErrorKind::InvalidData)?;
            let read = (self.decoder.total_out() - produced) as usize;
            if read > remaining {
                return Err(io::ErrorKind::InvalidData.into());
            }
            self.decoded += read;
            self.finished = status == Status::StreamEnd;
            if read > 0 || self.finished {
                return Ok(read);
            }
            if self.decoder.total_in() == consumed {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }
}
