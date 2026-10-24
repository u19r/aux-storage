use std::{
    io::{self, Write},
    num::NonZeroUsize,
    sync::Arc,
};

use replication_types::protocol::{CanonicalBytes, CanonicalDigest};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    bounded_json::{BoundedJsonCounter, BoundedJsonWriter},
    constants::{DURABLE_HEADER_BYTES, DURABLE_RECORD_MAGIC, DURABLE_RECORD_VERSION},
    error::RecordError,
};

/// Bounded envelope for native protected records, not a wire event codec.
/// Deserialization creates observations only; capability types deliberately do
/// not implement Deserialize.
pub struct RecordCodec {
    limit: NonZeroUsize,
}
impl RecordCodec {
    pub fn try_new(limit: NonZeroUsize) -> Result<Self, RecordError> {
        if limit.get() <= DURABLE_HEADER_BYTES {
            return Err(RecordError::TooLarge);
        }
        Ok(Self { limit })
    }
    pub fn encoded_payload_bound(&self, bytes: NonZeroUsize) -> Result<NonZeroUsize, RecordError> {
        let encoded = bytes
            .get()
            .checked_add(DURABLE_HEADER_BYTES)
            .filter(|encoded| *encoded <= self.limit.get())
            .ok_or(RecordError::TooLarge)?;
        NonZeroUsize::new(encoded).ok_or(RecordError::TooLarge)
    }
    /// Count the maintained payload grammar without retaining an encoded copy.
    /// The record envelope remains reserved within the same physical limit.
    pub fn payload_length<T: Serialize>(&self, value: &T) -> Result<NonZeroUsize, RecordError> {
        let mut counter = BoundedJsonCounter::new(self.limit.get() - DURABLE_HEADER_BYTES);
        let encoded = serde_json::to_writer(&mut counter, value);
        counter
            .require_capacity()
            .map_err(|error| Self::map_output_error(error.kind()))?;
        encoded.map_err(Self::map_encoding_error)?;
        NonZeroUsize::new(counter.length()).ok_or(RecordError::Encoding)
    }

    fn map_encoding_error(error: serde_json::Error) -> RecordError {
        error
            .io_error_kind()
            .map_or(RecordError::Encoding, Self::map_output_error)
    }

    fn map_output_error(kind: io::ErrorKind) -> RecordError {
        match kind {
            io::ErrorKind::FileTooLarge | io::ErrorKind::OutOfMemory => {
                RecordError::CapacityRejected
            }
            _ => RecordError::Encoding,
        }
    }

    pub fn encode<T: Serialize>(&self, value: &T) -> Result<CanonicalBytes, RecordError> {
        let mut writer = BoundedJsonWriter::new(self.limit.get());
        writer
            .write_all(&[0; DURABLE_HEADER_BYTES])
            .map_err(|error| Self::map_output_error(error.kind()))?;
        let encoded = serde_json::to_writer(&mut writer, value);
        writer
            .require_capacity()
            .map_err(|error| Self::map_output_error(error.kind()))?;
        encoded.map_err(Self::map_encoding_error)?;
        let mut bytes = Vec::from(writer);
        let digest = CanonicalDigest::of(&bytes[DURABLE_HEADER_BYTES..]);
        bytes[..4].copy_from_slice(&DURABLE_RECORD_MAGIC);
        bytes[4..6].copy_from_slice(&DURABLE_RECORD_VERSION.to_be_bytes());
        bytes[6..DURABLE_HEADER_BYTES].copy_from_slice(digest.as_ref());
        CanonicalBytes::try_new(Arc::from(bytes), self.limit).map_err(|_| RecordError::TooLarge)
    }
    pub fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, RecordError> {
        if bytes.len() > self.limit.get() {
            return Err(RecordError::TooLarge);
        }
        if bytes.len() < DURABLE_HEADER_BYTES || bytes[..4] != DURABLE_RECORD_MAGIC {
            return Err(RecordError::Corrupt);
        }
        if bytes[4..6] != DURABLE_RECORD_VERSION.to_be_bytes() {
            return Err(RecordError::IncompatibleVersion);
        }
        let payload = &bytes[DURABLE_HEADER_BYTES..];
        if bytes[6..DURABLE_HEADER_BYTES] != *CanonicalDigest::of(payload).as_ref() {
            return Err(RecordError::Corrupt);
        }
        serde_json::from_slice(payload).map_err(|_| RecordError::Encoding)
    }
}
