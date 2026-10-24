//! Typed custody payloads share one bounded and verified native envelope.

use infrastructure_provider::record::{
    DurableRecord, DurableRecordKey, RecordChange, RecordExpectation,
};
use sha2::{Digest as _, Sha256};
use storage_types::{Revision, StorageResult};

use super::{
    custody_read::CustodyReader,
    custody_write::CustodyWriter,
    error::{capacity_rejected, invalid_image},
    payload::RetainedPayload,
};
use crate::constants::{IMAGE_CUSTODY_RECORD_HEADER_BYTES, IMAGE_CUSTODY_RECORD_VERSION};

pub(super) trait CustodyRecord: Sized {
    const MAGIC: &'static [u8; 4];
    const TAG: u8;
    fn payload_length(&self) -> StorageResult<usize>;
    fn encode_fields(&self, writer: &mut CustodyWriter) -> StorageResult<()>;
    fn decode_fields(reader: &mut CustodyReader<'_>) -> StorageResult<Self>;
}

pub(super) struct CustodyRecordCodec;

impl CustodyRecordCodec {
    pub(super) fn encoding_bound(payload_bytes: usize, maximum: usize) -> StorageResult<usize> {
        if payload_bytes
            > maximum
                .checked_sub(IMAGE_CUSTODY_RECORD_HEADER_BYTES)
                .ok_or_else(invalid_image)?
        {
            return Err(invalid_image());
        }
        RetainedPayload::encoded_length_bound(payload_bytes)?
            .checked_add(IMAGE_CUSTODY_RECORD_HEADER_BYTES)
            .ok_or_else(invalid_image)
    }

    pub(super) fn decode<T: CustodyRecord>(bytes: &[u8], maximum: usize) -> StorageResult<T> {
        if bytes.len() > maximum {
            return Err(invalid_image());
        }
        let header = bytes
            .get(..IMAGE_CUSTODY_RECORD_HEADER_BYTES)
            .ok_or_else(invalid_image)?;
        if header[..4] != T::MAGIC[..] || header[4..6] != IMAGE_CUSTODY_RECORD_VERSION.to_be_bytes()
        {
            return Err(invalid_image());
        }
        let maximum = maximum
            .checked_sub(IMAGE_CUSTODY_RECORD_HEADER_BYTES)
            .ok_or_else(invalid_image)?;
        let payload =
            RetainedPayload::decode(&bytes[IMAGE_CUSTODY_RECORD_HEADER_BYTES..], maximum)?;
        if header[6..] != <[u8; 32]>::from(Sha256::digest(&payload)) {
            return Err(invalid_image());
        }
        let mut reader = CustodyReader::new(&payload);
        if reader.decode_u8()? != T::TAG {
            return Err(invalid_image());
        }
        let value = T::decode_fields(&mut reader)?;
        reader.require_end()?;
        Ok(value)
    }

    pub(super) fn prepare<T: CustodyRecord>(
        key: DurableRecordKey,
        expected: Option<Revision>,
        value: &T,
        maximum: usize,
    ) -> StorageResult<RecordChange> {
        let payload = Self::encode_payload(value, maximum)?;
        let maximum = maximum
            .checked_sub(IMAGE_CUSTODY_RECORD_HEADER_BYTES)
            .ok_or_else(invalid_image)?;
        let encoded = RetainedPayload::encode(&payload, maximum)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(IMAGE_CUSTODY_RECORD_HEADER_BYTES + encoded.len())
            .map_err(|_| capacity_rejected())?;
        bytes.extend_from_slice(T::MAGIC);
        bytes.extend_from_slice(&IMAGE_CUSTODY_RECORD_VERSION.to_be_bytes());
        bytes.extend_from_slice(&Sha256::digest(&payload));
        bytes.extend_from_slice(&encoded);
        let revision = expected.map_or(Ok(Revision::INITIAL), Revision::checked_next)?;
        Ok(RecordChange::Store {
            expected: expected.map_or(RecordExpectation::Absent, RecordExpectation::Revision),
            record: DurableRecord {
                key,
                revision,
                bytes: bytes.into(),
            },
        })
    }

    fn encode_payload<T: CustodyRecord>(value: &T, maximum: usize) -> StorageResult<Vec<u8>> {
        let length = value.payload_length()?;
        let maximum = maximum
            .checked_sub(IMAGE_CUSTODY_RECORD_HEADER_BYTES)
            .ok_or_else(invalid_image)?;
        let mut writer = CustodyWriter::try_new(length, maximum)?;
        writer.encode_u8(T::TAG)?;
        value.encode_fields(&mut writer)?;
        writer.finish()
    }
}
