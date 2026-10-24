//! Exact byte adjustments shared by custody admission and final body release.

use infrastructure_provider::record::{DurableRecordKey, RecordChange};
use storage_types::{Revision, StorageResult};

use super::{
    custody_read::CustodyReader,
    custody_record::{CustodyRecord, CustodyRecordCodec},
    custody_write::CustodyWriter,
    error::invalid_image,
};

/// The account address is part of its content, so an observation cannot be
/// transplanted into another native capacity scope. Budget policy stays with
/// the caller; this value owns only checked byte accounting.
pub struct ImageChargeAccount {
    key: DurableRecordKey,
    bytes: u64,
}

impl ImageChargeAccount {
    /// Only the initialization owner may publish a newly constructed account.
    /// Construction does not establish that an absent native account is fresh.
    pub fn new(key: DurableRecordKey, bytes: u64) -> Self {
        Self { key, bytes }
    }

    pub fn require_key(&self, key: &DurableRecordKey) -> StorageResult<()> {
        if &self.key != key {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Bound raw address fields and the complete fixed-width account total.
    pub fn encoded_length_bound(&self, maximum: usize) -> StorageResult<usize> {
        CustodyRecordCodec::encoding_bound(self.payload_length()?, maximum)
    }

    pub fn decode(bytes: &[u8], maximum: usize) -> StorageResult<Self> {
        CustodyRecordCodec::decode(bytes, maximum)
    }

    /// Use the same bounded codec and conditional envelope for regional growth
    /// and ordinary final-owner refunds. The caller owns transaction
    /// settlement.
    pub fn prepare_change(
        &self,
        expected: Option<Revision>,
        maximum: usize,
    ) -> StorageResult<RecordChange> {
        CustodyRecordCodec::prepare((&self.key).try_into()?, expected, self, maximum)
    }

    /// Negative deltas release only bytes proven by the caller's protected
    /// observations. Overflow and underflow reject rather than saturating.
    pub fn adjust(&self, delta: i128) -> StorageResult<Self> {
        let bytes = i128::from(self.bytes)
            .checked_add(delta)
            .ok_or_else(invalid_image)?;
        let bytes = u64::try_from(bytes).map_err(|_| invalid_image())?;
        Ok(Self {
            key: (&self.key).try_into()?,
            bytes,
        })
    }
}

impl CustodyRecord for ImageChargeAccount {
    const MAGIC: &'static [u8; 4] = crate::constants::IMAGE_CHARGE_RECORD_MAGIC;
    const TAG: u8 = 0;

    fn payload_length(&self) -> StorageResult<usize> {
        CustodyWriter::key_length(&self.key)?
            .checked_add(1 + 8)
            .ok_or_else(invalid_image)
    }

    fn encode_fields(&self, writer: &mut CustodyWriter) -> StorageResult<()> {
        writer.encode_key(&self.key)?;
        writer.encode_u64(self.bytes)
    }

    fn decode_fields(reader: &mut CustodyReader<'_>) -> StorageResult<Self> {
        Ok(Self {
            key: reader.decode_key()?,
            bytes: reader.decode_u64()?,
        })
    }
}
