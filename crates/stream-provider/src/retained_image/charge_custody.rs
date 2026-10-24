//! Durable accounting observations are bound to the exact body and account.

use infrastructure_provider::record::{DurableRecordKey, RecordChange};
use storage_types::{ImageAddress, StorageResult};

use super::{
    custody_read::CustodyReader,
    custody_record::{CustodyRecord, CustodyRecordCodec},
    custody_write::CustodyWriter,
    error::invalid_image,
};

/// This record identifies an established byte charge, not last-owner release
/// authority. A final release still needs the complete native ownership graph.
#[derive(Clone, PartialEq, Eq)]
pub struct ImageChargeCustody {
    account: DurableRecordKey,
    receipt: DurableRecordKey,
    address: ImageAddress,
    bytes: u64,
}

impl ImageChargeCustody {
    pub fn stage<A: super::RetainedOwnershipAttempt>(&self, attempt: &mut A) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = settlement
            .as_mut()
            .ownership_changes()
            .stage_charge(self, self.address);
        settlement.complete(result)
    }

    pub(super) fn refund_keys(&self) -> (&DurableRecordKey, &DurableRecordKey) {
        (&self.account, &self.receipt)
    }

    pub(super) fn require_body(
        &self,
        body: &infrastructure_provider::record::DurableRecord,
    ) -> StorageResult<()> {
        if body.key != super::address::image_record_key(self.address)? {
            return Err(invalid_image());
        }
        self.require_image_length(self.address, body.bytes.len())
    }

    pub(super) fn require_image_length(
        &self,
        address: ImageAddress,
        encoded_bytes: usize,
    ) -> StorageResult<()> {
        let key = super::address::image_record_key(address)?;
        if self.address != address || self.bytes != physical_record_length(&key, encoded_bytes)? {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn require_refund(
        &self,
        body: (&DurableRecordKey, usize),
        receipt: &infrastructure_provider::record::DurableRecord,
        maximum: usize,
    ) -> StorageResult<u64> {
        if *body.0 != super::address::image_record_key(self.address)?
            || receipt.key != self.receipt
            || receipt.revision != storage_types::Revision::INITIAL
        {
            return Err(invalid_image());
        }
        let observed = Self::decode(&receipt.bytes, maximum)?;
        let body_bytes = physical_record_length(body.0, body.1)?;
        if observed != *self || self.bytes != body_bytes {
            return Err(invalid_image());
        }
        body_bytes
            .checked_add(physical_record_bytes(receipt)?)
            .ok_or_else(invalid_image)
    }
    pub fn try_new(
        account: DurableRecordKey,
        receipt: DurableRecordKey,
        address: ImageAddress,
        bytes: u64,
    ) -> StorageResult<Self> {
        if bytes == 0 || account == receipt {
            return Err(invalid_image());
        }
        Ok(Self {
            account,
            receipt,
            address,
            bytes,
        })
    }

    pub fn decode(bytes: &[u8], maximum: usize) -> StorageResult<Self> {
        CustodyRecordCodec::decode(bytes, maximum)
    }

    pub fn require_binding(
        &self,
        account: &DurableRecordKey,
        receipt: &DurableRecordKey,
        address: ImageAddress,
        bytes: u64,
    ) -> StorageResult<()> {
        if &self.account != account
            || &self.receipt != receipt
            || self.address != address
            || self.bytes != bytes
        {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub fn prepare_change(&self, maximum: usize) -> StorageResult<RecordChange> {
        CustodyRecordCodec::prepare((&self.receipt).try_into()?, None, self, maximum)
    }

    pub fn encoded_length_bound(&self, maximum: usize) -> StorageResult<usize> {
        CustodyRecordCodec::encoding_bound(self.payload_length()?, maximum)
    }
}

fn physical_record_bytes(
    row: &infrastructure_provider::record::DurableRecord,
) -> StorageResult<u64> {
    physical_record_length(&row.key, row.bytes.len())
}

fn physical_record_length(key: &DurableRecordKey, payload_bytes: usize) -> StorageResult<u64> {
    let key = key.as_ref();
    let bytes = key
        .namespace
        .len()
        .checked_add(key.resource.len())
        .and_then(|bytes| bytes.checked_add(key.suffix.len()))
        .and_then(|bytes| bytes.checked_add(16))
        .and_then(|bytes| bytes.checked_add(payload_bytes))
        .ok_or_else(invalid_image)?;
    u64::try_from(bytes).map_err(|_| invalid_image())
}

impl CustodyRecord for ImageChargeCustody {
    const MAGIC: &'static [u8; 4] = crate::constants::IMAGE_CHARGE_RECORD_MAGIC;
    const TAG: u8 = 1;

    fn payload_length(&self) -> StorageResult<usize> {
        CustodyWriter::key_length(&self.account)?
            .checked_add(CustodyWriter::key_length(&self.receipt)?)
            .and_then(|bytes| {
                bytes.checked_add(1 + crate::constants::CUSTODY_IMAGE_ADDRESS_BYTES + 8)
            })
            .ok_or_else(invalid_image)
    }

    fn encode_fields(&self, writer: &mut CustodyWriter) -> StorageResult<()> {
        writer.encode_key(&self.account)?;
        writer.encode_key(&self.receipt)?;
        writer.encode_image(self.address)?;
        writer.encode_u64(self.bytes)
    }

    fn decode_fields(reader: &mut CustodyReader<'_>) -> StorageResult<Self> {
        Self::try_new(
            reader.decode_key()?,
            reader.decode_key()?,
            reader.decode_image()?,
            reader.decode_u64()?,
        )
    }
}

impl TryFrom<&ImageChargeCustody> for ImageChargeCustody {
    type Error = storage_types::StorageError;

    fn try_from(custody: &ImageChargeCustody) -> StorageResult<Self> {
        Ok(Self {
            account: (&custody.account).try_into()?,
            receipt: (&custody.receipt).try_into()?,
            address: custody.address,
            bytes: custody.bytes,
        })
    }
}
