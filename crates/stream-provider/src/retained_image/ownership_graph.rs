//! Persisted graph authority is bound to exact body and owner addresses.

use std::num::NonZeroU64;

use infrastructure_provider::record::{
    DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
};
use sha2::{Digest as _, Sha256};
use storage_types::{ImageAddress, Revision, StorageResult};

use super::{
    ImageChargeCustody, RetainedOwnerKind,
    custody_read::CustodyReader,
    custody_record::{CustodyRecord, CustodyRecordCodec},
    custody_write::CustodyWriter,
    error::{capacity_rejected, invalid_image},
};

pub(super) struct ImageOwnershipHead {
    format: u16,
    pub(super) address: ImageAddress,
    pub(super) raw_bytes: usize,
    pub(super) encoded_bytes: usize,
    pub(super) owners: NonZeroU64,
    pub(super) charge: Option<ImageChargeCustody>,
}

impl ImageOwnershipHead {
    pub(super) fn encoded_length_bound(key_bytes: usize) -> StorageResult<usize> {
        let payload = key_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(crate::constants::IMAGE_HEAD_PAYLOAD_FIXED_BYTES))
            .ok_or_else(invalid_image)?;
        CustodyRecordCodec::encoding_bound(payload, usize::MAX)
    }
    pub(super) fn try_new(
        address: ImageAddress,
        lengths: (usize, usize),
        owners: u64,
        charge: Option<ImageChargeCustody>,
    ) -> StorageResult<Self> {
        if lengths.0 == 0 || lengths.1 == 0 {
            return Err(invalid_image());
        }
        if let Some(charge) = &charge {
            charge.require_image_length(address, lengths.1)?;
        }
        Ok(Self {
            format: 1,
            address,
            raw_bytes: lengths.0,
            encoded_bytes: lengths.1,
            owners: NonZeroU64::new(owners).ok_or_else(invalid_image)?,
            charge,
        })
    }

    pub(super) fn key(address: ImageAddress) -> StorageResult<DurableRecordKey> {
        let mut suffix = Vec::new();
        suffix
            .try_reserve(address.identity().as_ref().len())
            .map_err(|_| capacity_rejected())?;
        suffix.extend_from_slice(address.identity().as_ref());
        graph_key(crate::constants::IMAGE_HEAD_NAMESPACE, address, suffix)
    }

    pub(super) fn decode(
        row: &DurableRecord,
        address: ImageAddress,
        maximum: usize,
    ) -> StorageResult<Self> {
        if row.key != Self::key(address)? {
            return Err(invalid_image());
        }
        let head: Self = CustodyRecordCodec::decode(&row.bytes, maximum)?;
        if head.address != address {
            return Err(invalid_image());
        }
        Ok(head)
    }

    pub(super) fn prepare_change(
        &self,
        expected: Option<Revision>,
        maximum: usize,
    ) -> StorageResult<RecordChange> {
        CustodyRecordCodec::prepare(Self::key(self.address)?, expected, self, maximum)
    }
}

pub(super) struct ImageOwnerEdge {
    format: u16,
    address: ImageAddress,
    owner: DurableRecordKey,
}

impl ImageOwnerEdge {
    pub(super) fn encoded_length_bound() -> StorageResult<usize> {
        CustodyRecordCodec::encoding_bound(Self::maximum_shape()?.payload_length()?, usize::MAX)
    }

    pub(super) fn key_length_bound() -> StorageResult<usize> {
        let key = Self::maximum_shape()?.key()?;
        let key = key.as_ref();
        key.namespace
            .len()
            .checked_add(key.resource.len())
            .and_then(|bytes| bytes.checked_add(key.suffix.len()))
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or_else(invalid_image)
    }

    fn maximum_shape() -> StorageResult<Self> {
        let address = ImageAddress::try_new([u8::MAX; 16].into(), [u8::MAX; 32].into())?;
        let mut suffix = Vec::new();
        suffix
            .try_reserve(crate::constants::RETAINED_OWNER_SUFFIX_BYTES)
            .map_err(|_| capacity_rejected())?;
        suffix.push(RetainedOwnerKind::SystemRecord.tag());
        suffix.extend_from_slice(&[u8::MAX; 32]);
        let owner = graph_key(crate::constants::RETAINED_OWNER_NAMESPACE, address, suffix)?;
        Self::try_new(address, owner)
    }
    pub(super) fn try_new(address: ImageAddress, owner: DurableRecordKey) -> StorageResult<Self> {
        let kind = RetainedOwnerKind::from_key(&owner).ok_or_else(invalid_image)?;
        if kind != RetainedOwnerKind::Event && owner.as_ref().resource != address.history().as_ref()
        {
            return Err(invalid_image());
        }
        Ok(Self {
            format: 2,
            address,
            owner,
        })
    }

    pub(super) fn key(&self) -> StorageResult<DurableRecordKey> {
        let mut suffix = Vec::new();
        suffix
            .try_reserve(crate::constants::IMAGE_EDGE_SUFFIX_BYTES)
            .map_err(|_| capacity_rejected())?;
        suffix.extend_from_slice(self.address.identity().as_ref());
        suffix.push(0);
        suffix.push(
            RetainedOwnerKind::from_key(&self.owner)
                .ok_or_else(invalid_image)?
                .tag(),
        );
        suffix.extend_from_slice(&Sha256::digest(
            super::json_output::RetainedJsonOutput::encode(&self.owner)?,
        ));
        graph_key(crate::constants::IMAGE_EDGE_NAMESPACE, self.address, suffix)
    }

    pub(super) fn require_observation(
        &self,
        row: &DurableRecord,
        maximum: usize,
    ) -> StorageResult<()> {
        if row.key != self.key()? || row.revision != Revision::INITIAL {
            return Err(invalid_image());
        }
        let observed: Self = CustodyRecordCodec::decode(&row.bytes, maximum)?;
        if observed.format != 2 || observed.address != self.address || observed.owner != self.owner
        {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn prepare_change(&self, maximum: usize) -> StorageResult<RecordChange> {
        CustodyRecordCodec::prepare(self.key()?, None, self, maximum)
    }
}

pub(super) fn graph_key(
    namespace: &str,
    address: ImageAddress,
    suffix: Vec<u8>,
) -> StorageResult<DurableRecordKey> {
    let mut name = String::new();
    name.try_reserve(namespace.len())
        .map_err(|_| capacity_rejected())?;
    name.push_str(namespace);
    let mut resource = Vec::new();
    resource
        .try_reserve(address.history().as_ref().len())
        .map_err(|_| capacity_rejected())?;
    resource.extend_from_slice(address.history().as_ref());
    RecordAddress {
        namespace: name,
        resource,
        generation: 0,
        suffix,
    }
    .try_into()
}

impl CustodyRecord for ImageOwnershipHead {
    const MAGIC: &'static [u8; 4] = crate::constants::IMAGE_HEAD_RECORD_MAGIC;
    const TAG: u8 = 0;

    fn payload_length(&self) -> StorageResult<usize> {
        let charge = self
            .charge
            .as_ref()
            .map(CustodyRecord::payload_length)
            .transpose()?
            .map_or(Ok(0), |bytes| {
                bytes.checked_sub(1).ok_or_else(invalid_image)
            })?;
        crate::constants::IMAGE_HEAD_BASE_PAYLOAD_BYTES
            .checked_add(charge)
            .ok_or_else(invalid_image)
    }

    fn encode_fields(&self, writer: &mut CustodyWriter) -> StorageResult<()> {
        writer.encode_u16(self.format)?;
        writer.encode_image(self.address)?;
        writer.encode_length(self.raw_bytes)?;
        writer.encode_length(self.encoded_bytes)?;
        writer.encode_u64(self.owners.get())?;
        writer.encode_u8(u8::from(self.charge.is_some()))?;
        if let Some(charge) = &self.charge {
            charge.encode_fields(writer)?;
        }
        Ok(())
    }

    fn decode_fields(reader: &mut CustodyReader<'_>) -> StorageResult<Self> {
        if reader.decode_u16()? != 1 {
            return Err(invalid_image());
        }
        let address = reader.decode_image()?;
        let lengths = (reader.decode_length()?, reader.decode_length()?);
        let owners = reader.decode_u64()?;
        let charge = match reader.decode_u8()? {
            0 => None,
            1 => Some(ImageChargeCustody::decode_fields(reader)?),
            _ => return Err(invalid_image()),
        };
        Self::try_new(address, lengths, owners, charge)
    }
}

impl CustodyRecord for ImageOwnerEdge {
    const MAGIC: &'static [u8; 4] = crate::constants::IMAGE_EDGE_RECORD_MAGIC;
    const TAG: u8 = 0;

    fn payload_length(&self) -> StorageResult<usize> {
        CustodyWriter::key_length(&self.owner)?
            .checked_add(1 + 2 + crate::constants::CUSTODY_IMAGE_ADDRESS_BYTES)
            .ok_or_else(invalid_image)
    }

    fn encode_fields(&self, writer: &mut CustodyWriter) -> StorageResult<()> {
        writer.encode_u16(self.format)?;
        writer.encode_image(self.address)?;
        writer.encode_key(&self.owner)
    }

    fn decode_fields(reader: &mut CustodyReader<'_>) -> StorageResult<Self> {
        if reader.decode_u16()? != 2 {
            return Err(invalid_image());
        }
        Self::try_new(reader.decode_image()?, reader.decode_key()?)
    }
}
