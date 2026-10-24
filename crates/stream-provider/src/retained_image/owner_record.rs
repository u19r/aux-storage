//! An immutable descriptor receipt records exact body dependencies, not
//! final-release authority.

use std::num::NonZeroUsize;

use infrastructure_provider::{
    limits::InfrastructureLimits,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange},
};
use sha2::{Digest as _, Sha256};
use storage_types::{ImageAddress, Revision, StorageResult, TableHistoryId};

use super::{
    RetainedImageManifest,
    custody_read::CustodyReader,
    custody_record::{CustodyRecord, CustodyRecordCodec},
    custody_write::CustodyWriter,
    error::{capacity_rejected, invalid_image},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RetainedOwnerKind {
    Primary,
    Projection,
    ItemHistory,
    TablePointer,
    SystemPointer,
    SystemRecord,
    Event,
    Baseline,
}

#[derive(Clone, PartialEq, Eq)]
pub struct RetainedOwnerRecord {
    pub(super) kind: RetainedOwnerKind,
    pub(super) locator: DurableRecordKey,
    pub(super) history: TableHistoryId,
    pub(super) descriptor: [u8; 32],
    pub(super) bodies: Vec<(ImageAddress, usize)>,
}

impl RetainedImageManifest {
    pub fn prepare_owner_record(
        &self,
        kind: RetainedOwnerKind,
        locator: DurableRecordKey,
        descriptor: &[u8],
    ) -> StorageResult<Option<RetainedOwnerRecord>> {
        self.references()?.prepare_owner(kind, locator, descriptor)
    }
}

impl RetainedOwnerRecord {
    /// The caller has conditionally published this exact receipt in this
    /// attempt.
    pub fn stage_registration<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = self.try_into().and_then(|record| {
            settlement
                .as_mut()
                .ownership_changes()
                .stage_owner(None, Some(record))
        });
        settlement.complete(result)
    }

    /// The caller protects and conditionally deletes this exact established
    /// receipt.
    pub fn stage_retirement<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = self.try_into().and_then(|record| {
            settlement
                .as_mut()
                .ownership_changes()
                .stage_owner(Some(record), None)
        });
        settlement.complete(result)
    }

    pub fn key(&self) -> StorageResult<DurableRecordKey> {
        let mut digest = Sha256::new();
        digest.update([self.kind.tag()]);
        digest.update(super::json_output::RetainedJsonOutput::encode(
            &self.locator,
        )?);
        let mut suffix = Vec::new();
        suffix
            .try_reserve(crate::constants::RETAINED_OWNER_SUFFIX_BYTES)
            .map_err(|_| capacity_rejected())?;
        suffix.push(self.kind.tag());
        suffix.extend_from_slice(&digest.finalize());
        let mut namespace = String::new();
        namespace
            .try_reserve(crate::constants::RETAINED_OWNER_NAMESPACE.len())
            .map_err(|_| capacity_rejected())?;
        namespace.push_str(crate::constants::RETAINED_OWNER_NAMESPACE);
        let mut resource = Vec::new();
        resource
            .try_reserve(self.history.as_ref().len())
            .map_err(|_| capacity_rejected())?;
        resource.extend_from_slice(self.history.as_ref());
        RecordAddress {
            namespace,
            resource,
            generation: 0,
            suffix,
        }
        .try_into()
    }

    /// Raw locator bytes plus fixed binary metadata and every legal body
    /// reference.
    pub fn encoded_length_bound(key_bytes: usize, maximum: usize) -> StorageResult<usize> {
        let payload = key_bytes
            .checked_add(crate::constants::IMAGE_OWNER_PAYLOAD_FIXED_BYTES)
            .and_then(|bytes| {
                bytes.checked_add(
                    crate::constants::MAX_RETAINED_MANIFEST_BODIES
                        * crate::constants::IMAGE_OWNER_BODY_BYTES,
                )
            })
            .ok_or_else(invalid_image)?;
        CustodyRecordCodec::encoding_bound(payload, maximum)
    }

    pub(super) fn receipt_limits<'a>(
        receipts: impl Iterator<Item = &'a Self>,
    ) -> StorageResult<InfrastructureLimits> {
        let mut count = 0usize;
        let mut key_bytes = 0usize;
        let mut record_bytes = 0usize;
        for receipt in receipts {
            count = count.checked_add(1).ok_or_else(invalid_image)?;
            key_bytes = key_bytes.max(record_key_length(&receipt.key()?)?);
            record_bytes = record_bytes.max(Self::encoded_length_bound(
                record_key_length(&receipt.locator)?,
                usize::MAX,
            )?);
        }
        let count = count.min(crate::constants::IMAGE_OWNER_READ_RECORDS);
        let bytes = key_bytes
            .checked_add(record_bytes)
            .and_then(|bytes| bytes.checked_mul(count))
            .ok_or_else(invalid_image)?;
        InfrastructureLimits::try_new(
            NonZeroUsize::new(key_bytes).ok_or_else(invalid_image)?,
            NonZeroUsize::new(record_bytes).ok_or_else(invalid_image)?,
            NonZeroUsize::new(bytes).ok_or_else(invalid_image)?,
            NonZeroUsize::new(count).ok_or_else(invalid_image)?,
        )
    }

    pub fn prepare_change(&self, maximum: usize) -> StorageResult<RecordChange> {
        CustodyRecordCodec::prepare(self.key()?, None, self, maximum)
    }

    pub fn require_descriptor(&self, row: &DurableRecord) -> StorageResult<()> {
        if row.key != self.locator
            || row.revision != Revision::INITIAL
            || <[u8; 32]>::from(Sha256::digest(&row.bytes)) != self.descriptor
        {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub fn require_observation(&self, row: &DurableRecord, maximum: usize) -> StorageResult<()> {
        if row.key != self.key()? || row.revision != Revision::INITIAL {
            return Err(invalid_image());
        }
        let observed: Self = CustodyRecordCodec::decode(&row.bytes, maximum)?;
        if observed != *self {
            return Err(invalid_image());
        }
        Ok(())
    }
}

impl RetainedOwnerKind {
    pub(super) fn is_mutable(self) -> bool {
        matches!(self, Self::Primary | Self::Projection)
    }

    pub fn from_key(key: &DurableRecordKey) -> Option<Self> {
        let address = key.as_ref();
        if address.namespace != crate::constants::RETAINED_OWNER_NAMESPACE
            || address.resource.len() != 16
            || address.resource == [0; 16]
            || address.generation != 0
            || address.suffix.len() != crate::constants::RETAINED_OWNER_SUFFIX_BYTES
        {
            return None;
        }
        Self::from_tag(address.suffix[0])
    }

    pub(super) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Primary),
            1 => Some(Self::Projection),
            2 => Some(Self::ItemHistory),
            3 => Some(Self::TablePointer),
            4 => Some(Self::SystemPointer),
            5 => Some(Self::SystemRecord),
            6 => Some(Self::Event),
            7 => Some(Self::Baseline),
            _ => None,
        }
    }

    pub(super) fn tag(self) -> u8 {
        match self {
            Self::Primary => 0,
            Self::Projection => 1,
            Self::ItemHistory => 2,
            Self::TablePointer => 3,
            Self::SystemPointer => 4,
            Self::SystemRecord => 5,
            Self::Event => 6,
            Self::Baseline => 7,
        }
    }
}

fn record_key_length(key: &DurableRecordKey) -> StorageResult<usize> {
    let key = key.as_ref();
    key.namespace
        .len()
        .checked_add(key.resource.len())
        .and_then(|length| length.checked_add(key.suffix.len()))
        .and_then(|length| length.checked_add(16))
        .ok_or_else(invalid_image)
}

impl CustodyRecord for RetainedOwnerRecord {
    const MAGIC: &'static [u8; 4] = crate::constants::IMAGE_OWNER_RECORD_MAGIC;
    const TAG: u8 = 0;

    fn payload_length(&self) -> StorageResult<usize> {
        self.require_body_references()?;
        CustodyWriter::key_length(&self.locator)?
            .checked_add(1 + 1 + 16 + 32 + 1)
            .and_then(|bytes| {
                bytes.checked_add(self.bodies.len() * crate::constants::IMAGE_OWNER_BODY_BYTES)
            })
            .ok_or_else(invalid_image)
    }

    fn encode_fields(&self, writer: &mut CustodyWriter) -> StorageResult<()> {
        writer.encode_u8(self.kind.tag())?;
        writer.append(self.history.as_ref())?;
        writer.encode_key(&self.locator)?;
        writer.append(&self.descriptor)?;
        writer.encode_u8(u8::try_from(self.bodies.len()).map_err(|_| invalid_image())?)?;
        for (address, length) in &self.bodies {
            writer.encode_image(*address)?;
            writer.encode_length(*length)?;
        }
        Ok(())
    }

    fn decode_fields(reader: &mut CustodyReader<'_>) -> StorageResult<Self> {
        let kind = RetainedOwnerKind::from_tag(reader.decode_u8()?).ok_or_else(invalid_image)?;
        let history = reader.decode_array::<16>()?.into();
        let locator = reader.decode_key()?;
        let descriptor = reader.decode_array()?;
        let count = usize::from(reader.decode_u8()?);
        if count == 0 || count > crate::constants::MAX_RETAINED_MANIFEST_BODIES {
            return Err(invalid_image());
        }
        let mut bodies = Vec::new();
        bodies.try_reserve(count).map_err(|_| capacity_rejected())?;
        for _ in 0..count {
            bodies.push((reader.decode_image()?, reader.decode_length()?));
        }
        let record = Self {
            kind,
            locator,
            history,
            descriptor,
            bodies,
        };
        record.require_body_references()?;
        Ok(record)
    }
}

impl RetainedOwnerRecord {
    fn require_body_scope(&self, address: ImageAddress) -> StorageResult<()> {
        if address.history() == self.history {
            return Ok(());
        }
        if self.kind != RetainedOwnerKind::Event {
            return Err(invalid_image());
        }
        let scope = super::causal_body::CausalBodyScope::try_from(&self.locator)?;
        if scope.address(address.identity())? != address {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn require_body_references(&self) -> StorageResult<()> {
        if self.history.as_ref() == &[0; 16]
            || self.bodies.is_empty()
            || self.bodies.len() > crate::constants::MAX_RETAINED_MANIFEST_BODIES
        {
            return Err(invalid_image());
        }
        let mut foreign = 0usize;
        let mut previous = None;
        for (address, length) in &self.bodies {
            self.require_body_scope(*address)?;
            foreign += usize::from(address.history() != self.history);
            if foreign > 1
                || *length == 0
                || previous.is_some_and(|identity: storage_types::ImmutableImageId| {
                    identity.as_ref() >= address.identity().as_ref()
                })
            {
                return Err(invalid_image());
            }
            previous = Some(address.identity());
        }
        Ok(())
    }
}

impl TryFrom<&RetainedOwnerRecord> for RetainedOwnerRecord {
    type Error = storage_types::StorageError;

    fn try_from(record: &RetainedOwnerRecord) -> StorageResult<Self> {
        let mut bodies = Vec::new();
        bodies
            .try_reserve(record.bodies.len())
            .map_err(|_| capacity_rejected())?;
        bodies.extend_from_slice(&record.bodies);
        Ok(Self {
            kind: record.kind,
            locator: (&record.locator).try_into()?,
            history: record.history,
            descriptor: record.descriptor,
            bodies,
        })
    }
}
