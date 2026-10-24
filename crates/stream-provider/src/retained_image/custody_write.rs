//! Exact binary custody field widths bound allocation before serialization.

use infrastructure_provider::record::DurableRecordKey;
use storage_types::{ImageAddress, StorageResult};

use super::error::{capacity_rejected, invalid_image};

pub(super) struct CustodyWriter {
    bytes: Vec<u8>,
    length: usize,
}

impl CustodyWriter {
    pub(super) fn try_new(length: usize, maximum: usize) -> StorageResult<Self> {
        if length > maximum {
            return Err(capacity_rejected());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| capacity_rejected())?;
        Ok(Self { bytes, length })
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> StorageResult<()> {
        if bytes.len() > self.length - self.bytes.len() {
            return Err(invalid_image());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub(super) fn encode_u8(&mut self, value: u8) -> StorageResult<()> {
        self.append(&[value])
    }
    pub(super) fn encode_u16(&mut self, value: u16) -> StorageResult<()> {
        self.append(&value.to_be_bytes())
    }
    pub(super) fn encode_u64(&mut self, value: u64) -> StorageResult<()> {
        self.append(&value.to_be_bytes())
    }
    pub(super) fn encode_length(&mut self, value: usize) -> StorageResult<()> {
        self.encode_u64(u64::try_from(value).map_err(|_| invalid_image())?)
    }

    pub(super) fn encode_key(&mut self, key: &DurableRecordKey) -> StorageResult<()> {
        let key = key.as_ref();
        self.encode_span(key.namespace.as_bytes())?;
        self.encode_span(&key.resource)?;
        self.encode_u64(key.generation)?;
        self.encode_span(&key.suffix)
    }

    pub(super) fn key_length(key: &DurableRecordKey) -> StorageResult<usize> {
        let key = key.as_ref();
        [key.namespace.len(), key.resource.len(), key.suffix.len()]
            .into_iter()
            .try_fold(
                crate::constants::CUSTODY_KEY_HEADER_BYTES,
                |total, length| {
                    u32::try_from(length).map_err(|_| invalid_image())?;
                    total.checked_add(length).ok_or_else(invalid_image)
                },
            )
    }

    pub(super) fn encode_image(&mut self, address: ImageAddress) -> StorageResult<()> {
        self.append(address.history().as_ref())?;
        self.append(address.identity().as_ref())
    }

    pub(super) fn finish(self) -> StorageResult<Vec<u8>> {
        if self.bytes.len() != self.length {
            return Err(invalid_image());
        }
        Ok(self.bytes)
    }

    fn encode_span(&mut self, bytes: &[u8]) -> StorageResult<()> {
        self.append(
            &u32::try_from(bytes.len())
                .map_err(|_| invalid_image())?
                .to_be_bytes(),
        )?;
        self.append(bytes)
    }
}
