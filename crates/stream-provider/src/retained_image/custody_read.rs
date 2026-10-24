//! Binary custody decoding checks complete declared spans before allocating.

use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use storage_types::{ImageAddress, StorageResult};

use super::error::{capacity_rejected, invalid_image};

pub(super) struct CustodyReader<'a> {
    bytes: &'a [u8],
}

impl<'a> CustodyReader<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(super) fn take(&mut self, length: usize) -> StorageResult<&'a [u8]> {
        let (value, remaining) = self
            .bytes
            .split_at_checked(length)
            .ok_or_else(invalid_image)?;
        self.bytes = remaining;
        Ok(value)
    }

    pub(super) fn decode_array<const N: usize>(&mut self) -> StorageResult<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| invalid_image())
    }

    pub(super) fn decode_u8(&mut self) -> StorageResult<u8> {
        Ok(self.decode_array::<1>()?[0])
    }
    pub(super) fn decode_u16(&mut self) -> StorageResult<u16> {
        Ok(u16::from_be_bytes(self.decode_array()?))
    }
    pub(super) fn decode_u64(&mut self) -> StorageResult<u64> {
        Ok(u64::from_be_bytes(self.decode_array()?))
    }
    pub(super) fn decode_length(&mut self) -> StorageResult<usize> {
        usize::try_from(self.decode_u64()?).map_err(|_| invalid_image())
    }

    pub(super) fn decode_key(&mut self) -> StorageResult<DurableRecordKey> {
        let namespace = self.decode_span()?;
        let resource = self.decode_span()?;
        let generation = self.decode_u64()?;
        let suffix = self.decode_span()?;
        let namespace = std::str::from_utf8(namespace).map_err(|_| invalid_image())?;
        RecordAddress {
            namespace: String::from_utf8(Self::copy_span(namespace.as_bytes())?)
                .map_err(|_| invalid_image())?,
            resource: Self::copy_span(resource)?,
            generation,
            suffix: Self::copy_span(suffix)?,
        }
        .try_into()
    }

    pub(super) fn decode_image(&mut self) -> StorageResult<ImageAddress> {
        ImageAddress::try_new(
            self.decode_array::<16>()?.into(),
            self.decode_array::<32>()?.into(),
        )
    }

    pub(super) fn require_end(self) -> StorageResult<()> {
        if !self.bytes.is_empty() {
            return Err(invalid_image());
        }
        Ok(())
    }

    fn copy_span(bytes: &[u8]) -> StorageResult<Vec<u8>> {
        let mut copy = Vec::new();
        copy.try_reserve_exact(bytes.len())
            .map_err(|_| capacity_rejected())?;
        copy.extend_from_slice(bytes);
        Ok(copy)
    }

    fn decode_span(&mut self) -> StorageResult<&'a [u8]> {
        let length = usize::try_from(u32::from_be_bytes(self.decode_array()?))
            .map_err(|_| invalid_image())?;
        self.take(length)
    }
}
