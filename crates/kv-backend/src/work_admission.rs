use storage_types::{InfrastructureResultKind, NativeAttemptSettlement, StorageResult};

use crate::KvRangePage;

pub(crate) struct NativeReadWork<'attempt>(&'attempt NativeAttemptSettlement);

impl<'attempt> From<&'attempt NativeAttemptSettlement> for NativeReadWork<'attempt> {
    fn from(settlement: &'attempt NativeAttemptSettlement) -> Self {
        Self(settlement)
    }
}

impl NativeReadWork<'_> {
    pub(crate) fn charge_keys(&self, keys: &[Vec<u8>]) -> StorageResult<()> {
        for key in keys {
            self.0.charge_data(
                key.len()
                    .checked_add(std::mem::size_of::<Vec<u8>>())
                    .ok_or(InfrastructureResultKind::NativeCapacityRejected)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn charge_values(&self, values: &[Option<Vec<u8>>]) -> StorageResult<()> {
        for value in values {
            self.0.charge_data(
                value
                    .as_ref()
                    .map_or(0, Vec::len)
                    .checked_add(std::mem::size_of::<Option<Vec<u8>>>())
                    .ok_or(InfrastructureResultKind::NativeCapacityRejected)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn charge_pages(&self, pages: &[KvRangePage]) -> StorageResult<()> {
        for page in pages {
            self.0.charge_data(std::mem::size_of::<KvRangePage>())?;
            for entry in &page.entries {
                let bytes = entry
                    .key
                    .len()
                    .checked_add(entry.value.len())
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of_val(entry)))
                    .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
                self.0.charge_data(bytes)?;
            }
        }
        Ok(())
    }
}
