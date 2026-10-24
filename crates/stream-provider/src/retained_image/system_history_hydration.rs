//! System history and companion pointers share complete immutable-body
//! verification.

use storage_types::StorageResult;

use super::{
    error::{capacity_rejected, invalid_image},
    stream_hydration::{HydratedStreamContent, PreparedStreamHydration},
    stream_row::StoredStreamRow,
    system_stream::RetainedSystemStreamRecord,
    verified_system_stream::VerifiedSystemStreamRecord,
};
use crate::{StreamItem, SystemStreamStoredRecord};

pub(super) struct PreparedSystemHistoryHydration {
    images: PreparedStreamHydration,
    groups: Vec<Vec<bool>>,
    maximum: usize,
}

impl PreparedSystemHistoryHydration {
    pub(super) fn try_new(
        records: Vec<Vec<RetainedSystemStreamRecord>>,
        pointers: Vec<Vec<Option<StoredStreamRow>>>,
        maximum: usize,
    ) -> StorageResult<Self> {
        if records.len() != pointers.len() {
            return Err(invalid_image());
        }
        let bound = records
            .len()
            .checked_mul(maximum)
            .ok_or_else(capacity_rejected)?;
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(records.len())
            .map_err(|_| capacity_rejected())?;
        let mut prepared = Self {
            images: PreparedStreamHydration::default(),
            groups,
            maximum: bound,
        };
        for (records, pointers) in records.into_iter().zip(pointers) {
            RetainedSystemStreamRecord::admit_pointer_group(&records, &pointers, maximum)?;
            let mut present = Vec::new();
            present
                .try_reserve_exact(records.len())
                .map_err(|_| capacity_rejected())?;
            for (record, pointer) in records.into_iter().zip(pointers) {
                prepared.images.include_system(record)?;
                present.push(pointer.is_some());
                if let Some(pointer) = pointer {
                    prepared.images.include_row(pointer.item, pointer.content)?;
                }
            }
            prepared.groups.push(present);
        }
        Ok(prepared)
    }

    pub(super) async fn load<A: super::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<Vec<(Vec<SystemStreamStoredRecord>, Vec<Option<StreamItem>>)>> {
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(self.groups.len())
            .map_err(|_| capacity_rejected())?;
        let mut hydrated = self.images.load(attempt, self.maximum).await?.into_iter();
        for present in self.groups {
            groups.push(Self::restore_group(present, &mut hydrated)?);
        }
        if hydrated.next().is_some() {
            return Err(invalid_image());
        }
        Ok(groups)
    }

    fn restore_group(
        present: Vec<bool>,
        hydrated: &mut impl Iterator<Item = HydratedStreamContent>,
    ) -> StorageResult<(Vec<SystemStreamStoredRecord>, Vec<Option<StreamItem>>)> {
        let mut records = Vec::new();
        let mut pointers = Vec::new();
        records
            .try_reserve_exact(present.len())
            .map_err(|_| capacity_rejected())?;
        pointers
            .try_reserve_exact(present.len())
            .map_err(|_| capacity_rejected())?;
        for present in present {
            let record =
                VerifiedSystemStreamRecord::try_from(hydrated.next().ok_or_else(invalid_image)?)?;
            let pointer = if present {
                Some(StreamItem::try_from(
                    hydrated.next().ok_or_else(invalid_image)?,
                )?)
            } else {
                None
            };
            record.require_pointer(pointer.as_ref())?;
            records.push(record.into_record());
            pointers.push(pointer);
        }
        Ok((records, pointers))
    }
}
