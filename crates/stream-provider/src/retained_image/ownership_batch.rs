//! Coalescing keeps the first native ownership state and final descriptor
//! state.

use infrastructure_provider::record::DurableRecordKey;
use storage_types::{ImageAddress, StorageResult};

use super::{
    RetainedOwnershipChanges,
    address::image_record_key,
    error::invalid_image,
    ownership_changes::{OwnerNetChange, PreparedBodyObservation},
    ownership_inventory::OwnershipRecords,
};

pub(super) struct OwnershipNetPlan {
    pub(super) bodies: OwnershipRecords<BodyOwnershipChanges>,
}

pub(super) struct BodyOwnershipChanges {
    pub(super) address: ImageAddress,
    pub(super) raw_bytes: usize,
    pub(super) observation: Option<PreparedBodyObservation>,
    pub(super) owners: OwnershipRecords<OwnerEdgeTransition>,
}

#[derive(Clone, Copy)]
pub(super) enum OwnerEdgeTransition {
    Attach,
    Detach,
    Retain,
}

impl TryFrom<RetainedOwnershipChanges> for OwnershipNetPlan {
    type Error = storage_types::StorageError;

    fn try_from(pending: RetainedOwnershipChanges) -> StorageResult<Self> {
        if !matches!(
            &pending.phase,
            super::ownership_changes::OwnershipPhase::Collecting
        ) {
            return Err(invalid_image());
        }
        let mut prepared = Self {
            bodies: OwnershipRecords::default(),
        };
        for (owner, change) in pending.owners {
            prepared.include_owner(owner, change)?;
        }
        for (key, observation) in pending.bodies {
            let body = prepared.include_body(observation.address, observation.raw_bytes)?;
            if image_record_key(observation.address)? != key {
                return Err(invalid_image());
            }
            body.observation = Some(observation);
        }
        Ok(prepared)
    }
}

impl OwnershipNetPlan {
    fn include_owner(
        &mut self,
        owner: DurableRecordKey,
        change: OwnerNetChange,
    ) -> StorageResult<()> {
        let previous = change
            .previous
            .as_ref()
            .map_or(&[][..], |receipt| receipt.bodies.as_slice());
        let next = change
            .next
            .as_ref()
            .map_or(&[][..], |receipt| receipt.bodies.as_slice());
        for (address, bytes) in previous {
            let retained = next.iter().find(|(candidate, _)| candidate == address);
            if retained.is_some_and(|(_, length)| length != bytes) {
                return Err(invalid_image());
            }
            let transition = if retained.is_some() {
                OwnerEdgeTransition::Retain
            } else {
                OwnerEdgeTransition::Detach
            };
            self.include_owner_reference(&owner, *address, (*bytes, transition))?;
        }
        for (address, bytes) in next {
            if previous.iter().any(|(candidate, _)| candidate == address) {
                continue;
            }
            self.include_owner_reference(&owner, *address, (*bytes, OwnerEdgeTransition::Attach))?;
        }
        Ok(())
    }

    fn include_owner_reference(
        &mut self,
        owner: &DurableRecordKey,
        address: ImageAddress,
        reference: (usize, OwnerEdgeTransition),
    ) -> StorageResult<()> {
        let body = self.include_body(address, reference.0)?;
        if body
            .owners
            .try_insert(owner.try_into()?, reference.1)?
            .is_some()
        {
            return Err(invalid_image());
        }
        Ok(())
    }

    fn include_body(
        &mut self,
        address: ImageAddress,
        raw_bytes: usize,
    ) -> StorageResult<&mut BodyOwnershipChanges> {
        if raw_bytes == 0 {
            return Err(invalid_image());
        }
        let body = self
            .bodies
            .try_get_or_insert(image_record_key(address)?, || BodyOwnershipChanges {
                address,
                raw_bytes,
                observation: None,
                owners: OwnershipRecords::default(),
            })?;
        if body.address != address || body.raw_bytes != raw_bytes {
            return Err(invalid_image());
        }
        Ok(body)
    }
}
