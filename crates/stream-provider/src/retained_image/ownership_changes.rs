//! Exact transient ownership facts live for the entire native transaction.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ImageAddress, StorageResult};

use super::{
    RetainedOwnerRecord, address::image_record_key, error::invalid_image,
    ownership_inventory::OwnershipRecords,
};

pub trait RetainedOwnershipAttempt: PhysicalRecordAttempt {
    fn ownership_changes(&mut self) -> &mut RetainedOwnershipChanges;

    /// Complete graph work inside this attempt; native capacity qualification
    /// is separate.
    fn finalize_ownership(&mut self) -> impl Future<Output = StorageResult<()>> + Send
    where Self: Sized {
        async move {
            let mut settlement_guard =
                infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
            let attempt = settlement_guard.as_mut();
            let result = async {
                attempt.ownership_changes().prepare_net()?;
                let phase = std::mem::replace(
                    &mut attempt.ownership_changes().phase,
                    OwnershipPhase::Finalizing,
                );
                if matches!(phase, OwnershipPhase::Finalizing) {
                    return Err(invalid_image());
                }
                if let OwnershipPhase::Planned(plan) = phase
                    && !plan.bodies.is_empty()
                {
                    let limits = plan.work_limits()?;
                    super::ownership_preparation::PreparedOwnershipChanges::prepare(
                        attempt, plan, limits,
                    )
                    .await?
                    .store(attempt, limits)
                    .await?;
                }
                attempt.ownership_changes().phase = OwnershipPhase::Finalized;
                Ok(())
            }
            .await;
            settlement_guard.complete(result)
        }
    }
}

#[derive(Default)]
pub struct RetainedOwnershipChanges {
    pub(super) phase: OwnershipPhase,
    pub(super) owners: OwnershipRecords<OwnerNetChange>,
    pub(super) bodies: OwnershipRecords<PreparedBodyObservation>,
}

#[derive(Default)]
pub(super) enum OwnershipPhase {
    #[default]
    Collecting,
    Planned(super::ownership_batch::OwnershipNetPlan),
    Finalizing,
    Finalized,
}

pub(super) struct OwnerNetChange {
    pub(super) previous: Option<RetainedOwnerRecord>,
    pub(super) next: Option<RetainedOwnerRecord>,
}

pub(super) struct PreparedBodyObservation {
    pub(super) address: ImageAddress,
    pub(super) raw_bytes: usize,
    pub(super) encoded_bytes: usize,
    pub(super) created: bool,
    pub(super) charge: Option<super::ImageChargeCustody>,
}

impl RetainedOwnershipChanges {
    pub(super) fn reserve_bodies(&mut self, count: usize) -> StorageResult<()> {
        self.stage_change(|changes| changes.bodies.try_reserve(count))
    }

    pub fn is_empty(&self) -> bool {
        match &self.phase {
            OwnershipPhase::Collecting => self.owners.is_empty() && self.bodies.is_empty(),
            OwnershipPhase::Planned(plan) => plan.bodies.is_empty(),
            OwnershipPhase::Finalized => true,
            OwnershipPhase::Finalizing => false,
        }
    }

    fn prepare_net(&mut self) -> StorageResult<()> {
        if matches!(&self.phase, OwnershipPhase::Finalizing) {
            return Err(invalid_image());
        }
        if !matches!(&self.phase, OwnershipPhase::Collecting) || self.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(self);
        self.phase = OwnershipPhase::Finalizing;
        self.phase = OwnershipPhase::Planned(pending.try_into()?);
        Ok(())
    }

    pub(super) fn stage_owner(
        &mut self,
        previous: Option<RetainedOwnerRecord>,
        next: Option<RetainedOwnerRecord>,
    ) -> StorageResult<()> {
        self.stage_change(move |changes| {
            let identity = previous
                .as_ref()
                .or(next.as_ref())
                .ok_or_else(invalid_image)?;
            let key = identity.key()?;
            for receipt in previous.iter().chain(next.iter()) {
                if receipt.key()? != key {
                    return Err(invalid_image());
                }
                receipt.require_body_references()?;
            }
            if let Some(staged) = changes.owners.get_mut(&key) {
                if staged.next != previous {
                    return Err(invalid_image());
                }
                staged.next = next;
            } else {
                changes
                    .owners
                    .try_insert(key, OwnerNetChange { previous, next })?;
            }
            Ok(())
        })
    }

    pub(super) fn stage_body(&mut self, body: PreparedBodyObservation) -> StorageResult<()> {
        self.stage_change(move |changes| {
            if body.raw_bytes == 0 || body.encoded_bytes == 0 {
                return Err(invalid_image());
            }
            let key = image_record_key(body.address)?;
            if let Some(staged) = changes.bodies.get(&key) {
                if staged.address != body.address
                    || staged.raw_bytes != body.raw_bytes
                    || staged.encoded_bytes != body.encoded_bytes
                    || body.created
                {
                    return Err(invalid_image());
                }
            } else {
                changes.bodies.try_insert(key, body)?;
            }
            Ok(())
        })
    }

    pub(super) fn stage_charge(
        &mut self,
        charge: &super::ImageChargeCustody,
        address: ImageAddress,
    ) -> StorageResult<()> {
        self.stage_change(|changes| {
            let key = image_record_key(address)?;
            let observation = changes.bodies.get_mut(&key).ok_or_else(invalid_image)?;
            if observation
                .charge
                .as_ref()
                .is_some_and(|existing| existing != charge)
            {
                return Err(invalid_image());
            }
            observation.charge = Some(charge.try_into()?);
            Ok(())
        })
    }

    /// A failed stage may follow native receipt/body effects. Even if its
    /// caller catches the error, this attempt must never accept those
    /// partial effects.
    fn stage_change(
        &mut self,
        change: impl FnOnce(&mut Self) -> StorageResult<()>,
    ) -> StorageResult<()> {
        let result = if matches!(&self.phase, OwnershipPhase::Collecting) {
            change(self)
        } else {
            Err(invalid_image())
        };
        if result.is_err() {
            self.phase = OwnershipPhase::Finalizing;
        }
        result
    }
}
