//! Shared physical bodies acquire regional custody charges independently of
//! creation.
use infrastructure_provider::{
    batch::RecordBatch,
    record::{DurableRecordKey, RecordAddress, RecordChange},
};
use storage_types::{ImageAddress, ReadProtection, StorageResult};
use stream_provider::retained_image::ImageChargeCustody;

use crate::native_record::{NativeRecords, corrupt_record};

struct PreparedImageCharge {
    custody: ImageChargeCustody,
    change: RecordChange,
    bytes: u64,
}

impl<A: stream_provider::retained_image::RetainedOwnershipAttempt>
    stream_provider::retained_image::RetainedImageCustody<A> for NativeRecords
{
    async fn retain(
        &self,
        attempt: &mut A,
        bodies: &[stream_provider::retained_image::PreparedImageCustody],
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let prepared = self.prepare_image_charges(bodies)?;
            let mut keys = Vec::new();
            keys.try_reserve_exact(prepared.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for entry in &prepared {
                keys.push(entry.change.key().try_into()?);
            }
            let waves = self.read_waves(&keys, |key| key)?;
            let mut entries = prepared.into_iter();
            let mut receipts = Vec::new();
            receipts
                .try_reserve_exact(bodies.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut bytes = 0u64;
            for wave in waves {
                let rows = self
                    .load_batch(attempt, &keys[wave], ReadProtection::Conflict, self.limits)
                    .await?;
                for row in rows {
                    let entry = entries.next().ok_or_else(corrupt_record)?;
                    if let Some(receipt) = row {
                        if receipt.revision != storage_types::Revision::INITIAL
                            || ImageChargeCustody::decode(
                                &receipt.bytes,
                                self.limits.max_record_bytes().get(),
                            )? != entry.custody
                        {
                            return Err(corrupt_record());
                        }
                    } else {
                        receipts.push(entry.change);
                        bytes = bytes.checked_add(entry.bytes).ok_or_else(corrupt_record)?;
                    }
                    entry.custody.stage(attempt)?;
                }
            }
            if entries.next().is_some() {
                return Err(corrupt_record());
            }
            if receipts.is_empty() {
                return Ok(());
            }
            let charge = self
                .prepare_history_charge(attempt, (&receipts, bytes))
                .await?;
            let waves =
                RecordBatch::plan_waves([charge.as_slice(), receipts.as_slice()], self.limits)?;
            for batch in waves.into_iter().flatten() {
                self.store_batch(attempt, &batch).await?;
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }
}

impl NativeRecords {
    pub(crate) fn require_image_custody_capacity(&self) -> StorageResult<usize> {
        let address = ImageAddress::try_new(
            storage_types::TableHistoryId::from([u8::MAX; 16]),
            storage_types::ImmutableImageId::from([u8::MAX; 32]),
        )?;
        let key = self.image_charge_key(address)?;
        let custody = ImageChargeCustody::try_new(
            self.capacity.root_key().try_into()?,
            (&key).try_into()?,
            address,
            u64::MAX,
        )?;
        let encoded = custody.encoded_length_bound(self.limits.max_record_bytes().get())?;
        if encoded > self.limits.max_record_bytes().get() {
            return Err(corrupt_record());
        }
        let bodies = stream_provider::MAX_RETAINED_MANIFEST_BODIES;
        let bytes = self
            .limits
            .validate_key(&key)?
            .checked_add(encoded)
            .ok_or_else(corrupt_record)?;
        self.limits.check_batch(1, bytes)?;
        bytes.checked_mul(bodies).ok_or_else(corrupt_record)
    }

    fn prepare_image_charges(
        &self,
        bodies: &[stream_provider::retained_image::PreparedImageCustody],
    ) -> StorageResult<Vec<PreparedImageCharge>> {
        let mut prepared = Vec::new();
        let mut keys = std::collections::HashSet::new();
        prepared
            .try_reserve_exact(bodies.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        keys.try_reserve(bodies.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut total = 0u64;
        for body in bodies {
            let key = self.image_charge_key(body.address())?;
            if !keys.insert((
                *body.address().history().as_ref(),
                body.address().identity(),
            )) {
                return Err(corrupt_record());
            }
            let bytes = u64::try_from(body.physical_bytes()).map_err(|_| corrupt_record())?;
            total = total.checked_add(bytes).ok_or_else(corrupt_record)?;
            let custody = ImageChargeCustody::try_new(
                self.capacity.root_key().try_into()?,
                key,
                body.address(),
                bytes,
            )?;
            let change = custody.prepare_change(self.limits.max_record_bytes().get())?;
            change.validate(self.limits)?;
            prepared.push(PreparedImageCharge {
                custody,
                change,
                bytes,
            });
        }
        Ok(prepared)
    }

    fn image_charge_key(&self, address: ImageAddress) -> StorageResult<DurableRecordKey> {
        let mut scope: RecordAddress = DurableRecordKey::try_from(self.capacity.root_key())?.into();
        scope.suffix.clear();
        let bytes = crate::constants::HISTORY_IMAGE_CHARGE_SUFFIX
            .len()
            .checked_add(address.history().as_ref().len())
            .and_then(|bytes| bytes.checked_add(address.identity().as_ref().len()))
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        scope
            .suffix
            .try_reserve_exact(bytes)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        scope
            .suffix
            .extend_from_slice(crate::constants::HISTORY_IMAGE_CHARGE_SUFFIX);
        scope.suffix.extend_from_slice(address.history().as_ref());
        scope.suffix.extend_from_slice(address.identity().as_ref());
        scope.try_into()
    }
}
