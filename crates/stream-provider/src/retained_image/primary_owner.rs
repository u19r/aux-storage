//! Primary descriptor receipt CAS joins ordinary and imported customer
//! attempts.

use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use storage_types::{
    BoundTableInfo, ItemKey, KeyAttributes, StorageResult, StoredImageSource, TableHistoryId,
    VerifiedWireItem,
};

use super::{RetainedItemRecord, RetainedOwnerKind, RetainedOwnerRecord, error::invalid_image};

pub struct PrimaryImageOwner {
    locator: DurableRecordKey,
    history: TableHistoryId,
}

impl PrimaryImageOwner {
    pub fn try_new(table: &BoundTableInfo, key: &KeyAttributes) -> StorageResult<Self> {
        let suffix = ItemKey::from_key_schema(table.table_name.clone(), &table.key_schema, key)?
            .sorted_storage_suffix()?;
        let mut namespace = String::new();
        let mut resource = Vec::new();
        namespace
            .try_reserve_exact(crate::constants::PRIMARY_IMAGE_LOCATOR_NAMESPACE.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        resource
            .try_reserve_exact(table.history_id.as_ref().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        namespace.push_str(crate::constants::PRIMARY_IMAGE_LOCATOR_NAMESPACE);
        resource.extend_from_slice(table.history_id.as_ref());
        let locator = RecordAddress {
            namespace,
            resource,
            generation: 0,
            suffix,
        }
        .try_into()?;
        Ok(Self {
            locator,
            history: table.history_id,
        })
    }

    pub(super) async fn require_items<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        items: &[(&BoundTableInfo, &storage_types::StoredWireItem)],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut receipts = Vec::new();
            receipts
                .try_reserve_exact(items.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (table, item) in items {
                let mut names = Vec::new();
                names
                    .try_reserve_exact(table.key_schema.len())
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                names.extend(
                    table
                        .key_schema
                        .iter()
                        .map(|element| element.attribute_name.as_str()),
                );
                let values = item.item.select_attribute_values(&names)?;
                let mut key = KeyAttributes::with_capacity(names.len());
                for (name, value) in names.into_iter().zip(values) {
                    key.insert(name, value.ok_or_else(invalid_image)?);
                }
                let owner = Self::try_new(table, &key)?;
                receipts.push(owner.prepare_receipt(item.source.ok_or_else(invalid_image)?)?);
            }
            RetainedOwnerRecord::require_current_batch(attempt, &receipts).await
        }
        .await;
        settlement.complete(result)
    }

    pub async fn replace<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        previous: Option<&VerifiedWireItem>,
        record: &RetainedItemRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let next = self.prepare_receipt(record.image_source()?)?;
            self.transition_receipt(attempt, previous, Some(next)).await
        }
        .await;
        settlement.complete(result)
    }

    pub async fn release<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        previous: Option<&VerifiedWireItem>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async { self.transition_receipt(attempt, previous, None).await }.await;
        settlement.complete(result)
    }

    fn prepare_receipt(&self, source: StoredImageSource) -> StorageResult<RetainedOwnerRecord> {
        if source.descriptor.references()[1] != (source.address, source.raw_bytes.get())
            || source.address.history() != self.history
        {
            return Err(invalid_image());
        }
        let mut bodies = Vec::new();
        bodies
            .try_reserve_exact(source.descriptor.references().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        bodies.extend_from_slice(&source.descriptor.references());
        bodies.sort_unstable_by(|left, right| {
            left.0.identity().as_ref().cmp(right.0.identity().as_ref())
        });
        bodies.dedup();
        Ok(RetainedOwnerRecord {
            kind: RetainedOwnerKind::Primary,
            locator: DurableRecordKey::try_from(&self.locator)?,
            history: self.history,
            descriptor: source.descriptor.digest(),
            bodies,
        })
    }

    async fn transition_receipt<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        previous: Option<&VerifiedWireItem>,
        next: Option<RetainedOwnerRecord>,
    ) -> StorageResult<()> {
        let previous = previous
            .map(|item| self.prepare_receipt(item.stored_source().ok_or_else(invalid_image)?))
            .transpose()?;
        let transition = super::RetainedOwnerTransition::try_new(
            RetainedOwnerKind::Primary,
            DurableRecordKey::try_from(&self.locator)?,
            self.history,
            (previous, next),
        )?;
        let mut transitions = Vec::new();
        transitions
            .try_reserve_exact(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        transitions.push(transition);
        super::RetainedOwnerTransition::prepare_batch(attempt, transitions).await
    }
}
