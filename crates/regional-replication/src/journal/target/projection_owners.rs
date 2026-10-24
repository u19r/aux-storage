use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordAddress},
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::{content::CanonicalEventCodec, identity::CanonicalItemKey},
    identity::resource::ControlOperationId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_backfill::PreparedGsiPut;
use storage_provider::{GsiProjectionMetadataAttempt, NativeProjectionAddress};
use storage_types::{BoundTableInfo, IndexerDeclaration, ReadProtection, StorageResult};

use crate::{
    constants::PROJECTION_OWNER_SUFFIX,
    journal::{JournalStore, reconstruction::WinnerAuthority},
    native_record::corrupt_record,
};

/// Derived operation-local ownership only; immutable history retains images.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionOwner {
    operation: ControlOperationId,
    address: NativeProjectionAddress,
    key: CanonicalItemKey,
}

impl JournalStore {
    fn projection_owner_key(
        &self,
        operation: ControlOperationId,
        address: &NativeProjectionAddress,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self
            .identity_key(PROJECTION_OWNER_SUFFIX, &operation)?
            .into();
        let serialized_digest = CanonicalDigest::of_json(address).map_err(|_| corrupt_record())?;
        key.suffix.extend_from_slice(serialized_digest.as_ref());
        key.try_into()
    }

    pub(super) async fn register_projection_authority<
        A: PhysicalRecordAttempt + GsiProjectionMetadataAttempt,
    >(
        &self,
        attempt: &mut A,
        target: (ControlOperationId, &BoundTableInfo),
        input: (&WinnerAuthority, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let images = input.0.images()?;
            let key = input
                .1
                .encode_key(input.0.key()?)
                .map_err(|_| corrupt_record())?;
            for (image, indexers) in [
                (images.old, images.old_indexers),
                (images.new, images.new_indexers),
            ] {
                let Some(image) = image else {
                    continue;
                };
                let declaration = IndexerDeclaration::try_new(
                    indexers.unwrap_or_default().to_vec(),
                    target.1.max_indexers,
                )?;
                let prepared = PreparedGsiPut::try_new(&target.1.info, image, &declaration)?;
                let keys = prepared
                    .rows()
                    .map(|row| row.key.clone())
                    .collect::<Vec<_>>();
                let addresses = attempt
                    .encode_gsi_projection_addresses(target.1, &keys)
                    .await?;
                if addresses.len() != keys.len() {
                    return Err(corrupt_record());
                }
                for address in addresses {
                    self.retain_projection_owner(attempt, target.0, (address, &key))
                        .await?;
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn retain_projection_owner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        input: (NativeProjectionAddress, &CanonicalItemKey),
    ) -> StorageResult<()> {
        let key = self.projection_owner_key(operation, &input.0)?;
        let rows = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?;
        if let Some(row) = &rows[0] {
            let existing: ProjectionOwner = self.records.decode(row)?;
            if existing.operation != operation
                || existing.address != input.0
                || &existing.key != input.1
            {
                return Err(corrupt_record());
            }
        } else {
            let owner = ProjectionOwner {
                operation,
                address: input.0,
                key: input.1.clone(),
            };
            self.records
                .store(attempt, &[self.records.prepare(key, None, &owner)?])
                .await?;
        }
        Ok(())
    }

    pub(super) async fn load_projection_owner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        address: &NativeProjectionAddress,
    ) -> StorageResult<CanonicalItemKey> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.projection_owner_key(operation, address)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let owner: ProjectionOwner = self
                .records
                .decode(rows[0].as_ref().ok_or_else(corrupt_record)?)?;
            if owner.operation != operation || &owner.address != address {
                return Err(corrupt_record());
            }
            Ok(owner.key)
        }
        .await;
        settlement.complete(result)
    }
}
