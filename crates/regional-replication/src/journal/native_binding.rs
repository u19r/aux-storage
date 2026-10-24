//! One regional origin owner for the initialized native record namespace.
use infrastructure_provider::record::{
    DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
};
use replication_types::identity::{
    journal_partition::JournalPartitionCount,
    replica::{DatabaseGroupId, RegionId},
    resource::TenantId,
};
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageError, StorageResult};

use super::JournalStore;
use crate::{
    constants::{
        HISTORY_CAPACITY_ROOT_GENERATION, NATIVE_JOURNAL_BINDING_RESOURCE,
        NATIVE_JOURNAL_BINDING_SUFFIX, REGIONAL_RECORD_NAMESPACE,
    },
    native_record::corrupt_record,
};

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeJournalBinding {
    physical_identity: [u8; 16],
    region: RegionId,
    group: DatabaseGroupId,
    tenant: TenantId,
    partitions: JournalPartitionCount,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeArchiveBinding {
    identity: [u8; 16],
    physical_identity: [u8; 16],
}

impl JournalStore {
    pub(crate) async fn bind_archive<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        physical_identity: [u8; 16],
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let keys = [archive_binding_key()?, native_binding_key()?];
            let rows = self
                .records
                .load(attempt, &keys, storage_types::ReadProtection::Conflict)
                .await?;
            if self
                .prepare_native_binding(rows[1].as_ref(), physical_identity)?
                .is_some()
            {
                return Err(corrupt_record());
            }
            if let Some(change) = self.prepare_archive_registration(rows[0].as_ref())? {
                self.records.store(attempt, &[change]).await?;
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn observe_archive_binding<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        physical_identity: [u8; 16],
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = [archive_binding_key()?, native_binding_key()?];
            let rows = self
                .records
                .load(attempt, &keys, storage_types::ReadProtection::Snapshot)
                .await?;
            if self
                .prepare_native_binding(rows[1].as_ref(), physical_identity)?
                .is_some()
            {
                return Err(corrupt_record());
            }
            if rows[0].is_none() {
                return Ok(self.archive.get().is_none());
            }
            self.prepare_archive_registration(rows[0].as_ref())
                .map(|change| change.is_none())
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_archive_registration(
        &self,
        row: Option<&DurableRecord>,
    ) -> StorageResult<Option<RecordChange>> {
        let binding = row
            .map(|row| {
                let binding: NativeArchiveBinding = self.records.decode(row)?;
                if !is_archive_binding_key(&row.key)
                    || row.revision != Revision::INITIAL
                    || (binding.identity == [0; 16] || binding.physical_identity == [0; 16])
                {
                    return Err(corrupt_record());
                }
                Ok(binding)
            })
            .transpose()?;
        match (binding, self.archive.get()) {
            (None, None) => Ok(None),
            (Some(binding), Some(archive)) => {
                if binding.identity != archive.identity()
                    || binding.physical_identity != archive.physical_identity()
                {
                    return Err(StorageError::validation(
                        "Archive directory does not match the native database custody binding",
                    ));
                }
                Ok(None)
            }
            (Some(_), None) => Err(StorageError::validation(
                "Native archive custody requires its configured directory",
            )),
            (None, Some(archive)) => self
                .prepare_archive_binding((archive.identity(), archive.physical_identity()))
                .map(Some),
        }
    }

    pub(super) fn prepare_archive_binding(
        &self,
        binding: ([u8; 16], [u8; 16]),
    ) -> StorageResult<RecordChange> {
        self.records.prepare(
            archive_binding_key()?,
            None,
            &NativeArchiveBinding {
                identity: binding.0,
                physical_identity: binding.1,
            },
        )
    }

    pub(super) fn prepare_native_binding(
        &self,
        row: Option<&DurableRecord>,
        physical_identity: [u8; 16],
    ) -> StorageResult<Option<RecordChange>> {
        if physical_identity == [0; 16] {
            return Err(corrupt_record());
        }
        let expected = NativeJournalBinding {
            physical_identity,
            region: self.region.clone(),
            group: self.group,
            tenant: self.tenant,
            partitions: self.partitions,
        };
        if let Some(row) = row {
            if !is_native_binding_key(&row.key) || row.revision != Revision::INITIAL {
                return Err(corrupt_record());
            }
            if self.records.decode::<NativeJournalBinding>(row)? != expected {
                return Err(StorageError::validation(
                    "Regional configuration does not match the native database binding",
                ));
            }
            return Ok(None);
        }
        self.records
            .prepare(native_binding_key()?, None, &expected)
            .map(Some)
    }
}

pub(super) fn native_binding_key() -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: REGIONAL_RECORD_NAMESPACE.into(),
        resource: NATIVE_JOURNAL_BINDING_RESOURCE.to_vec(),
        generation: HISTORY_CAPACITY_ROOT_GENERATION,
        suffix: NATIVE_JOURNAL_BINDING_SUFFIX.to_vec(),
    }
    .try_into()
}

pub(crate) fn is_native_binding_key(key: &DurableRecordKey) -> bool {
    let address = key.as_ref();
    address.namespace == REGIONAL_RECORD_NAMESPACE
        && address.resource == NATIVE_JOURNAL_BINDING_RESOURCE
        && address.generation == HISTORY_CAPACITY_ROOT_GENERATION
        && address.suffix == NATIVE_JOURNAL_BINDING_SUFFIX
}

fn archive_binding_key() -> StorageResult<DurableRecordKey> {
    let mut address: RecordAddress = native_binding_key()?.into();
    address.suffix = crate::constants::NATIVE_ARCHIVE_BINDING_SUFFIX.to_vec();
    address.try_into()
}

pub(crate) fn is_archive_binding_key(key: &DurableRecordKey) -> bool {
    let address = key.as_ref();
    address.namespace == REGIONAL_RECORD_NAMESPACE
        && address.resource == NATIVE_JOURNAL_BINDING_RESOURCE
        && address.generation == HISTORY_CAPACITY_ROOT_GENERATION
        && address.suffix == crate::constants::NATIVE_ARCHIVE_BINDING_SUFFIX
}
