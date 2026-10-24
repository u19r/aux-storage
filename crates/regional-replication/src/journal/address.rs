use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use replication_types::{
    identity::{journal_partition::JournalPartition, origin::OriginId},
    protocol::CanonicalDigest,
};
use serde::Serialize;
use storage_types::StorageResult;

use super::store::JournalStore;
use crate::{constants::REGIONAL_RECORD_NAMESPACE, native_record::corrupt_record};

impl JournalStore {
    pub(super) fn record_prefix_range(
        &self,
        first: &DurableRecordKey,
    ) -> StorageResult<infrastructure_provider::page::BoundedRecordRange> {
        let mut upper = first.as_ref().clone();
        let index = upper
            .suffix
            .iter()
            .rposition(|byte| *byte != u8::MAX)
            .ok_or_else(corrupt_record)?;
        upper.suffix[index] += 1;
        upper.suffix.truncate(index + 1);
        infrastructure_provider::page::BoundedRecordRange::try_new(
            first.clone(),
            upper.try_into()?,
            self.records.limits.max_records(),
        )
    }

    pub(super) fn record_key(&self, suffix: Vec<u8>) -> StorageResult<DurableRecordKey> {
        Self::build_record_key((&self.region, self.group, self.generation), suffix)
    }

    pub(super) fn build_record_key(
        identity: (
            &replication_types::identity::replica::RegionId,
            replication_types::identity::replica::DatabaseGroupId,
            replication_types::identity::origin::JournalGeneration,
        ),
        suffix: Vec<u8>,
    ) -> StorageResult<DurableRecordKey> {
        let region = identity.0.as_ref().as_bytes();
        let mut resource = Vec::with_capacity(region.len() + 1 + identity.1.as_ref().len());
        resource.extend_from_slice(region);
        resource.push(0);
        resource.extend_from_slice(identity.1.as_ref());
        RecordAddress {
            namespace: REGIONAL_RECORD_NAMESPACE.into(),
            resource,
            generation: identity.2.into(),
            suffix,
        }
        .try_into()
    }

    pub(super) fn identity_key<T: Serialize>(
        &self,
        prefix: &[u8],
        identity: &T,
    ) -> StorageResult<DurableRecordKey> {
        let serialized_digest = CanonicalDigest::of_json(identity).map_err(|_| corrupt_record())?;
        let mut suffix = prefix.to_vec();
        suffix.extend_from_slice(serialized_digest.as_ref());
        self.record_key(suffix)
    }

    pub(super) fn source_key(
        &self,
        origin: &OriginId,
        position: replication_types::identity::origin::OriginPosition,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self
            .identity_key(crate::constants::SOURCE_SUFFIX, origin)?
            .into();
        key.suffix
            .extend_from_slice(&u64::from(position).to_be_bytes());
        key.try_into()
    }

    pub(super) fn partition_key(
        &self,
        partition: JournalPartition,
    ) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::PARTITION_SUFFIX.to_vec();
        suffix.extend_from_slice(&u16::from(partition).to_be_bytes());
        self.record_key(suffix)
    }

    pub(crate) fn is_local_origin(&self, origin: &OriginId) -> bool {
        origin.region == self.region
            && origin.database_group == self.group
            && origin.journal_generation == self.generation
    }

    pub fn origin(&self, partition: JournalPartition) -> StorageResult<OriginId> {
        self.partitions
            .require_partition(partition)
            .map_err(|_| corrupt_record())?;
        Ok(OriginId {
            region: self.region.clone(),
            database_group: self.group,
            journal_partition: partition,
            journal_generation: self.generation,
        })
    }
}
