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

    /// Keep each physical table's immutable copies contiguous within a family.
    pub(super) fn table_identity_key<T: Serialize>(
        &self,
        prefix: &[u8],
        history: storage_types::TableHistoryId,
        identity: &T,
    ) -> StorageResult<DurableRecordKey> {
        let digest = CanonicalDigest::of_json(identity).map_err(|_| corrupt_record())?;
        let mut suffix = Vec::with_capacity(prefix.len() + 16 + 32);
        suffix.extend_from_slice(prefix);
        suffix.extend_from_slice(history.as_ref());
        suffix.extend_from_slice(digest.as_ref());
        self.record_key(suffix)
    }

    pub(in crate::journal) fn select_original_history(
        &self,
        tables: &[(
            replication_types::identity::resource::TableIdentity,
            replication_types::identity::resource::TableLineage,
            storage_types::TableHistoryId,
        )],
        source: (
            replication_types::identity::resource::TableIdentity,
            replication_types::identity::resource::TableLineage,
        ),
    ) -> StorageResult<storage_types::TableHistoryId> {
        tables
            .iter()
            .find(|table| (table.0, table.1) == source)
            .map(|table| table.2)
            .ok_or_else(corrupt_record)
    }

    pub(in crate::journal) fn table_record_history(
        &self,
        key: &DurableRecordKey,
        prefix: &[u8],
    ) -> StorageResult<storage_types::TableHistoryId> {
        let suffix = key
            .as_ref()
            .suffix
            .strip_prefix(prefix)
            .ok_or_else(corrupt_record)?;
        let bytes: [u8; 16] = suffix
            .get(..16)
            .ok_or_else(corrupt_record)?
            .try_into()
            .map_err(|_| corrupt_record())?;
        if bytes == [0; 16] {
            return Err(corrupt_record());
        }
        Ok(storage_types::TableHistoryId::from(bytes))
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
