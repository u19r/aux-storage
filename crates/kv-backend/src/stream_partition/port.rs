//! Physical family records and per-publisher samples; all policy is shared.
use std::{num::NonZeroU32, sync::Arc};

use storage_common::{
    PARTITION_SAMPLE_PAGE_LIMIT,
    partition::{PartitionLoadSampleRecord, ResolvedPartitionFamily},
};
use storage_provider::maintenance::{
    PartitionPort, PartitionReconciliationRepository, PartitionReconciliationSource,
};
use storage_types::{StorageError, StorageResult, StreamName, storage_serde};

use super::{
    key_layout::{sample_key, scoped_key, state_key},
    state::{decode_state, load_state},
};
use crate::{
    KvAttempt, KvBackend, KvMutation, KvRange, KvStore, ReadProtection,
    catalog::KvCatalogAttempt,
    constants::{PARTITION_SAMPLE_PREFIX, STREAM_READ_BYTES},
    stream_write::key_layout::read_segment,
};

impl<B: KvBackend + 'static> PartitionReconciliationSource for KvStore<B> {
    fn admitted_partition_reconciliation(
        self: Arc<Self>,
        binding: Arc<storage_provider::native_admission::NativeAdmissionBinding>,
    ) -> StorageResult<Option<Arc<dyn PartitionReconciliationRepository>>> {
        Ok(Some(Arc::new(
            storage_provider::maintenance::AdmittedPartitionRepository::new(self, binding),
        )))
    }

    fn partition_reconciliation(
        self: Arc<Self>,
    ) -> Option<Arc<dyn PartitionReconciliationRepository>> {
        Some(self)
    }
}

impl<A: KvAttempt> PartitionPort for KvCatalogAttempt<'_, A> {
    type SampleFence = Vec<Vec<u8>>;

    async fn scan_partition_sample_stream(
        &mut self,
        after: Option<&StreamName>,
    ) -> StorageResult<Option<StreamName>> {
        let mut range = KvRange::try_for_prefix(
            PARTITION_SAMPLE_PREFIX.to_vec(),
            NonZeroU32::MIN,
            STREAM_READ_BYTES,
        )?;
        if let Some(after) = after {
            range.begin =
                crate::range::prefix_upper_bound(&scoped_key(PARTITION_SAMPLE_PREFIX, after)?)
                    .ok_or_else(|| {
                        StorageError::internal("partition sample prefix has no upper bound")
                    })?;
        }
        let page = self.attempt.scan(range, ReadProtection::Conflict).await?;
        let Some(entry) = page.entries.into_iter().next() else {
            return Ok(None);
        };
        let key = entry
            .key
            .strip_prefix(PARTITION_SAMPLE_PREFIX)
            .ok_or_else(|| StorageError::internal("partition sample prefix differs"))?;
        let (stream, _) = read_segment(key)?;
        Ok(Some(StreamName::new(stream)))
    }

    async fn load_partition_family(
        &mut self,
        stream: &StreamName,
    ) -> StorageResult<ResolvedPartitionFamily> {
        load_state(self.attempt, stream, ReadProtection::Conflict).await
    }

    async fn load_partition_samples(
        &mut self,
        stream: &StreamName,
    ) -> StorageResult<storage_provider::maintenance::PartitionSamplePage<Self::SampleFence>> {
        let prefix = scoped_key(PARTITION_SAMPLE_PREFIX, stream)?;
        let range = KvRange::try_for_prefix(
            prefix,
            NonZeroU32::MIN.saturating_add(PARTITION_SAMPLE_PAGE_LIMIT - 1),
            STREAM_READ_BYTES,
        )?;
        let key = state_key(stream)?;
        let mut end = key.clone();
        end.push(0);
        let family_range = KvRange {
            entry_bounds: None,
            begin: key.clone(),
            end,
            reverse: false,
            limit: NonZeroU32::MIN,
            max_bytes: STREAM_READ_BYTES,
        };
        let mut pages = self
            .attempt
            .scan_batch(vec![family_range, range], ReadProtection::Conflict)
            .await?
            .into_iter();
        let family_page = pages
            .next()
            .ok_or_else(|| StorageError::internal("partition family page missing"))?;
        let page = pages
            .next()
            .ok_or_else(|| StorageError::internal("partition sample page missing"))?;
        let family = family_page
            .entries
            .into_iter()
            .next()
            .map(|entry| {
                if entry.key != key {
                    return Err(StorageError::internal("partition family identity differs"));
                }
                decode_state(&entry.value)
            })
            .transpose()?;
        let mut records = Vec::with_capacity(page.entries.len());
        let mut keys = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let record = storage_serde::from_bytes(&entry.value)?;
            if sample_key(stream, &record)? != entry.key {
                return Err(StorageError::internal("partition sample identity differs"));
            }
            records.push(record);
            keys.push(entry.key);
        }
        Ok(storage_provider::maintenance::PartitionSamplePage {
            family,
            records,
            more: page.more,
            consumed: keys,
        })
    }

    async fn store_partition_reconciliation(
        &mut self,
        stream: &StreamName,
        family: Option<&ResolvedPartitionFamily>,
        consumed: Self::SampleFence,
    ) -> StorageResult<()> {
        let mut changes: Vec<_> = consumed
            .into_iter()
            .map(|key| KvMutation::Delete { key })
            .collect();
        if let Some(family) = family {
            changes.push(KvMutation::Put {
                key: state_key(stream)?,
                value: storage_serde::to_bytes(family)?,
            });
        }
        self.attempt.stage(changes)
    }

    async fn store_partition_samples(
        &mut self,
        stream: &StreamName,
        records: &[PartitionLoadSampleRecord],
    ) -> StorageResult<()> {
        let changes = records
            .iter()
            .map(|record| {
                Ok(KvMutation::Put {
                    key: sample_key(stream, record)?,
                    value: storage_serde::to_bytes(record)?,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        self.attempt.stage(changes)
    }
}
