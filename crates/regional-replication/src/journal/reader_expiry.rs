//! Deadline-ordered native settlement; expiry never reclaims canonical history.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordChange},
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use super::{
    JournalStore,
    reader_hold::{ReaderHoldKind, ReaderHoldReference, RetentionHoldReference},
};
use crate::{history::retention::CheckpointRetention, native_record::corrupt_record};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct ReaderExpiryEntry {
    pub reference: ReaderHoldReference,
    pub deadline: TimestampMillis,
}

impl JournalStore {
    pub(in crate::journal) fn prepare_reader_expiry(
        &self,
        reference: ReaderHoldReference,
        retention: CheckpointRetention,
    ) -> StorageResult<Option<RecordChange>> {
        let CheckpointRetention::Until(deadline) = retention else {
            return Ok(None);
        };
        let entry = ReaderExpiryEntry {
            reference,
            deadline,
        };
        self.records
            .prepare(self.reader_expiry_key(entry)?, None, &entry)
            .map(Some)
    }

    pub(crate) async fn settle_next_reader_expiry<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observed_at: TimestampMillis,
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let first = self.record_key(crate::constants::READER_EXPIRY_SUFFIX.to_vec())?;
            let mut upper = first.as_ref().clone();
            upper
                .suffix
                .extend_from_slice(&(observed_at.timestamp_millis() ^ i64::MIN).to_be_bytes());
            upper.suffix.push(u8::MAX);
            let range =
                BoundedRecordRange::try_new(first, upper.try_into()?, std::num::NonZeroUsize::MIN)?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after: None,
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Conflict,
                    },
                )
                .await?;
            let Some(row) = page.records.into_iter().next() else {
                return Ok(false);
            };
            let entry: ReaderExpiryEntry = self.records.decode(&row)?;
            if row.revision != storage_types::Revision::INITIAL
                || entry.deadline > observed_at
                || row.key != self.reader_expiry_key(entry)?
            {
                return Err(corrupt_record());
            }
            match entry.reference.kind {
                ReaderHoldKind::Receiver => {
                    self.expire_receiver_hold(attempt, entry, observed_at)
                        .await?
                }
                ReaderHoldKind::Source => {
                    self.expire_source_hold(attempt, entry, observed_at).await?
                }
            }
            Ok(true)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn store_terminal_reader<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (ReaderHoldReference, CheckpointRetention, RecordChange),
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (reference, retention, change) = input;
            let hold_key = self.reader_hold_key(&reference)?;
            let mut keys = vec![hold_key];
            let expiry = match retention {
                CheckpointRetention::Until(deadline) => Some(ReaderExpiryEntry {
                    reference,
                    deadline,
                }),
                CheckpointRetention::ExplicitRelease => None,
            };
            if let Some(entry) = expiry {
                keys.push(self.reader_expiry_key(entry)?);
            }
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            let hold = rows[0].as_ref().ok_or_else(corrupt_record)?;
            if self.decode_retention_hold(hold)? != RetentionHoldReference::Reader(reference) {
                return Err(corrupt_record());
            }
            if let Some(entry) = expiry {
                let row = rows[1].as_ref().ok_or_else(corrupt_record)?;
                if row.revision != storage_types::Revision::INITIAL
                    || self.records.decode::<ReaderExpiryEntry>(row)? != entry
                {
                    return Err(corrupt_record());
                }
            }
            let mut changes = Vec::with_capacity(keys.len() + 1);
            changes.push(change);
            for (key, row) in keys.into_iter().zip(rows) {
                let row = row.ok_or_else(corrupt_record)?;
                if row.key != key {
                    return Err(corrupt_record());
                }
                changes.push(RecordChange::Delete {
                    key,
                    expected: row.revision,
                });
            }
            self.retain_history_epoch(attempt, reference.lineage)
                .await?;
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement_guard.complete(result)
    }

    fn reader_expiry_key(&self, entry: ReaderExpiryEntry) -> StorageResult<DurableRecordKey> {
        let mut suffix = crate::constants::READER_EXPIRY_SUFFIX.to_vec();
        suffix.extend_from_slice(&(entry.deadline.timestamp_millis() ^ i64::MIN).to_be_bytes());
        suffix.push(match entry.reference.kind {
            ReaderHoldKind::Receiver => 1,
            ReaderHoldKind::Source => 2,
        });
        suffix.extend_from_slice(uuid::Uuid::from(entry.reference.operation).as_bytes());
        self.record_key(suffix)
    }
}
