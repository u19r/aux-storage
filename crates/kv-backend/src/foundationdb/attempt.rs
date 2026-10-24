use std::sync::Arc;

use foundationdb::{
    RangeOption, Transaction,
    options::{MutationType, StreamingMode},
};
use storage_types::{StorageError, StorageResult};

use super::{
    error::{map_commit_error, map_operation_error},
    input_limits::NativeInputLimits,
    input_preparation::PreparedRange,
    read_window::settle_read_window,
};
use crate::{
    AttemptMode, KvAttempt, KvMutation, KvRange, KvRangePage, KvVersionstampedMutation,
    ReadProtection,
    constants::{FOUNDATIONDB_NATIVE_VALUE_BYTES, FOUNDATIONDB_PIPELINE_REQUESTS},
};

/// One scoped native transaction. All physical keys carry the configured
/// prefix, which is removed again before the KV family sees a range result.
pub struct FoundationDbAttempt {
    settlement: storage_types::NativeAttemptSettlement,
    ownership: stream_provider::retained_image::RetainedOwnershipChanges,
    transaction: Transaction,
    input_limits: NativeInputLimits,
    read_slots: tokio::sync::Semaphore,
    mode: AttemptMode,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

impl FoundationDbAttempt {
    pub(super) fn new(
        transaction: Transaction,
        prefix: Arc<[u8]>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            ownership: Default::default(),
            settlement: storage_types::NativeAttemptSettlement::new(limits),
            transaction,
            input_limits: NativeInputLimits::new(prefix),
            read_slots: tokio::sync::Semaphore::new(FOUNDATIONDB_PIPELINE_REQUESTS),
            mode,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        }
    }

    pub(super) fn require_settlement(&self) -> StorageResult<()> {
        self.settlement.require_open()
    }

    pub(super) async fn commit(self) -> StorageResult<()> {
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::KvCommitBoundary::BeforeCommit
            .observe(self.fault_probe.as_deref())
            .await?;
        let settled = self.transaction.commit().await;
        settled.map_err(map_commit_error)?;
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::KvCommitBoundary::AfterDurableAcknowledgement
            .observe(self.fault_probe.as_deref())
            .await?;
        Ok(())
    }

    pub(super) async fn commit_versionstamped(self) -> StorageResult<Option<[u8; 10]>> {
        let stamp = self.transaction.get_versionstamp();
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::KvCommitBoundary::BeforeCommit
            .observe(self.fault_probe.as_deref())
            .await?;
        let settled = self.transaction.commit().await.map_err(map_commit_error)?;
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::KvCommitBoundary::AfterDurableAcknowledgement
            .observe(self.fault_probe.as_deref())
            .await?;
        let version = settled.committed_version().map_err(|_| {
            StorageError::sync_submission_ambiguous(
                None,
                "FoundationDB committed but its committed version was unavailable",
            )
        })?;
        if version == -1 {
            return Ok(None);
        }
        let committed = stamp.await.map_err(|_| {
            StorageError::sync_submission_ambiguous(
                None,
                "FoundationDB committed but its versionstamp was unavailable",
            )
        })?;
        committed.as_ref().try_into().map(Some).map_err(|_| {
            StorageError::sync_submission_ambiguous(
                None,
                "FoundationDB committed with an invalid versionstamp",
            )
        })
    }

    async fn acquire_read(&self) -> StorageResult<tokio::sync::SemaphorePermit<'_>> {
        let permit = self
            .read_slots
            .acquire()
            .await
            .map_err(|_| StorageError::internal("FoundationDB read pipeline stopped"))?;
        self.settlement.require_open()?;
        Ok(permit)
    }

    fn snapshot(&self, protection: ReadProtection) -> StorageResult<bool> {
        protection.validate(self.mode)?;
        Ok(protection == ReadProtection::Snapshot)
    }

    async fn scan_range(&self, range: PreparedRange, snapshot: bool) -> StorageResult<KvRangePage> {
        let call = self.settlement.begin_call()?;
        let result = self
            .scan_prepared_range(range, snapshot)
            .await
            .and_then(|page| {
                crate::work_admission::NativeReadWork::from(&self.settlement)
                    .charge_pages(std::slice::from_ref(&page))?;
                Ok(page)
            });
        call.complete(result)
    }

    async fn scan_prepared_range(
        &self,
        range: PreparedRange,
        snapshot: bool,
    ) -> StorageResult<KvRangePage> {
        let mut range = range.0;
        if range.begin >= range.end {
            return Ok(KvRangePage {
                entries: Vec::new(),
                more: false,
            });
        }
        let _read = self.acquire_read().await?;
        let mut buffer = crate::range::KvRangeBuffer::default();
        loop {
            let mut option = RangeOption::from((range.begin.as_slice(), range.end.as_slice()));
            option.limit = Some(buffer.remaining_rows(&range));
            option.target_bytes = buffer.remaining_bytes(&range);
            option.mode = StreamingMode::Exact;
            option.reverse = range.reverse;
            self.settlement.require_open()?;
            let rows = self
                .transaction
                .get_range(&option, 1, snapshot)
                .await
                .map_err(map_operation_error)?;
            let native_more = rows.more();
            let byte_limited =
                !self.append_range_rows(&rows, &range, (&range.begin, &range.end), &mut buffer)?;
            let more = native_more || byte_limited;
            if !more || buffer.is_full(&range) || byte_limited {
                return Ok(buffer.into_page(more));
            }
            let last_key = rows
                .last()
                .ok_or_else(|| StorageError::internal("FoundationDB range page cannot advance"))?
                .key();
            let mut next_key = Vec::new();
            next_key
                .try_reserve_exact(last_key.len() + 1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            next_key.extend_from_slice(last_key);
            if range.reverse {
                range.end = next_key;
            } else {
                range.begin = next_key;
                range.begin.push(0);
            }
        }
    }

    fn append_range_rows(
        &self,
        rows: &foundationdb::future::FdbValues,
        range: &KvRange,
        bounds: (&[u8], &[u8]),
        buffer: &mut crate::range::KvRangeBuffer,
    ) -> StorageResult<bool> {
        for row in rows {
            NativeInputLimits::row(row.key(), row.value(), bounds.0, bounds.1)?;
            let key = self.input_limits.logical_key(row.key())?;
            if !buffer.append(range, key, row.value())? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl KvAttempt for FoundationDbAttempt {
    fn settlement(&self) -> &storage_types::NativeAttemptSettlement {
        &self.settlement
    }

    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges {
        &mut self.ownership
    }

    async fn native_has_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_data(
                begin
                    .len()
                    .checked_add(end.len())
                    .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?,
            )?;
            let snapshot = self.snapshot(protection)?;
            let (begin, end) = self.input_limits.prepare_bounds(begin, end)?;
            if begin >= end {
                return Ok(false);
            }
            if !snapshot {
                self.transaction
                    .add_conflict_range(
                        &begin,
                        &end,
                        foundationdb::options::ConflictRangeType::Read,
                    )
                    .map_err(map_operation_error)?;
            }
            let selector = foundationdb::KeySelector::first_greater_or_equal(begin.as_slice());
            let _read = self.acquire_read().await?;
            let key = self
                .transaction
                .get_key(&selector, true)
                .await
                .map_err(map_operation_error)?;
            Ok(key.as_ref() >= begin.as_slice() && key.as_ref() < end.as_slice())
        }
        .await;
        call.complete(result)
    }

    fn backend_engine(&self) -> storage_common::diagnostics::NativeEngine {
        storage_common::diagnostics::NativeEngine::FoundationDb
    }

    #[cfg(feature = "fault-injection")]
    fn fault_probe(&self) -> Option<&dyn crate::KvFaultProbe> {
        self.fault_probe.as_deref()
    }

    fn native_value_limit(&self) -> usize {
        FOUNDATIONDB_NATIVE_VALUE_BYTES
    }

    async fn native_get_batch(
        &self,
        keys: &[Vec<u8>],
        protection: ReadProtection,
        mut allowance: crate::KvValueAllowance,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            crate::work_admission::NativeReadWork::from(&self.settlement).charge_keys(keys)?;
            let snapshot = self.snapshot(protection)?;
            let keys = self.input_limits.prepare_points(keys)?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut keys = keys.iter();
            while keys.len() != 0 {
                self.settlement.require_open()?;
                let window = std::array::from_fn(|_| keys.next());
                let results = settle_read_window(window, |key| async move {
                    let call = self.settlement.begin_call()?;
                    let result = async {
                        let _read = self.acquire_read().await?;
                        self.transaction
                            .get(key, snapshot)
                            .await
                            .map_err(map_operation_error)
                    }
                    .await;
                    call.complete(result)
                })
                .await;
                for result in results.into_iter().flatten() {
                    let value = result?;
                    if let Some(bytes) = &value {
                        NativeInputLimits::observed_value(bytes.as_ref())?;
                    }
                    values.push(allowance.copy_value(value.as_ref().map(|bytes| bytes.as_ref()))?);
                }
            }
            crate::work_admission::NativeReadWork::from(&self.settlement).charge_values(&values)?;
            Ok(values)
        }
        .await;
        call.complete(result)
    }

    async fn native_scan(
        &self,
        range: KvRange,
        protection: ReadProtection,
    ) -> StorageResult<KvRangePage> {
        let call = self.settlement.begin_call()?;
        let result = async {
            range.charge_work(&self.settlement)?;
            let snapshot = self.snapshot(protection)?;
            let range = self.input_limits.prepare_range(range)?;
            self.scan_range(range, snapshot).await
        }
        .await;
        call.complete(result)
    }

    async fn native_scan_batch(
        &self,
        ranges: Vec<KvRange>,
        protection: ReadProtection,
    ) -> StorageResult<Vec<KvRangePage>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            for range in &ranges {
                range.charge_work(&self.settlement)?;
            }
            let snapshot = self.snapshot(protection)?;
            let ranges = self.input_limits.prepare_ranges(ranges)?;
            let mut pages = Vec::new();
            pages
                .try_reserve_exact(ranges.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut ranges = ranges.into_iter();
            while ranges.len() != 0 {
                self.settlement.require_open()?;
                let window = std::array::from_fn(|_| ranges.next());
                let results =
                    settle_read_window(window, |range| self.scan_range(range, snapshot)).await;
                for result in results.into_iter().flatten() {
                    pages.push(result?);
                }
            }
            Ok(pages)
        }
        .await;
        call.complete(result)
    }

    fn native_stage(&mut self, changes: Vec<KvMutation>) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.settlement.charge_calls(changes.len())?;
            for change in &changes {
                change.charge_work(&self.settlement)?;
            }
            self.mode.require_write()?;
            self.input_limits
                .prepare_mutations(changes)?
                .stage(&self.transaction);
            Ok(())
        })();
        call.complete(result)
    }

    async fn native_read_version(&self) -> StorageResult<Option<u64>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            let _read = self.acquire_read().await?;
            let version = self
                .transaction
                .get_read_version()
                .await
                .map_err(map_operation_error)?;
            u64::try_from(version)
                .map(Some)
                .map_err(|_| StorageError::internal("Invalid native read version"))
        }
        .await;
        call.complete(result)
    }

    fn native_stage_byte_max(&mut self, key: Vec<u8>, value: Vec<u8>) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.settlement.charge_data(
                key.len()
                    .checked_add(value.len())
                    .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?,
            )?;
            self.mode.require_write()?;
            self.input_limits.point(&key)?;
            NativeInputLimits::value(&value)?;
            let key = self.input_limits.prefix_key(key)?;
            self.transaction
                .atomic_op(&key, &value, MutationType::ByteMax);
            Ok(())
        })();
        call.complete(result)
    }

    fn native_stage_versionstamped_value(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        stamp_offset: u32,
    ) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.settlement.charge_data(
                key.len()
                    .checked_add(value.len())
                    .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?,
            )?;
            self.mode.require_write()?;
            let (key, value) = self
                .input_limits
                .prepare_stamped_value(key, value, stamp_offset)?;
            self.transaction
                .atomic_op(&key, &value, MutationType::SetVersionstampedValue);
            Ok(())
        })();
        call.complete(result)
    }

    fn native_stage_versionstamped(
        &mut self,
        changes: Vec<KvVersionstampedMutation>,
    ) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.settlement.charge_calls(changes.len())?;
            for change in &changes {
                change.charge_work(&self.settlement)?;
            }
            self.mode.require_write()?;
            self.input_limits
                .prepare_stamped_mutations(changes)?
                .stage(&self.transaction)?;
            Ok(())
        })();
        call.complete(result)
    }
}
