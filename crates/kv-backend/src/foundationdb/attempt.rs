use std::sync::Arc;

use foundationdb::{
    FdbError, RangeOption, Transaction,
    options::{MutationType, StreamingMode},
};
use storage_common::{
    native_attempt_lifecycle::{AttemptLifecycle, settle_or_drop},
    native_observer::{CommitBoundary, observe_commit_boundary},
    native_outcome::classify_commit,
};
use storage_types::{StorageError, StorageResult};

use super::{
    error::{commit_error, map_operation_error},
    input_limits::NativeInputLimits,
    input_preparation::PreparedRange,
};
use crate::{
    AttemptMode, KvAttempt, KvMutation, KvRange, KvRangePage, KvVersionstampedMutation,
    ReadProtection,
    constants::{FOUNDATIONDB_NATIVE_VALUE_BYTES, FOUNDATIONDB_PIPELINE_REQUESTS},
    fault_injection::{
        KvWaveScope, observe, observe_keys_read, observe_mutations, observe_ranges_read,
        observe_reads,
    },
    native_deadline::NativeDeadline,
};

impl NativeDeadline {
    /// The native relative timer ends at `settle_by`, rounded up. Zero
    /// disables the native timer, so a positive remainder is at least 1 ms.
    pub(super) fn native_timeout_ms(&self) -> i64 {
        let millis = self
            .deadline
            .settle()
            .remaining()
            .as_nanos()
            .div_ceil(1_000_000)
            .max(1);
        i64::try_from(millis).map_or(i64::from(i32::MAX), |millis| {
            millis.min(i64::from(i32::MAX))
        })
    }
}

/// One scoped native transaction. All physical keys carry the configured
/// prefix, which is removed again before the KV family sees a range result.
pub struct FoundationDbAttempt {
    settlement: storage_types::NativeAttemptSettlement,
    transaction: Transaction,
    input_limits: NativeInputLimits,
    read_slots: tokio::sync::Semaphore,
    mode: AttemptMode,
    catalog: crate::native_catalog::NativeCatalogState,
    catalog_pipeline: super::catalog_pipeline::CatalogVersionPipeline,
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    /// The first read fetches the read version: one more wave.
    read_version_observed: std::sync::atomic::AtomicBool,
    requested_cached_read_version: bool,
}

impl FoundationDbAttempt {
    pub(super) fn new(
        transaction: Transaction,
        prefix: Arc<[u8]>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        read_version: storage_types::ReadVersionPolicy,
        fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        if let Some(probe) = fault_probe.as_deref() {
            probe.observe(crate::KvNativeEvent::Begin);
            probe.observe(crate::KvNativeEvent::AttemptRequested { mode, read_version });
        }
        Self {
            settlement: storage_types::NativeAttemptSettlement::new(limits),
            transaction,
            input_limits: NativeInputLimits::new(prefix),
            read_slots: tokio::sync::Semaphore::new(FOUNDATIONDB_PIPELINE_REQUESTS),
            mode,
            catalog: Default::default(),
            catalog_pipeline: Default::default(),
            fault_probe,
            read_version_observed: std::sync::atomic::AtomicBool::new(false),
            requested_cached_read_version: read_version == storage_types::ReadVersionPolicy::Cached,
        }
    }

    /// Reports `Abort` when the callback failed.
    pub(super) fn observe_failure<T>(&self, value: &StorageResult<T>) {
        if let Some(probe) = self.fault_probe.as_deref()
            && value.is_err()
        {
            probe.observe(crate::KvNativeEvent::Abort);
        }
    }

    pub(super) fn require_settlement(&self) -> StorageResult<()> {
        self.settlement.require_open()
    }

    /// Native fixtures set options that production derives elsewhere.
    #[cfg(test)]
    pub(super) fn native_transaction(&self) -> &Transaction {
        &self.transaction
    }

    /// Gates and submits the commit, then waits for its acknowledgement until
    /// `settle_by`. A missing or late acknowledgement is an unknown outcome.
    /// Returns the probe so `commit` can consult it after the transaction is
    /// consumed.
    async fn submit_commit(
        self,
        deadline: &NativeDeadline,
        lifecycle: &AttemptLifecycle,
    ) -> StorageResult<(
        foundationdb::TransactionCommitted,
        Option<Arc<dyn crate::KvFaultProbe>>,
    )> {
        let mode = self.mode;
        if let Some(observer) = lifecycle.observer() {
            observe_commit_boundary(observer, CommitBoundary::BeforeCommit, mode).await?;
        }
        let probe = self.fault_probe;
        let wave = KvWaveScope::start(probe.as_deref(), crate::KvWaveKind::Commit, 0).await?;
        // The one commit-submission point. Returning here destroys the
        // unsubmitted transaction, which discards its staged mutations.
        // `commit` submits eagerly, so no await separates this from submission.
        self.catalog.debug_assert_change_protected();
        let ticket = lifecycle.submit_commit(&deadline.deadline)?;
        let commit = self.transaction.commit();
        // An answer that has arrived wins over the settle timer, also at the
        // bound.
        let settled = settle_or_drop(deadline.clock.as_ref(), deadline.deadline, commit).await;
        KvWaveScope::finish(wave, 0);
        if let Some(probe) = probe.as_deref()
            && let Some(Err(error)) = &settled
        {
            observe_conflicting_keys(probe, &self.input_limits.prefix, error).await;
        }
        let Some(settled) = settled else {
            // The unsettled ticket leaves the lifecycle `CommitSubmitted`.
            return Err(StorageError::commit_unknown(
                "FoundationDB commit was not acknowledged before its deadline",
            ));
        };
        let settled = settled.map_err(FdbError::from);
        // FIN-d: a fault probe replaces an acknowledged answer with a code.
        #[cfg(feature = "fault-injection")]
        let settled = match probe
            .as_deref()
            .and_then(crate::KvFaultProbe::replace_commit_answer)
        {
            Some(code) => settled.and(Err(FdbError::from_code(code))),
            None => settled,
        };
        let settled = ticket.settle(settled.map_err(|error| {
            let native_code = error.code();
            let error = commit_error(error);
            if let Some(probe) = probe.as_deref() {
                probe.observe(crate::KvNativeEvent::CommitFailed {
                    native_code,
                    kind: classify_commit(true, error.clone()).attempt_kind(),
                });
            }
            error
        }))?;
        observe(probe.as_deref(), crate::KvNativeEvent::Commit);
        if let Some(observer) = lifecycle.observer() {
            observe_commit_boundary(observer, CommitBoundary::AfterDurableAcknowledgement, mode)
                .await?;
        }
        Ok((settled, probe))
    }

    pub(super) async fn commit(
        self,
        deadline: &NativeDeadline,
        lifecycle: &AttemptLifecycle,
    ) -> StorageResult<i64> {
        let (settled, probe) = self.submit_commit(deadline, lifecycle).await?;
        if let Some(probe) = probe.as_deref()
            && let Some(error) = probe.fail(crate::KvFaultCall {
                operation: crate::KvFaultOperation::ReadCommittedVersion,
                mutations: 0,
            })
        {
            return Err(committed_version_unavailable(error));
        }
        settled
            .committed_version()
            .map_err(committed_version_unavailable)
    }

    pub(super) async fn commit_versionstamped(
        self,
        deadline: &NativeDeadline,
        lifecycle: &AttemptLifecycle,
    ) -> StorageResult<Option<[u8; 10]>> {
        let stamp = self.transaction.get_versionstamp();
        let (settled, _) = self.submit_commit(deadline, lifecycle).await?;
        let version = settled.committed_version().map_err(|_| {
            StorageError::commit_unknown(
                "FoundationDB committed but its committed version was unavailable",
            )
        })?;
        if version == -1 {
            return Ok(None);
        }
        let committed =
            match settle_or_drop(deadline.clock.as_ref(), deadline.deadline, stamp).await {
                Some(Ok(committed)) => committed,
                _ => {
                    return Err(StorageError::commit_unknown(
                        "FoundationDB committed but its versionstamp was unavailable",
                    ));
                }
            };
        committed.as_ref().try_into().map(Some).map_err(|_| {
            StorageError::commit_unknown("FoundationDB committed with an invalid versionstamp")
        })
    }

    async fn load_first_key(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> StorageResult<Option<Vec<u8>>> {
        let snapshot = self.snapshot(protection)?;
        observe_reads(
            self.fault_probe.as_deref(),
            crate::KvAccessKind::ReadRange,
            [begin],
            !snapshot,
        );
        let (begin, end) = self.input_limits.prepare_bounds(begin, end)?;
        if begin >= end {
            return Ok(None);
        }
        let selector = foundationdb::KeySelector::first_greater_or_equal(begin.as_slice());
        let wave =
            KvWaveScope::start(self.fault_probe.as_deref(), crate::KvWaveKind::FirstKey, 1).await?;
        let _read = self.acquire_read().await?;
        let key = self
            .transaction
            .get_key(&selector, snapshot)
            .await
            .map_err(map_operation_error)?;
        KvWaveScope::finish(wave, 0);
        if key.as_ref() < begin.as_slice() || key.as_ref() >= end.as_slice() {
            return Ok(None);
        }
        self.settlement.charge_data(key.len())?;
        crate::backend::copy_native_key(self.input_limits.logical_key(key.as_ref())?).map(Some)
    }

    /// The `\xff/metadataVersion` read with its accounting, without a wave of
    /// its own: the caller holds a read permit inside its wave.
    async fn read_metadata_version(&self) -> StorageResult<Option<storage_types::CatalogVersion>> {
        let key = crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY;
        self.transaction
            .set_option(foundationdb::options::TransactionOption::ReadSystemKeys)
            .map_err(map_operation_error)?;
        self.settlement.charge_data(key.len())?;
        // A snapshot read: the version is only a cache hint. Callers that
        // trust a cached binding at this version protect that table's name
        // lookup with `native_protect_reads`, so a schema change on another
        // table never aborts this attempt.
        observe_reads(
            self.fault_probe.as_deref(),
            crate::KvAccessKind::Read,
            [key],
            false,
        );
        // The native metadata promise does not forward read-version errors.
        // Await the same transaction's memoized read version first so its
        // error reaches the existing attempt owner.
        self.transaction
            .get_read_version()
            .await
            .map_err(map_operation_error)?;
        let value = self
            .transaction
            .get(key, true)
            .await
            .map_err(map_operation_error)?;
        if let Some(probe) = self.fault_probe.as_deref() {
            probe.observe(crate::KvNativeEvent::ReturnedData {
                rows: usize::from(value.is_some()),
                bytes: value.as_ref().map_or(0, |value| value.len()),
            });
            probe.observe(crate::KvNativeEvent::KeysRead {
                requested: 1,
                found: usize::from(value.is_some()),
            });
        }
        self.settlement
            .charge_data(value.as_ref().map_or(0, |value| value.len()))?;
        value
            .map(|value| crate::native_catalog::decode_foundationdb_version(value.as_ref()))
            .transpose()
    }

    async fn acquire_read(&self) -> StorageResult<tokio::sync::SemaphorePermit<'_>> {
        let permit = self
            .read_slots
            .acquire()
            .await
            .map_err(|_| StorageError::internal("FoundationDB read pipeline stopped"))?;
        self.settlement.require_open()?;
        if let Some(probe) = self.fault_probe.as_deref()
            && !self
                .read_version_observed
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            probe.observe(crate::KvNativeEvent::Wave {
                kind: crate::KvWaveKind::ReadVersion,
                keys: 0,
                cached_read_version: self.requested_cached_read_version,
            });
        }
        Ok(permit)
    }

    /// One native wave of point reads; `snapshots[i]` (or `uniform`) is the
    /// snapshot flag of `keys[i]`.
    async fn read_points(
        &self,
        keys: &[Vec<u8>],
        snapshots: PointSnapshots<'_>,
        mut allowance: crate::KvValueAllowance,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            crate::work_admission::NativeReadWork::from(&self.settlement).charge_keys(keys)?;
            if let Some(probe) = self.fault_probe.as_deref() {
                for conflict in [true, false] {
                    observe_reads(
                        Some(probe),
                        crate::KvAccessKind::Read,
                        keys.iter()
                            .enumerate()
                            .filter(|(index, _)| !snapshots.at(*index) == conflict)
                            .map(|(_, key)| key.as_slice()),
                        conflict,
                    );
                }
            }
            let keys = self.input_limits.prepare_points(keys)?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(keys.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            // Every point read of the call is submitted in the first poll; one
            // permit covers the whole call, so the batch is one native wave.
            let carry_catalog = self.catalog_pipeline.take_request();
            let wave = KvWaveScope::start(
                self.fault_probe.as_deref(),
                crate::KvWaveKind::GetBatch,
                keys.len() + usize::from(carry_catalog),
            )
            .await?;
            let (results, catalog) = {
                let _read = self.acquire_read().await?;
                futures::future::join(
                    futures::future::join_all(keys.iter().enumerate().map(
                        |(index, key)| async move {
                            let call = self.settlement.begin_call()?;
                            let result = self
                                .transaction
                                .get(key, snapshots.at(index))
                                .await
                                .map_err(map_operation_error);
                            call.complete(result)
                        },
                    )),
                    async {
                        if carry_catalog {
                            Some(self.read_metadata_version().await)
                        } else {
                            None
                        }
                    },
                )
                .await
            };
            KvWaveScope::finish(wave, 0);
            let catalog = catalog.transpose()?;
            // Record the version before any data check: a stale binding must be
            // confirmed by stamps even when the data read then fails.
            if let Some(observation) = catalog {
                self.catalog_pipeline.record(observation);
            }
            let probe = self.fault_probe.as_deref();
            for result in results {
                let value = result?;
                if let Some(probe) = probe {
                    probe.observe(crate::KvNativeEvent::ReturnedData {
                        rows: usize::from(value.is_some()),
                        bytes: value.as_ref().map_or(0, |bytes| bytes.len()),
                    });
                }
                if let Some(bytes) = &value {
                    NativeInputLimits::observed_value(bytes.as_ref())?;
                }
                values.push(allowance.copy_value(value.as_ref().map(|bytes| bytes.as_ref()))?);
            }
            crate::work_admission::NativeReadWork::from(&self.settlement).charge_values(&values)?;
            observe_keys_read(self.fault_probe.as_deref(), &values);
            Ok(values)
        }
        .await;
        call.complete(result)
    }

    fn snapshot(&self, protection: ReadProtection) -> StorageResult<bool> {
        protection.validate(self.mode)?;
        Ok(protection == ReadProtection::Snapshot)
    }

    /// One range page and the native pages (round trips) it took.
    async fn scan_range(
        &self,
        range: PreparedRange,
        snapshot: bool,
    ) -> StorageResult<(KvRangePage, usize)> {
        let call = self.settlement.begin_call()?;
        let result = self
            .scan_prepared_range(range, snapshot)
            .await
            .and_then(|(page, pages)| {
                crate::work_admission::NativeReadWork::from(&self.settlement)
                    .charge_pages(std::slice::from_ref(&page))?;
                Ok((page, pages))
            });
        call.complete(result)
    }

    async fn scan_prepared_range(
        &self,
        range: PreparedRange,
        snapshot: bool,
    ) -> StorageResult<(KvRangePage, usize)> {
        let mut range = range.0;
        if range.begin >= range.end {
            return Ok((
                KvRangePage {
                    entries: Vec::new(),
                    more: false,
                },
                0,
            ));
        }
        let mut buffer = crate::range::KvRangeBuffer::default();
        let mut pages = 0_usize;
        loop {
            pages += 1;
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
            if let Some(probe) = self.fault_probe.as_deref() {
                probe.observe(crate::KvNativeEvent::ReturnedData {
                    rows: rows.len(),
                    bytes: rows
                        .iter()
                        .map(|row| row.key().len() + row.value().len())
                        .sum(),
                });
            }
            let native_more = rows.more();
            let byte_limited =
                !self.append_range_rows(&rows, &range, (&range.begin, &range.end), &mut buffer)?;
            let more = native_more || byte_limited;
            if !more || buffer.is_full(&range) || byte_limited {
                return Ok((buffer.into_page(more), pages));
            }
            let last_key = rows
                .last()
                .ok_or_else(|| StorageError::internal("FoundationDB range page cannot advance"))?
                .key();
            let mut next_key = Vec::new();
            next_key
                .try_reserve_exact(last_key.len() + 1)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
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

    fn catalog_changed(&self) -> bool {
        self.catalog.changed
    }

    fn writer_routes(&self) -> Option<Arc<stream_provider::SystemStreamLineageCatalog>> {
        self.catalog.writer_routes.clone()
    }

    fn install_writer_routes(
        &mut self,
        routes: Arc<stream_provider::SystemStreamLineageCatalog>,
    ) -> StorageResult<()> {
        self.settlement.require_open()?;
        self.catalog.writer_routes = Some(routes);
        Ok(())
    }

    fn reserve_system_event_number(&mut self) -> StorageResult<u16> {
        self.catalog.reserve_event_number(&self.settlement)
    }

    fn native_pipeline_catalog_version(
        &mut self,
        expected: storage_types::CatalogVersion,
    ) -> StorageResult<bool> {
        self.settlement.require_open()?;
        if self.catalog.changed || self.catalog.observation.is_some() {
            return Ok(false);
        }
        self.catalog_pipeline.request(expected);
        Ok(true)
    }

    fn native_pipelined_version_moved(&mut self) -> bool {
        !self.catalog.changed && self.catalog_pipeline.moved()
    }

    fn native_pipelined_catalog_version(
        &mut self,
    ) -> Option<Option<storage_types::CatalogVersion>> {
        if self.catalog.changed {
            return None;
        }
        if let Some(observation) = self.catalog_pipeline.take_observed() {
            self.catalog.observation = Some(observation);
        }
        self.catalog.observation
    }

    async fn native_catalog_version(
        &mut self,
    ) -> StorageResult<Option<storage_types::CatalogVersion>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            if self.catalog.changed {
                return Ok(None);
            }
            if let Some(observation) = self.catalog.observation {
                return Ok(observation);
            }
            if let Some(observation) = self.catalog_pipeline.take_observed() {
                return Ok(observation);
            }
            // Read it now; a pending pipelined request is no longer needed.
            self.catalog_pipeline.take_request();
            let wave =
                KvWaveScope::start(self.fault_probe.as_deref(), crate::KvWaveKind::GetBatch, 1)
                    .await?;
            let observation = {
                let _read = self.acquire_read().await?;
                self.read_metadata_version().await?
            };
            KvWaveScope::finish(wave, 0);
            Ok(observation)
        }
        .await;
        if let Ok(observation) = result
            && !self.catalog.changed
        {
            self.catalog.observation = Some(observation);
        }
        call.complete(result)
    }

    fn native_protect_reads(&mut self, keys: &[Vec<u8>], ranges: &[KvRange]) -> StorageResult<()> {
        self.settlement.require_open()?;
        if self.mode.read_protection() == ReadProtection::Snapshot {
            return Ok(());
        }
        observe_reads(
            self.fault_probe.as_deref(),
            crate::KvAccessKind::ProtectKey,
            keys.iter().map(Vec::as_slice),
            true,
        );
        observe_reads(
            self.fault_probe.as_deref(),
            crate::KvAccessKind::ProtectRange,
            ranges.iter().map(|range| range.begin.as_slice()),
            true,
        );
        for key in self.input_limits.prepare_points(keys)? {
            let mut end = Vec::new();
            end.try_reserve_exact(key.len() + 1)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            end.extend_from_slice(&key);
            end.push(0);
            self.transaction
                .add_conflict_range(&key, &end, foundationdb::options::ConflictRangeType::Read)
                .map_err(map_operation_error)?;
        }
        for range in ranges {
            let (begin, end) = self.input_limits.prepare_bounds(&range.begin, &range.end)?;
            self.transaction
                .add_conflict_range(&begin, &end, foundationdb::options::ConflictRangeType::Read)
                .map_err(map_operation_error)?;
        }
        Ok(())
    }

    async fn native_record_catalog_change(&mut self) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.mode.require_write()?;
            self.catalog.record_change();
            self.catalog_pipeline.clear();
            if self.catalog.publication_staged {
                return Ok(());
            }
            // The SDK stages ten stamp bytes plus its four-byte offset trailer.
            self.settlement
                .charge_data(crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY.len() + 14)?;
            self.transaction
                .set_option(foundationdb::options::TransactionOption::AccessSystemKeys)
                .map_err(map_operation_error)?;
            self.transaction.update_metadata_version();
            if let Some(probe) = self.fault_probe.as_deref() {
                observe_puts(probe, 1);
                probe.observe_access(crate::KvAccess {
                    kind: crate::KvAccessKind::Write,
                    key: crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY,
                    native_bytes: crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY.len() + 14,
                    conflict: true,
                });
            }
            self.catalog.publication_staged = true;
            Ok(())
        })();
        call.complete(result)
    }

    async fn native_first_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> StorageResult<Option<Vec<u8>>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_data(
                begin
                    .len()
                    .checked_add(end.len())
                    .ok_or(storage_types::AttemptFailure::CAPACITY)?,
            )?;
            self.load_first_key(begin, end, protection).await
        }
        .await;
        call.complete(result)
    }

    fn backend_engine(&self) -> storage_common::diagnostics::NativeEngine {
        storage_common::diagnostics::NativeEngine::FoundationDb
    }

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
        allowance: crate::KvValueAllowance,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let snapshot = self.snapshot(protection)?;
        self.read_points(keys, PointSnapshots::Uniform(snapshot), allowance)
            .await
    }

    async fn native_get_batch_mixed(
        &self,
        keys: &[Vec<u8>],
        protections: &[ReadProtection],
        allowance: crate::KvValueAllowance,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.len() != protections.len() {
            return Err(StorageError::internal(
                "KV mixed batch protections differ in count",
            ));
        }
        let snapshots = protections
            .iter()
            .map(|protection| self.snapshot(*protection))
            .collect::<StorageResult<Vec<bool>>>()?;
        self.read_points(keys, PointSnapshots::Each(&snapshots), allowance)
            .await
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
            observe_reads(
                self.fault_probe.as_deref(),
                crate::KvAccessKind::ReadRange,
                [range.begin.as_slice()],
                !snapshot,
            );
            let range = self.input_limits.prepare_range(range)?;
            let carry_catalog = self.catalog_pipeline.take_request();
            let wave =
                KvWaveScope::start(self.fault_probe.as_deref(), crate::KvWaveKind::Scan, 1).await?;
            let (scanned, catalog) = {
                let _read = self.acquire_read().await?;
                futures::future::join(self.scan_range(range, snapshot), async {
                    if carry_catalog {
                        Some(self.read_metadata_version().await)
                    } else {
                        None
                    }
                })
                .await
            };
            let catalog = catalog.transpose()?;
            // Record the version before any data check: a stale binding must be
            // confirmed by stamps even when the data read then fails.
            if let Some(observation) = catalog {
                self.catalog_pipeline.record(observation);
            }
            let scanned = scanned?;
            if let Some(probe) = self.fault_probe.as_deref() {
                // An empty range sends no native page; a carried catalog read
                // still makes the wave.
                if let Some(extra) = scanned.1.max(usize::from(carry_catalog)).checked_sub(1) {
                    KvWaveScope::finish(wave, extra);
                }
                if scanned.1 > 0 {
                    observe_ranges_read(Some(probe), std::slice::from_ref(&scanned.0));
                }
            }
            Ok(scanned.0)
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
            observe_reads(
                self.fault_probe.as_deref(),
                crate::KvAccessKind::ReadRange,
                ranges.iter().map(|range| range.begin.as_slice()),
                !snapshot,
            );
            let ranges = self.input_limits.prepare_ranges(ranges)?;
            let mut pages = Vec::new();
            pages
                .try_reserve_exact(ranges.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            // Every range of the call starts in the first poll; the longest
            // range sets the sequential round trips.
            let carry_catalog = self.catalog_pipeline.take_request();
            let wave = KvWaveScope::start(
                self.fault_probe.as_deref(),
                crate::KvWaveKind::Scan,
                ranges.len(),
            )
            .await?;
            let (results, catalog) = {
                let _read = self.acquire_read().await?;
                futures::future::join(
                    futures::future::join_all(
                        ranges
                            .into_iter()
                            .map(|range| self.scan_range(range, snapshot)),
                    ),
                    async {
                        if carry_catalog {
                            Some(self.read_metadata_version().await)
                        } else {
                            None
                        }
                    },
                )
                .await
            };
            if self.fault_probe.is_some()
                && let Some(extra) = results
                    .iter()
                    .filter_map(|result| result.as_ref().ok())
                    .map(|(_, native_pages)| *native_pages)
                    .max()
                    .map_or(usize::from(carry_catalog), |longest| {
                        longest.max(usize::from(carry_catalog))
                    })
                    .checked_sub(1)
            {
                KvWaveScope::finish(wave, extra);
            }
            let catalog = catalog.transpose()?;
            // Record the version before any data check: a stale binding must be
            // confirmed by stamps even when the data read then fails.
            if let Some(observation) = catalog {
                self.catalog_pipeline.record(observation);
            }
            for result in results {
                pages.push(result?.0);
            }
            observe_ranges_read(self.fault_probe.as_deref(), &pages);
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
            observe_mutations(self.fault_probe.as_deref(), &changes);
            for change in &changes {
                self.catalog.observe_write(change);
            }
            self.input_limits.prepare_mutations(changes)?.stage(
                &self.transaction,
                (self.fault_probe.as_deref(), self.input_limits.prefix.len()),
            );
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
                    .ok_or(storage_types::AttemptFailure::CAPACITY)?,
            )?;
            self.mode.require_write()?;
            self.catalog.observe_write_key(&key);
            self.input_limits.point(&key)?;
            NativeInputLimits::value(&value)?;
            let key = self.input_limits.prefix_key(key)?;
            if let Some(probe) = self.fault_probe.as_deref() {
                observe_puts(probe, 1);
                observe_native_write(
                    probe,
                    self.input_limits.prefix.len(),
                    (&key, value.len()),
                    true,
                );
            }
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
                    .ok_or(storage_types::AttemptFailure::CAPACITY)?,
            )?;
            self.mode.require_write()?;
            self.catalog.observe_write_key(&key);
            let (key, value) = self
                .input_limits
                .prepare_stamped_value(key, value, stamp_offset)?;
            if let Some(probe) = self.fault_probe.as_deref() {
                observe_puts(probe, 1);
                observe_native_write(
                    probe,
                    self.input_limits.prefix.len(),
                    (&key, value.len()),
                    true,
                );
            }
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
            if let Some(probe) = self.fault_probe.as_deref() {
                observe_puts(probe, changes.len());
            }
            self.input_limits
                .prepare_stamped_mutations(changes)?
                .stage(
                    &self.transaction,
                    (self.fault_probe.as_deref(), self.input_limits.prefix.len()),
                )?;
            Ok(())
        })();
        call.complete(result)
    }
}

fn observe_puts(probe: &dyn crate::KvFaultProbe, puts: usize) {
    probe.observe(crate::KvNativeEvent::Mutations {
        puts,
        deletes: 0,
        range_deletes: 0,
    });
}

/// Reports the conflicting ranges of a 1020 commit conflict. The keyspace is
/// resolved client-side from the commit response; read failures are ignored.
async fn observe_conflicting_keys(
    probe: &dyn crate::KvFaultProbe,
    prefix: &[u8],
    error: &foundationdb::TransactionCommitError,
) {
    const NOT_COMMITTED: i32 = 1020;
    if error.code() != NOT_COMMITTED {
        return;
    }
    let Ok(ranges) = error.conflicting_keys().await else {
        return;
    };
    for range in &ranges {
        let key = range.begin();
        probe.observe_access(crate::KvAccess {
            kind: crate::KvAccessKind::ReportedConflict,
            key: key.strip_prefix(prefix).unwrap_or(key),
            native_bytes: 0,
            conflict: false,
        });
    }
}

/// Reports one staged set or atomic operation with its physical key.
pub(super) fn observe_native_write(
    probe: &dyn crate::KvFaultProbe,
    prefix_bytes: usize,
    (key, value_bytes): (&[u8], usize),
    conflict: bool,
) {
    probe.observe_access(crate::KvAccess {
        kind: crate::KvAccessKind::Write,
        key: key.get(prefix_bytes..).unwrap_or_default(),
        native_bytes: key.len() + value_bytes,
        conflict,
    });
}

/// A durable commit cannot be replayed when its local version lookup fails.
fn committed_version_unavailable(_: impl std::fmt::Display) -> StorageError {
    StorageError::commit_unknown("FoundationDB committed but its committed version was unavailable")
}

/// The snapshot flag of each point read of one wave.
#[derive(Clone, Copy)]
enum PointSnapshots<'a> {
    Uniform(bool),
    Each(&'a [bool]),
}

impl PointSnapshots<'_> {
    fn at(self, index: usize) -> bool {
        match self {
            Self::Uniform(snapshot) => snapshot,
            Self::Each(snapshots) => snapshots[index],
        }
    }
}
