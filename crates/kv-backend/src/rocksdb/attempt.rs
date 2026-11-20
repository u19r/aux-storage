use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use storage_types::{StorageError, StorageResult};
use tokio::sync::{mpsc, oneshot};

use crate::{
    AttemptMode, KvAttempt, KvMutation, KvRange, KvRangePage, KvVersionstampedMutation,
    ReadProtection,
};

// RocksDB's synchronous transaction borrows its DB on a blocking worker, so
// this channel is its async bridge. FoundationDB owns an async transaction and
// implements the same KvAttempt trait without a command channel.
pub(super) enum Command {
    SnapshotSequence {
        answer: oneshot::Sender<StorageResult<u64>>,
    },
    FirstKeyInRange {
        begin: Vec<u8>,
        end: Vec<u8>,
        protection: ReadProtection,
        answer: oneshot::Sender<StorageResult<Option<Vec<u8>>>>,
    },
    GetBatch {
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
        answer: oneshot::Sender<StorageResult<Vec<Option<Vec<u8>>>>>,
        probe: Option<Arc<dyn crate::KvFaultProbe>>,
    },
    ScanBatch {
        ranges: Vec<KvRange>,
        protection: ReadProtection,
        answer: oneshot::Sender<StorageResult<Vec<KvRangePage>>>,
        probe: Option<Arc<dyn crate::KvFaultProbe>>,
    },
    ApplyBatch {
        changes: Vec<KvMutation>,
        answer: oneshot::Sender<StorageResult<()>>,
    },
    Finish {
        commit: bool,
        changes: Vec<KvMutation>,
        /// Records the commit ticket on the writer thread after commit.
        recorder: Option<storage_common::durability::CommitRecorder>,
        answer: oneshot::Sender<StorageResult<()>>,
        probe: Option<(tokio::runtime::Handle, Arc<dyn crate::KvFaultProbe>)>,
    },
}

/// Async handle to a native transaction held only by the blocking worker.
pub struct RocksDbAttempt {
    settlement: Arc<storage_types::NativeAttemptSettlement>,
    sender: mpsc::UnboundedSender<Command>,
    /// Commands sent and not yet answered: the writer guard's evidence that
    /// a pending operation waits for native work.
    native_requests: Arc<AtomicUsize>,
    mode: AttemptMode,
    catalog: crate::native_catalog::NativeCatalogState,
    snapshot_sequence: tokio::sync::OnceCell<u64>,
    // Reads flush this buffer first so they observe preceding writes.
    pending: Mutex<Vec<KvMutation>>,
    // The first staged mutation registers this attempt as a pending durable
    // writer before its commit, even if the attempt later fails.
    durability: Option<Arc<storage_common::durability::DurabilityBatch>>,
    durable_write: Option<storage_common::durability::PendingDurableWrite>,
    /// `None` in production.
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

impl RocksDbAttempt {
    pub(super) fn new(
        sender: mpsc::UnboundedSender<Command>,
        mode: AttemptMode,
        settlement: Arc<storage_types::NativeAttemptSettlement>,
        durability: Option<Arc<storage_common::durability::DurabilityBatch>>,
        fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    ) -> Self {
        crate::fault_injection::observe(fault_probe.as_deref(), crate::KvNativeEvent::Begin);
        Self {
            settlement,
            sender,
            native_requests: Arc::default(),
            mode,
            catalog: Default::default(),
            snapshot_sequence: tokio::sync::OnceCell::new(),
            pending: Mutex::new(Vec::new()),
            durability,
            durable_write: None,
            fault_probe,
        }
    }

    pub(super) async fn finish(&mut self, commit: bool) -> StorageResult<()> {
        let (changes, flush) = if commit {
            match self
                .settlement
                .require_open()
                .and_then(|()| self.take_pending())
            {
                Ok(changes) => {
                    self.catalog.debug_assert_change_protected();
                    (changes, Ok(()))
                }
                Err(error) => (Vec::new(), Err(error)),
            }
        } else {
            (Vec::new(), Ok(()))
        };
        let wave = crate::fault_injection::KvWaveScope::start(
            self.fault_probe.as_deref(),
            if commit {
                crate::KvWaveKind::Commit
            } else {
                crate::KvWaveKind::Abort
            },
            0,
        )
        .await?;
        let (answer, reply) = oneshot::channel();
        let commit_native = commit && flush.is_ok();
        self.sender
            .send(Command::Finish {
                commit: commit_native,
                changes,
                recorder: self
                    .durable_write
                    .as_ref()
                    .map(storage_common::durability::PendingDurableWrite::recorder),
                answer,
                probe: self
                    .fault_probe
                    .as_ref()
                    .map(|probe| (tokio::runtime::Handle::current(), Arc::clone(probe))),
            })
            .map_err(|_| StorageError::internal("RocksDB attempt ended before settlement"))?;
        let settled = reply
            .await
            .map_err(|_| StorageError::commit_unknown("RocksDB settlement was not acknowledged"))?;
        crate::fault_injection::KvWaveScope::finish(wave, 0);
        if let Some(probe) = self.fault_probe.as_deref() {
            probe.observe(if commit && settled.is_ok() && flush.is_ok() {
                crate::KvNativeEvent::Commit
            } else {
                crate::KvNativeEvent::Abort
            });
        }
        settled?;
        flush
    }

    async fn load_catalog_counter(&self) -> StorageResult<Option<std::num::NonZeroU64>> {
        let value = self
            .native_get(
                crate::constants::ROCKSDB_CATALOG_VERSION_KEY,
                self.mode.read_protection(),
                crate::KvValueAllowance::new(8, 8),
            )
            .await?;
        value
            .map(|value| {
                let value = u64::from_be_bytes(
                    value
                        .try_into()
                        .map_err(|_| StorageError::internal("Invalid RocksDB catalog version"))?,
                );
                std::num::NonZeroU64::new(value)
                    .ok_or_else(|| StorageError::internal("Invalid RocksDB catalog version"))
            })
            .transpose()
    }

    pub(super) fn native_requests(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.native_requests)
    }

    /// Counts one command from its send until its answer or cancellation.
    fn begin_native_request(&self) -> NativeRequest<'_> {
        self.native_requests.fetch_add(1, Ordering::Relaxed);
        NativeRequest(&self.native_requests)
    }

    /// The pending durable writer registration, if a mutation was staged.
    pub(super) fn take_durable_write(
        &mut self,
    ) -> Option<storage_common::durability::PendingDurableWrite> {
        self.durable_write.take()
    }

    fn take_pending(&self) -> StorageResult<Vec<KvMutation>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StorageError::internal("RocksDB pending writes were poisoned"))?;
        Ok(std::mem::take(&mut *pending))
    }

    fn dispatch_pending(&self) -> StorageResult<Option<oneshot::Receiver<StorageResult<()>>>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StorageError::internal("RocksDB pending writes were poisoned"))?;
        if pending.is_empty() {
            return Ok(None);
        }
        self.settlement.require_open()?;
        let changes = std::mem::take(&mut *pending);
        let (answer, reply) = oneshot::channel();
        self.sender
            .send(Command::ApplyBatch { changes, answer })
            .map_err(|_| StorageError::internal("RocksDB write worker stopped"))?;
        Ok(Some(reply))
    }

    async fn flush_pending(&self) -> StorageResult<()> {
        let _request = self.begin_native_request();
        let Some(reply) = self.dispatch_pending()? else {
            return Ok(());
        };
        crate::fault_injection::observe(
            self.fault_probe.as_deref(),
            crate::KvNativeEvent::Wave {
                kind: crate::KvWaveKind::Write,
                keys: 0,
                cached_read_version: false,
            },
        );
        reply
            .await
            .map_err(|_| StorageError::internal("RocksDB write was not answered"))?
    }
}

struct NativeRequest<'attempt>(&'attempt AtomicUsize);

impl Drop for NativeRequest<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl KvAttempt for RocksDbAttempt {
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

    async fn native_catalog_version(
        &mut self,
    ) -> StorageResult<Option<storage_types::CatalogVersion>> {
        let settlement = Arc::clone(&self.settlement);
        let call = settlement.begin_call()?;
        let result = async {
            if self.catalog.changed {
                return Ok(None);
            }
            if let Some(observation) = self.catalog.observation {
                return Ok(observation);
            }
            let observation = self
                .load_catalog_counter()
                .await?
                .map(storage_types::CatalogVersion::RocksDb);
            self.catalog.observation = Some(observation);
            Ok(observation)
        }
        .await;
        call.complete(result)
    }

    async fn native_record_catalog_change(&mut self) -> StorageResult<()> {
        let settlement = Arc::clone(&self.settlement);
        let call = settlement.begin_call()?;
        let result = async {
            self.mode.require_write()?;
            // A private original-counter read remains legal after invalidation.
            let observation = self.catalog.observation;
            self.catalog.record_change();
            if self.catalog.publication_staged {
                return Ok(());
            }
            let previous = match observation {
                Some(Some(storage_types::CatalogVersion::RocksDb(value))) => Some(value),
                Some(None) => None,
                _ => self.load_catalog_counter().await?,
            };
            let next = previous
                .map_or(Some(1), |value| value.get().checked_add(1))
                .ok_or_else(|| StorageError::internal("RocksDB catalog version exhausted"))?;
            self.native_stage(vec![KvMutation::PutUnchunked {
                key: crate::constants::ROCKSDB_CATALOG_VERSION_KEY.to_vec(),
                value: next.to_be_bytes().to_vec(),
            }])?;
            self.catalog.publication_staged = true;
            Ok(())
        }
        .await;
        call.complete(result)
    }

    async fn native_snapshot_sequence(&self) -> StorageResult<Option<u64>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.snapshot_sequence
                .get_or_try_init(|| async {
                    let _request = self.begin_native_request();
                    let (answer, reply) = oneshot::channel();
                    self.sender
                        .send(Command::SnapshotSequence { answer })
                        .map_err(|_| StorageError::internal("RocksDB snapshot worker stopped"))?;
                    reply
                        .await
                        .map_err(|_| StorageError::internal("RocksDB snapshot was not answered"))?
                })
                .await
                .map(|sequence| Some(*sequence))
        }
        .await;
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
            crate::fault_injection::observe_reads(
                self.fault_probe.as_deref(),
                crate::KvAccessKind::ReadRange,
                [begin],
                false,
            );
            let begin = crate::backend::copy_native_key(begin)?;
            let end = crate::backend::copy_native_key(end)?;
            let wave = crate::fault_injection::KvWaveScope::start(
                self.fault_probe.as_deref(),
                crate::KvWaveKind::FirstKey,
                1,
            )
            .await?;
            self.flush_pending().await?;
            let _request = self.begin_native_request();
            let (answer, reply) = oneshot::channel();
            self.sender
                .send(Command::FirstKeyInRange {
                    begin,
                    end,
                    protection,
                    answer,
                })
                .map_err(|_| StorageError::internal("RocksDB range worker stopped"))?;
            let key = reply
                .await
                .map_err(|_| StorageError::internal("RocksDB range was not answered"))?;
            crate::fault_injection::KvWaveScope::finish(wave, 0);
            key
        }
        .await;
        call.complete(result)
    }

    fn backend_engine(&self) -> storage_common::diagnostics::NativeEngine {
        storage_common::diagnostics::NativeEngine::RocksDb
    }

    fn fault_probe(&self) -> Option<&dyn crate::KvFaultProbe> {
        self.fault_probe.as_deref()
    }

    fn native_value_limit(&self) -> usize {
        usize::MAX
    }

    async fn native_get_batch(
        &self,
        keys: &[Vec<u8>],
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            crate::work_admission::NativeReadWork::from(&*self.settlement).charge_keys(keys)?;
            // Plain reads: the optimistic commit validates written keys only.
            crate::fault_injection::observe_reads(
                self.fault_probe.as_deref(),
                crate::KvAccessKind::Read,
                keys.iter().map(Vec::as_slice),
                false,
            );
            let keys = crate::backend::copy_native_keys(keys)?;
            let wave = crate::fault_injection::KvWaveScope::start(
                self.fault_probe.as_deref(),
                crate::KvWaveKind::GetBatch,
                keys.len(),
            )
            .await?;
            self.flush_pending().await?;
            let _request = self.begin_native_request();
            let (answer, reply) = oneshot::channel();
            self.sender
                .send(Command::GetBatch {
                    keys,
                    protection,
                    allowance,
                    probe: self.fault_probe.clone(),
                    answer,
                })
                .map_err(|_| StorageError::internal("RocksDB read worker stopped"))?;
            let values = reply
                .await
                .map_err(|_| StorageError::internal("RocksDB read was not answered"))??;
            crate::fault_injection::KvWaveScope::finish(wave, 0);
            crate::fault_injection::observe_keys_read(self.fault_probe.as_deref(), &values);
            Ok(values)
        }
        .await;
        let result = result.and_then(|values| {
            crate::work_admission::NativeReadWork::from(&*self.settlement)
                .charge_values(&values)?;
            Ok(values)
        });
        call.complete(result)
    }

    async fn native_scan(
        &self,
        range: KvRange,
        protection: ReadProtection,
    ) -> StorageResult<KvRangePage> {
        let call = self.settlement.begin_call()?;
        let result = async {
            let mut ranges = Vec::new();
            ranges
                .try_reserve_exact(1)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            ranges.push(range);
            let mut pages = self.native_scan_batch(ranges, protection).await?;
            pages
                .pop()
                .ok_or_else(|| StorageError::internal("RocksDB range page is missing"))
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
            crate::fault_injection::observe_reads(
                self.fault_probe.as_deref(),
                crate::KvAccessKind::ReadRange,
                ranges.iter().map(|range| range.begin.as_slice()),
                false,
            );
            let wave = crate::fault_injection::KvWaveScope::start(
                self.fault_probe.as_deref(),
                crate::KvWaveKind::Scan,
                ranges.len(),
            )
            .await?;
            self.flush_pending().await?;
            let _request = self.begin_native_request();
            let (answer, reply) = oneshot::channel();
            self.sender
                .send(Command::ScanBatch {
                    ranges,
                    protection,
                    probe: self.fault_probe.clone(),
                    answer,
                })
                .map_err(|_| StorageError::internal("RocksDB range worker stopped"))?;
            let pages = reply
                .await
                .map_err(|_| StorageError::internal("RocksDB range was not answered"))??;
            crate::fault_injection::KvWaveScope::finish(wave, 0);
            crate::fault_injection::observe_ranges_read(self.fault_probe.as_deref(), &pages);
            Ok(pages)
        }
        .await;
        let result = result.and_then(|pages| {
            crate::work_admission::NativeReadWork::from(&*self.settlement).charge_pages(&pages)?;
            Ok(pages)
        });
        call.complete(result)
    }

    fn native_stage(&mut self, changes: Vec<KvMutation>) -> StorageResult<()> {
        if !changes.is_empty()
            && self.durable_write.is_none()
            && let Some(durability) = &self.durability
        {
            self.durable_write = Some(durability.begin_write());
        }
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.settlement.charge_calls(changes.len())?;
            for change in &changes {
                change.charge_work(&self.settlement)?;
            }
            self.mode.require_write()?;
            if let Some(probe) = self.fault_probe.as_deref() {
                crate::fault_injection::observe_mutations(Some(probe), &changes);
                observe_writes(probe, &changes);
            }
            for change in &changes {
                self.catalog.observe_write(change);
            }
            let pending = self
                .pending
                .get_mut()
                .map_err(|_| StorageError::internal("RocksDB pending writes were poisoned"))?;
            pending
                .try_reserve(changes.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            pending.extend(changes);
            Ok(())
        })();
        call.complete(result)
    }

    fn native_stage_versionstamped(
        &mut self,
        _changes: Vec<KvVersionstampedMutation>,
    ) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = Err(StorageError::unsupported(
            "RocksDB has no native versionstamped keys",
        ));
        call.complete(result)
    }
}

/// The optimistic commit validates each written key.
fn observe_writes(probe: &dyn crate::KvFaultProbe, changes: &[KvMutation]) {
    for change in changes {
        let (kind, key) = match change {
            KvMutation::Put { key, .. } | KvMutation::PutUnchunked { key, .. } => {
                (crate::KvAccessKind::Write, key)
            }
            KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
                (crate::KvAccessKind::Clear, key)
            }
            KvMutation::DeleteRange { begin, .. }
            | KvMutation::ClearUnchunkedRange { begin, .. } => {
                (crate::KvAccessKind::ClearRange, begin)
            }
        };
        probe.observe_access(crate::KvAccess {
            kind,
            key,
            native_bytes: 0,
            conflict: true,
        });
    }
}
