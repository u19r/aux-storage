//! Native-call hooks for fault and census tests. Always compiled; a probe
//! exists only when the `fault-injection` feature installs one.

use futures::future::BoxFuture;
/// The commit boundary that `KvFaultProbe::at_commit_boundary` receives.
pub use storage_common::native_observer::CommitBoundary as KvCommitBoundary;
use storage_types::{AttemptMode, StorageError, StorageResult};

/// A KV probe seen as the commit observer of every attempt of its store.
#[cfg(feature = "fault-injection")]
pub(crate) struct KvProbeObserver(pub(crate) std::sync::Arc<dyn KvFaultProbe>);

#[cfg(feature = "fault-injection")]
impl storage_common::native_observer::NativeObserver for KvProbeObserver {
    fn at_commit_boundary(
        &self,
        boundary: storage_common::native_observer::CommitBoundary,
        mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        self.0.at_commit_boundary(boundary, mode)
    }
}

/// The observer that `with_fault_probe` stores beside the probe.
#[cfg(feature = "fault-injection")]
pub(crate) fn commit_observer(
    probe: &std::sync::Arc<dyn KvFaultProbe>,
) -> std::sync::Arc<dyn storage_common::native_observer::NativeObserver> {
    std::sync::Arc::new(KvProbeObserver(std::sync::Arc::clone(probe)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvFaultOperation {
    GetBatch,
    Scan,
    ScanBatch,
    Stage,
    StageVersionstamped,
    /// Local committed-version lookup after an acknowledged FoundationDB
    /// commit.
    ReadCommittedVersion,
}

#[derive(Clone, Copy)]
pub struct KvFaultCall {
    pub operation: KvFaultOperation,
    /// The logical mutations of a `Stage` call (tests count rows written).
    pub mutations: usize,
}

/// What one native KV wave does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvWaveKind {
    /// The transaction read version (FoundationDB: implicit in the first read).
    ReadVersion,
    FirstKey,
    GetBatch,
    Scan,
    /// Staged mutations sent to the native transaction before a read
    /// (RocksDB worker hop).
    Write,
    Commit,
    Abort,
}

/// One native round trip or worker hop, given to `KvFaultProbe::before_wave`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvWave {
    pub kind: KvWaveKind,
    /// Point keys or ranges sent in the wave.
    pub keys: usize,
}

/// One native event of a KV attempt, reported to `KvFaultProbe::observe`.
/// Tests count these events for one customer operation (the wave census).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvNativeEvent {
    Begin,
    /// Requested native mode/policy; this does not prove a read-version cache
    /// hit.
    AttemptRequested {
        mode: storage_types::AttemptMode,
        read_version: storage_types::ReadVersionPolicy,
    },
    /// Actual SDK commit rejection, separate from synthetic boundary failures.
    CommitFailed {
        native_code: i32,
        kind: Option<storage_types::AttemptKind>,
    },
    Commit,
    Abort,
    Wave {
        kind: KvWaveKind,
        keys: usize,
        /// True only on a ReadVersion event whose request selected Cached.
        /// This records requested policy, never an observed SDK cache hit.
        cached_read_version: bool,
    },
    KeysRead {
        requested: usize,
        found: usize,
    },
    RangesRead {
        ranges: usize,
        entries: usize,
    },
    /// SDK-visible payload before local retention; point values or range
    /// key/value pairs.
    ReturnedData {
        rows: usize,
        bytes: usize,
    },
    Mutations {
        puts: usize,
        deletes: usize,
        range_deletes: usize,
    },
    DurabilityBarrier,
    /// The serialized bytes of the native write batch that a RocksDB commit
    /// makes durable.
    WriteBatchBytes {
        bytes: usize,
    },
}

/// What one native access does to the attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvAccessKind {
    /// A point read.
    Read,
    /// A range read or a first-key read; `key` is the range begin.
    ReadRange,
    /// A set or an atomic operation.
    Write,
    Clear,
    /// `key` is the range begin.
    ClearRange,
    /// A read conflict key added without a read (no I/O).
    ProtectKey,
    /// A read conflict range added without a read; `key` is the range begin.
    ProtectRange,
    /// The begin of a range that a 1020 commit conflict reported. It adds no
    /// conflict and no cost; fault builds read it from the client-side
    /// special keyspace.
    ReportedConflict,
}

/// One native key access, given to `KvFaultProbe::observe_access`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvAccess<'a> {
    pub kind: KvAccessKind,
    /// The logical key (without the provider prefix), or the range begin.
    pub key: &'a [u8],
    /// Native bytes that a mutation stages: prefixed key (begin and end for a
    /// clear range) plus value. FoundationDB only; zero elsewhere.
    pub native_bytes: usize,
    /// The access adds a key or a range to the conflict set: FoundationDB
    /// read and write conflicts, RocksDB keys that the optimistic commit
    /// validates.
    pub conflict: bool,
}

/// Whether a logical key holds a value chunk, in a table, a queue or the
/// global chunk home.
#[must_use]
pub fn is_value_chunk_key(key: &[u8]) -> bool {
    key.starts_with(crate::constants::KV_CHUNK_KEY_FAMILY)
        || key.starts_with(crate::constants::QUEUE_MESSAGE_CHUNK_PREFIX)
        || matches!(
            crate::table_key_layout::table_stream_family(key),
            Some((_, crate::table_key_layout::TableStreamFamily::Chunk))
        )
        || matches!(
            crate::table_key_layout::table_data_family(key),
            Some((_, crate::table_key_layout::TableSubFamily::Chunk))
        )
}

/// A short label for the record kind of a logical key: one-byte families by
/// name, `\x7f` families as `~` and their two path parts, `\0` families as
/// `0:` and their name.
#[must_use]
pub fn key_family(key: &[u8]) -> String {
    use crate::table_key_layout::{TableSubFamily, table_data_family};
    if let Some((_, family)) = table_data_family(key) {
        return match family {
            TableSubFamily::GsiNameLock => "gsi_name_lock",
            TableSubFamily::RegionalMarker => "regional_marker",
            TableSubFamily::RegionalProgress => "regional_progress",
            TableSubFamily::GsiBuild => "gsi_build",
            TableSubFamily::TtlConfig => "ttl_config",
            TableSubFamily::TtlIndex => "ttl_index",
            TableSubFamily::GsiEntry => "gsi",
            TableSubFamily::Chunk => "chunk",
            TableSubFamily::Catalog => "meta",
            TableSubFamily::Item => "item",
        }
        .to_owned();
    }
    if let Some((_, family)) = crate::table_key_layout::table_stream_family(key) {
        use crate::table_key_layout::TableStreamFamily;
        return match family {
            TableStreamFamily::Entry => "stream_entry",
            TableStreamFamily::AppendRow => "stream_append",
            TableStreamFamily::AppendReceipt => "stream_receipt",
            TableStreamFamily::AppendSequence => "stream_sequence",
            TableStreamFamily::TrimState => "trim_state",
            TableStreamFamily::Chunk => "stream_chunk",
        }
        .to_owned();
    }
    let named = match key.first() {
        Some(b'M') => "lookup",
        Some(b'S') => "table_stream",
        Some(b'd') => "trim_due",
        Some(b'f') => "gsi_runnable",
        Some(0x7f | 0) => "",
        None => "empty",
        Some(_) => "",
    };
    if !named.is_empty() {
        return named.to_owned();
    }
    match key.first() {
        Some(0x7f) => {
            let path = key
                .get(1..)
                .unwrap_or_default()
                .split(|byte| *byte == b'/')
                .take(2)
                .map(|part| {
                    // Maintenance keys append binary discriminants directly to
                    // their namespace; those bytes do not name another family.
                    let end = part
                        .iter()
                        .position(|byte| !byte.is_ascii_graphic())
                        .unwrap_or(part.len());
                    String::from_utf8_lossy(&part[..end])
                })
                .collect::<Vec<_>>();
            format!("~{}", path.join("/"))
        }
        Some(0) => {
            let name = key
                .get(1..)
                .unwrap_or_default()
                .split(|byte| *byte == b'/')
                .next()
                .unwrap_or_default();
            format!("0:{}", String::from_utf8_lossy(name))
        }
        Some(byte) => format!("x{byte:02x}"),
        None => "empty".to_owned(),
    }
}

/// One wave that the driver reports: the probe can hold it before it is
/// sent and observes it when it completes.
pub(crate) struct KvWaveScope<'probe> {
    probe: &'probe dyn KvFaultProbe,
    wave: KvWave,
}

impl<'probe> KvWaveScope<'probe> {
    pub(crate) async fn start(
        probe: Option<&'probe dyn KvFaultProbe>,
        kind: KvWaveKind,
        keys: usize,
    ) -> StorageResult<Option<Self>> {
        let Some(probe) = probe else {
            return Ok(None);
        };
        let wave = KvWave { kind, keys };
        storage_common::writer_occupancy::exempt_wait(probe.before_wave(wave)).await?;
        Ok(Some(Self { probe, wave }))
    }

    /// Reports the wave, and `extra` further sequential waves of the same
    /// kind (FoundationDB range continuation pages).
    pub(crate) fn finish(scope: Option<Self>, extra: usize) {
        let Some(scope) = scope else {
            return;
        };
        scope.probe.observe(KvNativeEvent::Wave {
            kind: scope.wave.kind,
            keys: scope.wave.keys,
            cached_read_version: false,
        });
        for _ in 0..extra {
            scope.probe.observe(KvNativeEvent::Wave {
                kind: scope.wave.kind,
                keys: 0,
                cached_read_version: false,
            });
        }
    }
}

pub(crate) fn observe(probe: Option<&dyn KvFaultProbe>, event: KvNativeEvent) {
    if let Some(probe) = probe {
        probe.observe(event);
    }
}

/// Reports one access for each logical key of a read.
pub(crate) fn observe_reads<'a>(
    probe: Option<&dyn KvFaultProbe>,
    kind: KvAccessKind,
    keys: impl IntoIterator<Item = &'a [u8]>,
    conflict: bool,
) {
    let Some(probe) = probe else {
        return;
    };
    for key in keys {
        probe.observe_access(KvAccess {
            kind,
            key,
            native_bytes: 0,
            conflict,
        });
    }
}

pub(crate) fn observe_keys_read(probe: Option<&dyn KvFaultProbe>, values: &[Option<Vec<u8>>]) {
    let Some(probe) = probe else {
        return;
    };
    probe.observe(KvNativeEvent::KeysRead {
        requested: values.len(),
        found: values.iter().filter(|value| value.is_some()).count(),
    });
}

pub(crate) fn observe_ranges_read(probe: Option<&dyn KvFaultProbe>, pages: &[crate::KvRangePage]) {
    let Some(probe) = probe else {
        return;
    };
    probe.observe(KvNativeEvent::RangesRead {
        ranges: pages.len(),
        entries: pages.iter().map(|page| page.entries.len()).sum(),
    });
}

pub(crate) fn observe_mutations(probe: Option<&dyn KvFaultProbe>, changes: &[crate::KvMutation]) {
    let Some(probe) = probe else {
        return;
    };
    let (mut puts, mut deletes, mut range_deletes) = (0, 0, 0);
    for change in changes {
        match change {
            crate::KvMutation::Put { .. } | crate::KvMutation::PutUnchunked { .. } => puts += 1,
            crate::KvMutation::Delete { .. } | crate::KvMutation::DeleteUnchunked { .. } => {
                deletes += 1;
            }
            crate::KvMutation::DeleteRange { .. }
            | crate::KvMutation::ClearUnchunkedRange { .. } => range_deletes += 1,
        }
    }
    probe.observe(KvNativeEvent::Mutations {
        puts,
        deletes,
        range_deletes,
    });
}

pub trait KvFaultProbe: Send + Sync {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError>;

    /// FoundationDB only: an FDB error code that replaces the answer of a
    /// submitted commit that the cluster acknowledged (FIN-d). The commit has
    /// applied; the attempt sees the code, as if the answer had been lost.
    #[cfg(feature = "fault-injection")]
    fn replace_commit_answer(&self) -> Option<i32> {
        None
    }

    /// Holds the commit of a write attempt (`mode`) at `boundary`; KV
    /// read attempts submit no commit.
    fn at_commit_boundary(
        &self,
        _boundary: KvCommitBoundary,
        _mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn alter_get_batch(
        &self,
        _call: KvFaultCall,
        values: Vec<Option<Vec<u8>>>,
    ) -> Vec<Option<Vec<u8>>> {
        values
    }

    /// Receives every native event of every attempt of the store.
    fn observe(&self, _event: KvNativeEvent) {}

    /// Receives every native key access of every attempt of the store.
    fn observe_access(&self, _access: KvAccess<'_>) {}

    /// Runs once before a wave is sent; the whole wave waits.
    fn before_wave(&self, _wave: KvWave) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }
}
