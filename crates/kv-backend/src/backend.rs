use std::{future::Future, num::NonZeroUsize};

use futures::future::BoxFuture;
use storage_common::diagnostics::{NativeEngine, NativeOperation, trace_native_failure};
pub use storage_types::{AttemptMode, ReadProtection};
use storage_types::{AttemptRequest, StorageError, StorageResult};

use crate::{
    KvMutation, KvRange, KvRangePage, KvVersionstampedMutation,
    mutation_preparation::{expand_mutations, expand_versionstamped_puts},
    value_hydration::resolve_values_with_layouts,
    value_layout::{load_value_heads, resolve_pages},
};

/// Native deployment topology used for limits, not a persisted key format.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum KvTopology {
    Local,
    Distributed,
}

/// One physical native attempt. Implementors supply only driver calls.
///
/// Default methods apply one value layout to point reads, ranges, streams,
/// jobs, and writes on both KV providers.
pub trait KvAttempt: Send + Sync {
    fn settlement(&self) -> &storage_types::NativeAttemptSettlement;

    fn reject_settlement(&mut self) {
        self.settlement().reject();
    }

    /// Opaque control records use the original native address space. Data
    /// views override this method while ordinary attempts return themselves;
    /// both paths borrow the same transaction and share its settlement.
    fn root_attempt(&mut self) -> &mut impl KvAttempt
    where Self: Sized {
        self
    }

    /// Cached protected native catalog observation for this attempt. The shared
    /// metadata-cache phase will use equality before replacing catalog reads.
    fn native_catalog_version(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<storage_types::CatalogVersion>>> + Send;

    /// PERF-FDB-1. Read the catalog version in the next native data wave of
    /// this attempt. `false` (default): the engine reads it only in
    /// `native_catalog_version`'s own wave. No I/O.
    fn native_pipeline_catalog_version(
        &mut self,
        expected: storage_types::CatalogVersion,
    ) -> StorageResult<bool> {
        let _ = expected;
        Ok(false)
    }

    /// PERF-FDB-1 gate: a data wave carried a catalog version that differs
    /// from the requested `expected`, and it has not been taken yet. A
    /// lowerer then drops its raw data undecoded. No I/O.
    fn native_pipelined_version_moved(&mut self) -> bool {
        false
    }

    /// The observation a data wave recorded after
    /// `native_pipeline_catalog_version`, or the attempt's cached one; `None`
    /// when neither exists or the catalog changed. No I/O and no settlement
    /// check.
    fn native_pipelined_catalog_version(
        &mut self,
    ) -> Option<Option<storage_types::CatalogVersion>> {
        None
    }

    /// Protect catalog keys whose values the caller trusts from a cache
    /// validated at this attempt's catalog version, without reading them: a
    /// later commit to any of them aborts this attempt. Engines whose catalog
    /// version read already conflicts (or that serialize writers) need none.
    /// No I/O.
    fn native_protect_reads(&mut self, keys: &[Vec<u8>], ranges: &[KvRange]) -> StorageResult<()> {
        let _ = (keys, ranges);
        Ok(())
    }

    /// Invalidate the observation before the first catalog mutation and stage
    /// exactly one atomic native publication. This may load a protected
    /// counter.
    fn native_record_catalog_change(&mut self) -> impl Future<Output = StorageResult<()>> + Send;

    fn catalog_changed(&self) -> bool;

    fn writer_routes(&self) -> Option<std::sync::Arc<stream_provider::SystemStreamLineageCatalog>>;
    fn install_writer_routes(
        &mut self,
        routes: std::sync::Arc<stream_provider::SystemStreamLineageCatalog>,
    ) -> StorageResult<()>;
    fn reserve_system_event_number(&mut self) -> StorageResult<u16>;

    fn backend_engine(&self) -> NativeEngine;

    /// The store's probe; `None` in production.
    fn fault_probe(&self) -> Option<&dyn crate::KvFaultProbe> {
        None
    }

    fn native_value_limit(&self) -> usize;

    /// A native committed-version frontier, absent on engines without
    /// versionstamps.
    fn native_read_version(&self) -> impl Future<Output = StorageResult<Option<u64>>> + Send {
        std::future::ready(Ok(None))
    }

    /// Sequence of the native RocksDB snapshot captured after writer admission.
    fn native_snapshot_sequence(&self) -> impl Future<Output = StorageResult<Option<u64>>> + Send {
        std::future::ready(Ok(None))
    }

    fn native_stage_byte_max(&mut self, _key: Vec<u8>, _value: Vec<u8>) -> StorageResult<()> {
        self.settlement().require_open()?;
        self.reject_settlement();
        Err(StorageError::internal("Native byte maximum is unsupported"))
    }

    fn native_stage_versionstamped_value(
        &mut self,
        _key: Vec<u8>,
        _value: Vec<u8>,
        _stamp_offset: u32,
    ) -> StorageResult<()> {
        self.settlement().require_open()?;
        self.reject_settlement();
        Err(StorageError::internal(
            "Native versionstamped values are unsupported",
        ))
    }

    /// Read one raw physical value within the caller's explicit response
    /// allowance.
    fn native_get(
        &self,
        key: &[u8],
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                let keys = [copy_native_key(key)?];
                let mut values = self.native_get_batch(&keys, protection, allowance).await?;
                if values.len() != 1 {
                    return Err(StorageError::internal(
                        "native KV point returned the wrong count",
                    ));
                }
                Ok(values.pop().flatten())
            }
            .await;
            call.complete(result)
        }
    }

    fn native_scan(
        &self,
        range: KvRange,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<KvRangePage>> + Send;

    fn native_first_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send;

    fn native_stage(&mut self, changes: Vec<KvMutation>) -> StorageResult<()>;

    fn native_stage_versionstamped(
        &mut self,
        changes: Vec<KvVersionstampedMutation>,
    ) -> StorageResult<()>;

    fn native_get_batch(
        &self,
        keys: &[Vec<u8>],
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send;

    /// Point values of `keys`, each read with the protection at the same
    /// index of `protections`. A networked driver sends them in ONE native
    /// wave (W3 C8). The default reads each protection group with
    /// `native_get_batch`, in first-appearance order: one hop per group, for a
    /// local engine. `allowance` covers all keys; the default gives each group
    /// the head allowance of its own key count.
    fn native_get_batch_mixed(
        &self,
        keys: &[Vec<u8>],
        protections: &[ReadProtection],
        _allowance: crate::KvValueAllowance,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        async move {
            if keys.len() != protections.len() {
                return Err(StorageError::internal(
                    "KV mixed batch protections differ in count",
                ));
            }
            let mut values: Vec<Option<Vec<u8>>> = Vec::new();
            values
                .try_reserve_exact(keys.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            values.resize(keys.len(), None);
            let mut distinct: Vec<ReadProtection> = Vec::new();
            for protection in protections.iter().copied() {
                if !distinct.contains(&protection) {
                    distinct.push(protection);
                }
            }
            for protection in distinct {
                let indexes: Vec<usize> = (0..keys.len())
                    .filter(|&index| protections[index] == protection)
                    .collect();
                let group: Vec<Vec<u8>> =
                    indexes.iter().map(|&index| keys[index].clone()).collect();
                let allowance = crate::KvValueAllowance::for_unqualified_values(group.len())
                    .for_head_reads(group.len())?;
                let read = self.native_get_batch(&group, protection, allowance).await?;
                if read.len() != group.len() {
                    return Err(StorageError::internal(
                        "native KV batch returned the wrong count",
                    ));
                }
                for (index, value) in indexes.into_iter().zip(read) {
                    values[index] = value;
                }
            }
            Ok(values)
        }
    }

    fn native_scan_batch(
        &self,
        ranges: Vec<KvRange>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Vec<KvRangePage>>> + Send;

    fn get(
        &self,
        key: &[u8],
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send {
        self.get_admitted(
            key,
            protection,
            crate::KvValueAllowance::for_unqualified_values(1),
        )
    }

    /// One logical point value shares complete head and hydration admission.
    fn get_admitted(
        &self,
        key: &[u8],
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = (async move {
                let mut copied = Vec::new();
                copied
                    .try_reserve_exact(key.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                copied.extend_from_slice(key);
                let mut keys = Vec::new();
                keys.try_reserve_exact(1)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                keys.push(copied);
                let values = self.get_batch_admitted(keys, protection, allowance).await?;
                let [value] = values
                    .try_into()
                    .map_err(|_| StorageError::internal("one KV value was not returned"))?;
                Ok(value)
            })
            .await;
            call.complete(result)
        }
    }

    fn get_batch(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        let allowance = crate::KvValueAllowance::for_unqualified_values(keys.len());
        self.get_batch_admitted(keys, protection, allowance)
    }

    /// `get_batch` with the one-value-per-key proof built here. No keys, no
    /// native call.
    fn get_batch_exact(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<Vec<u8>>>>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(storage_types::Exact::empty());
            }
            let expected = keys.len();
            storage_types::Exact::try_new(expected, self.get_batch(keys, protection).await?)
        }
    }

    /// `get_batch_mixed` with the one-value-per-key proof built here.
    fn get_batch_mixed_exact(
        &self,
        keys: Vec<Vec<u8>>,
        protections: Vec<ReadProtection>,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<Option<Vec<u8>>>>> + Send {
        async move {
            let expected = keys.len();
            storage_types::Exact::try_new(expected, self.get_batch_mixed(keys, protections).await?)
        }
    }

    /// `get_batch_presence` with the one-flag-per-key proof built here.
    fn get_batch_presence_exact(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<bool>>> + Send {
        async move {
            let expected = keys.len();
            storage_types::Exact::try_new(
                expected,
                self.get_batch_presence(keys, protection).await?,
            )
        }
    }

    /// `scan_batch` with the one-page-per-range proof built here.
    fn scan_batch_exact(
        &self,
        ranges: Vec<KvRange>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<KvRangePage>>> + Send {
        async move {
            let expected = ranges.len();
            storage_types::Exact::try_new(expected, self.scan_batch(ranges, protection).await?)
        }
    }

    /// `native_scan_batch` with the one-page-per-range proof built here.
    fn native_scan_batch_exact(
        &self,
        ranges: Vec<KvRange>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<storage_types::Exact<KvRangePage>>> + Send {
        async move {
            let expected = ranges.len();
            storage_types::Exact::try_new(
                expected,
                self.native_scan_batch(ranges, protection).await?,
            )
        }
    }

    /// Logical values of independent keys, each with its own protection, in
    /// one native wave (W3 C8). Chunked values (rare for the small records
    /// that use this) are hydrated as in `get_batch`, one group per
    /// protection.
    fn get_batch_mixed(
        &self,
        keys: Vec<Vec<u8>>,
        protections: Vec<ReadProtection>,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                let heads = crate::KvValueAllowance::for_unqualified_values(keys.len())
                    .for_head_reads(keys.len())?;
                let raw =
                    crate::value_layout::load_value_heads_mixed(self, &keys, &protections, heads)
                        .await?;
                let mut output: Vec<Option<Vec<u8>>> = Vec::new();
                output
                    .try_reserve_exact(keys.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                output.resize(keys.len(), None);
                let mut entries: Vec<(usize, Vec<u8>, Option<Vec<u8>>)> = keys
                    .into_iter()
                    .zip(raw)
                    .enumerate()
                    .map(|(index, (key, value))| (index, key, value))
                    .collect();
                let mut distinct: Vec<ReadProtection> = Vec::new();
                for protection in protections.iter().copied() {
                    if !distinct.contains(&protection) {
                        distinct.push(protection);
                    }
                }
                for protection in distinct {
                    let (group, rest): (Vec<_>, Vec<_>) = entries
                        .into_iter()
                        .partition(|(index, _, _)| protections[*index] == protection);
                    entries = rest;
                    if group.is_empty() {
                        continue;
                    }
                    let indexes: Vec<usize> = group.iter().map(|(index, _, _)| *index).collect();
                    let inputs = group
                        .into_iter()
                        .map(|(_, key, value)| (key, value))
                        .collect();
                    let allowance = crate::KvValueAllowance::for_unqualified_values(indexes.len());
                    let resolved = crate::value_hydration::resolve_values_with_layouts(
                        self, inputs, protection, allowance, None,
                    )
                    .await?;
                    for (index, (_, value)) in indexes.into_iter().zip(resolved) {
                        output[index] = value;
                    }
                }
                Ok(output)
            }
            .await;
            call.complete(result.inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvGetBatch, error)
            }))
        }
    }

    /// Observe presence without hydrating values whose existence already
    /// rejects the operation.
    fn get_batch_presence(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Vec<bool>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                let mut output = Vec::new();
                output
                    .try_reserve_exact(keys.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                let allowance = crate::KvValueAllowance::for_unqualified_values(keys.len())
                    .for_head_reads(keys.len())?;
                for row in load_value_heads(self, &keys, protection, allowance).await? {
                    output.push(row.is_some());
                }
                Ok(output)
            }
            .await;
            call.complete(result.inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvGetBatch, error)
            }))
        }
    }

    /// Observe up to the requested logical prefix under the same head/chunk
    /// protection. Short values return their complete bytes; codecs own their
    /// required header length.
    fn get_batch_prefix(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        prefix_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Vec<Option<crate::KvValuePrefix>>>> + Send {
        let allowance = crate::KvValueAllowance::for_unqualified_values(keys.len());
        self.get_batch_prefix_admitted(keys, protection, prefix_bytes, allowance)
    }

    fn get_batch_prefix_admitted(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        prefix_bytes: NonZeroUsize,
        allowance: crate::KvValueAllowance,
    ) -> impl Future<Output = StorageResult<Vec<Option<crate::KvValuePrefix>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = crate::value_prefix::load_value_prefixes(
                self,
                &keys,
                crate::value_prefix::KvPrefixRead {
                    protection,
                    prefix_bytes,
                    allowance,
                },
            )
            .await
            .inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvGetBatch, error)
            });
            call.complete(result)
        }
    }

    /// Admit logical value lengths from native heads before fetching chunk
    /// rows. This value allowance excludes keys and includes caller-owned
    /// value headers.
    fn get_batch_bounded(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        maximum_value_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        self.get_batch_admitted(
            keys,
            protection,
            crate::KvValueAllowance::new(
                crate::constants::KV_MAX_VALUE_BYTES,
                maximum_value_bytes.get(),
            ),
        )
    }

    /// One head admission and hydration path for explicit and maintained family
    /// allowances.
    fn get_batch_admitted(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        self.get_batch_admitted_with_layouts(keys, protection, allowance, None)
    }

    /// Retain protected primary head layouts in the owning catalog attempt.
    fn get_batch_primary_layouts(
        &self,
        keys: Vec<Vec<u8>>,
        layouts: &mut Vec<(Vec<u8>, bool)>,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        let allowance = crate::KvValueAllowance::for_unqualified_values(keys.len());
        // Unlimited-value engines never create chunks, so retain no keys for
        // chunk cleanup there.
        let layouts = (self.native_value_limit() != usize::MAX).then_some(layouts);
        self.get_batch_admitted_with_layouts(keys, ReadProtection::Conflict, allowance, layouts)
    }

    fn get_batch_admitted_with_layouts(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
        layouts: Option<&mut Vec<(Vec<u8>, bool)>>,
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                let heads = allowance.for_head_reads(keys.len())?;
                let mut inputs = Vec::new();
                let mut output = Vec::new();
                inputs
                    .try_reserve_exact(keys.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                output
                    .try_reserve_exact(keys.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                let raw = load_value_heads(self, &keys, protection, heads).await?;
                inputs.extend(keys.into_iter().zip(raw));
                for (_, value) in
                    resolve_values_with_layouts(self, inputs, protection, allowance, layouts)
                        .await?
                {
                    output.push(value);
                }
                Ok(output)
            }
            .await;
            call.complete(result.inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvGetBatch, error)
            }))
        }
    }

    fn has_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<bool>> + Send {
        async move {
            self.first_key_in_range(begin, end, protection)
                .await
                .map(|key| key.is_some())
        }
    }

    fn first_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                if let Some(error) = self.fault_probe().and_then(|probe| {
                    probe.fail(crate::KvFaultCall {
                        operation: crate::KvFaultOperation::Scan,
                        mutations: 0,
                    })
                }) {
                    return Err(error);
                }
                self.native_first_key_in_range(begin, end, protection).await
            }
            .await;
            call.complete(result.inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvScan, error)
            }))
        }
    }

    fn scan(
        &self,
        range: KvRange,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<KvRangePage>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = (crate::value_layout::load_page(
                self,
                range,
                protection,
                crate::value_layout::PageValueLayout::Chunkable,
            ))
            .await;
            call.complete(result)
        }
    }

    /// Read a fixed family without accepting chunk headers or issuing chunk
    /// reads.
    fn scan_unchunked(
        &self,
        range: KvRange,
        protection: ReadProtection,
        maximum_value_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<KvRangePage>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = (crate::value_layout::load_page(
                self,
                range,
                protection,
                crate::value_layout::PageValueLayout::Unchunked(maximum_value_bytes),
            ))
            .await;
            call.complete(result)
        }
    }

    fn scan_batch(
        &self,
        ranges: Vec<KvRange>,
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<Vec<KvRangePage>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                let budgets = ranges
                    .iter()
                    .map(|range| range.max_bytes.get())
                    .collect::<Vec<_>>();
                let pages = crate::value_layout::load_range_heads(self, ranges, protection).await?;
                resolve_pages(self, pages, budgets, protection).await
            }
            .await;
            call.complete(result.inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvScanBatch, error)
            }))
        }
    }

    fn stage(&mut self, changes: Vec<KvMutation>) -> StorageResult<()> {
        self.stage_with_primary_layouts(changes, &mut [])
    }

    /// Expand primary changes in order, preserving their layout across flushes.
    fn stage_with_primary_layouts(
        &mut self,
        changes: Vec<KvMutation>,
        layouts: &mut [(Vec<u8>, bool)],
    ) -> StorageResult<()> {
        let backend = self.backend_engine();
        let result = (|| {
            self.settlement().require_open()?;
            let logical = changes.len();
            let physical = expand_mutations(changes, self.native_value_limit(), layouts)?;
            if let Some(error) = self.fault_probe().and_then(|probe| {
                probe.fail(crate::KvFaultCall {
                    operation: crate::KvFaultOperation::Stage,
                    mutations: logical,
                })
            }) {
                return Err(error);
            }
            self.native_stage(physical)
        })();
        if let Err(error) = &result {
            self.settlement().reject_error(error);
        }
        result.inspect_err(|error| trace_native_failure(backend, NativeOperation::KvStage, error))
    }

    /// Insert the native commit stamp at a logical value offset,
    /// including values that require physical chunks.
    fn stage_versionstamped_value(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        stamp_offset: u32,
    ) -> StorageResult<()> {
        let result = (|| {
            self.settlement().require_open()?;
            let plan = crate::versionstamped_value::VersionstampedValue::try_new(
                key,
                value,
                stamp_offset,
                self.native_value_limit(),
            )?;
            self.native_stage(plan.changes)?;
            self.native_stage_versionstamped_value(
                plan.stamped_key,
                plan.stamped_value,
                plan.offset,
            )
        })();
        if let Err(error) = &result {
            self.settlement().reject_error(error);
        }
        result
    }

    /// `stage_versionstamped_value` for a family that never uses chunks.
    fn stage_versionstamped_unchunked_value(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        stamp_offset: u32,
    ) -> StorageResult<()> {
        let result = (|| {
            self.settlement().require_open()?;
            let plan = crate::versionstamped_value::VersionstampedValue::try_new_unchunked(
                key,
                value,
                stamp_offset,
                self.native_value_limit(),
            )?;
            self.native_stage_versionstamped_value(
                plan.stamped_key,
                plan.stamped_value,
                plan.offset,
            )
        })();
        if let Err(error) = &result {
            self.settlement().reject_error(error);
        }
        result
    }

    fn stage_versionstamped_puts(
        &mut self,
        puts: Vec<(Vec<u8>, [u8; 2], Vec<u8>)>,
    ) -> StorageResult<()> {
        let backend = self.backend_engine();
        let result = (|| {
            self.settlement().require_open()?;
            let changes = expand_versionstamped_puts(puts, self.native_value_limit())?;
            if let Some(error) = self.fault_probe().and_then(|probe| {
                probe.fail(crate::KvFaultCall {
                    operation: crate::KvFaultOperation::StageVersionstamped,
                    mutations: 0,
                })
            }) {
                return Err(error);
            }
            self.native_stage_versionstamped(changes)
        })();
        if let Err(error) = &result {
            self.settlement().reject_error(error);
        }
        result.inspect_err(|error| {
            trace_native_failure(backend, NativeOperation::KvStageVersionstamped, error)
        })
    }
}

/// Native KV driver. Only this owner begins and settles the attempt.
/// Conflicts and unknown commits remain distinguishable for shared retries.
pub trait KvBackend: Send + Sync {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines;

    fn ordinary_write_read_version_policy(&self) -> storage_types::ReadVersionPolicy {
        storage_types::ReadVersionPolicy::Fresh
    }

    fn bind_work_limits(&self, _limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        Err(storage_types::StorageError::validation(
            "native backend does not enforce work budgets",
        ))
    }

    type Attempt: KvAttempt;

    /// Report actual initialized native scheduling, independently of registry
    /// settings.
    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity>;
    /// Whether the store's durability batch stopped after a failed sync.
    /// Stores that make each commit durable before it is visible have none.
    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        None
    }

    fn topology(&self) -> KvTopology;

    /// The process board of a store shared by processes (FoundationDB, N1b).
    fn queue_wake_board(&self) -> Option<std::sync::Arc<queue_provider::QueueWakeBoard>> {
        None
    }

    fn native_layout(&self) -> crate::KvNativeLayout;

    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static;

    /// Return the committed 10-byte versionstamp after the lent attempt
    /// settles, or None when the native commit acknowledges no writes. Only
    /// a backend with native versionstamped keys implements it.
    fn with_versionstamped_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> impl Future<Output = StorageResult<(R, Option<[u8; 10]>)>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static;
}

/// Reserve complete channel-owned addresses before native dispatch.
pub(crate) fn copy_native_key(key: &[u8]) -> StorageResult<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(key.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    owned.extend_from_slice(key);
    Ok(owned)
}

#[cfg(feature = "rocksdb")]
pub(crate) fn copy_native_keys(keys: &[Vec<u8>]) -> StorageResult<Vec<Vec<u8>>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for key in keys {
        owned.push(copy_native_key(key)?);
    }
    Ok(owned)
}
