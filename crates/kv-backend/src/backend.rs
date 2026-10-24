use std::{future::Future, num::NonZeroUsize};

use futures::future::BoxFuture;
use storage_common::diagnostics::{NativeEngine, NativeOperation, trace_native_failure};
pub use storage_types::{AttemptMode, ReadProtection};
use storage_types::{StorageError, StorageResult};

use crate::{
    KvMutation, KvRange, KvRangePage, KvVersionstampedMutation,
    mutation_preparation::{expand_mutations, expand_versionstamped_puts},
    value_hydration::resolve_values,
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

    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges;

    /// Opaque control records use the original native address space. Data
    /// views override this method while ordinary attempts return themselves;
    /// both paths borrow the same transaction and share its settlement.
    fn root_attempt(&mut self) -> &mut impl KvAttempt
    where Self: Sized {
        self
    }

    fn backend_engine(&self) -> NativeEngine;

    #[cfg(feature = "fault-injection")]
    fn fault_probe(&self) -> Option<&dyn crate::KvFaultProbe> {
        None
    }

    fn native_value_limit(&self) -> usize;

    /// A native committed-version frontier, absent on engines without
    /// versionstamps.
    fn native_read_version(&self) -> impl Future<Output = StorageResult<Option<u64>>> + Send {
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

    fn native_has_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> impl Future<Output = StorageResult<bool>> + Send;

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
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                copied.extend_from_slice(key);
                let mut keys = Vec::new();
                keys.try_reserve_exact(1)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
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
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
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

    /// Observe a fixed logical prefix under the same head/chunk protection.
    fn get_batch_prefix(
        &self,
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        prefix_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Vec<Option<crate::KvValuePrefix>>>> + Send {
        async move {
            let call = self.settlement().begin_call()?;
            let result =
                crate::value_prefix::load_value_prefixes(self, &keys, protection, prefix_bytes)
                    .await
                    .inspect_err(|error| {
                        trace_native_failure(
                            self.backend_engine(),
                            NativeOperation::KvGetBatch,
                            error,
                        )
                    });
            call.complete(result)
        }
    }

    /// Admit logical value lengths from native heads before fetching chunk
    /// trees. This value allowance excludes keys and includes caller-owned
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
        async move {
            let call = self.settlement().begin_call()?;
            let result = async {
                let heads = allowance.for_head_reads(keys.len())?;
                let mut inputs = Vec::new();
                let mut output = Vec::new();
                inputs
                    .try_reserve_exact(keys.len())
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                output
                    .try_reserve_exact(keys.len())
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                let raw = load_value_heads(self, &keys, protection, heads).await?;
                inputs.extend(keys.into_iter().zip(raw));
                for (_, value) in resolve_values(self, inputs, protection, allowance).await? {
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
            let call = self.settlement().begin_call()?;
            let result = async {
                #[cfg(feature = "fault-injection")]
                if let Some(error) = self.fault_probe().and_then(|probe| {
                    probe.fail(crate::KvFaultCall {
                        operation: crate::KvFaultOperation::Scan,
                    })
                }) {
                    return Err(error);
                }
                self.native_has_key_in_range(begin, end, protection).await
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
                #[cfg(feature = "fault-injection")]
                if let Some(error) = self.fault_probe().and_then(|probe| {
                    probe.fail(crate::KvFaultCall {
                        operation: crate::KvFaultOperation::ScanBatch,
                    })
                }) {
                    return Err(error);
                }
                let budgets = ranges
                    .iter()
                    .map(|range| range.max_bytes.get())
                    .collect::<Vec<_>>();
                let pages = self.native_scan_batch(ranges, protection).await?;
                if pages.len() != budgets.len() {
                    return Err(StorageError::internal(
                        "native KV range batch returned the wrong count",
                    ));
                }
                resolve_pages(self, pages, budgets, protection).await
            }
            .await;
            call.complete(result.inspect_err(|error| {
                trace_native_failure(self.backend_engine(), NativeOperation::KvScanBatch, error)
            }))
        }
    }

    fn stage(&mut self, changes: Vec<KvMutation>) -> StorageResult<()> {
        let backend = self.backend_engine();
        let result = (|| {
            self.settlement().require_open()?;
            let physical = expand_mutations(changes, self.native_value_limit())?;
            #[cfg(feature = "fault-injection")]
            if let Some(error) = self.fault_probe().and_then(|probe| {
                probe.fail(crate::KvFaultCall {
                    operation: crate::KvFaultOperation::Stage,
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

    fn stage_versionstamped_puts(
        &mut self,
        puts: Vec<(Vec<u8>, [u8; 2], Vec<u8>)>,
    ) -> StorageResult<()> {
        let backend = self.backend_engine();
        let result = (|| {
            self.settlement().require_open()?;
            let changes = expand_versionstamped_puts(puts, self.native_value_limit())?;
            #[cfg(feature = "fault-injection")]
            if let Some(error) = self.fault_probe().and_then(|probe| {
                probe.fail(crate::KvFaultCall {
                    operation: crate::KvFaultOperation::StageVersionstamped,
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
    fn bind_work_limits(&self, _limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        Err(storage_types::StorageError::validation(
            "native backend does not enforce work budgets",
        ))
    }

    type Attempt: KvAttempt;

    /// Report actual initialized native scheduling, independently of registry
    /// settings.
    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity>;

    fn topology(&self) -> KvTopology;

    fn native_layout(&self) -> crate::KvNativeLayout;

    fn with_attempt<R, F>(
        &self,
        mode: AttemptMode,
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
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    owned.extend_from_slice(key);
    Ok(owned)
}

#[cfg(feature = "rocksdb")]
pub(crate) fn copy_native_keys(keys: &[Vec<u8>]) -> StorageResult<Vec<Vec<u8>>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for key in keys {
        owned.push(copy_native_key(key)?);
    }
    Ok(owned)
}
