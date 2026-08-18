use std::{collections::HashMap, time::Instant};

use foundationdb::{FdbError, Transaction, options};
use futures_util::future::try_join_all;
#[cfg(test)]
use storage_common::provider_perf;
use storage_types::{StorageEnum, StorageResult, StreamItemId};

use crate::{
    backends::{
        common::{
            KvMutation, operation_requires_stream_entries, plan_table_write_preflighted_with_codec,
            plan_transact_operation, preflight_table_write_operations, table_operation_primary_key,
        },
        fdb::{
            error::map_fdb_error,
            store::{
                FdbTableWriteExecution, FdbTableWriteExecutionError, FoundationDbKvStore,
                OrderedLogFamilyCache, PendingOrderedLogWrite, adjust_versionstamp_offset,
            },
        },
    },
    key_template::{
        KeyTemplate, PlaceholderBinding, PlaceholderId, VersionstampedWriteConflictPolicy,
    },
    sorted_kv_store::{
        DirectWriteOperation, OldNewItems, TransactWriteOperation, TransactWriteTableOperation,
    },
};

fn write_versionstamped_template(
    trx: &Transaction,
    template: &KeyTemplate,
    prefix: &[u8],
    value: &[u8],
) -> Result<bool, FdbError> {
    let Some(mut versioned) = template.foundationdb_key() else {
        return Ok(false);
    };
    let mut composed = Vec::with_capacity(prefix.len() + versioned.len());
    composed.extend_from_slice(prefix);
    composed.extend_from_slice(&versioned);
    adjust_versionstamp_offset(&mut composed, prefix.len());
    versioned = composed;

    // Only the typed unique-template variant is allowed to omit this conflict
    // range.
    if matches!(
        template.versionstamped_write_conflict_policy(),
        VersionstampedWriteConflictPolicy::OmitWriteConflictForUniqueKey
    ) {
        trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)?;
    }
    trx.atomic_op(
        &versioned,
        value,
        options::MutationType::SetVersionstampedKey,
    );
    Ok(true)
}

impl FoundationDbKvStore {
    pub(crate) async fn apply_mutations(
        &self,
        prefix: &[u8],
        trx: &Transaction,
        mutations: Vec<KvMutation>,
        ordered_log_writes: &mut Vec<PendingOrderedLogWrite>,
        ordered_log_family_cache: &mut OrderedLogFamilyCache,
    ) -> StorageResult<()> {
        for mutation in &mutations {
            match mutation {
                KvMutation::Put { key, value } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.set(&prefixed, value);
                }
                KvMutation::Delete { key } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.clear(&prefixed);
                }
                KvMutation::PutTemplate { .. } => {}
            }
        }

        for mutation in &mutations {
            let KvMutation::PutTemplate { template, value } = mutation else {
                continue;
            };
            let (template, ordered_log_write) = self
                .rewrite_partitioned_pointer_template(
                    trx,
                    prefix,
                    template,
                    value,
                    ordered_log_family_cache,
                )
                .await?;
            if let Some(ordered_log_write) = ordered_log_write {
                ordered_log_writes.push(ordered_log_write);
            }
            if !write_versionstamped_template(trx, &template, prefix, value)
                .map_err(|err| map_fdb_error("write versionstamped template", err))?
            {
                let key = template.rocks_key();
                let prefixed = Self::prefix_bytes(prefix, &key);
                trx.set(&prefixed, value);
            }
        }

        Ok(())
    }

    pub(crate) async fn execute_transact_write_tx(
        &self,
        trx: &Transaction,
        operations: &[TransactWriteOperation],
        prefix: &[u8],
    ) -> StorageResult<(
        Vec<OldNewItems>,
        HashMap<PlaceholderId, PlaceholderBinding>,
        Vec<PendingOrderedLogWrite>,
    )> {
        let mut results = Vec::with_capacity(operations.len());
        let mut bindings: HashMap<PlaceholderId, PlaceholderBinding> = HashMap::new();
        let mut ordered_log_writes = Vec::new();
        let mut ordered_log_family_cache = OrderedLogFamilyCache::new();

        let current_read_keys = operations
            .iter()
            .map(|operation| {
                match operation {
                    TransactWriteOperation::Put { key, condition, .. } => {
                        if condition.is_some() {
                            Some(Self::prefix_bytes(prefix, key))
                        } else {
                            // Shortcut: unconditional Put writes do not need a
                            // current-item read for correctness.
                            None
                        }
                    }
                    TransactWriteOperation::Delete { key, .. }
                    | TransactWriteOperation::Check { key, .. }
                    | TransactWriteOperation::CheckValue { key, .. }
                    | TransactWriteOperation::Update { key, .. } => {
                        Some(Self::prefix_bytes(prefix, key))
                    }
                    TransactWriteOperation::PutTemplate { .. } => None,
                }
            })
            .collect::<Vec<_>>();

        let current_values = try_join_all(current_read_keys.into_iter().map(|key| async move {
            let Some(key) = key else {
                return Ok(None);
            };
            trx.get(&key, false)
                .await
                .map_err(|err| map_fdb_error("read key", err))
                .map(|value| value.map(|value| value.to_vec()))
        }))
        .await?;

        for (index, (operation, current)) in operations.iter().zip(current_values).enumerate() {
            // FDB retries may re-run this loop with the same operation slice, so
            // keep owned planning local by cloning here.
            let (old_new, mutations) =
                plan_transact_operation(operation.clone(), current.as_deref(), index)?;

            for mutation in &mutations {
                if let KvMutation::PutTemplate { template, .. } = mutation
                    && let Some(binding) = template.placeholder_binding().cloned()
                {
                    bindings.entry(binding.id()).or_insert(binding);
                }
            }

            self.apply_mutations(
                prefix,
                trx,
                mutations,
                &mut ordered_log_writes,
                &mut ordered_log_family_cache,
            )
            .await?;
            results.push(old_new);
        }

        Ok((results, bindings, ordered_log_writes))
    }

    pub(crate) async fn execute_transact_write_table_tx(
        &self,
        trx: &Transaction,
        operations: &[TransactWriteTableOperation],
        stream_ids: &[Option<StreamItemId>],
        prefix: &[u8],
        immediate_gsi_consistency: bool,
    ) -> Result<FdbTableWriteExecution, FdbTableWriteExecutionError> {
        preflight_table_write_operations(operations)?;
        let read_started = Instant::now();
        let current_reads = operations
            .iter()
            .map(|operation| -> StorageResult<Option<Vec<u8>>> {
                let key_bytes = table_operation_primary_key(operation)?;
                Ok(Some(Self::prefix_bytes(prefix, &key_bytes)))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let current_read_count = current_reads.iter().filter(|key| key.is_some()).count();
        let current_values = try_join_all(current_reads.into_iter().map(|key| async move {
            let Some(key) = key else {
                return Ok(None);
            };
            trx.get(&key, false)
                .await
                .map_err(|err| FdbTableWriteExecutionError::fdb("read table item", err))
                .map(|value| value.map(|value| value.to_vec()))
        }))
        .await?;
        #[cfg(test)]
        provider_perf::record(
            "foundationdb",
            "table_write_current_read",
            read_started.elapsed(),
        );

        let plan_started = Instant::now();
        let plan = plan_table_write_preflighted_with_codec(
            operations,
            current_values,
            stream_ids,
            immediate_gsi_consistency,
            crate::sorted_kv_store::ItemValueCodec::FoundationDbTuple,
        )?;
        let plan_elapsed = plan_started.elapsed();
        #[cfg(test)]
        provider_perf::record("foundationdb", "table_write_plan", plan_elapsed);
        let applied_mutation_count = plan.mutations.len();
        #[cfg(test)]
        {
            provider_perf::record_amount(
                "foundationdb",
                "table_write_mutations",
                plan.stats.mutation_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_applied_mutations",
                applied_mutation_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_gsi_mutations",
                plan.stats.gsi_mutation_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_gsi_key_overlap",
                plan.stats.gsi_key_overlap_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_gsi_collapsed",
                plan.stats.collapsed_gsi_mutation_count as u64,
            );
        }

        let apply_started = Instant::now();
        let mut ordered_log_writes = Vec::new();
        let mut ordered_log_family_cache = OrderedLogFamilyCache::new();
        self.apply_mutations(
            prefix,
            trx,
            plan.mutations,
            &mut ordered_log_writes,
            &mut ordered_log_family_cache,
        )
        .await?;
        let apply_elapsed = apply_started.elapsed();
        #[cfg(test)]
        provider_perf::record("foundationdb", "table_write_apply", apply_elapsed);
        tracing::debug!(
            operation_count = operations.len(),
            current_read_count,
            current_read_ms = read_started.elapsed().as_secs_f64() * 1000.0,
            plan_ms = plan_elapsed.as_secs_f64() * 1000.0,
            apply_ms = apply_elapsed.as_secs_f64() * 1000.0,
            mutation_count = plan.stats.mutation_count,
            applied_mutation_count,
            gsi_mutation_count = plan.stats.gsi_mutation_count,
            gsi_distinct_key_count = plan.stats.gsi_distinct_key_count,
            gsi_key_overlap_count = plan.stats.gsi_key_overlap_count,
            collapsed_gsi_mutation_count = plan.stats.collapsed_gsi_mutation_count,
            ordered_log_write_count = ordered_log_writes.len(),
            immediate_gsi_consistency,
            "foundationdb transact_write_table phase timing"
        );

        Ok(FdbTableWriteExecution {
            results: plan.results,
            ordered_log_writes,
        })
    }

    pub(crate) async fn execute_transact_write_table_with_direct_writes_tx(
        &self,
        trx: &Transaction,
        table_operations: &[TransactWriteTableOperation],
        direct_operations: &[DirectWriteOperation],
        stream_ids: &[Option<StreamItemId>],
        prefix: &[u8],
        immediate_gsi_consistency: bool,
    ) -> Result<FdbTableWriteExecution, FdbTableWriteExecutionError> {
        let mut execution = self
            .execute_transact_write_table_tx(
                trx,
                table_operations,
                stream_ids,
                prefix,
                immediate_gsi_consistency,
            )
            .await?;
        execution.ordered_log_writes.extend(
            self.execute_transact_write_unchecked_tx(trx, direct_operations, prefix)
                .await?,
        );
        Ok(execution)
    }

    pub(crate) async fn execute_transact_write_unchecked_tx(
        &self,
        trx: &Transaction,
        operations: &[DirectWriteOperation],
        prefix: &[u8],
    ) -> Result<Vec<PendingOrderedLogWrite>, FdbTableWriteExecutionError> {
        let mut ordered_log_writes = Vec::new();
        let mut ordered_log_family_cache = OrderedLogFamilyCache::new();
        for operation in operations {
            match operation {
                DirectWriteOperation::Put { key, value } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.set(&prefixed, value);
                }
                DirectWriteOperation::PutTemplate { template, value } => {
                    let (template, ordered_log_write) = self
                        .rewrite_partitioned_pointer_template(
                            trx,
                            prefix,
                            template,
                            value,
                            &mut ordered_log_family_cache,
                        )
                        .await?;
                    if let Some(ordered_log_write) = ordered_log_write {
                        ordered_log_writes.push(ordered_log_write);
                    }
                    if !write_versionstamped_template(trx, &template, prefix, value).map_err(
                        |err| {
                            FdbTableWriteExecutionError::fdb("write versionstamped template", err)
                        },
                    )? {
                        let key = template.rocks_key();
                        let prefixed = Self::prefix_bytes(prefix, &key);
                        trx.set(&prefixed, value);
                    }
                }
                DirectWriteOperation::Delete { key } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.clear(&prefixed);
                }
                DirectWriteOperation::DeleteRange {
                    start,
                    exclusive_end,
                } => {
                    let prefixed_start = Self::prefix_bytes(prefix, start);
                    let prefixed_end = Self::prefix_bytes(prefix, exclusive_end);
                    trx.clear_range(&prefixed_start, &prefixed_end);
                }
                DirectWriteOperation::CheckValue {
                    key,
                    expected_value,
                } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    let current = trx
                        .get(&prefixed, false)
                        .await
                        .map_err(|err| {
                            FdbTableWriteExecutionError::fdb("read key for exact value check", err)
                        })?
                        .map(|value| value.to_vec());
                    if current != *expected_value {
                        return Err(FdbTableWriteExecutionError::Storage(
                            StorageEnum::ConditionalCheckFailed.into(),
                        ));
                    }
                }
            }
        }

        Ok(ordered_log_writes)
    }

    pub(crate) async fn build_stream_ids(
        &self,
        operations: &[TransactWriteTableOperation],
    ) -> Vec<Option<StreamItemId>> {
        let mut ids = Vec::with_capacity(operations.len());
        for operation in operations {
            if operation_requires_stream_entries(operation, self.config.immediate_gsi_consistency) {
                ids.push(Some(storage_types::StreamItemId::random()));
            } else {
                ids.push(None);
            }
        }
        ids
    }
}
