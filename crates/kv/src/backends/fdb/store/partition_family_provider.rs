use super::*;

#[async_trait::async_trait]
impl PartitionFamilyKvStore for FoundationDbKvStore {
    fn supports_partition_families(&self) -> bool {
        true
    }

    async fn load_partition_family_state_raw(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<Option<ResolvedPartitionFamily>> {
        let prefix = self.config.subspace_prefix.clone();
        let family_config_key = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        let partition_prefix = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_info_prefix(family_kind, family_component),
        );
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            match Self::load_partition_family_state_tx_retryable(
                &trx,
                prefix.as_ref(),
                family_kind,
                family_component,
            )
            .await
            {
                Ok(family) => return Ok(family),
                Err(FdbTransactionAttemptError::Storage(storage_err)) => return Err(storage_err),
                Err(FdbTransactionAttemptError::Fdb { scope, error }) => {
                    let candidate_keys = vec![family_config_key.clone(), partition_prefix.clone()];
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "load_partition_family_state_raw",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    async fn append_partitioned_ordered_log_item(
        &self,
        stream_name: &StreamName,
        routing_key: &[u8],
        value: &[u8],
        fallback_item_id: StreamItemId,
    ) -> StorageResult<Option<StreamItemId>> {
        let prefix = self.config.subspace_prefix.clone();
        let family_component = ordered_log_family_component(stream_name);
        let family_config_key = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_family_config_key(
                PartitionFamilyKind::OrderedLog,
                &family_component,
            ),
        );
        let value = value.to_vec();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let mut ordered_log_family_cache = OrderedLogFamilyCache::new();
            let family = match Self::ensure_ordered_log_family_state_tx_retryable(
                &trx,
                prefix.as_ref(),
                stream_name,
            )
            .await
            {
                Ok(family) => family,
                Err(FdbTransactionAttemptError::Storage(storage_err)) => {
                    return Err(storage_err);
                }
                Err(FdbTransactionAttemptError::Fdb { scope, error }) => {
                    let candidate_keys = vec![family_config_key.clone()];
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "append_partitioned_ordered_log_item",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            ordered_log_family_cache.insert(family_component.clone(), family.clone());
            let partition =
                find_partition_for_hash(&family.partitions, ordered_log_hash(routing_key))
                    .ok_or_else(|| {
                        StorageError::internal("ordered log family has no writable partition")
                    })?;
            let partition_prefix = ordered_log_partition_prefix_with_slot(
                stream_name,
                partition.placement_slot,
                partition.partition_id,
            );
            let binding = PlaceholderBinding::unique(fallback_item_id.as_bytes().to_vec());
            let template = crate::key_template::KeyTemplate::placeholder(
                partition_prefix.clone(),
                Vec::new(),
                binding.clone(),
            );
            let version_future = trx.get_versionstamp();

            self.apply_mutations(
                prefix.as_ref(),
                &trx,
                vec![KvMutation::PutTemplate {
                    template,
                    value: value.clone(),
                }],
                &mut Vec::new(),
                &mut ordered_log_family_cache,
            )
            .await?;

            match trx.commit().await {
                Ok(_) => {
                    self.runtime_partition_load_tracker
                        .record(RuntimePartitionLoadSample {
                            family_kind: PartitionFamilyKind::OrderedLog,
                            family_component: family_component.clone(),
                            partition_id: partition.partition_id,
                            sample: PartitionLoadSample {
                                writes: 1,
                                bytes: u64::try_from(value.len()).unwrap_or(u64::MAX),
                                conflicts: u64::from(attempt.saturating_sub(1)),
                                routing_key_bucket_bitmap: routing_key_bucket_bit(
                                    ordered_log_hash(routing_key),
                                ),
                                queue_scan_work: 0,
                                queue_claim_conflicts: 0,
                                oldest_visible_age_ms: 0,
                                visible_count: 0,
                                invisible_count: 0,
                            },
                        });
                    let committed = version_future
                        .await
                        .map_err(|err| map_fdb_error("get versionstamp", err))?;
                    let data = committed.as_ref();
                    if data.len() != 10 {
                        return Err(StorageError::internal("unexpected versionstamp length"));
                    }
                    let mut bytes = [0u8; 12];
                    bytes[..10].copy_from_slice(data);
                    bytes[10..].copy_from_slice(&binding.user_bytes);
                    return Ok(Some(StreamItemId::from(bytes)));
                }
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = vec![
                                family_config_key.clone(),
                                Self::prefix_bytes(prefix.as_ref(), &partition_prefix),
                            ];
                            self.log_conflict_details(
                                &new_trx,
                                "append_partitioned_ordered_log_item",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error(
                                "append partitioned ordered log commit",
                                retry_err,
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn drain_runtime_partition_load_samples(
        &self,
    ) -> StorageResult<Vec<RuntimePartitionLoadSample>> {
        Ok(self.runtime_partition_load_tracker.drain())
    }

    fn partition_runtime_load_hint(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        partition_id: u16,
    ) -> u64 {
        self.runtime_partition_load_tracker
            .load_hint(family_kind, family_component, partition_id)
    }

    async fn wait_for_change(&self, key: &[u8], timeout: Duration) -> StorageResult<bool> {
        let prefixed_key = self.prefix_slice(key);
        let trx = self.create_transaction()?;
        Self::configure_transaction(&trx, Some("kv.wait_for_change"), true)?;
        let watch = trx.watch(&prefixed_key);
        trx.commit()
            .await
            .map_err(|err| map_fdb_error("commit FoundationDB watch", *err))?;

        match time::timeout(timeout, watch).await {
            Ok(Ok(())) => Ok(true),
            Ok(Err(err)) => Err(map_fdb_error("await FoundationDB watch", err)),
            Err(_) => Ok(false),
        }
    }

    async fn split_partitioned_ordered_log_family(
        &self,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> StorageResult<bool> {
        let prefix = self.config.subspace_prefix.clone();
        let family_config_key = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_family_config_key(
                PartitionFamilyKind::OrderedLog,
                family_component,
            ),
        );
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let changed = match self
                .split_partitioned_ordered_log_family_tx(
                    &trx,
                    prefix.as_ref(),
                    family_component,
                    partition_id,
                    now_ms,
                )
                .await
            {
                Ok(changed) => changed,
                Err(FdbTransactionAttemptError::Storage(storage_err)) => return Err(storage_err),
                Err(FdbTransactionAttemptError::Fdb { scope, error }) => {
                    let candidate_keys = vec![family_config_key.clone()];
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "split_partitioned_ordered_log_family",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            if !changed {
                return Ok(false);
            }

            match trx.commit().await {
                Ok(_) => return Ok(true),
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = vec![family_config_key.clone()];
                            self.log_conflict_details(
                                &new_trx,
                                "split_partitioned_ordered_log_family",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error(
                                "split partitioned ordered log family commit",
                                retry_err,
                            ));
                        }
                    }
                }
            }
        }
    }
}
