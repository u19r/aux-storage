use crate::{queue_provider::*, sorted_kv_store::TransactionPriority};

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn ensure_queue_execution_context(
        &self,
        queue_url: &str,
    ) -> QueueResult<QueueExecutionContext> {
        let queue_id = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?
            .queue_id;
        let routing_state = QueueRoutingState::Control(
            self.ensure_queue_family_state(queue_url, DEFAULT_STANDARD_QUEUE_PARTITION_COUNT)
                .await?,
        );
        Ok(QueueExecutionContext {
            queue_id,
            routing_state,
        })
    }

    pub(crate) async fn queue_execution_context(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<QueueExecutionContext>> {
        let Some(routing_state) = self.queue_routing_state(queue_url).await? else {
            return Ok(None);
        };
        let queue_id = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?
            .queue_id;
        Ok(Some(QueueExecutionContext {
            queue_id,
            routing_state,
        }))
    }

    pub(crate) fn record_queue_partition_load(
        &self,
        queue_url: &str,
        partition_id: u16,
        sample: crate::partition_family::PartitionLoadSample,
    ) {
        self.runtime_partition_load_tracker.record(
            crate::partition_family::RuntimePartitionLoadSample {
                family_kind: PartitionFamilyKind::StandardQueue,
                family_component: queue_family_component(queue_url),
                partition_id,
                sample,
            },
        );
    }

    pub(crate) fn local_queue_partition_load_hint(
        &self,
        queue_url: &str,
        partition_id: u16,
    ) -> u64 {
        let family_component = queue_family_component(queue_url);
        self.runtime_partition_load_tracker.load_hint(
            PartitionFamilyKind::StandardQueue,
            &family_component,
            partition_id,
        ) + self.kv_store.partition_runtime_load_hint(
            PartitionFamilyKind::StandardQueue,
            &family_component,
            partition_id,
        )
    }

    pub(crate) fn queue_partition_route_for_send(
        &self,
        routing_state: &QueueRoutingState,
        queue_url: &str,
        message_id: &MessageId,
    ) -> Option<QueuePartitionRoute> {
        match routing_state {
            QueueRoutingState::Control(family) => {
                let writable: Vec<_> = family
                    .partitions
                    .iter()
                    .filter(|partition| partition.is_writable())
                    .collect();
                let first = writable.first()?;
                if writable.len() == 1 {
                    return Some(QueuePartitionRoute {
                        partition_id: first.partition_id,
                        placement_slot: first.placement_slot,
                    });
                }

                let primary_index = usize::try_from(queue_partition_hash(queue_url, message_id))
                    .unwrap_or(0)
                    % writable.len();
                // Power-of-two choices: compare the primary hash route with a
                // second deterministic route so hot queues can avoid a locally
                // overloaded partition without making send placement random.
                let mut secondary_route_key = queue_url.as_bytes().to_vec();
                secondary_route_key.extend_from_slice(message_id.as_bytes());
                secondary_route_key.extend_from_slice(b"/secondary-route");
                let secondary_index = usize::try_from(ordered_log_hash(&secondary_route_key))
                    .unwrap_or(0)
                    % writable.len();

                let primary = writable[primary_index];
                let secondary = writable[secondary_index];
                let primary_hint =
                    self.local_queue_partition_load_hint(queue_url, primary.partition_id);
                let secondary_hint =
                    self.local_queue_partition_load_hint(queue_url, secondary.partition_id);
                let chosen = match primary_hint.cmp(&secondary_hint) {
                    std::cmp::Ordering::Less => primary,
                    std::cmp::Ordering::Greater => secondary,
                    std::cmp::Ordering::Equal => primary,
                };

                Some(QueuePartitionRoute {
                    partition_id: chosen.partition_id,
                    placement_slot: chosen.placement_slot,
                })
            }
        }
    }

    pub(crate) async fn queue_roots_for_payload_cleanup(
        &self,
    ) -> StorageResult<Vec<StoredQueueIdentity>> {
        self.list_queue_identities()
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))
    }

    pub(crate) async fn cleanup_queue_payload_orphans(&self, limit: u32) -> StorageResult<u64> {
        if limit == 0 {
            return Ok(0);
        }
        let now = TimestampMillis::now();
        let mut remaining = limit;
        let mut scanned = 0u64;
        let mut deleted = 0u64;
        let mut oldest_age_ms = 0i64;
        let mut pending_deletes = Vec::new();
        let mut ledger_scanned = 0u64;

        for identity in self.queue_roots_for_payload_cleanup().await? {
            if remaining == 0 {
                break;
            }
            let queue_url = identity.queue.queue_url;
            let queue_id = identity.queue_id;
            let ledger_entries = self
                .kv_store
                .get_prefix(
                    &queue_delete_ledger_prefix_for_id(queue_id),
                    true,
                    Some(remaining),
                    false,
                )
                .await?;
            let ledger_entries_found = !ledger_entries.items.is_empty();
            if ledger_entries_found {
                ledger_scanned = ledger_scanned
                    .saturating_add(u64::try_from(ledger_entries.items.len()).unwrap_or(u64::MAX));
                remaining = remaining
                    .saturating_sub(u32::try_from(ledger_entries.items.len()).unwrap_or(u32::MAX));
                for (ledger_key, ledger_value) in ledger_entries.items {
                    let Ok(entry) = storage_types::storage_serde::from_bytes::<
                        QueueDeleteLedgerEntry,
                    >(&ledger_value) else {
                        pending_deletes.push(DirectWriteOperation::Delete {
                            key: ledger_key.into_vec(),
                        });
                        continue;
                    };
                    oldest_age_ms = oldest_age_ms.max(
                        now.timestamp_millis()
                            .saturating_sub(entry.created_at.timestamp_millis()),
                    );
                    pending_deletes.extend([
                        queue_payload_delete_range(entry.payload_key),
                        DirectWriteOperation::Delete {
                            key: entry.state_key,
                        },
                        DirectWriteOperation::Delete {
                            key: ledger_key.into_vec(),
                        },
                    ]);
                    deleted = deleted.saturating_add(1);
                }
            }
            if remaining == 0 {
                break;
            }
            if ledger_entries_found {
                continue;
            }
            let Some(family) = self
                .load_queue_family_state(&queue_url)
                .await
                .map_err(|error| StorageError::internal(&error.to_string()))?
            else {
                continue;
            };
            for route in queue_partition_routes(&QueueRoutingState::Control(family.clone())) {
                if remaining == 0 {
                    break;
                }
                let payload_prefix =
                    queue_body_prefix_with_slot(queue_id, route.placement_slot, route.partition_id);
                let payloads = self
                    .kv_store
                    .get_prefix(&payload_prefix, true, Some(remaining), false)
                    .await?;
                if payloads.items.is_empty() {
                    continue;
                }

                scanned =
                    scanned.saturating_add(u64::try_from(payloads.items.len()).unwrap_or(u64::MAX));
                remaining = remaining
                    .saturating_sub(u32::try_from(payloads.items.len()).unwrap_or(u32::MAX));
                let payload_infos = payloads
                    .items
                    .into_iter()
                    .filter_map(|(payload_key, payload_value)| {
                        if queue_payload_is_chunk_key(&payload_key) {
                            return None;
                        }
                        let message_id_hex =
                            std::str::from_utf8(payload_key.get(payload_prefix.len()..)?)
                                .ok()?
                                .to_string();
                        if message_id_hex.contains('/') {
                            return None;
                        }
                        let state_key = queue_state_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &message_id_hex,
                        );
                        Some((payload_key, payload_value, message_id_hex, state_key))
                    })
                    .collect::<Vec<_>>();
                let states = self
                    .kv_store
                    .multi_get(
                        payload_infos
                            .iter()
                            .map(|(_, _, _, state_key)| state_key.clone())
                            .collect(),
                        false,
                    )
                    .await?;
                let mut payload_deletes = 0u64;
                for ((payload_key, payload_value, message_id_hex, state_key), state) in
                    payload_infos.into_iter().zip(states)
                {
                    let mut delete_state = false;
                    if let Some(state_bytes) = state {
                        let Ok(state) = storage_types::storage_serde::from_bytes::<
                            PartitionedQueueState,
                        >(&state_bytes) else {
                            continue;
                        };
                        let Ok(message_id) = message_id_hex.parse::<MessageId>() else {
                            continue;
                        };
                        let visibility_key = MessageVisibilityKey(visibility_key(
                            state.visibility_timestamp,
                            &message_id,
                        ));
                        let ready_key = queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &visibility_key,
                        );
                        if self.kv_store.get(&ready_key, false).await?.is_some() {
                            continue;
                        }
                        delete_state = true;
                    }
                    if let Ok(body) = storage_types::storage_serde::from_bytes::<PartitionedQueueBody>(
                        &payload_value,
                    ) {
                        oldest_age_ms = oldest_age_ms.max(
                            now.timestamp_millis()
                                .saturating_sub(body.created_at.timestamp_millis()),
                        );
                    }
                    pending_deletes.push(queue_payload_delete_range(payload_key.into_vec()));
                    payload_deletes = payload_deletes.saturating_add(1);
                    if delete_state {
                        pending_deletes.push(DirectWriteOperation::Delete { key: state_key });
                    }
                }
                if payload_deletes == 0 {
                    continue;
                }
                deleted = deleted.saturating_add(payload_deletes);
            }
        }
        if !pending_deletes.is_empty() {
            self.kv_store
                .transact_write_unchecked(pending_deletes)
                .await?;
        }

        record_queue_storage_operation("queue_payload_cleanup", "orphan_scanned", scanned);
        record_queue_storage_operation("queue_payload_cleanup", "ledger_scanned", ledger_scanned);
        record_queue_storage_operation("queue_payload_cleanup", "orphan_deleted", deleted);
        set_queue_storage_gauge(
            "queue_payload_cleanup",
            "oldest_orphan_age_ms",
            oldest_age_ms as f64,
        );
        set_queue_storage_gauge(
            "queue_payload_cleanup",
            "last_orphan_scan_count",
            scanned as f64,
        );
        Ok(deleted)
    }

    pub(crate) async fn start_queue_payload_cleanup_task(&self) -> StorageResult<()> {
        if !self.database_jobs_enabled || !self.kv_store.supports_partition_families() {
            return Ok(());
        }
        if self
            .job_manager
            .is_job_running(QUEUE_PAYLOAD_CLEANUP_JOB_ID)
            .await
        {
            return Ok(());
        }
        let cleanup_provider = self.with_transaction_priority(TransactionPriority::Batch);
        let job = QueuePayloadCleanupJob::new(Arc::new(cleanup_provider));
        let config = JobConfig {
            start_immediately: false,
            sleep_duration: std::time::Duration::from_secs(QUEUE_PAYLOAD_CLEANUP_INTERVAL_SECONDS),
            jitter_percent: 10,
        };
        self.job_manager
            .register_job(QUEUE_PAYLOAD_CLEANUP_JOB_ID, job, config)
            .await
            .map_err(|error| {
                StorageError::internal(&format!(
                    "register queue payload cleanup job failed: {error}"
                ))
            })?;
        Ok(())
    }
}
