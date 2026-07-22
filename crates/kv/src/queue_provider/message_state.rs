use crate::queue_provider::*;

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn send_partitioned_message(
        &self,
        message: QueueMessage,
    ) -> QueueResult<MessageId> {
        let mut sent_ids = self.send_partitioned_messages(vec![message]).await?;
        sent_ids
            .pop()
            .ok_or_else(|| QueueError::internal(QueueInternalKind::NoWritableQueuePartition))
    }

    pub(crate) async fn receive_partitioned_messages_once(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        routing_state: &QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        let started_at = Instant::now();
        let mut messages = Vec::new();
        let per_partition_claim_limit = max_messages.max(1);
        let per_partition_scan_limit = max_messages
            .saturating_mul(PARTITIONED_QUEUE_RECEIVE_SCAN_OVERFETCH_MULTIPLIER)
            .clamp(RECEIVE_SCAN_MIN_LIMIT, RECEIVE_SCAN_MAX_LIMIT);
        let now = TimestampMillis::now();
        let mut scanned_partitions = 0usize;
        let mut claim_ranges = Vec::new();
        let routes = queue_partition_routes(routing_state);
        let receive_scan_shards = max_messages.max(1);
        let scan_shards = usize::try_from(receive_scan_shards)
            .unwrap_or(1)
            .clamp(1, PARTITIONED_QUEUE_RECEIVE_SCAN_SHARDS)
            .min(routes.len());
        let route_window = self
            .queue_receive_hinted_route_window(queue_url, queue_id, &routes, scan_shards, now)
            .await?;

        for route in route_window {
            if messages.len() >= usize::try_from(max_messages).unwrap_or(10) {
                break;
            }
            scanned_partitions = scanned_partitions.saturating_add(1);

            let ready_start = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &MessageVisibilityKey(visibility_key(
                    TimestampMillis::from(0),
                    &MessageId::default(),
                )),
            );
            let ready_end = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &MessageVisibilityKey(visibility_key(now + 1, &MessageId::default())),
            );
            claim_ranges.push(QueueClaimRange {
                queue_id,
                placement_slot: route.placement_slot,
                partition_id: route.partition_id,
                ready_start,
                ready_end,
                ready_hint_key: queue_ready_hint_key(
                    queue_id,
                    route.placement_slot,
                    route.partition_id,
                ),
                limit: per_partition_scan_limit,
                scan_limit: per_partition_scan_limit,
                claim_limit: per_partition_claim_limit,
                candidate_seed: Self::queue_claim_seed(queue_url, route.partition_id),
            });
        }

        let mut claimed_messages = self
            .kv_store
            .claim_queue_messages_from_ranges(
                claim_ranges,
                now,
                visibility_timeout,
                usize::try_from(max_messages).unwrap_or(10),
            )
            .await
            .map_err(QueueError::TransactWrite)?;
        let ready_entries_seen = claimed_messages.ready_entries_seen;
        claimed_messages.messages.sort_by(|left, right| {
            left.visibility_timestamp
                .cmp(&right.visibility_timestamp)
                .then_with(|| left.message_id_hex.cmp(&right.message_id_hex))
        });

        for claimed in claimed_messages.messages {
            let message_id = claimed
                .message_id_hex
                .parse::<MessageId>()
                .map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )
                })?;
            let body: PartitionedQueueBody =
                storage_types::storage_serde::from_bytes(&claimed.body_bytes).map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::PartitionedQueueBodyDecode,
                        format!(
                            "deserialize claimed partitioned queue body message_id={} error={:?}",
                            claimed.message_id_hex,
                            error.as_ref()
                        ),
                    )
                })?;
            let receipt_handle = QueueReceiptHandleData {
                partition_id: claimed.partition_id,
                message_id_hex: claimed.message_id_hex,
                visibility_timestamp_ms: claimed.visibility_timestamp.timestamp_millis(),
                delivery_attempt: claimed.delivery_attempt,
                claim_nonce: claimed.claim_nonce,
            }
            .encode()?;
            messages.push(MessageResponse::from_message(
                QueueMessage {
                    message_id,
                    queue_url: queue_url.to_string(),
                    body: body.body,
                    message_attributes: body.message_attributes,
                    receipt_handle: Some(ReceiptHandle(receipt_handle.clone())),
                    created_at: body.created_at,
                    visibility_timestamp: Some(claimed.visibility_timestamp),
                },
                &ReceiptHandle(receipt_handle),
            ));
        }

        tracing::debug!(
            queue_url,
            max_messages,
            scanned_partitions,
            ready_entries_seen,
            claimed_messages = messages.len(),
            elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
            "received partitioned queue messages"
        );

        Ok(messages)
    }

    pub(crate) async fn receive_partitioned_messages(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
        routing_state: QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(u64::from(*wait_time_seconds));
        let mut messages = Vec::new();
        loop {
            let remaining_max_messages =
                max_messages.saturating_sub(u32::try_from(messages.len()).unwrap_or(u32::MAX));
            if remaining_max_messages == 0 {
                return Ok(messages);
            }

            let mut received = self
                .receive_partitioned_messages_once_coalesced(
                    queue_url,
                    queue_id,
                    remaining_max_messages,
                    visibility_timeout,
                    &routing_state,
                )
                .await?;
            let received_messages = !received.is_empty();
            messages.append(&mut received);

            if messages.len() >= usize::try_from(max_messages).unwrap_or(10) {
                return Ok(messages);
            }
            if *wait_time_seconds == 0 {
                return Ok(messages);
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(messages);
            }

            if received_messages {
                tokio::task::yield_now().await;
                continue;
            }

            let sample_delay = remaining.min(std::time::Duration::from_millis(
                PARTITIONED_QUEUE_EMPTY_RECEIVE_POLL_MS,
            ));
            let _ = self
                .kv_store
                .wait_for_change(&queue_wake_key(queue_id), sample_delay)
                .await?;
        }
    }

    pub(crate) async fn load_partitioned_state(
        &self,
        handle: &QueueReceiptHandleData,
        context: &QueueExecutionContext,
    ) -> QueueResult<(
        PartitionedQueueState,
        MessageId,
        QueuePartitionRoute,
        QueueStorageId,
    )> {
        let message_id = handle
            .message_id_hex
            .parse::<MessageId>()
            .map_err(|error| {
                QueueError::internal_with_detail(
                    QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                    error,
                )
            })?;
        let route = queue_partition_route_for_id(&context.routing_state, handle.partition_id)
            .ok_or_else(|| QueueError::internal(QueueInternalKind::MissingQueuePartitionState))?;
        let queue_id = context.queue_id;
        let state_key = queue_state_key_with_slot(
            queue_id,
            route.placement_slot,
            route.partition_id,
            &handle.message_id_hex,
        );
        let Some(state_bytes) = self.kv_store.get(&state_key, true).await? else {
            return Err(QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: handle.message_id_hex.clone(),
            });
        };
        let state: PartitionedQueueState = storage_types::storage_serde::from_bytes(&state_bytes)
            .map_err(|error| {
            QueueError::internal_with_detail(
                QueueInternalKind::PartitionedQueueStateDecode,
                format!(
                    "deserialize partitioned queue receipt state key={} error={:?}",
                    String::from_utf8_lossy(&state_key),
                    error.as_ref()
                ),
            )
        })?;
        let Some(claim_nonce) = &state.claim_nonce else {
            return Err(QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: handle.message_id_hex.clone(),
            });
        };
        if state.delivery_attempt != handle.delivery_attempt || claim_nonce != &handle.claim_nonce {
            return Err(QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: handle.message_id_hex.clone(),
            });
        }
        Ok((state, message_id, route, queue_id))
    }

    pub(crate) async fn delete_partitioned_message_with_state(
        &self,
        handle: &QueueReceiptHandleData,
        context: &QueueExecutionContext,
        started_at: Instant,
    ) -> QueueResult<()> {
        let now = TimestampMillis::now();
        let (state, message_id, route, queue_id) =
            self.load_partitioned_state(handle, context).await?;
        if state.visibility_timestamp < now {
            return Err(QueueError::validation(
                QueueValidationKind::CannotOperateVisibleMessage,
            ));
        }
        let visibility_key =
            MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
        let ready_key = queue_ready_key_with_slot(
            queue_id,
            route.placement_slot,
            route.partition_id,
            &visibility_key,
        );
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::CheckValue {
                    key: ready_key.clone(),
                    expected_value: state.claim_nonce.clone().map(|nonce| nonce.into_bytes()),
                },
                DirectWriteOperation::Delete { key: ready_key },
                DirectWriteOperation::Put {
                    key: queue_delete_ledger_key(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &handle.message_id_hex,
                    ),
                    value: queue_delete_ledger_entry_bytes(
                        queue_id,
                        route,
                        &handle.message_id_hex,
                    )?,
                },
            ])
            .await
            .map_err(QueueError::TransactWrite)?;
        record_deferred_payload_cleanup(1);
        tracing::debug!(
            queue_id = queue_id.get(),
            partition_id = handle.partition_id,
            elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
            "deleted partitioned queue message through state fallback"
        );
        Ok(())
    }

    pub(crate) async fn queue_identity_by_id(
        &self,
        queue_id: QueueStorageId,
    ) -> QueueResult<Option<StoredQueueIdentity>> {
        self.kv_store
            .get(&compact::queue_metadata_key(queue_id), true)
            .await?
            .map(|value| decode_queue_identity(&value))
            .transpose()
    }

    pub(crate) async fn queue_identity_by_url(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<StoredQueueIdentity>> {
        let queue_id = queue_storage_id_from_url(queue_url)?;
        Ok(self
            .queue_identity_by_id(queue_id)
            .await?
            .filter(|identity| identity.queue.queue_url == queue_url))
    }

    pub(crate) async fn queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        let queue_id = queue_storage_id_from_url(queue_url)?;
        let family_component = queue_family_component(queue_url);
        let metadata_key = compact::queue_metadata_key(queue_id);
        let config_key =
            partition_family_config_key(PartitionFamilyKind::StandardQueue, &family_component);
        let partition_prefix =
            partition_info_prefix(PartitionFamilyKind::StandardQueue, &family_component);
        let partition_end = increment_bytes(partition_prefix.clone());
        let read_context = self.kv_store.begin_read_context().await?;
        let (metadata, config, partition_values) = tokio::try_join!(
            read_context.get(&metadata_key, true),
            read_context.get(&config_key, true),
            read_context.get_range_values(&partition_prefix, &partition_end, None, None, true,),
        )?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        let identity = decode_queue_identity(&metadata)?;
        if identity.queue.queue_url != queue_url {
            return Ok(None);
        }
        let Some(config) = config else {
            return Ok(Some((identity.queue, QueueMessageCounts::default())));
        };
        let mut partitions = partition_values
            .values
            .into_iter()
            .map(|value| parse_partition_info(&value))
            .collect::<StorageResult<Vec<_>>>()?;
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });
        let routing_state = QueueRoutingState::Control(ResolvedPartitionFamily {
            config: parse_partition_family_config(&config)?,
            partitions,
        });
        let now = TimestampMillis::now();
        let state_ranges = try_join_all(queue_partition_routes(&routing_state).into_iter().map(
            |route| {
                let prefix = queue_state_key_with_slot(
                    queue_id,
                    route.placement_slot,
                    route.partition_id,
                    "",
                );
                let end = increment_bytes(prefix.clone());
                let read_context = &read_context;
                async move {
                    read_context
                        .get_range_values(&prefix, &end, None, None, true)
                        .await
                }
            },
        ))
        .await?;
        let mut counts = QueueMessageCounts::default();
        for state in state_ranges.into_iter().flat_map(|range| range.values) {
            if is_queue_prewarm_marker_bytes(queue_url, &state) {
                continue;
            }
            let state = storage_types::storage_serde::from_bytes::<PartitionedQueueState>(&state)?;
            if state.visibility_timestamp <= now {
                counts.visible = counts.visible.saturating_add(1);
            } else if state.claim_nonce.is_some() {
                counts.not_visible = counts.not_visible.saturating_add(1);
            } else {
                counts.delayed = counts.delayed.saturating_add(1);
            }
        }
        Ok(Some((identity.queue, counts)))
    }
}
