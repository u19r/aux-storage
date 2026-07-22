use crate::queue_provider::*;

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn queue_receive_hinted_route_window(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        routes: &[QueuePartitionRoute],
        scan_shards: usize,
        now: TimestampMillis,
    ) -> StorageResult<Vec<QueuePartitionRoute>> {
        if routes.is_empty() || scan_shards == 0 {
            return Ok(Vec::new());
        }
        let ready_limit = routes.len();
        let start = {
            let mut cursors = match self.queue_receive_hint_cursors.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let cursor = cursors.entry(queue_url.to_string()).or_insert(0);
            let start = *cursor % routes.len();
            *cursor = (start + ready_limit.max(1)) % routes.len();
            start
        };

        let sampled_routes = (0..routes.len())
            .map(|offset| routes[(start + offset) % routes.len()])
            .collect::<Vec<_>>();
        let hint_keys = sampled_routes
            .iter()
            .map(|route| queue_ready_hint_key(queue_id, route.placement_slot, route.partition_id))
            .collect::<Vec<_>>();
        let hints = self.kv_store.multi_get(hint_keys, false).await?;
        let mut ready_routes = Vec::with_capacity(ready_limit);
        for (route, hint) in sampled_routes.into_iter().zip(hints) {
            let Some(value) = hint else {
                continue;
            };
            let Some((partition_id, next_visible_at)) = parse_ready_hint_value(&value) else {
                continue;
            };
            if partition_id == route.partition_id && next_visible_at <= now {
                ready_routes.push(route);
                if ready_routes.len() >= ready_limit {
                    break;
                }
            }
        }
        Ok(ready_routes)
    }

    pub(crate) fn queue_claim_seed(queue_url: &str, partition_id: u16) -> u64 {
        // Claims rotate visible candidates per receive call so repeated scans
        // do not keep contending on the same oldest ready entries.
        let nonce = Uuid::now_v7();
        let bytes = nonce.as_bytes();
        let mut seed_bytes = [0u8; 8];
        seed_bytes.copy_from_slice(&bytes[..8]);
        let nonce_seed = u64::from_be_bytes(seed_bytes);
        let route_seed =
            ordered_log_hash(&[queue_url.as_bytes(), &partition_id.to_be_bytes()].concat());
        nonce_seed ^ route_seed
    }

    pub(crate) async fn receive_partitioned_messages_once_coalesced(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        routing_state: &QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        self.receive_partitioned_messages_with_claim_rounds(
            queue_url,
            queue_id,
            max_messages,
            visibility_timeout,
            routing_state,
        )
        .await
    }

    pub(crate) async fn receive_partitioned_messages_with_claim_rounds(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        routing_state: &QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        let mut claimed_messages = Vec::new();
        for _ in 0..PARTITIONED_QUEUE_RECEIVE_COALESCE_CLAIM_ROUNDS {
            let remaining = max_messages
                .saturating_sub(u32::try_from(claimed_messages.len()).unwrap_or(u32::MAX));
            if remaining == 0 {
                break;
            }
            match self
                .receive_partitioned_messages_once(
                    queue_url,
                    queue_id,
                    remaining,
                    visibility_timeout,
                    routing_state,
                )
                .await
            {
                Ok(messages) if messages.is_empty() => break,
                Ok(mut messages) => claimed_messages.append(&mut messages),
                Err(error) => return Err(error),
            }
        }
        Ok(claimed_messages)
    }

    pub(crate) fn is_partition_routing_retryable(
        storage_error: &storage_types::StorageError,
    ) -> bool {
        matches!(
            storage_error.as_ref(),
            StorageEnum::ConditionalCheckFailed
                | StorageEnum::TransactionConflict { .. }
                | StorageEnum::TransactionCanceled { .. }
        )
    }

    pub(crate) fn invalid_receipt_handle(receipt_handle: &ReceiptHandle) -> QueueError {
        QueueError::validation_with_detail(
            QueueValidationKind::MessageNotFound,
            format!("receipt handle is invalid or expired: {receipt_handle}"),
        )
    }

    pub(crate) fn decode_receipt_handle(
        receipt_handle: &ReceiptHandle,
    ) -> QueueResult<QueueReceiptHandleData> {
        QueueReceiptHandleData::decode(receipt_handle.as_str())
            .map_err(|_| Self::invalid_receipt_handle(receipt_handle))
    }

    pub(crate) async fn load_queue_family_state(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<ResolvedPartitionFamily>> {
        Ok(self
            .load_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                &queue_family_component(queue_url),
            )
            .await?)
    }

    pub(crate) async fn ensure_queue_family_state(
        &self,
        queue_url: &str,
        initial_partition_count: u16,
    ) -> QueueResult<ResolvedPartitionFamily> {
        if let Some(existing) = self.load_queue_family_state(queue_url).await? {
            return Ok(existing);
        }

        let partitions = initial_partition_infos(initial_partition_count);
        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::StandardQueue,
                initial_partition_count,
            ),
            partitions,
        };
        self.save_partition_family_state(
            PartitionFamilyKind::StandardQueue,
            &queue_family_component(queue_url),
            &family,
        )
        .await?;
        Ok(family)
    }

    pub(crate) async fn queue_routing_state(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<QueueRoutingState>> {
        Ok(self
            .load_queue_family_state(queue_url)
            .await?
            .map(QueueRoutingState::Control))
    }

    pub(crate) fn prepare_partitioned_queue_message(
        mut message: QueueMessage,
    ) -> QueueResult<PreparedPartitionedQueueMessage> {
        if message.message_id == MessageId::default() {
            message.message_id = MessageId::random();
        }

        let visibility_timestamp = message
            .visibility_timestamp
            .unwrap_or_else(TimestampMillis::now);
        let state = PartitionedQueueState {
            queue_url: message.queue_url.clone(),
            visibility_timestamp,
            delivery_attempt: 0,
            claim_nonce: None,
            checkpoint_data: None,
        };
        let state_bytes = storage_types::storage_serde::to_bytes(&state)?;
        let payload_bytes =
            partitioned_body_bytes(message.body, message.message_attributes, message.created_at)?;
        let payload_record_bytes =
            crate::queue::storage::queue_payload_record_bytes(payload_bytes.len())?.map(Arc::from);
        Ok(PreparedPartitionedQueueMessage {
            message_id: message.message_id,
            message_id_hex: message.message_id.to_string(),
            queue_url: message.queue_url,
            visibility_timestamp,
            state_bytes: state_bytes.into(),
            payload_bytes: payload_bytes.into(),
            payload_record_bytes,
        })
    }

    pub(crate) fn partitioned_queue_write_for_route(
        prepared: &PreparedPartitionedQueueMessage,
        queue_id: QueueStorageId,
        route: QueuePartitionRoute,
        wake_key: &[u8],
        wake_bytes: &[u8],
    ) -> PartitionedQueueMessageWrite {
        let visibility_key = MessageVisibilityKey(visibility_key(
            prepared.visibility_timestamp,
            &prepared.message_id,
        ));
        PartitionedQueueMessageWrite {
            state_key: queue_state_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &prepared.message_id_hex,
            ),
            state_bytes: prepared.state_bytes.clone(),
            payload_key: queue_payload_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &prepared.message_id_hex,
            ),
            payload_bytes: prepared.payload_bytes.clone(),
            payload_record_bytes: prepared.payload_record_bytes.clone(),
            ready_key: queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &visibility_key,
            ),
            ready_hint_key: queue_ready_hint_key(
                queue_id,
                route.placement_slot,
                route.partition_id,
            ),
            ready_hint_bytes: queue_ready_hint_bytes(
                route.partition_id,
                prepared.visibility_timestamp,
            ),
            wake_key: wake_key.to_vec(),
            wake_bytes: wake_bytes.to_vec(),
        }
    }

    pub(crate) async fn send_partitioned_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<MessageId>> {
        let started_at = Instant::now();
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut prepared_messages = Vec::with_capacity(messages.len());
        for message in messages {
            prepared_messages.push(Self::prepare_partitioned_queue_message(message)?);
        }
        let queue_url = prepared_messages
            .first()
            .map(|message| message.queue_url.as_str())
            .ok_or_else(|| QueueError::internal(QueueInternalKind::NoWritableQueuePartition))?;
        if prepared_messages
            .iter()
            .any(|message| message.queue_url.as_str() != queue_url)
        {
            return Err(QueueError::validation_with_detail(
                QueueValidationKind::InvalidParameterValue,
                "send_messages requires all messages to target the same queue_url",
            ));
        }
        let context = self.ensure_queue_execution_context(queue_url).await?;
        let queue_id = context.queue_id;
        let mut initial_routing_state = Some(context.routing_state);
        let cache_key = crate::sorted_kv::PartitionFamilyCacheKey::new(
            PartitionFamilyKind::StandardQueue,
            &queue_family_component(queue_url),
        );
        let wake_key = queue_wake_key(queue_id);
        let wake_bytes = wake_value_bytes()?;
        for attempt in 0..PARTITIONED_QUEUE_SEND_MAX_ATTEMPTS {
            let routing_state = match initial_routing_state.take() {
                Some(routing_state) => routing_state,
                None => match self.queue_routing_state(queue_url).await? {
                    Some(routing_state) => routing_state,
                    None => QueueRoutingState::Control(
                        self.ensure_queue_family_state(
                            queue_url,
                            DEFAULT_STANDARD_QUEUE_PARTITION_COUNT,
                        )
                        .await?,
                    ),
                },
            };
            let mut send_messages = Vec::with_capacity(prepared_messages.len());
            let mut partition_load = HashMap::<u16, (u64, u64)>::new();
            for prepared in &prepared_messages {
                let route = self
                    .queue_partition_route_for_send(
                        &routing_state,
                        &prepared.queue_url,
                        &prepared.message_id,
                    )
                    .ok_or_else(|| {
                        QueueError::internal(QueueInternalKind::NoWritableQueuePartition)
                    })?;
                let entry = partition_load.entry(route.partition_id).or_default();
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry
                    .1
                    .saturating_add(u64::try_from(prepared.state_bytes.len()).unwrap_or(u64::MAX));
                send_messages.push(Self::partitioned_queue_write_for_route(
                    prepared,
                    queue_id,
                    route,
                    &wake_key,
                    &wake_bytes,
                ));
            }
            let QueueRoutingState::Control(_) = &routing_state;

            match self
                .kv_store
                .write_partitioned_queue_messages(send_messages)
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        queue_url = %queue_url,
                        message_count = prepared_messages.len(),
                        attempt,
                        elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        "sent partitioned queue messages"
                    );
                    for (partition_id, (writes, bytes)) in partition_load {
                        self.record_queue_partition_load(
                            queue_url,
                            partition_id,
                            crate::partition_family::PartitionLoadSample {
                                writes,
                                bytes,
                                conflicts: u64::try_from(attempt).unwrap_or(u64::MAX),
                                routing_key_bucket_bitmap: 0,
                                queue_scan_work: 0,
                                queue_claim_conflicts: 0,
                                oldest_visible_age_ms: 0,
                                visible_count: 0,
                                invisible_count: 0,
                            },
                        );
                    }
                    return Ok(prepared_messages
                        .into_iter()
                        .map(|message| message.message_id)
                        .collect());
                }
                Err(error) if Self::is_partition_routing_retryable(&error) => {
                    metrics_facade::counter!(
                        PARTITION_ROUTING_RETRIES_TOTAL_METRIC,
                        "family_kind" => "standard_queue",
                        "operation" => "send",
                        "reason" => "stale_topology"
                    )
                    .increment(1);
                    self.invalidate_partition_family_cache(&cache_key);
                }
                Err(error) => return Err(QueueError::TransactWrite(error)),
            }
        }
        Err(QueueError::internal(
            QueueInternalKind::PartitionedQueueSendRetriesExhausted,
        ))
    }
}
