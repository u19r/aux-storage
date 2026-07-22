use crate::queue_provider::*;

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn create_queue_operation(&self, mut queue: Queue) -> QueueResult<Queue> {
        if let Some(existing) = self.existing_queue_for_create(&queue).await? {
            return Ok(existing);
        }
        let requested_queue = queue.clone();
        let allocator_key = compact::queue_id_allocator_key();
        let allocator_value = self.kv_store.get(&allocator_key, true).await?;
        let queue_id = match allocator_value.as_deref() {
            Some(bytes) => decode_queue_storage_id(bytes)?,
            None => queue_storage_id(1)?,
        };
        let next_queue_id = queue_storage_id(queue_id.get().saturating_add(1))?;
        queue.queue_url = queue_url_with_storage_id(&queue.queue_url, queue_id);
        let identity = StoredQueueIdentity {
            queue_id,
            queue: queue.clone(),
        };
        let queue_id_bytes = encode_queue_storage_id(queue_id);
        if let Err(error) = self
            .kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::CheckValue {
                    key: allocator_key.clone(),
                    expected_value: allocator_value,
                },
                DirectWriteOperation::CheckValue {
                    key: compact::queue_name_lookup_key(&queue.queue_name),
                    expected_value: None,
                },
                DirectWriteOperation::Put {
                    key: allocator_key,
                    value: encode_queue_storage_id(next_queue_id),
                },
                DirectWriteOperation::Put {
                    key: compact::queue_name_lookup_key(&queue.queue_name),
                    value: queue_id_bytes,
                },
                DirectWriteOperation::Put {
                    key: compact::queue_metadata_key(queue_id),
                    value: encode_queue_identity(&identity)?,
                },
            ])
            .await
        {
            if let Some(existing) = self
                .existing_queue_for_create(&requested_queue)
                .await
                .unwrap_or(None)
            {
                return Ok(existing);
            }
            return Err(QueueError::TransactWrite(error));
        }
        if self.kv_store.supports_partition_families() {
            let family = self
                .ensure_queue_family_state(&queue.queue_url, DEFAULT_STANDARD_QUEUE_PARTITION_COUNT)
                .await?;
            let prewarm_partitions = queue_partition_routes(&QueueRoutingState::Control(family))
                .into_iter()
                .map(|route| QueuePrewarmPartition {
                    placement_slot: route.placement_slot,
                    partition_id: route.partition_id,
                    marker_key: queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        QUEUE_PREWARM_MESSAGE_ID,
                    ),
                })
                .collect();
            self.kv_store
                .prewarm_partitioned_queue(&queue.queue_url, prewarm_partitions)
                .await?;
            let marker_key = queue_partition_marker_key(&queue.queue_url);
            self.kv_store
                .put(
                    &marker_key,
                    &queue_partition_marker_bytes(DEFAULT_STANDARD_QUEUE_PARTITION_COUNT)?,
                    None,
                )
                .await?;
            self.kv_store
                .put(&queue_wake_key(queue_id), &wake_value_bytes()?, None)
                .await?;
        }
        Ok(queue)
    }

    pub(crate) async fn get_queue_with_message_counts_operation(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        self.queue_with_message_counts(queue_url).await
    }

    pub(crate) async fn get_queue_by_name_operation(
        &self,
        queue_name: &str,
    ) -> QueueResult<Option<Queue>> {
        self.find_queue_by_name(queue_name).await
    }

    pub(crate) async fn list_queues_operation(
        &self,
        queue_name_prefix: Option<&str>,
    ) -> QueueResult<Vec<Queue>> {
        Ok(self
            .list_queue_identities()
            .await?
            .into_iter()
            .map(|identity| identity.queue)
            .filter(|queue| {
                queue_name_prefix.is_none_or(|prefix| queue.queue_name.starts_with(prefix))
            })
            .collect())
    }

    pub(crate) async fn delete_queue_operation(&self, queue_url: &str) -> QueueResult<()> {
        let identity = self.queue_identity_by_url(queue_url).await?;
        if let Some(family) = self.load_queue_family_state(queue_url).await? {
            for route in queue_partition_routes(&QueueRoutingState::Control(family)) {
                if let Some(identity) = &identity {
                    self.kv_store
                        .delete_prefix(queue_ready_prefix_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                        ))
                        .await?;
                    self.kv_store
                        .delete_prefix(queue_state_key_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                            "",
                        ))
                        .await?;
                    self.kv_store
                        .delete_prefix(queue_body_prefix_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                        ))
                        .await?;
                    record_queue_storage_operation("queue_payload_cleanup", "prefix_delete", 1);
                    self.kv_store
                        .delete_prefix(queue_checkpoint_key_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                            "",
                        ))
                        .await?;
                }
            }
            self.delete_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                &queue_family_component(queue_url),
            )
            .await?;
        }
        self.kv_store
            .delete(&queue_partition_marker_key(queue_url))
            .await?;
        if let Some(identity) = &identity {
            let _ = self
                .kv_store
                .delete(&queue_wake_key(identity.queue_id))
                .await;
        }
        if let Some(identity) = identity {
            self.kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::Delete {
                        key: compact::queue_metadata_key(identity.queue_id),
                    },
                    DirectWriteOperation::Delete {
                        key: compact::queue_name_lookup_key(&identity.queue.queue_name),
                    },
                ])
                .await
                .map_err(QueueError::TransactWrite)?;
        }
        Ok(())
    }

    pub(crate) async fn purge_queue_operation(&self, queue_url: &str) -> QueueResult<()> {
        let queue_id = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?
            .queue_id;
        if let Some(family) = self.load_queue_family_state(queue_url).await? {
            for route in queue_partition_routes(&QueueRoutingState::Control(family)) {
                self.kv_store
                    .delete_prefix(queue_ready_prefix_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                    ))
                    .await?;
                self.kv_store
                    .delete_prefix(queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        "",
                    ))
                    .await?;
                self.kv_store
                    .delete_prefix(queue_body_prefix_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                    ))
                    .await?;
                record_queue_storage_operation("queue_payload_cleanup", "prefix_delete", 1);
                self.kv_store
                    .delete_prefix(queue_checkpoint_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        "",
                    ))
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn set_queue_attributes_operation(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        let mut identity = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?;
        identity.queue.attributes = attributes;
        self.kv_store
            .put(
                &compact::queue_metadata_key(identity.queue_id),
                &encode_queue_identity(&identity)?,
                None,
            )
            .await?;
        Ok(())
    }
    pub(crate) async fn send_messages_operation(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        let message_ids = self.send_partitioned_messages(messages).await?;
        Ok(message_ids.into_iter().map(Ok).collect())
    }

    pub(crate) async fn receive_messages_operation(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        let context = self.ensure_queue_execution_context(queue_url).await?;
        self.receive_partitioned_messages(
            queue_url,
            context.queue_id,
            max_messages,
            visibility_timeout,
            wait_time_seconds,
            context.routing_state,
        )
        .await
    }

    pub(crate) async fn delete_message_operation(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        let started_at = Instant::now();
        let handle = Self::decode_receipt_handle(&receipt_handle)?;
        if let Some(context) = self.queue_execution_context(queue_url).await? {
            let now = TimestampMillis::now();
            let message_id = handle
                .message_id_hex
                .parse::<MessageId>()
                .map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )
                })?;
            let visibility_timestamp = TimestampMillis::from(handle.visibility_timestamp_ms);
            if visibility_timestamp < now {
                return Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                ));
            }
            let route = queue_partition_route_for_id(&context.routing_state, handle.partition_id)
                .ok_or_else(|| {
                QueueError::internal(QueueInternalKind::MissingQueuePartitionState)
            })?;
            let queue_id = context.queue_id;
            let visibility_key =
                MessageVisibilityKey(visibility_key(visibility_timestamp, &message_id));
            let ready_key = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &visibility_key,
            );
            if let Err(error) = self
                .kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: ready_key.clone(),
                        expected_value: Some(handle.claim_nonce.as_bytes().to_vec()),
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
            {
                return match error.as_ref() {
                    StorageEnum::ConditionalCheckFailed
                    | StorageEnum::TransactionCanceled { .. } => {
                        self.delete_partitioned_message_with_state(&handle, &context, started_at)
                            .await
                    }
                    _ => Err(QueueError::TransactWrite(error)),
                };
            }
            record_deferred_payload_cleanup(1);
            tracing::debug!(
                queue_url,
                partition_id = handle.partition_id,
                "deleted partitioned queue message"
            );
            return Ok(());
        }
        Err(Self::invalid_receipt_handle(&receipt_handle))
    }

    pub(crate) async fn delete_messages_operation(
        &self,
        queue_url: &str,
        receipt_handles: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        let now = TimestampMillis::now();
        let mut results = std::iter::repeat_with(|| None)
            .take(receipt_handles.len())
            .collect::<Vec<Option<QueueResult<()>>>>();
        let context = self.queue_execution_context(queue_url).await?;
        let mut decoded_handles = Vec::with_capacity(receipt_handles.len());
        for (index, receipt_handle) in receipt_handles.iter().enumerate() {
            match Self::decode_receipt_handle(receipt_handle) {
                Ok(handle) => decoded_handles.push(Some(handle)),
                Err(error) => {
                    results[index] = Some(Err(error));
                    decoded_handles.push(None);
                }
            }
        }
        let Some(context) = context else {
            return Ok(results
                .into_iter()
                .enumerate()
                .map(|(index, result)| {
                    result.unwrap_or_else(|| {
                        Err(Self::invalid_receipt_handle(&receipt_handles[index]))
                    })
                })
                .collect());
        };

        let mut operations = Vec::with_capacity(decoded_handles.len().saturating_mul(2));
        let mut deferred_payload_cleanup = 0u64;
        let queue_id = context.queue_id;

        for (index, handle) in decoded_handles.iter().enumerate() {
            let Some(handle) = handle else {
                continue;
            };
            let message_id = match handle.message_id_hex.parse::<MessageId>() {
                Ok(message_id) => message_id,
                Err(error) => {
                    results[index] = Some(Err(QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )));
                    continue;
                }
            };
            let Some(route) =
                queue_partition_route_for_id(&context.routing_state, handle.partition_id)
            else {
                results[index] = Some(Err(QueueError::internal(
                    QueueInternalKind::MissingQueuePartitionState,
                )));
                continue;
            };
            let visibility_timestamp = TimestampMillis::from(handle.visibility_timestamp_ms);
            if visibility_timestamp < now {
                results[index] = Some(Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                )));
                continue;
            }

            let visibility_key =
                MessageVisibilityKey(visibility_key(visibility_timestamp, &message_id));
            let ready_key = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &visibility_key,
            );
            operations.extend([
                DirectWriteOperation::CheckValue {
                    key: ready_key.clone(),
                    expected_value: Some(handle.claim_nonce.as_bytes().to_vec()),
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
            ]);
            deferred_payload_cleanup = deferred_payload_cleanup.saturating_add(1);
            results[index] = Some(Ok(()));
        }

        if !operations.is_empty()
            && let Err(error) = self.kv_store.transact_write_unchecked(operations).await
        {
            if matches!(
                error.as_ref(),
                StorageEnum::ConditionalCheckFailed | StorageEnum::TransactionCanceled { .. }
            ) {
                let mut fallback_results = Vec::with_capacity(decoded_handles.len());
                for (index, handle) in decoded_handles.into_iter().enumerate() {
                    fallback_results.push(match handle {
                        Some(handle) => {
                            self.delete_partitioned_message_with_state(
                                &handle,
                                &context,
                                Instant::now(),
                            )
                            .await
                        }
                        None => Err(Self::invalid_receipt_handle(&receipt_handles[index])),
                    });
                }
                return Ok(fallback_results);
            }
            return Err(QueueError::TransactWrite(error));
        }
        record_deferred_payload_cleanup(deferred_payload_cleanup);

        Ok(results
            .into_iter()
            .map(|result| result.unwrap_or(Ok(())))
            .collect())
    }
}
