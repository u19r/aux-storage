use crate::queue_provider::*;

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn change_message_visibility_operation(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()> {
        let handle = Self::decode_receipt_handle(&receipt_handle)?;
        if let Some(context) = self.queue_execution_context(queue_url).await? {
            let now = TimestampMillis::now();
            let (mut state, message_id, route, queue_id) =
                self.load_partitioned_state(&handle, &context).await?;
            if state.visibility_timestamp < now {
                return Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                ));
            }
            let old_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let Some(claim_nonce) = state.claim_nonce.clone() else {
                return Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                });
            };
            state.visibility_timestamp = now + visibility_timeout;
            let new_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let message_id_hex = message_id.to_string();
            self.kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &old_visibility_key,
                        ),
                        expected_value: Some(claim_nonce.as_bytes().to_vec()),
                    },
                    DirectWriteOperation::Delete {
                        key: queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &old_visibility_key,
                        ),
                    },
                    DirectWriteOperation::Put {
                        key: queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &new_visibility_key,
                        ),
                        value: claim_nonce.into_bytes(),
                    },
                    DirectWriteOperation::Put {
                        key: queue_state_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &message_id_hex,
                        ),
                        value: storage_types::storage_serde::to_bytes(&state)?,
                    },
                    DirectWriteOperation::Put {
                        key: queue_wake_key(queue_id),
                        value: wake_value_bytes()?,
                    },
                ])
                .await
                .map_err(QueueError::TransactWrite)?;
            return Ok(());
        }
        Err(Self::invalid_receipt_handle(&receipt_handle))
    }

    pub(crate) async fn change_message_visibilities_operation(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        let now = TimestampMillis::now();
        let mut results = std::iter::repeat_with(|| None)
            .take(entries.len())
            .collect::<Vec<Option<QueueResult<()>>>>();
        let context = self.queue_execution_context(queue_url).await?;
        let mut decoded_entries = Vec::with_capacity(entries.len());
        for (index, (receipt_handle, visibility_timeout)) in entries.iter().enumerate() {
            match Self::decode_receipt_handle(receipt_handle) {
                Ok(handle) => decoded_entries.push((Some(handle), *visibility_timeout)),
                Err(error) => {
                    results[index] = Some(Err(error));
                    decoded_entries.push((None, *visibility_timeout));
                }
            }
        }
        let Some(context) = context else {
            return Ok(results
                .into_iter()
                .enumerate()
                .map(|(index, result)| {
                    result.unwrap_or_else(|| Err(Self::invalid_receipt_handle(&entries[index].0)))
                })
                .collect());
        };

        let mut operations = Vec::with_capacity(decoded_entries.len().saturating_mul(3) + 1);
        let mut changed_any = false;
        let queue_id = context.queue_id;

        for (index, (handle, visibility_timeout)) in decoded_entries.into_iter().enumerate() {
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
            let state_key = queue_state_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &handle.message_id_hex,
            );
            let Some(state_bytes) = self.kv_store.get(&state_key, true).await? else {
                results[index] = Some(Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                }));
                continue;
            };
            let mut state: PartitionedQueueState =
                storage_types::storage_serde::from_bytes(&state_bytes).map_err(|error| {
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
                results[index] = Some(Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                }));
                continue;
            };
            if state.delivery_attempt != handle.delivery_attempt
                || claim_nonce != &handle.claim_nonce
            {
                results[index] = Some(Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                }));
                continue;
            }
            if state.visibility_timestamp < now {
                results[index] = Some(Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                )));
                continue;
            }

            let old_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let claim_nonce = claim_nonce.clone();
            state.visibility_timestamp = now + visibility_timeout;
            let new_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let message_id_hex = message_id.to_string();
            operations.extend([
                DirectWriteOperation::CheckValue {
                    key: queue_ready_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &old_visibility_key,
                    ),
                    expected_value: Some(claim_nonce.as_bytes().to_vec()),
                },
                DirectWriteOperation::Delete {
                    key: queue_ready_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &old_visibility_key,
                    ),
                },
                DirectWriteOperation::Put {
                    key: queue_ready_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &new_visibility_key,
                    ),
                    value: claim_nonce.into_bytes(),
                },
                DirectWriteOperation::Put {
                    key: queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &message_id_hex,
                    ),
                    value: storage_types::storage_serde::to_bytes(&state)?,
                },
            ]);
            changed_any = true;
            results[index] = Some(Ok(()));
        }

        if changed_any {
            operations.push(DirectWriteOperation::Put {
                key: queue_wake_key(queue_id),
                value: wake_value_bytes()?,
            });
            self.kv_store
                .transact_write_unchecked(operations)
                .await
                .map_err(QueueError::TransactWrite)?;
        }

        Ok(results
            .into_iter()
            .map(|result| result.unwrap_or(Ok(())))
            .collect())
    }

    pub(crate) async fn update_message_snapshot_checkpoint_operation(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        let handle = Self::decode_receipt_handle(&receipt_handle)?;
        if let Some(context) = self.queue_execution_context(queue_url).await? {
            let (mut state, _, route, queue_id) =
                self.load_partitioned_state(&handle, &context).await?;
            state.checkpoint_data = Some(checkpoint_data);
            let value = storage_types::storage_serde::to_bytes(&state)?;
            self.kv_store
                .put(
                    &queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &handle.message_id_hex,
                    ),
                    &value,
                    None,
                )
                .await?;
            return Ok(());
        }
        Err(Self::invalid_receipt_handle(&receipt_handle))
    }
}
