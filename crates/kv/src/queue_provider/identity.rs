use crate::queue_provider::*;

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn find_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let Some(queue_id_bytes) = self
            .kv_store
            .get(&compact::queue_name_lookup_key(queue_name), true)
            .await?
        else {
            return Ok(None);
        };
        let queue_id = decode_queue_storage_id(&queue_id_bytes)?;
        Ok(self
            .queue_identity_by_id(queue_id)
            .await?
            .map(|identity| identity.queue))
    }

    pub(crate) async fn list_queue_identities(&self) -> QueueResult<Vec<StoredQueueIdentity>> {
        let range = compact::queue_metadata_prefix();
        let items = self
            .kv_store
            .get_range(&range.start, &range.end, None, None::<RawKey>, true)
            .await?;
        items
            .items
            .into_iter()
            .map(|(_key, value)| decode_queue_identity(&value))
            .collect()
    }
}

pub(crate) fn partitioned_ready_visibility_key(
    storage_key: &[u8],
) -> QueueResult<MessageVisibilityKey> {
    if let Ok(compact::ParsedCompactKey::PartitionedQueueData {
        kind: compact::QueueRecordKind::Ready,
        suffix,
        ..
    }) = compact::parse_compact_key(storage_key)
    {
        let visibility_key = std::str::from_utf8(suffix).map_err(|error| {
            QueueError::internal_with_detail(
                QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                error,
            )
        })?;
        return Ok(MessageVisibilityKey(visibility_key.to_string()));
    }
    let key_text = String::from_utf8(storage_key.to_vec()).map_err(|error| {
        QueueError::internal_with_detail(
            QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            error,
        )
    })?;
    let (_, suffix) = key_text.rsplit_once("/ready/").ok_or_else(|| {
        QueueError::internal_with_detail(
            QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            key_text.clone(),
        )
    })?;
    Ok(MessageVisibilityKey(suffix.to_string()))
}

pub(crate) fn queue_partition_hash(queue_url: &str, message_id: &MessageId) -> u64 {
    let mut key = queue_url.as_bytes().to_vec();
    key.extend_from_slice(message_id.as_bytes());
    ordered_log_hash(&key)
}

pub(crate) fn queue_partition_routes(
    routing_state: &QueueRoutingState,
) -> Vec<QueuePartitionRoute> {
    match routing_state {
        QueueRoutingState::Control(family) => family
            .partitions
            .iter()
            .filter(|partition| partition.is_readable())
            .map(|partition| QueuePartitionRoute {
                partition_id: partition.partition_id,
                placement_slot: partition.placement_slot,
            })
            .collect(),
    }
}

pub(crate) fn queue_partition_route_for_id(
    routing_state: &QueueRoutingState,
    partition_id: u16,
) -> Option<QueuePartitionRoute> {
    match routing_state {
        QueueRoutingState::Control(family) => {
            find_partition_by_id(&family.partitions, partition_id).map(|partition| {
                QueuePartitionRoute {
                    partition_id: partition.partition_id,
                    placement_slot: partition.placement_slot,
                }
            })
        }
    }
}

fn queue_metadata_matches(existing: &Queue, requested: &Queue) -> bool {
    existing.queue_name == requested.queue_name
        && (existing.queue_url == requested.queue_url
            || queue_url_without_storage_id(&existing.queue_url)
                .is_some_and(|queue_url| queue_url == requested.queue_url))
        && existing.attributes == requested.attributes
}

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn existing_queue_for_create(
        &self,
        queue: &Queue,
    ) -> QueueResult<Option<Queue>> {
        Ok(self
            .find_queue_by_name(&queue.queue_name)
            .await?
            .filter(|existing| queue_metadata_matches(existing, queue)))
    }
}
