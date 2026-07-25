use std::collections::HashMap;

use foundationdb::{Transaction, options};
use storage_types::{StorageError, StorageResult};
use stream_provider::StoredStreamPointer;

use crate::{
    backends::fdb::store::{
        FdbTransactionAttemptError, FoundationDbKvStore, OrderedLogFamilyCache,
        PendingOrderedLogWrite, adjust_versionstamp_offset,
    },
    key_template::{KeyTemplate, PlaceholderBinding, PlaceholderId},
    keyspace::compact,
    partition_family::{
        OrderedLogSplitMarker, PartitionFamilyKind, PartitionLoadSample,
        RuntimePartitionLoadSample, find_partition_for_hash, merge_partition_load,
        next_partition_id, next_placement_slot, ordered_log_family_component, ordered_log_hash,
        ordered_log_partition_prefix_with_slot, ordered_log_split_marker_bytes,
        ordered_log_split_marker_prefix, routing_key_bucket_bit, split_partition_children,
        supports_pointer_stream_partitioning,
    },
    stream::item_codec::decode_stream_item,
};

impl FoundationDbKvStore {
    pub(crate) async fn split_partitioned_ordered_log_family_tx(
        &self,
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> Result<bool, FdbTransactionAttemptError> {
        let Some(mut family) = Self::load_partition_family_state_tx_retryable(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            family_component,
        )
        .await?
        else {
            return Ok(false);
        };

        let Some(index) = family
            .partitions
            .iter()
            .position(|partition| partition.partition_id == partition_id)
        else {
            return Ok(false);
        };
        if !family.partitions[index].is_writable() {
            return Ok(false);
        }

        let parent = family.partitions[index].clone();
        let left_partition_id = next_partition_id(&family.partitions);
        let right_partition_id = left_partition_id.saturating_add(1);
        let left_slot = next_placement_slot(&family.partitions);
        let right_slot = left_slot.saturating_add(1);
        let Some((mut left_child, mut right_child)) = split_partition_children(
            &parent,
            left_partition_id,
            right_partition_id,
            left_slot,
            right_slot,
        ) else {
            return Ok(false);
        };

        let mut parent = parent;
        parent.mark_write_closed().map_err(|error| {
            StorageError::internal(&format!(
                "ordered-log split requires open parent partition, found {:?} -> {:?}",
                error.from(),
                error.to()
            ))
        })?;
        parent.sealed_after_id = None;
        left_child.opened_after_id = None;
        right_child.opened_after_id = None;

        family.partitions[index] = parent;
        family.partitions.push(left_child);
        family.partitions.push(right_child);
        family.sort_by_hash_range();
        family.config.note_topology_change(now_ms);
        family.refresh_partition_count();
        family.config.min_open_partitions = family.config.min_open_partitions.max(
            u16::try_from(
                family
                    .partitions
                    .iter()
                    .filter(|partition| partition.is_writable())
                    .count(),
            )
            .unwrap_or(u16::MAX),
        );

        Self::save_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            family_component,
            &family,
        )?;

        let split_marker = OrderedLogSplitMarker {
            parent_partition_id: partition_id,
            left_child_partition_id: left_partition_id,
            right_child_partition_id: right_partition_id,
        };
        let marker_bytes = ordered_log_split_marker_bytes(&split_marker)?;
        let marker_template = KeyTemplate::placeholder(
            ordered_log_split_marker_prefix(family_component, partition_id),
            Vec::new(),
            PlaceholderBinding::new(PlaceholderId::Shared(partition_id), vec![0; 12], [0, 0]),
        );
        let mut versioned_key = marker_template.foundationdb_key().ok_or_else(|| {
            StorageError::internal("ordered-log split marker template must be versionstamped")
        })?;
        if let Some(prefix_bytes) = prefix {
            let mut composed = prefix_bytes.clone();
            composed.extend_from_slice(&versioned_key);
            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
            versioned_key = composed;
        }
        trx.atomic_op(
            &versioned_key,
            &marker_bytes,
            options::MutationType::SetVersionstampedKey,
        );

        Ok(true)
    }

    pub(crate) async fn rewrite_partitioned_pointer_template(
        &self,
        trx: &Transaction,
        subspace_prefix: Option<&Vec<u8>>,
        template: &crate::key_template::KeyTemplate,
        value: &[u8],
        ordered_log_family_cache: &mut OrderedLogFamilyCache,
    ) -> StorageResult<(
        crate::key_template::KeyTemplate,
        Option<PendingOrderedLogWrite>,
    )> {
        let Some(template_prefix) = template.prefix() else {
            return Ok((template.clone(), None));
        };
        if template_prefix.is_empty() {
            return Ok((template.clone(), None));
        }
        let family_name = if template_prefix == compact::system_stream_prefix().start {
            storage_types::StreamName::system_table_stream()
        } else {
            storage_types::StreamName::from(
                &template_prefix[..template_prefix.len().saturating_sub(1)],
            )
        };
        if !supports_pointer_stream_partitioning(&family_name) {
            return Ok((template.clone(), None));
        }

        let stored_item = match decode_stream_item(value) {
            Ok(item) => item,
            Err(_) => return Ok((template.clone(), None)),
        };
        if stored_item.data_type != stream_provider::StreamDataType::StreamPointer {
            return Ok((template.clone(), None));
        }
        let pointer: StoredStreamPointer =
            match storage_types::storage_serde::from_bytes(&stored_item.data) {
                Ok(pointer) => pointer,
                Err(_) => return Ok((template.clone(), None)),
            };
        let family = Self::ensure_ordered_log_family_state_cached_tx(
            trx,
            subspace_prefix,
            &family_name,
            ordered_log_family_cache,
        )
        .await?;
        let routing_hash = ordered_log_hash(pointer.stream_name().as_ref());
        let partition =
            find_partition_for_hash(&family.partitions, routing_hash).ok_or_else(|| {
                StorageError::internal("pointer stream family has no writable partition")
            })?;

        Ok((
            template.with_replaced_prefix(ordered_log_partition_prefix_with_slot(
                &family_name,
                partition.placement_slot,
                partition.partition_id,
            )),
            Some(PendingOrderedLogWrite {
                family_component: ordered_log_family_component(&family_name),
                partition_id: partition.partition_id,
                bytes: u64::try_from(value.len()).unwrap_or(u64::MAX),
                routing_key_bucket_bitmap: routing_key_bucket_bit(routing_hash),
            }),
        ))
    }

    pub(crate) fn record_ordered_log_writes(
        &self,
        writes: &[PendingOrderedLogWrite],
        conflict_count: u64,
    ) {
        let mut aggregated: HashMap<(String, u16), PartitionLoadSample> = HashMap::new();
        for write in writes {
            let entry = aggregated
                .entry((write.family_component.clone(), write.partition_id))
                .or_default();
            merge_partition_load(
                entry,
                &PartitionLoadSample {
                    writes: 1,
                    bytes: write.bytes,
                    conflicts: 0,
                    routing_key_bucket_bitmap: write.routing_key_bucket_bitmap,
                    queue_scan_work: 0,
                    queue_claim_conflicts: 0,
                    oldest_visible_age_ms: 0,
                    visible_count: 0,
                    invisible_count: 0,
                },
            );
        }

        for ((family_component, partition_id), mut sample) in aggregated {
            sample.conflicts = sample.conflicts.saturating_add(conflict_count);
            self.runtime_partition_load_tracker
                .record(RuntimePartitionLoadSample {
                    family_kind: PartitionFamilyKind::OrderedLog,
                    family_component,
                    partition_id,
                    sample,
                });
        }
    }
}
