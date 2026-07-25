use std::collections::HashMap;

use foundationdb::options;
use storage_types::StorageResult;

use crate::{
    backends::fdb::{
        error::map_fdb_error,
        metrics::{
            record_fdb_operation, record_fdb_operation_bytes, record_fdb_transaction_start,
            record_fdb_write_shape,
        },
        store::{FoundationDbKvStore, queue_ready_hint_is_earlier},
    },
    queue::{
        PartitionedQueueMessageWrite, QueuePrewarmPartition,
        constants::QUEUE_PAYLOAD_CHUNK_BYTES,
        storage::{queue_payload_chunk_key, queue_prewarm_marker_bytes},
    },
};

impl FoundationDbKvStore {
    pub(crate) async fn write_partitioned_queue_message_operation(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()> {
        self.write_partitioned_queue_messages_operation(vec![message])
            .await
    }

    pub(crate) async fn write_partitioned_queue_messages_operation(
        &self,
        messages: Vec<PartitionedQueueMessageWrite>,
    ) -> StorageResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("queue_send");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let mut write_bytes = 0u64;
            let mut write_key_bytes = 0u64;
            let mut set_count = 0u64;
            let mut ready_hints = HashMap::<&[u8], &[u8]>::new();
            let mut wake_writes = HashMap::<&[u8], &[u8]>::new();
            for message in &messages {
                let mut set_value = |key: &[u8], value: &[u8]| -> StorageResult<()> {
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    let prefixed_key = Self::prefix_bytes(prefix.as_ref(), key);
                    write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                    trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)
                        .map_err(|err| map_fdb_error("disable queue send write conflict", err))?;
                    trx.set(&prefixed_key, value);
                    set_count = set_count.saturating_add(1);
                    Ok(())
                };
                set_value(&message.state_key, &message.state_bytes)?;
                set_value(&message.ready_key, &[])?;
                if let Some(record_bytes) = &message.payload_record_bytes {
                    set_value(&message.payload_key, record_bytes)?;
                    for (index, chunk) in message
                        .payload_bytes
                        .chunks(QUEUE_PAYLOAD_CHUNK_BYTES)
                        .enumerate()
                    {
                        let chunk_key = queue_payload_chunk_key(
                            &message.payload_key,
                            u16::try_from(index).unwrap_or(u16::MAX),
                        );
                        set_value(&chunk_key, chunk)?;
                    }
                } else {
                    set_value(&message.payload_key, &message.payload_bytes)?;
                }
                ready_hints
                    .entry(&message.ready_hint_key)
                    .and_modify(|existing| {
                        if queue_ready_hint_is_earlier(&message.ready_hint_bytes, existing) {
                            *existing = &message.ready_hint_bytes;
                        }
                    })
                    .or_insert(&message.ready_hint_bytes);
                wake_writes
                    .entry(&message.wake_key)
                    .or_insert(&message.wake_bytes);
            }
            for (key, value) in ready_hints.into_iter().chain(wake_writes) {
                write_bytes = write_bytes.saturating_add(value.len() as u64);
                let prefixed_key = Self::prefix_bytes(prefix.as_ref(), key);
                write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)
                    .map_err(|err| map_fdb_error("disable queue send write conflict", err))?;
                trx.set(&prefixed_key, value);
                set_count = set_count.saturating_add(1);
            }
            record_fdb_operation("queue_send", "set", set_count);
            record_fdb_write_shape("queue_send", set_count, 0);
            record_fdb_operation_bytes("queue_send", "write", write_bytes);
            record_fdb_operation_bytes("queue_send", "write_key", write_key_bytes);
            record_fdb_operation("queue_send", "commit", 1);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    record_fdb_operation("queue_send", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = messages
                                .iter()
                                .flat_map(|message| {
                                    [
                                        &message.state_key,
                                        &message.payload_key,
                                        &message.ready_key,
                                        &message.ready_hint_key,
                                        &message.wake_key,
                                    ]
                                })
                                .map(|key| Self::prefix_bytes(prefix.as_ref(), key))
                                .collect::<Vec<_>>();
                            self.log_conflict_details(
                                &new_trx,
                                "write_partitioned_queue_messages",
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
                            return Err(map_fdb_error("queue send commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn prewarm_partitioned_queue_operation(
        &self,
        queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        for chunk in partitions.chunks(64) {
            let trx = self.create_transaction()?;
            Self::configure_transaction(&trx, Some("queue.prewarm_partitioned_queue"), true)?;
            record_fdb_transaction_start("queue_prewarm");
            let mut write_bytes = 0u64;
            let mut write_key_bytes = 0u64;
            for partition in chunk {
                let marker_value = queue_prewarm_marker_bytes(
                    queue_url,
                    partition.placement_slot,
                    partition.partition_id,
                );
                write_bytes = write_bytes.saturating_add(marker_value.len() as u64);
                let prefixed_key = Self::prefix_bytes(prefix.as_ref(), &partition.marker_key);
                write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)
                    .map_err(|err| map_fdb_error("disable queue prewarm write conflict", err))?;
                trx.set(&prefixed_key, &marker_value);
            }
            record_fdb_operation("queue_prewarm", "set", chunk.len() as u64);
            record_fdb_write_shape("queue_prewarm", chunk.len() as u64, 0);
            record_fdb_operation_bytes("queue_prewarm", "write", write_bytes);
            record_fdb_operation_bytes("queue_prewarm", "write_key", write_key_bytes);
            record_fdb_operation("queue_prewarm", "commit", 1);
            trx.commit()
                .await
                .map_err(|err| map_fdb_error("queue prewarm commit", *err))?;
        }

        Ok(())
    }
}
