use foundationdb::{RangeOption, options};
use storage_types::{DurationSeconds, StorageError, StorageResult, TimestampMillis};
use uuid::Uuid;

use crate::{
    backends::fdb::{
        error::map_fdb_error,
        metrics::{
            record_fdb_operation, record_fdb_operation_bytes, record_fdb_point_read,
            record_fdb_range_read, record_fdb_transaction_start, record_fdb_write_shape,
        },
        store::{FoundationDbKvStore, read_fdb_keys_sequential, rotate_fdb_claim_candidates},
    },
    queue::{
        QueueClaimBatch, QueueClaimRange, QueueClaimedMessage,
        storage::read_partitioned_queue_payload,
    },
};

impl FoundationDbKvStore {
    pub(crate) async fn claim_queue_messages_from_ranges_operation(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: TimestampMillis,
        visibility_timeout: DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch> {
        let mut batch = QueueClaimBatch::default();
        if ranges.is_empty() || max_claims == 0 {
            return Ok(batch);
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("queue_claim");

        'retry: loop {
            attempt += 1;
            batch = QueueClaimBatch::default();
            Self::configure_transaction(&trx, None, true)?;
            let mut read_bytes = 0u64;
            let mut read_key_bytes = 0u64;
            let mut write_bytes = 0u64;
            let mut write_key_bytes = 0u64;
            let mut ordinary_gets = 0u64;
            let mut snapshot_gets = 0u64;
            let mut sets = 0u64;
            let mut clears = 0u64;
            let mut pending_claims = Vec::new();

            for range in &ranges {
                if batch.messages.len() >= max_claims {
                    break;
                }

                let start = Self::prefix_bytes(prefix.as_ref(), &range.ready_start);
                let end = Self::prefix_bytes(prefix.as_ref(), &range.ready_end);
                read_key_bytes =
                    read_key_bytes.saturating_add(start.len().saturating_add(end.len()) as u64);
                let scan_limit =
                    usize::try_from(range.scan_limit.max(range.limit)).unwrap_or(usize::MAX);
                let mut option = RangeOption::from((start, end));
                option.limit = Some(scan_limit);
                option.mode = options::StreamingMode::WantAll;
                let ready_entries = match trx.get_range(&option, 1, true).await {
                    Ok(ready_entries) => ready_entries,
                    Err(err) => {
                        let candidate_keys = ranges
                            .iter()
                            .flat_map(|range| {
                                [
                                    Self::prefix_bytes(prefix.as_ref(), &range.ready_start),
                                    Self::prefix_bytes(prefix.as_ref(), &range.ready_end),
                                ]
                            })
                            .collect::<Vec<_>>();
                        trx = self
                            .retry_transaction_after_fdb_error(
                                trx,
                                "queue_claim",
                                "read queue claim ready range",
                                attempt,
                                err,
                                &candidate_keys,
                            )
                            .await?;
                        continue 'retry;
                    }
                };
                record_fdb_range_read("queue_claim", true, 1);
                record_fdb_operation("queue_claim", "range_entry", ready_entries.len() as u64);
                let ready_entries_len = ready_entries.len();
                if ready_entries.is_empty() {
                    continue;
                }
                read_bytes = read_bytes.saturating_add(
                    ready_entries
                        .iter()
                        .map(|entry| entry.key().len().saturating_add(entry.value().len()) as u64)
                        .sum::<u64>(),
                );
                batch.ready_entries_seen =
                    batch.ready_entries_seen.saturating_add(ready_entries.len());
                let mut ready_entries = ready_entries.into_iter().collect::<Vec<_>>();
                rotate_fdb_claim_candidates(&mut ready_entries, range.candidate_seed);

                let range_claim_limit =
                    usize::try_from(range.claim_limit.max(1)).unwrap_or(usize::MAX);
                let mut range_claims = 0usize;
                let claim_budget = max_claims
                    .saturating_sub(batch.messages.len())
                    .min(range_claim_limit);
                let mut candidates = Vec::with_capacity(claim_budget.min(ready_entries.len()));
                for ready_entry in ready_entries {
                    if candidates.len() >= claim_budget {
                        break;
                    }
                    candidates.push(ready_entry);
                }
                let candidate_ready_keys = candidates
                    .iter()
                    .map(|entry| entry.key().to_vec())
                    .collect::<Vec<_>>();
                let ready_reads =
                    match read_fdb_keys_sequential(&trx, &candidate_ready_keys, false).await {
                        Ok(ready_reads) => ready_reads,
                        Err(err) => {
                            trx = self
                                .retry_transaction_after_fdb_error(
                                    trx,
                                    "queue_claim",
                                    "read queue claim ready keys",
                                    attempt,
                                    err,
                                    &candidate_ready_keys,
                                )
                                .await?;
                            continue 'retry;
                        }
                    };
                ordinary_gets = ordinary_gets
                    .saturating_add(u64::try_from(ready_reads.len()).unwrap_or(u64::MAX));

                let mut claim_candidates = Vec::with_capacity(candidates.len());
                for (ready_entry, ready_value) in candidates.into_iter().zip(ready_reads) {
                    if batch.messages.len() >= max_claims || range_claims >= range_claim_limit {
                        break;
                    }
                    read_bytes = read_bytes.saturating_add(
                        ready_entry
                            .key()
                            .len()
                            .saturating_add(ready_value.as_ref().map_or(0, |value| value.len()))
                            as u64,
                    );
                    read_key_bytes = read_key_bytes.saturating_add(ready_entry.key().len() as u64);
                    let Some(ready_value) = ready_value else {
                        continue;
                    };

                    let ready_key = self.strip_prefix(ready_entry.key()).to_vec();
                    let ready_visibility_key =
                        crate::queue_provider::partitioned_ready_visibility_key(&ready_key)
                            .map_err(|error| StorageError::internal(&error.to_string()))?;
                    let message_id = ready_visibility_key
                        .get_message_id()
                        .map_err(|error| StorageError::internal(&error.to_string()))?;
                    let message_id_hex = message_id.to_string();
                    let state_key = crate::partition_family::queue_state_key_with_slot(
                        range.queue_id,
                        range.placement_slot,
                        range.partition_id,
                        &message_id_hex,
                    );
                    let prefixed_state_key = Self::prefix_bytes(prefix.as_ref(), &state_key);
                    let payload_key = crate::partition_family::queue_payload_key_with_slot(
                        range.queue_id,
                        range.placement_slot,
                        range.partition_id,
                        &message_id_hex,
                    );

                    claim_candidates.push((
                        ready_entry.key().to_vec(),
                        ready_value.to_vec(),
                        message_id,
                        message_id_hex,
                        state_key,
                        prefixed_state_key,
                        payload_key,
                    ));
                }

                let prefixed_state_keys = claim_candidates
                    .iter()
                    .map(|(_, _, _, _, _, prefixed_state_key, _)| prefixed_state_key.clone())
                    .collect::<Vec<_>>();
                let state_reads =
                    match read_fdb_keys_sequential(&trx, &prefixed_state_keys, true).await {
                        Ok(state_reads) => state_reads,
                        Err(err) => {
                            trx = self
                                .retry_transaction_after_fdb_error(
                                    trx,
                                    "queue_claim",
                                    "read queue claim states",
                                    attempt,
                                    err,
                                    &prefixed_state_keys,
                                )
                                .await?;
                            continue 'retry;
                        }
                    };
                snapshot_gets = snapshot_gets
                    .saturating_add(u64::try_from(state_reads.len()).unwrap_or(u64::MAX));

                for (ready_candidate, state_bytes) in claim_candidates.into_iter().zip(state_reads)
                {
                    let (
                        ready_key,
                        ready_value,
                        message_id,
                        message_id_hex,
                        state_key,
                        prefixed_state_key,
                        payload_key,
                    ) = ready_candidate;
                    if batch.messages.len() >= max_claims || range_claims >= range_claim_limit {
                        break;
                    }
                    read_bytes = read_bytes.saturating_add(
                        prefixed_state_key
                            .len()
                            .saturating_add(state_bytes.as_ref().map_or(0, |value| value.len()))
                            as u64,
                    );
                    read_key_bytes = read_key_bytes.saturating_add(prefixed_state_key.len() as u64);
                    let Some(state_bytes) = state_bytes else {
                        trx.clear(&ready_key);
                        clears = clears.saturating_add(1);
                        continue;
                    };

                    let mut state: crate::queue_provider::PartitionedQueueState =
                        storage_types::storage_serde::from_bytes(&state_bytes).map_err(
                            |error| {
                                StorageError::internal(&format!(
                                    "deserialize partitioned queue state key={} error={:?}",
                                    String::from_utf8_lossy(&state_key),
                                    error.as_ref()
                                ))
                            },
                        )?;
                    if state.visibility_timestamp > now {
                        continue;
                    }
                    let expected_ready_value = state
                        .claim_nonce
                        .as_ref()
                        .map_or_else(Vec::new, |nonce| nonce.as_bytes().to_vec());
                    if ready_value != expected_ready_value {
                        continue;
                    }

                    state.delivery_attempt = state.delivery_attempt.saturating_add(1);
                    state.visibility_timestamp = now + visibility_timeout;
                    state.claim_nonce = Some(Uuid::now_v7().to_string());
                    let new_visibility_key = crate::newtypes::MessageVisibilityKey(
                        crate::queue_provider::visibility_key(
                            state.visibility_timestamp,
                            &message_id,
                        ),
                    );
                    let new_ready_key = crate::partition_family::queue_ready_key_with_slot(
                        range.queue_id,
                        range.placement_slot,
                        range.partition_id,
                        &new_visibility_key,
                    );
                    let prefixed_new_ready_key =
                        Self::prefix_bytes(prefix.as_ref(), &new_ready_key);
                    let state_value = storage_types::storage_serde::to_bytes(&state)?;

                    trx.clear(&ready_key);
                    write_key_bytes = write_key_bytes.saturating_add(ready_key.len() as u64);
                    trx.set(
                        &prefixed_new_ready_key,
                        state.claim_nonce.as_deref().unwrap_or_default().as_bytes(),
                    );
                    trx.set(&prefixed_state_key, &state_value);
                    write_key_bytes = write_key_bytes.saturating_add(
                        prefixed_new_ready_key
                            .len()
                            .saturating_add(prefixed_state_key.len())
                            as u64,
                    );
                    clears = clears.saturating_add(1);
                    sets = sets.saturating_add(2);
                    write_bytes = write_bytes.saturating_add(state_value.len() as u64);

                    range_claims = range_claims.saturating_add(1);
                    pending_claims.push((
                        payload_key,
                        QueueClaimedMessage {
                            partition_id: range.partition_id,
                            message_id_hex,
                            body_bytes: Vec::new(),
                            visibility_timestamp: state.visibility_timestamp,
                            delivery_attempt: state.delivery_attempt,
                            claim_nonce: state.claim_nonce.clone().unwrap_or_default(),
                        },
                    ));
                    batch.messages.push(QueueClaimedMessage {
                        partition_id: range.partition_id,
                        message_id_hex: String::new(),
                        body_bytes: Vec::new(),
                        visibility_timestamp: state.visibility_timestamp,
                        delivery_attempt: state.delivery_attempt,
                        claim_nonce: state.claim_nonce.clone().unwrap_or_default(),
                    });
                }
                if range_claims > 0
                    && range_claims == ready_entries_len
                    && ready_entries_len < scan_limit
                {
                    let prefixed_hint_key =
                        Self::prefix_bytes(prefix.as_ref(), &range.ready_hint_key);
                    let hint_value =
                        crate::partition_family::queue_ready_hint_bytes(range.partition_id, now);
                    trx.set(&prefixed_hint_key, &hint_value);
                    sets = sets.saturating_add(1);
                    write_key_bytes =
                        write_key_bytes.saturating_add(prefixed_hint_key.len() as u64);
                    write_bytes = write_bytes.saturating_add(hint_value.len() as u64);
                }
            }

            record_fdb_point_read("queue_claim", false, ordinary_gets);
            record_fdb_point_read("queue_claim", true, snapshot_gets);
            record_fdb_operation("queue_claim", "set", sets);
            record_fdb_operation("queue_claim", "clear", clears);
            record_fdb_write_shape("queue_claim", 0, sets.saturating_add(clears));
            record_fdb_operation_bytes("queue_claim", "read", read_bytes);
            record_fdb_operation_bytes("queue_claim", "read_key", read_key_bytes);
            record_fdb_operation_bytes("queue_claim", "write", write_bytes);
            record_fdb_operation_bytes("queue_claim", "write_key", write_key_bytes);
            record_fdb_operation("queue_claim", "commit", 1);
            match trx.commit().await {
                Ok(_) => {
                    if pending_claims.is_empty() {
                        return Ok(batch);
                    }
                    let payload_trx = self.create_transaction()?;
                    record_fdb_transaction_start("queue_claim_payload");
                    self.configure_read_transaction(&payload_trx, None, false)?;
                    let payload_read_count =
                        u64::try_from(pending_claims.len()).unwrap_or(u64::MAX);
                    let prefixed_payload_keys = pending_claims
                        .iter()
                        .map(|(payload_key, _)| Self::prefix_bytes(prefix.as_ref(), payload_key))
                        .collect::<Vec<_>>();
                    let payload_reads =
                        read_fdb_keys_sequential(&payload_trx, &prefixed_payload_keys, true)
                            .await
                            .map_err(|err| map_fdb_error("read queue claim payloads", err))?;
                    let mut claimed = Vec::with_capacity(payload_reads.len());
                    let mut payload_read_bytes = 0u64;
                    let mut payload_read_key_bytes = 0u64;
                    for ((payload_key, mut message), payload_bytes) in
                        pending_claims.into_iter().zip(payload_reads)
                    {
                        payload_read_key_bytes =
                            payload_read_key_bytes.saturating_add(payload_key.len() as u64);
                        let Some(payload_bytes) = payload_bytes else {
                            continue;
                        };
                        payload_read_bytes = payload_read_bytes.saturating_add(
                            payload_key.len().saturating_add(payload_bytes.len()) as u64,
                        );
                        message.body_bytes = read_partitioned_queue_payload(
                            self,
                            &payload_key,
                            payload_bytes.to_vec(),
                        )
                        .await?;
                        claimed.push(message);
                    }
                    record_fdb_point_read("queue_claim_payload", true, payload_read_count);
                    record_fdb_operation_bytes("queue_claim_payload", "read", payload_read_bytes);
                    record_fdb_operation_bytes(
                        "queue_claim_payload",
                        "read_key",
                        payload_read_key_bytes,
                    );
                    batch.messages = claimed;
                    return Ok(batch);
                }
                Err(commit_err) => {
                    record_fdb_operation("queue_claim", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = ranges
                                .iter()
                                .flat_map(|range| {
                                    [
                                        Self::prefix_bytes(prefix.as_ref(), &range.ready_start),
                                        Self::prefix_bytes(prefix.as_ref(), &range.ready_end),
                                    ]
                                })
                                .collect::<Vec<_>>();
                            self.log_conflict_details(
                                &new_trx,
                                "claim_queue_messages_from_ranges",
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
                            return Err(map_fdb_error("queue claim commit", retry_err));
                        }
                    }
                }
            }
        }
    }
}
