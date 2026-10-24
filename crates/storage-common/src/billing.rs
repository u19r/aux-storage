//! DynamoDB billing meters shared by KV and SQL backends.
//!
//! Metric names and `WriteCostTally` semantics are identical in both
//! providers. This module owns billing labels, logical request-byte counts,
//! and encoded storage-payload measurement; SQL's `Encode*` path retains
//! pre-encoded wire-item lengths.

use std::io;

use metrics_facade::{CounterMetric, counter};
use serde::Serialize;
use storage_types::{
    EncodeWriteRequest, StorageEnum, StorageResult, TransactEncodeItem, TransactWriteItem,
    WireItem, WriteRequest,
};

pub const STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC: CounterMetric =
    CounterMetric::StorageBilledItemOpsTotalMetric;
pub const STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::StorageLogicalItemBytesTotalMetric;

pub fn record_billed_item_ops(ddb_op: &str, item_kind: &str, direction: &str, count: u64) {
    counter!(
        STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC,
        "ddb_op" => ddb_op.to_string(),
        "item_kind" => item_kind.to_string(),
        "direction" => direction.to_string()
    )
    .increment(count);
}

pub fn record_logical_item_bytes(ddb_op: &str, item_kind: &str, direction: &str, bytes: u64) {
    counter!(
        STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC,
        "ddb_op" => ddb_op.to_string(),
        "item_kind" => item_kind.to_string(),
        "direction" => direction.to_string()
    )
    .increment(bytes);
}

pub fn record_read_cost(ddb_op: &str, item_kind: &str, count: usize, bytes: u64) {
    if count == 0 && bytes == 0 {
        return;
    }
    record_billed_item_ops(ddb_op, item_kind, "read", count as u64);
    record_logical_item_bytes(ddb_op, item_kind, "read", bytes);
}

/// Record one completed Query page using its pre-projection payload size.
pub fn record_query_result(items_returned: usize, bytes_read: u64) {
    crate::telemetry::record_read(items_returned, bytes_read as usize);
    record_read_cost("query_table", "query", 1, bytes_read);
}

pub fn record_write_cost(ddb_op: &str, item_kind: &str, count: usize, bytes: u64) {
    if count == 0 && bytes == 0 {
        return;
    }
    record_billed_item_ops(ddb_op, item_kind, "write", count as u64);
    record_logical_item_bytes(ddb_op, item_kind, "write", bytes);
}

pub fn attr_map_payload_bytes<T>(item: &T) -> u64
where T: Serialize + ?Sized {
    serialized_payload_bytes(item)
}

pub fn serializable_payload_bytes<T>(value: &T) -> u64
where T: Serialize + ?Sized {
    serialized_payload_bytes(value)
}

/// Measure the encoded size used by storage payload metrics.
///
/// Small values are counted while writing JSON to avoid an intermediate byte
/// buffer. Large values use the canonical storage encoder so its compression
/// decision remains part of the measured format. Serialization failures are
/// propagated to the write caller.
pub fn storage_payload_bytes<T>(value: &T) -> StorageResult<usize>
where T: Serialize {
    let mut counter = PayloadByteCounter::default();
    serde_json::to_writer(&mut counter, value).map_err(StorageEnum::from)?;
    if counter.bytes <= storage_types::STORAGE_SERDE_RAW_JSON_LIMIT_BYTES as u64 {
        let json_bytes = usize::try_from(counter.bytes).unwrap_or(usize::MAX);
        return Ok(json_bytes.saturating_add(storage_types::STORAGE_SERDE_ENCODED_PREFIX_LEN));
    }
    Ok(storage_types::storage_serde::to_bytes(value)?.len())
}

fn serialized_payload_bytes<T>(value: &T) -> u64
where T: Serialize + ?Sized {
    let mut counter = PayloadByteCounter::default();
    serde_json::to_writer(&mut counter, value).map_or(0, |_| counter.bytes)
}

#[derive(Default)]
struct PayloadByteCounter {
    bytes: u64,
}

impl io::Write for PayloadByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn wire_items_payload_bytes(items: &[WireItem]) -> u64 {
    items.iter().map(|item| item.payload_len() as u64).sum()
}

#[derive(Debug, Default)]
pub struct WriteCostTally {
    pub put_ops: usize,
    pub put_bytes: u64,
    pub delete_ops: usize,
    pub delete_bytes: u64,
    pub update_ops: usize,
    pub update_bytes: u64,
    pub condition_check_ops: usize,
    pub condition_check_bytes: u64,
}

impl WriteCostTally {
    pub fn record_write_request(&mut self, request: &WriteRequest) {
        if let Some(put_request) = request.put_request.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(attr_map_payload_bytes(&put_request.item));
        }
        if let Some(delete_request) = request.delete_request.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
    }

    pub fn record_encode_write_request(&mut self, request: &EncodeWriteRequest) {
        if let Some(put_request) = request.put_request.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(put_request.item.item().payload_len() as u64);
        }
        if let Some(delete_request) = request.delete_request.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
    }

    pub fn record_transact_item(&mut self, item: &TransactWriteItem) {
        if let Some(put_request) = item.put.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(attr_map_payload_bytes(&put_request.item));
        }
        if let Some(delete_request) = item.delete.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
        if let Some(update_request) = item.update.as_ref() {
            self.update_ops = self.update_ops.saturating_add(1);
            self.update_bytes = self
                .update_bytes
                .saturating_add(serializable_payload_bytes(update_request));
        }
        if let Some(condition_check) = item.condition_check.as_ref() {
            self.condition_check_ops = self.condition_check_ops.saturating_add(1);
            self.condition_check_bytes = self
                .condition_check_bytes
                .saturating_add(serializable_payload_bytes(condition_check));
        }
    }

    pub fn record_transact_encode_item(&mut self, item: &TransactEncodeItem) {
        if let Some(put_request) = item.put.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(put_request.item.item().payload_len() as u64);
        }
        if let Some(delete_request) = item.delete.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
        if let Some(update_request) = item.update.as_ref() {
            self.update_ops = self.update_ops.saturating_add(1);
            self.update_bytes = self
                .update_bytes
                .saturating_add(serializable_payload_bytes(update_request));
        }
        if let Some(condition_check) = item.condition_check.as_ref() {
            self.condition_check_ops = self.condition_check_ops.saturating_add(1);
            self.condition_check_bytes = self
                .condition_check_bytes
                .saturating_add(serializable_payload_bytes(condition_check));
        }
    }

    #[must_use]
    pub fn subtract(&self, other: &Self) -> Self {
        Self {
            put_ops: self.put_ops.saturating_sub(other.put_ops),
            put_bytes: self.put_bytes.saturating_sub(other.put_bytes),
            delete_ops: self.delete_ops.saturating_sub(other.delete_ops),
            delete_bytes: self.delete_bytes.saturating_sub(other.delete_bytes),
            update_ops: self.update_ops.saturating_sub(other.update_ops),
            update_bytes: self.update_bytes.saturating_sub(other.update_bytes),
            condition_check_ops: self
                .condition_check_ops
                .saturating_sub(other.condition_check_ops),
            condition_check_bytes: self
                .condition_check_bytes
                .saturating_sub(other.condition_check_bytes),
        }
    }

    pub fn emit(&self, ddb_op: &str) {
        record_write_cost(ddb_op, "put", self.put_ops, self.put_bytes);
        record_write_cost(ddb_op, "delete", self.delete_ops, self.delete_bytes);
        record_write_cost(ddb_op, "update", self.update_ops, self.update_bytes);
        record_write_cost(
            ddb_op,
            "condition_check",
            self.condition_check_ops,
            self.condition_check_bytes,
        );
    }
}
