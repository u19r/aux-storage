use metrics_facade::{CounterMetric, counter};
use serde::Serialize;
use storage_types::{
    EncodeWriteRequest, TransactEncodeItem, TransactWriteItem, WireItem, WriteRequest,
};

pub(crate) const STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC: CounterMetric =
    CounterMetric::StorageBilledItemOpsTotalMetric;
pub(crate) const STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC: CounterMetric =
    CounterMetric::StorageLogicalItemBytesTotalMetric;

pub(crate) fn record_billed_item_ops(ddb_op: &str, item_kind: &str, direction: &str, count: u64) {
    counter!(
        STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC,
        "ddb_op" => ddb_op.to_string(),
        "item_kind" => item_kind.to_string(),
        "direction" => direction.to_string()
    )
    .increment(count);
}

pub(crate) fn record_logical_item_bytes(
    ddb_op: &str,
    item_kind: &str,
    direction: &str,
    bytes: u64,
) {
    counter!(
        STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC,
        "ddb_op" => ddb_op.to_string(),
        "item_kind" => item_kind.to_string(),
        "direction" => direction.to_string()
    )
    .increment(bytes);
}

pub(crate) fn record_read_cost(ddb_op: &str, item_kind: &str, count: usize, bytes: u64) {
    if count == 0 && bytes == 0 {
        return;
    }
    record_billed_item_ops(ddb_op, item_kind, "read", count as u64);
    record_logical_item_bytes(ddb_op, item_kind, "read", bytes);
}

pub(crate) fn record_write_cost(ddb_op: &str, item_kind: &str, count: usize, bytes: u64) {
    if count == 0 && bytes == 0 {
        return;
    }
    record_billed_item_ops(ddb_op, item_kind, "write", count as u64);
    record_logical_item_bytes(ddb_op, item_kind, "write", bytes);
}

pub(crate) fn attr_map_payload_bytes<T>(item: &T) -> u64
where T: Serialize + ?Sized {
    serde_json::to_vec(item).map_or(0, |bytes| bytes.len() as u64)
}

pub(crate) fn serializable_payload_bytes<T>(value: &T) -> u64
where T: Serialize + ?Sized {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len() as u64)
}

pub(crate) fn wire_items_payload_bytes(items: &[WireItem]) -> u64 {
    items.iter().map(|item| item.payload_len() as u64).sum()
}

#[derive(Debug, Default)]
pub(crate) struct WriteCostTally {
    pub(crate) put_ops: usize,
    pub(crate) put_bytes: u64,
    pub(crate) delete_ops: usize,
    pub(crate) delete_bytes: u64,
    pub(crate) update_ops: usize,
    pub(crate) update_bytes: u64,
    pub(crate) condition_check_ops: usize,
    pub(crate) condition_check_bytes: u64,
}

impl WriteCostTally {
    pub(crate) fn record_write_request(&mut self, request: &WriteRequest) {
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

    pub(crate) fn record_encode_write_request(&mut self, request: &EncodeWriteRequest) {
        if let Some(put_request) = request.put_request.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(put_request.item.payload_len() as u64);
        }
        if let Some(delete_request) = request.delete_request.as_ref() {
            self.delete_ops = self.delete_ops.saturating_add(1);
            self.delete_bytes = self
                .delete_bytes
                .saturating_add(attr_map_payload_bytes(&delete_request.key));
        }
    }

    pub(crate) fn record_transact_item(&mut self, item: &TransactWriteItem) {
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

    pub(crate) fn record_transact_encode_item(&mut self, item: &TransactEncodeItem) {
        if let Some(put_request) = item.put.as_ref() {
            self.put_ops = self.put_ops.saturating_add(1);
            self.put_bytes = self
                .put_bytes
                .saturating_add(put_request.item.payload_len() as u64);
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

    pub(crate) fn subtract(&self, other: &Self) -> Self {
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

    pub(crate) fn emit(&self, ddb_op: &str) {
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
