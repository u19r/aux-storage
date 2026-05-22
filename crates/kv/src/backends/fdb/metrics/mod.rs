use std::sync::atomic::{AtomicU64, Ordering};

const FOUNDATIONDB_OPERATIONS_TOTAL: &str = "foundationdb_operations_total";
const FOUNDATIONDB_OPERATION_BYTES_TOTAL: &str = "foundationdb_operation_bytes_total";
const FOUNDATIONDB_OPERATION_LATENCY_MICROS_TOTAL: &str =
    "foundationdb_operation_latency_micros_total";
const FOUNDATIONDB_OPERATION_LATENCY_COUNT_TOTAL: &str =
    "foundationdb_operation_latency_count_total";

static OPERATION_COUNTERS: [OperationCounters; 12] = [
    OperationCounters::new("range"),
    OperationCounters::new("queue_send"),
    OperationCounters::new("queue_prewarm"),
    OperationCounters::new("queue_claim"),
    OperationCounters::new("queue_claim_payload"),
    OperationCounters::new("transact_write"),
    OperationCounters::new("transact_write_unchecked"),
    OperationCounters::new("transact_write_table"),
    OperationCounters::new("get"),
    OperationCounters::new("multi_get"),
    OperationCounters::new("put"),
    OperationCounters::new("delete"),
];

static BYTE_COUNTERS: [ByteCounters; 12] = [
    ByteCounters::new("range"),
    ByteCounters::new("queue_send"),
    ByteCounters::new("queue_prewarm"),
    ByteCounters::new("queue_claim"),
    ByteCounters::new("queue_claim_payload"),
    ByteCounters::new("transact_write"),
    ByteCounters::new("transact_write_unchecked"),
    ByteCounters::new("transact_write_table"),
    ByteCounters::new("get"),
    ByteCounters::new("multi_get"),
    ByteCounters::new("put"),
    ByteCounters::new("delete"),
];

static LATENCY_COUNTERS: [LatencyCounters; 12] = [
    LatencyCounters::new("range"),
    LatencyCounters::new("queue_send"),
    LatencyCounters::new("queue_prewarm"),
    LatencyCounters::new("queue_claim"),
    LatencyCounters::new("queue_claim_payload"),
    LatencyCounters::new("transact_write"),
    LatencyCounters::new("transact_write_unchecked"),
    LatencyCounters::new("transact_write_table"),
    LatencyCounters::new("get"),
    LatencyCounters::new("multi_get"),
    LatencyCounters::new("put"),
    LatencyCounters::new("delete"),
];

struct OperationCounters {
    path: &'static str,
    transaction: AtomicU64,
    transaction_start: AtomicU64,
    snapshot_point_read: AtomicU64,
    ordinary_point_read: AtomicU64,
    get: AtomicU64,
    snapshot_range_read: AtomicU64,
    ordinary_range_read: AtomicU64,
    range_read: AtomicU64,
    blind_write: AtomicU64,
    read_modify_write: AtomicU64,
    conflict_retry: AtomicU64,
    conflict_key: AtomicU64,
    read_conflict_range: AtomicU64,
    write_conflict_range: AtomicU64,
    candidate_key: AtomicU64,
    range_entry: AtomicU64,
    set: AtomicU64,
    commit: AtomicU64,
    retry: AtomicU64,
    clear: AtomicU64,
    range_clear: AtomicU64,
}

impl OperationCounters {
    const fn new(path: &'static str) -> Self {
        Self {
            path,
            transaction: AtomicU64::new(0),
            transaction_start: AtomicU64::new(0),
            snapshot_point_read: AtomicU64::new(0),
            ordinary_point_read: AtomicU64::new(0),
            get: AtomicU64::new(0),
            snapshot_range_read: AtomicU64::new(0),
            ordinary_range_read: AtomicU64::new(0),
            range_read: AtomicU64::new(0),
            blind_write: AtomicU64::new(0),
            read_modify_write: AtomicU64::new(0),
            conflict_retry: AtomicU64::new(0),
            conflict_key: AtomicU64::new(0),
            read_conflict_range: AtomicU64::new(0),
            write_conflict_range: AtomicU64::new(0),
            candidate_key: AtomicU64::new(0),
            range_entry: AtomicU64::new(0),
            set: AtomicU64::new(0),
            commit: AtomicU64::new(0),
            retry: AtomicU64::new(0),
            clear: AtomicU64::new(0),
            range_clear: AtomicU64::new(0),
        }
    }

    fn counter(&self, operation: &str) -> Option<&AtomicU64> {
        match operation {
            "transaction" => Some(&self.transaction),
            "transaction_start" => Some(&self.transaction_start),
            "snapshot_point_read" => Some(&self.snapshot_point_read),
            "ordinary_point_read" => Some(&self.ordinary_point_read),
            "get" => Some(&self.get),
            "snapshot_range_read" => Some(&self.snapshot_range_read),
            "ordinary_range_read" => Some(&self.ordinary_range_read),
            "range_read" => Some(&self.range_read),
            "blind_write" => Some(&self.blind_write),
            "read_modify_write" => Some(&self.read_modify_write),
            "conflict_retry" => Some(&self.conflict_retry),
            "conflict_key" => Some(&self.conflict_key),
            "read_conflict_range" => Some(&self.read_conflict_range),
            "write_conflict_range" => Some(&self.write_conflict_range),
            "candidate_key" => Some(&self.candidate_key),
            "range_entry" => Some(&self.range_entry),
            "set" => Some(&self.set),
            "commit" => Some(&self.commit),
            "retry" => Some(&self.retry),
            "clear" => Some(&self.clear),
            "range_clear" => Some(&self.range_clear),
            _ => None,
        }
    }

    fn visit(&self, mut visit: impl FnMut(&'static str, &AtomicU64)) {
        for (operation, counter) in [
            ("transaction", &self.transaction),
            ("transaction_start", &self.transaction_start),
            ("snapshot_point_read", &self.snapshot_point_read),
            ("ordinary_point_read", &self.ordinary_point_read),
            ("get", &self.get),
            ("snapshot_range_read", &self.snapshot_range_read),
            ("ordinary_range_read", &self.ordinary_range_read),
            ("range_read", &self.range_read),
            ("blind_write", &self.blind_write),
            ("read_modify_write", &self.read_modify_write),
            ("conflict_retry", &self.conflict_retry),
            ("conflict_key", &self.conflict_key),
            ("read_conflict_range", &self.read_conflict_range),
            ("write_conflict_range", &self.write_conflict_range),
            ("candidate_key", &self.candidate_key),
            ("range_entry", &self.range_entry),
            ("set", &self.set),
            ("commit", &self.commit),
            ("retry", &self.retry),
            ("clear", &self.clear),
            ("range_clear", &self.range_clear),
        ] {
            visit(operation, counter);
        }
    }
}

struct ByteCounters {
    path: &'static str,
    read_key: AtomicU64,
    write_key: AtomicU64,
    read: AtomicU64,
    write: AtomicU64,
}

impl ByteCounters {
    const fn new(path: &'static str) -> Self {
        Self {
            path,
            read_key: AtomicU64::new(0),
            write_key: AtomicU64::new(0),
            read: AtomicU64::new(0),
            write: AtomicU64::new(0),
        }
    }

    fn counter(&self, direction: &str) -> Option<&AtomicU64> {
        match direction {
            "read_key" => Some(&self.read_key),
            "write_key" => Some(&self.write_key),
            "read" => Some(&self.read),
            "write" => Some(&self.write),
            _ => None,
        }
    }

    fn visit(&self, mut visit: impl FnMut(&'static str, &AtomicU64)) {
        for (direction, counter) in [
            ("read_key", &self.read_key),
            ("write_key", &self.write_key),
            ("read", &self.read),
            ("write", &self.write),
        ] {
            visit(direction, counter);
        }
    }
}

struct LatencyCounters {
    path: &'static str,
    commit: LatencyStageCounters,
    on_error: LatencyStageCounters,
    execute: LatencyStageCounters,
    point_read: LatencyStageCounters,
    point_read_batch: LatencyStageCounters,
    range_read: LatencyStageCounters,
}

impl LatencyCounters {
    const fn new(path: &'static str) -> Self {
        Self {
            path,
            commit: LatencyStageCounters::new("commit"),
            on_error: LatencyStageCounters::new("on_error"),
            execute: LatencyStageCounters::new("execute"),
            point_read: LatencyStageCounters::new("point_read"),
            point_read_batch: LatencyStageCounters::new("point_read_batch"),
            range_read: LatencyStageCounters::new("range_read"),
        }
    }

    fn stage(&self, stage: &str) -> Option<&LatencyStageCounters> {
        match stage {
            "commit" => Some(&self.commit),
            "on_error" => Some(&self.on_error),
            "execute" => Some(&self.execute),
            "point_read" => Some(&self.point_read),
            "point_read_batch" => Some(&self.point_read_batch),
            "range_read" => Some(&self.range_read),
            _ => None,
        }
    }

    fn visit(&self, mut visit: impl FnMut(&LatencyStageCounters)) {
        for stage in [
            &self.commit,
            &self.on_error,
            &self.execute,
            &self.point_read,
            &self.point_read_batch,
            &self.range_read,
        ] {
            visit(stage);
        }
    }
}

struct LatencyStageCounters {
    stage: &'static str,
    micros: AtomicU64,
    count: AtomicU64,
}

impl LatencyStageCounters {
    const fn new(stage: &'static str) -> Self {
        Self {
            stage,
            micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

pub fn foundationdb_operation_metrics_snapshot() -> String {
    let mut lines = String::new();
    lines.push_str("# TYPE foundationdb_operations_total counter\n");
    lines.push_str("# TYPE foundationdb_operation_bytes_total counter\n");
    lines.push_str("# TYPE foundationdb_operation_latency_micros_total counter\n");
    lines.push_str("# TYPE foundationdb_operation_latency_count_total counter\n");

    for counters in &OPERATION_COUNTERS {
        counters.visit(|operation, counter| {
            append_metric_line(
                &mut lines,
                FOUNDATIONDB_OPERATIONS_TOTAL,
                counters.path,
                "operation",
                operation,
                counter,
            );
        });
    }

    for counters in &BYTE_COUNTERS {
        counters.visit(|direction, counter| {
            append_metric_line(
                &mut lines,
                FOUNDATIONDB_OPERATION_BYTES_TOTAL,
                counters.path,
                "direction",
                direction,
                counter,
            );
        });
    }

    for counters in &LATENCY_COUNTERS {
        counters.visit(|stage| {
            append_metric_line(
                &mut lines,
                FOUNDATIONDB_OPERATION_LATENCY_MICROS_TOTAL,
                counters.path,
                "stage",
                stage.stage,
                &stage.micros,
            );
            append_metric_line(
                &mut lines,
                FOUNDATIONDB_OPERATION_LATENCY_COUNT_TOTAL,
                counters.path,
                "stage",
                stage.stage,
                &stage.count,
            );
        });
    }

    lines
}

pub fn foundationdb_operation_metrics_reset() {
    for counters in &OPERATION_COUNTERS {
        counters.visit(|_, counter| counter.store(0, Ordering::Relaxed));
    }
    for counters in &BYTE_COUNTERS {
        counters.visit(|_, counter| counter.store(0, Ordering::Relaxed));
    }
    for counters in &LATENCY_COUNTERS {
        counters.visit(|stage| {
            stage.micros.store(0, Ordering::Relaxed);
            stage.count.store(0, Ordering::Relaxed);
        });
    }
}

pub(super) fn record_fdb_operation(path: &'static str, operation: &'static str, count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counter) = operation_counters(path).and_then(|counters| counters.counter(operation))
    {
        counter.fetch_add(count, Ordering::Relaxed);
    }
}

pub(super) fn record_fdb_transaction_start(path: &'static str) {
    record_fdb_operation(path, "transaction", 1);
    record_fdb_operation(path, "transaction_start", 1);
}

pub(super) fn record_fdb_point_read(path: &'static str, snapshot: bool, count: u64) {
    if snapshot {
        record_fdb_operation(path, "snapshot_point_read", count);
    } else {
        record_fdb_operation(path, "ordinary_point_read", count);
    }
    record_fdb_operation(path, "get", count);
}

pub(super) fn record_fdb_range_read(path: &'static str, snapshot: bool, count: u64) {
    if snapshot {
        record_fdb_operation(path, "snapshot_range_read", count);
    } else {
        record_fdb_operation(path, "ordinary_range_read", count);
    }
    record_fdb_operation(path, "range_read", count);
}

pub(super) fn record_fdb_write_shape(
    path: &'static str,
    blind_writes: u64,
    read_modify_writes: u64,
) {
    record_fdb_operation(path, "blind_write", blind_writes);
    record_fdb_operation(path, "read_modify_write", read_modify_writes);
}

pub(super) fn record_fdb_operation_bytes(path: &'static str, direction: &'static str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    if let Some(counter) = byte_counters(path).and_then(|counters| counters.counter(direction)) {
        counter.fetch_add(bytes, Ordering::Relaxed);
    }
}

pub(super) fn record_fdb_operation_latency(
    path: &'static str,
    stage: &'static str,
    elapsed: std::time::Duration,
) {
    let Some(counters) = latency_counters(path).and_then(|counters| counters.stage(stage)) else {
        return;
    };
    counters.micros.fetch_add(
        elapsed.as_micros().try_into().unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    counters.count.fetch_add(1, Ordering::Relaxed);
}

pub(super) fn record_fdb_conflict_artifacts(
    operation: &'static str,
    conflict_keys: u64,
    read_conflict_ranges: u64,
    write_conflict_ranges: u64,
    candidate_keys: u64,
) {
    record_fdb_operation(operation, "conflict_retry", 1);
    record_fdb_operation(operation, "conflict_key", conflict_keys);
    record_fdb_operation(operation, "read_conflict_range", read_conflict_ranges);
    record_fdb_operation(operation, "write_conflict_range", write_conflict_ranges);
    record_fdb_operation(operation, "candidate_key", candidate_keys);
}

fn operation_counters(path: &str) -> Option<&'static OperationCounters> {
    match path {
        "range" => Some(&OPERATION_COUNTERS[0]),
        "queue_send" => Some(&OPERATION_COUNTERS[1]),
        "queue_prewarm" => Some(&OPERATION_COUNTERS[2]),
        "queue_claim" => Some(&OPERATION_COUNTERS[3]),
        "queue_claim_payload" => Some(&OPERATION_COUNTERS[4]),
        "transact_write" => Some(&OPERATION_COUNTERS[5]),
        "transact_write_unchecked" => Some(&OPERATION_COUNTERS[6]),
        "transact_write_table" => Some(&OPERATION_COUNTERS[7]),
        "get" => Some(&OPERATION_COUNTERS[8]),
        "multi_get" => Some(&OPERATION_COUNTERS[9]),
        "put" => Some(&OPERATION_COUNTERS[10]),
        "delete" => Some(&OPERATION_COUNTERS[11]),
        _ => None,
    }
}

fn byte_counters(path: &str) -> Option<&'static ByteCounters> {
    match path {
        "range" => Some(&BYTE_COUNTERS[0]),
        "queue_send" => Some(&BYTE_COUNTERS[1]),
        "queue_prewarm" => Some(&BYTE_COUNTERS[2]),
        "queue_claim" => Some(&BYTE_COUNTERS[3]),
        "queue_claim_payload" => Some(&BYTE_COUNTERS[4]),
        "transact_write" => Some(&BYTE_COUNTERS[5]),
        "transact_write_unchecked" => Some(&BYTE_COUNTERS[6]),
        "transact_write_table" => Some(&BYTE_COUNTERS[7]),
        "get" => Some(&BYTE_COUNTERS[8]),
        "multi_get" => Some(&BYTE_COUNTERS[9]),
        "put" => Some(&BYTE_COUNTERS[10]),
        "delete" => Some(&BYTE_COUNTERS[11]),
        _ => None,
    }
}

fn latency_counters(path: &str) -> Option<&'static LatencyCounters> {
    match path {
        "range" => Some(&LATENCY_COUNTERS[0]),
        "queue_send" => Some(&LATENCY_COUNTERS[1]),
        "queue_prewarm" => Some(&LATENCY_COUNTERS[2]),
        "queue_claim" => Some(&LATENCY_COUNTERS[3]),
        "queue_claim_payload" => Some(&LATENCY_COUNTERS[4]),
        "transact_write" => Some(&LATENCY_COUNTERS[5]),
        "transact_write_unchecked" => Some(&LATENCY_COUNTERS[6]),
        "transact_write_table" => Some(&LATENCY_COUNTERS[7]),
        "get" => Some(&LATENCY_COUNTERS[8]),
        "multi_get" => Some(&LATENCY_COUNTERS[9]),
        "put" => Some(&LATENCY_COUNTERS[10]),
        "delete" => Some(&LATENCY_COUNTERS[11]),
        _ => None,
    }
}

fn append_metric_line(
    lines: &mut String,
    metric: &'static str,
    path: &'static str,
    label: &'static str,
    label_value: &'static str,
    counter: &AtomicU64,
) {
    let value = counter.load(Ordering::Relaxed);
    if value == 0 {
        return;
    }
    lines.push_str(&format!(
        "{metric}{{path=\"{path}\",{label}=\"{label_value}\"}} {value}\n"
    ));
}

#[cfg(test)]
mod metrics_tests;
