use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

static QUEUE_OPERATION_TOTALS: OnceLock<Mutex<HashMap<QueueOperationMetricKey, u64>>> =
    OnceLock::new();
static QUEUE_OPERATION_GAUGES: OnceLock<Mutex<HashMap<QueueOperationMetricKey, f64>>> =
    OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct QueueOperationMetricKey {
    metric: &'static str,
    path: &'static str,
    kind: &'static str,
    value: &'static str,
}

pub fn queue_operation_metrics_snapshot() -> String {
    let Some(totals) = QUEUE_OPERATION_TOTALS.get() else {
        return String::new();
    };
    let totals = match totals.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut lines = String::new();
    lines.push_str("# TYPE queue_storage_operations_total counter\n");
    for (key, value) in totals.iter() {
        lines.push_str(&format!(
            "{}{{path=\"{}\",{}=\"{}\"}} {}\n",
            key.metric, key.path, key.kind, key.value, value
        ));
    }
    if let Some(gauges) = QUEUE_OPERATION_GAUGES.get() {
        let gauges = match gauges.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        lines.push_str("# TYPE queue_storage_gauge gauge\n");
        for (key, value) in gauges.iter() {
            lines.push_str(&format!(
                "{}{{path=\"{}\",{}=\"{}\"}} {}\n",
                key.metric, key.path, key.kind, key.value, value
            ));
        }
    }
    lines
}

pub(crate) fn record_queue_storage_operation(
    path: &'static str,
    operation: &'static str,
    count: u64,
) {
    if count == 0 {
        return;
    }
    let totals = QUEUE_OPERATION_TOTALS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut totals = match totals.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = totals
        .entry(QueueOperationMetricKey {
            metric: "queue_storage_operations_total",
            path,
            kind: "operation",
            value: operation,
        })
        .or_default();
    *entry = entry.saturating_add(count);
}

pub(crate) fn set_queue_storage_gauge(path: &'static str, gauge: &'static str, value: f64) {
    let gauges = QUEUE_OPERATION_GAUGES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gauges = match gauges.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    gauges.insert(
        QueueOperationMetricKey {
            metric: "queue_storage_gauge",
            path,
            kind: "gauge",
            value: gauge,
        },
        value,
    );
}
