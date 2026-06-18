#![allow(dead_code)]

use std::{
    cmp::Reverse,
    collections::HashMap,
    sync::{Mutex, MutexGuard, OnceLock},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct PerfCounterSnapshot {
    pub name: &'static str,
    pub calls: u64,
    pub total: Duration,
    pub max: Duration,
    pub total_amount: u64,
    pub max_amount: u64,
}

#[derive(Debug, Default, Clone)]
struct PerfCounter {
    calls: u64,
    total: Duration,
    max: Duration,
    total_amount: u64,
    max_amount: u64,
}

static COUNTERS: OnceLock<Mutex<HashMap<(&'static str, &'static str), PerfCounter>>> =
    OnceLock::new();

pub fn reset_provider(provider: &'static str) {
    let mut counters = lock_counters();
    counters.retain(|(counter_provider, _), _| *counter_provider != provider);
}

pub fn record(provider: &'static str, name: &'static str, elapsed: Duration) {
    let mut counters = lock_counters();
    let counter = counters.entry((provider, name)).or_default();
    counter.calls += 1;
    counter.total += elapsed;
    counter.max = counter.max.max(elapsed);
}

pub fn record_amount(provider: &'static str, name: &'static str, amount: u64) {
    let mut counters = lock_counters();
    let counter = counters.entry((provider, name)).or_default();
    counter.calls += 1;
    counter.total_amount = counter.total_amount.saturating_add(amount);
    counter.max_amount = counter.max_amount.max(amount);
}

pub fn snapshot_provider(provider: &'static str) -> Vec<PerfCounterSnapshot> {
    let counters = lock_counters();
    let mut snapshots = counters
        .iter()
        .filter_map(|((counter_provider, name), counter)| {
            if *counter_provider != provider {
                return None;
            }
            Some(PerfCounterSnapshot {
                name,
                calls: counter.calls,
                total: counter.total,
                max: counter.max,
                total_amount: counter.total_amount,
                max_amount: counter.max_amount,
            })
        })
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|snapshot| Reverse(snapshot.total));
    snapshots
}

pub fn emit_runtime_report(
    module_path: &str,
    test_name: &str,
    label: &str,
    mutation_count: usize,
    elapsed: Duration,
) {
    let elapsed_micros = elapsed.as_secs_f64() * 1_000_000.0;
    let micros_per_mutation = elapsed_micros / mutation_count.max(1) as f64;
    let mutations_per_second = mutation_count as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    println!(
        "{{\"schema_version\":1,\"event\":\"runtime_report\",\"module_path\":\"{}\",\"test_name\":\
         \"{}\",\"label\":\"{}\",\"mutation_count\":{},\"elapsed_micros\":{},\"\
         micros_per_mutation\":{},\"mutations_per_second\":{}}}",
        json_string(module_path),
        json_string(test_name),
        json_string(label),
        mutation_count,
        elapsed_micros,
        micros_per_mutation,
        mutations_per_second
    );
}

fn counters() -> &'static Mutex<HashMap<(&'static str, &'static str), PerfCounter>> {
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_counters() -> MutexGuard<'static, HashMap<(&'static str, &'static str), PerfCounter>> {
    match counters().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}
