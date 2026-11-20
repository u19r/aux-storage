//! Native read-wait samples and bounded retention discovery cost.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures::FutureExt as _;
use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use storage_types::{StreamRetentionDuration, TableName, TimestampMillis};
use stream_provider::SystemStreamPageRequest;

use super::{
    native_census_test_support::NativeCensus,
    test_support::{TestStore, hash_table_request},
};

struct Sample(AtomicU64);
impl Sample {
    fn read(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}
impl GaugeFn for Sample {
    fn increment(&self, _: f64) {
        panic!("these gauges only set samples")
    }
    fn decrement(&self, _: f64) {
        panic!("these gauges only set samples")
    }
    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}
struct ReadWaitSamples {
    lag: Arc<Sample>,
    age: Arc<Sample>,
}
impl Recorder for ReadWaitSamples {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        match key.name() {
            "storage.postgres.stream.frontier.lag.xids" => Gauge::from_arc(self.lag.clone()),
            "storage.postgres.stream.frontier.oldest_xact.age.ms" => {
                Gauge::from_arc(self.age.clone())
            }
            _ => Gauge::noop(),
        }
    }
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN; own nextest process"]
async fn given_assigned_old_transaction_when_reading_and_discovering_then_metrics_and_batch_cost_are_measured()
-> anyhow::Result<()> {
    let lag = Arc::new(Sample(AtomicU64::new(f64::NAN.to_bits())));
    let age = Arc::new(Sample(AtomicU64::new(f64::NAN.to_bits())));
    metrics::set_global_recorder(ReadWaitSamples {
        lag: lag.clone(),
        age: age.clone(),
    })?;
    let dsn = std::env::var("TEST_POSTGRES_DSN")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    let connection_task = tokio::spawn(connection);
    client
        .batch_execute("BEGIN; SELECT pg_current_xact_id();")
        .await?;
    let census = NativeCensus::new();
    let fixture = TestStore::new_with_census(census.clone()).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let scope = fixture.stream_operations.system_stream_scope().await?;
        fixture
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(scope, None, 10)?)
            .await?;
        anyhow::ensure!(
            lag.read().is_finite() && lag.read() > 0.0,
            "lag sample {}",
            lag.read()
        );
        println!(
            "held assigned transaction: frontier lag={} xids",
            lag.read()
        );
        let horizon = TimestampMillis::from_timestamp(i64::MAX);
        for expected in [0_u64, 1, 3] {
            if expected > 0 {
                let names: &[&str] = if expected == 1 {
                    &["metrics-a"]
                } else {
                    &["metrics-b", "metrics-c"]
                };
                for name in names {
                    let mut request = hash_table_request(TableName::new(*name));
                    request.aux_stream_duration_hours =
                        Some(StreamRetentionDuration::from_hours(24)?);
                    fixture.operations.create_table(request).await?;
                }
            }
            // Warm plans before counting. A positive-limit empty page must
            // still sample.
            fixture
                .maintenance
                .list_due_stream_trim_markers(horizon, 10)
                .await?;
            age.0.store(f64::NAN.to_bits(), Ordering::Relaxed);
            census.reset();
            census.record();
            let markers = fixture
                .maintenance
                .list_due_stream_trim_markers(horizon, 10)
                .await?;
            let counts = census.snapshot();
            let sql = census.recorded();
            let queries = sql.iter().filter(|sql| sql.starts_with("SELECT")).count();
            println!(
                "markers={expected} age_ms={} queries={queries} {counts}",
                age.read()
            );
            anyhow::ensure!(markers.len() as u64 == expected);
            anyhow::ensure!(
                queries == 2 && counts.statements == 2 && counts.rows_read == expected + 1
            );
            anyhow::ensure!(counts.waves == 2, "one query batch with BEGIN, commit");
            anyhow::ensure!(counts.begins == 1 && counts.commits == 1 && counts.rollbacks == 0);
            anyhow::ensure!(counts.rows_written == 0 && counts.touched_written.is_empty());
            anyhow::ensure!(age.read().is_finite() && age.read() > 0.0);
        }
        census.reset();
        let previous_age = age.read();
        fixture
            .maintenance
            .list_due_stream_trim_markers(horizon, 0)
            .await?;
        let zero = census.snapshot();
        println!("zero-limit discovery: {zero}");
        anyhow::ensure!(zero.rows_read == 0 && zero.rows_written == 0 && zero.statements == 0);
        anyhow::ensure!(age.read() == previous_age, "zero limit must not resample");
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    let rollback = client.batch_execute("ROLLBACK").await;
    drop(client);
    let connection_result = connection_task.await;
    fixture.finish().await;
    rollback?;
    connection_result??;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
