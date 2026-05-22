use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use smallvec::SmallVec;
use tokio::time::sleep;
use tracing::{error, instrument};

use crate::{errors::WorkerError, jitter::jittered};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub lease_duration: Duration,
    pub poll_interval: Duration,
    pub batch_size: u32,
    pub jitter_percent: u8,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: default_worker_id(),
            lease_duration: Duration::from_secs(60),
            poll_interval: Duration::from_secs(5),
            batch_size: 50,
            jitter_percent: 20,
        }
    }
}

impl WorkerConfig {
    #[must_use]
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    #[must_use]
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[must_use]
    pub fn with_jitter_percent(mut self, jitter: u8) -> Self {
        self.jitter_percent = jitter.min(100);
        self
    }

    #[must_use]
    pub fn lease_until_ms(&self, now_ms: i64) -> i64 {
        now_ms.saturating_add(i64::try_from(self.lease_duration.as_millis()).unwrap_or(i64::MAX))
    }

    #[must_use]
    pub fn lease_duration_ms(&self) -> i64 {
        i64::try_from(self.lease_duration.as_millis()).unwrap_or(i64::MAX)
    }
}

#[must_use]
pub fn default_worker_id() -> String {
    let hostname = hostname::get().map_or_else(
        |_| "unknown".to_string(),
        |h| h.to_string_lossy().into_owned(),
    );
    let pid = std::process::id();
    format!("{hostname}-{pid}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseResult {
    Acquired,
    Conflict,
}

#[async_trait::async_trait]
pub trait WorkItemStore<T>: Send + Sync
where T: Send + Sync + 'static
{
    type Error: std::error::Error + Send + Sync + 'static;

    async fn query_due_items(
        &self,
        shard: Option<u8>,
        now_ms: i64,
        limit: u32,
    ) -> Result<Vec<T>, Self::Error>;

    async fn acquire_lease(
        &self,
        item: &T,
        worker_id: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> Result<LeaseResult, Self::Error>;

    async fn mark_completed(&self, item: &T) -> Result<(), Self::Error>;

    async fn mark_failed(&self, item: &T, error: &str) -> Result<(), Self::Error>;

    fn shard_count(&self) -> Option<u8> {
        None
    }
}

#[async_trait::async_trait]
pub trait WorkItemProcessor<T>: Send + Sync
where T: Send + Sync + 'static
{
    type Error: std::error::Error + Send + Sync + 'static;

    async fn process(&self, item: &T) -> Result<(), Self::Error>;
}

pub struct DistributedWorker<T, S, P>
where
    T: Send + Sync + 'static,
    S: WorkItemStore<T>,
    P: WorkItemProcessor<T>,
{
    config: WorkerConfig,
    store: Arc<S>,
    processor: Arc<P>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, S, P> DistributedWorker<T, S, P>
where
    T: Send + Sync + Debug + 'static,
    S: WorkItemStore<T> + 'static,
    P: WorkItemProcessor<T> + 'static,
{
    #[must_use]
    pub fn new(config: WorkerConfig, store: Arc<S>, processor: Arc<P>) -> Self {
        Self {
            config,
            store,
            processor,
            _marker: std::marker::PhantomData,
        }
    }

    #[instrument(skip_all, fields(feature = "jobs", worker_id = %self.config.worker_id))]
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            let run_start = Instant::now();
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                processed = self.run_once() => {
                    match processed {
                        Ok(count) => {
                            let elapsed_ms = duration_ms_u64(run_start.elapsed());
                            metrics_facade::histogram!(
                                metrics_facade::HistogramMetric::BgWorkerRunOnceMsMetric,
                                "worker_id" => self.config.worker_id.clone()
                            )
                            .record(u64_to_f64(elapsed_ms));
                            if count > 0 {
                                continue;
                            }
                        }
                        Err(e) => {
                            metrics_facade::counter!(
                                metrics_facade::CounterMetric::BgWorkerRunErrorsTotalMetric,
                                "worker_id" => self.config.worker_id.clone()
                            )
                            .increment(1);
                            error!(worker_id = %self.config.worker_id, error = %e, "background.worker.loop.failed");
                        }
                    }
                    let sleep_duration =
                        jittered(self.config.poll_interval, self.config.jitter_percent);
                    sleep(sleep_duration).await;
                }
            }
        }
    }

    #[instrument(skip_all, fields(feature = "jobs", worker_id = %self.config.worker_id))]
    pub async fn run_once(&self) -> Result<u32, WorkerError> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut processed = 0u32;
        let mut conflicts = 0u32;
        let mut errors = 0u32;

        let shards: Vec<Option<u8>> = match self.store.shard_count() {
            Some(count) => (0..count).map(Some).collect(),
            None => vec![None],
        };

        for shard in shards {
            let due_items = self
                .store
                .query_due_items(shard, now_ms, self.config.batch_size)
                .await
                .map_err(|e| WorkerError::store(e.to_string()))?;

            for item in due_items {
                match self.process_item(&item, now_ms).await {
                    Ok(true) => processed = processed.saturating_add(1),
                    Ok(false) => {
                        conflicts = conflicts.saturating_add(1);
                    }
                    Err(e) => {
                        errors = errors.saturating_add(1);
                        error!(error = %e, "background.worker.item.process.failed");
                    }
                }
            }
        }

        if processed > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::BgWorkerItemsProcessedTotalMetric,
                "worker_id" => self.config.worker_id.clone()
            )
            .increment(u64::from(processed));
        }
        if conflicts > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::BgWorkerLeaseConflictsTotalMetric,
                "worker_id" => self.config.worker_id.clone()
            )
            .increment(u64::from(conflicts));
        }
        if errors > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::BgWorkerProcessErrorsTotalMetric,
                "worker_id" => self.config.worker_id.clone()
            )
            .increment(u64::from(errors));
        }

        Ok(processed)
    }

    async fn process_item(&self, item: &T, now_ms: i64) -> Result<bool, WorkerError> {
        let lease_until = self.config.lease_until_ms(now_ms);

        let lease_result = self
            .store
            .acquire_lease(item, &self.config.worker_id, lease_until, now_ms)
            .await
            .map_err(|e| WorkerError::store(e.to_string()))?;

        if lease_result == LeaseResult::Conflict {
            return Ok(false);
        }

        match self.processor.process(item).await {
            Ok(()) => {
                self.store
                    .mark_completed(item)
                    .await
                    .map_err(|e| WorkerError::store(e.to_string()))?;
                Ok(true)
            }
            Err(e) => {
                let error_msg = e.to_string();
                self.store
                    .mark_failed(item, &error_msg)
                    .await
                    .map_err(|e| WorkerError::store(e.to_string()))?;
                Err(WorkerError::processing(error_msg))
            }
        }
    }
}

#[must_use]
pub fn is_conditional_check_failure(error: &str) -> bool {
    let lower = error.to_lowercase();
    lower.contains("conditional") || lower.contains("conditioncheck")
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[expect(clippy::cast_precision_loss)]
fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

#[derive(Debug, Clone)]
pub struct LeaseUpdateBuilder {
    pub lease_until_attr: Cow<'static, str>,
    pub leased_by_attr: Cow<'static, str>,
    pub status_attr: Cow<'static, str>,
    pub acquirable_statuses: Vec<Cow<'static, str>>,
    pub in_progress_status: Cow<'static, str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseExprNameRef<'a> {
    pub placeholder: &'static str,
    pub name: &'a str,
}

impl<'a> LeaseExprNameRef<'a> {
    #[must_use]
    pub fn new(placeholder: &'static str, name: &'a str) -> Self {
        Self { placeholder, name }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseAttrValueRef<'a> {
    String(&'a str),
    Number(i64),
}

impl LeaseAttrValueRef<'_> {
    #[must_use]
    pub fn to_owned(self) -> LeaseAttrValue {
        match self {
            Self::String(value) => LeaseAttrValue::String(value.to_string()),
            Self::Number(value) => LeaseAttrValue::Number(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseExprValueRef<'a> {
    pub placeholder: Cow<'static, str>,
    pub value: LeaseAttrValueRef<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseUpdateStatement {
    pub update_expression: String,
    pub condition_expression: String,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: HashMap<String, LeaseAttrValue>,
}

impl<'a> LeaseExprValueRef<'a> {
    #[must_use]
    pub fn new(placeholder: &'static str, value: LeaseAttrValueRef<'a>) -> Self {
        Self {
            placeholder: Cow::Borrowed(placeholder),
            value,
        }
    }

    #[must_use]
    pub fn with_placeholder(placeholder: Cow<'static, str>, value: LeaseAttrValueRef<'a>) -> Self {
        Self { placeholder, value }
    }
}

const STATUS_PLACEHOLDERS: [&str; 8] = [
    ":status0", ":status1", ":status2", ":status3", ":status4", ":status5", ":status6", ":status7",
];

fn status_placeholder(index: usize) -> Cow<'static, str> {
    if index < STATUS_PLACEHOLDERS.len() {
        Cow::Borrowed(STATUS_PLACEHOLDERS[index])
    } else {
        Cow::Owned(format!(":status{index}"))
    }
}

fn lease_expr_name_refs_to_map(values: &[LeaseExprNameRef<'_>]) -> HashMap<String, String> {
    let mut out = HashMap::with_capacity(values.len());
    for pair in values {
        out.insert(pair.placeholder.to_string(), pair.name.to_string());
    }
    out
}

fn lease_expr_value_refs_to_map(
    values: &[LeaseExprValueRef<'_>],
) -> HashMap<String, LeaseAttrValue> {
    let mut out = HashMap::with_capacity(values.len());
    for pair in values {
        out.insert(pair.placeholder.to_string(), pair.value.to_owned());
    }
    out
}

impl Default for LeaseUpdateBuilder {
    fn default() -> Self {
        Self {
            lease_until_attr: Cow::Borrowed("lease_until_ms"),
            leased_by_attr: Cow::Borrowed("leased_by"),
            status_attr: Cow::Borrowed("status"),
            acquirable_statuses: vec![Cow::Borrowed("queued"), Cow::Borrowed("in_flight")],
            in_progress_status: Cow::Borrowed("in_flight"),
        }
    }
}

impl LeaseUpdateBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_lease_until_attr(mut self, attr: impl Into<Cow<'static, str>>) -> Self {
        self.lease_until_attr = attr.into();
        self
    }

    #[must_use]
    pub fn with_leased_by_attr(mut self, attr: impl Into<Cow<'static, str>>) -> Self {
        self.leased_by_attr = attr.into();
        self
    }

    #[must_use]
    pub fn with_status_attr(mut self, attr: impl Into<Cow<'static, str>>) -> Self {
        self.status_attr = attr.into();
        self
    }

    #[must_use]
    pub fn with_acquirable_statuses<I, S>(mut self, statuses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Cow<'static, str>>,
    {
        self.acquirable_statuses = statuses.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_in_progress_status(mut self, status: impl Into<Cow<'static, str>>) -> Self {
        self.in_progress_status = status.into();
        self
    }

    #[must_use]
    pub fn build_update_statement(
        &self,
        worker_id: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> LeaseUpdateStatement {
        LeaseUpdateStatement {
            update_expression: self.update_expression(),
            condition_expression: self.condition_expression(),
            expression_attribute_names: self.expression_attribute_names(),
            expression_attribute_values: self.expression_attribute_values(
                worker_id,
                lease_until_ms,
                now_ms,
            ),
        }
    }

    fn update_expression(&self) -> String {
        format!(
            "SET {} = :lease, {} = :worker, #status = :in_progress",
            self.lease_until_attr, self.leased_by_attr
        )
    }

    fn condition_expression(&self) -> String {
        let mut status_conditions = String::new();
        for (i, _) in self.acquirable_statuses.iter().enumerate() {
            if i > 0 {
                status_conditions.push_str(" OR ");
            }
            let _ = std::fmt::Write::write_fmt(
                &mut status_conditions,
                format_args!("#status = :status{i}"),
            );
        }

        format!(
            "(attribute_not_exists({}) OR {} < :now) AND ({})",
            self.lease_until_attr, self.lease_until_attr, status_conditions
        )
    }

    fn expression_attribute_name_refs(&self) -> [LeaseExprNameRef<'_>; 1] {
        [LeaseExprNameRef::new("#status", self.status_attr.as_ref())]
    }

    fn expression_attribute_value_refs<'a>(
        &'a self,
        worker_id: &'a str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> SmallVec<[LeaseExprValueRef<'a>; 8]> {
        let mut values = SmallVec::with_capacity(self.acquirable_statuses.len().saturating_add(4));
        values.push(LeaseExprValueRef::new(
            ":lease",
            LeaseAttrValueRef::Number(lease_until_ms),
        ));
        values.push(LeaseExprValueRef::new(
            ":worker",
            LeaseAttrValueRef::String(worker_id),
        ));
        values.push(LeaseExprValueRef::new(
            ":now",
            LeaseAttrValueRef::Number(now_ms),
        ));
        values.push(LeaseExprValueRef::new(
            ":in_progress",
            LeaseAttrValueRef::String(self.in_progress_status.as_ref()),
        ));

        for (i, status) in self.acquirable_statuses.iter().enumerate() {
            values.push(LeaseExprValueRef::with_placeholder(
                status_placeholder(i),
                LeaseAttrValueRef::String(status.as_ref()),
            ));
        }

        values
    }

    fn expression_attribute_names(&self) -> HashMap<String, String> {
        lease_expr_name_refs_to_map(&self.expression_attribute_name_refs())
    }

    fn expression_attribute_values(
        &self,
        worker_id: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> HashMap<String, LeaseAttrValue> {
        lease_expr_value_refs_to_map(
            self.expression_attribute_value_refs(worker_id, lease_until_ms, now_ms)
                .as_slice(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseAttrValue {
    String(String),
    Number(i64),
}
