use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use storage_types::{
    AttributeValue, IndexName, QueryTableRequest, StorageError, StorageResult, TimestampMillis,
    from_hashmap,
};
use tokio::sync::Mutex;

use crate::{
    namespace_routing::{
        CutoverEvent, CutoverEventSerde, CutoverEventStatus, NamespaceRouteResolver,
        is_missing_sys_namespaces_table_error, is_retryable_cutover_watcher_error,
    },
    newtypes::DatabaseTrait,
    tables::Tables,
};

const CUTOVER_QUERY: &str = "gsi3pk = :pk AND gsi3sk BETWEEN :from AND :to";
const CUTOVER_GSI_PK: &str = "CUTOVER";
const CUTOVER_POLL_INTERVAL: Duration = Duration::from_secs(60 * 5);
const CUTOVER_LOOKAHEAD: Duration = Duration::from_secs(60 * 10);
const CUTOVER_OVERLAP: Duration = CUTOVER_POLL_INTERVAL;

pub struct CutoverWatcher {
    resolver: Arc<NamespaceRouteResolver>,
    control_plane: Arc<dyn DatabaseTrait>,
    poll_interval: Duration,
    lookahead: Duration,
    overlap: Duration,
    scheduled: Arc<Mutex<HashSet<String>>>,
}

impl CutoverWatcher {
    #[must_use]
    pub fn new(
        resolver: Arc<NamespaceRouteResolver>,
        control_plane: Arc<dyn DatabaseTrait>,
    ) -> Self {
        Self {
            resolver,
            control_plane,
            poll_interval: CUTOVER_POLL_INTERVAL,
            lookahead: CUTOVER_LOOKAHEAD,
            overlap: CUTOVER_OVERLAP,
            scheduled: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self: Arc<Self>) {
        loop {
            if let Err(error) = self.poll_once().await {
                if is_retryable_cutover_watcher_error(&error) {
                    tracing::debug!(error = %error, "namespace cutover watcher poll failed");
                } else {
                    tracing::warn!(error = %error, "namespace cutover watcher poll failed");
                }
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    pub async fn poll_once(&self) -> StorageResult<()> {
        let now_ms = TimestampMillis::now().timestamp_millis();
        let from = TimestampMillis::from_timestamp(now_ms - self.overlap.as_millis() as i64);
        let to = TimestampMillis::from_timestamp(now_ms + self.lookahead.as_millis() as i64);
        let events = query_cutover_events(Arc::clone(&self.control_plane), from, to).await?;
        for event in events {
            self.schedule_or_apply(event).await;
        }
        Ok(())
    }

    async fn schedule_or_apply(&self, event: CutoverEvent) {
        if matches!(
            event.status,
            CutoverEventStatus::Canceled | CutoverEventStatus::Failed
        ) {
            let key = event_key(&event);
            self.scheduled.lock().await.remove(&key);
            let _ = self.resolver.apply_cutover_event(&event).await;
            return;
        }

        let now_ms = TimestampMillis::now().timestamp_millis();
        let effective_ms = event.effective_at_ms.timestamp_millis();
        if effective_ms <= now_ms {
            tracing::warn!(
                namespace = %event.namespace,
                migration_id = %event.migration_id,
                effective_at_ms = effective_ms,
                now_ms,
                "cutover event discovered after effective timestamp, applying immediately"
            );
            let _ = self.resolver.apply_cutover_event(&event).await;
            return;
        }

        let key = event_key(&event);
        {
            let mut scheduled = self.scheduled.lock().await;
            if !scheduled.insert(key.clone()) {
                return;
            }
        }

        let delay_ms = (effective_ms - now_ms) as u64;
        let resolver = Arc::clone(&self.resolver);
        let scheduled = Arc::clone(&self.scheduled);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            if let Err(error) = resolver.apply_cutover_event(&event).await {
                tracing::warn!(
                    error = %error,
                    namespace = %event.namespace,
                    migration_id = %event.migration_id,
                    "cutover application failed"
                );
            }
            scheduled.lock().await.remove(&key);
        });
    }
}

fn event_key(event: &CutoverEvent) -> String {
    format!(
        "{}#{}#{}",
        event.namespace, event.migration_id, event.effective_at_ms
    )
}

async fn query_cutover_events(
    control_plane: Arc<dyn DatabaseTrait>,
    from: TimestampMillis,
    to: TimestampMillis,
) -> StorageResult<Vec<CutoverEvent>> {
    let mut events = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let from_value = format!("{:020}#", from.timestamp_millis());
        let to_value = format!("{:020}~", to.timestamp_millis());
        let request = QueryTableRequest {
            table_name: Tables::sys_namespaces(),
            index_name: Some(IndexName::new("gsi3")),
            key_condition_expression: CUTOVER_QUERY.to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([
                (
                    ":pk".to_string(),
                    AttributeValue::S(CUTOVER_GSI_PK.to_string()),
                ),
                (":from".to_string(), AttributeValue::S(from_value.clone())),
                (":to".to_string(), AttributeValue::S(to_value.clone())),
            ])),
            projection_expression: None,
            limit: Some(1_000),
            exclusive_start_key: next.clone(),
            scan_index_forward: Some(true),
            consistent_read: false,
        };
        let (items, token) = match control_plane.query_table(&request).await {
            Ok(result) => result,
            Err(error) if is_missing_sys_namespaces_table_error(&error) => return Ok(events),
            Err(error) => return Err(error),
        };
        for item in items {
            let map = item.into_attribute_map()?;
            let decoded: CutoverEventSerde =
                from_hashmap(map).map_err(|error| StorageError::internal(&error.to_string()))?;
            events.push(CutoverEvent {
                namespace: decoded.namespace,
                migration_id: decoded.migration_id,
                old_loc: decoded.old_loc,
                new_loc: decoded.new_loc,
                effective_at_ms: decoded.effective_at_ms,
                status: decoded.status.into(),
            });
        }
        if token.is_none() {
            break;
        }
        next = token;
    }
    Ok(events)
}
