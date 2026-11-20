//! Catalog fixture shared by SQL statement unit tests.

use std::sync::Arc;

use storage_types::{BoundTableInfo, StoredTableInfo, TableName, TableStatus, TimestampMillis};

pub(crate) fn table() -> BoundTableInfo {
    BoundTableInfo {
        binding: Default::default(),
        history_id: uuid::Uuid::from_bytes([1; 16]).into(),
        info: Arc::new(StoredTableInfo {
            table_name: TableName::new("table"),
            table_status: TableStatus::Active,
            created_at: TimestampMillis::now(),
            attribute_definitions: vec![],
            key_schema: vec![],
            max_indexers: storage_types::MaxIndexers::ZERO,
            global_secondary_indexes: None,
            table_size_bytes: 0,
            item_count: 0,
            stream_specification: None,
            stream_enabled_at: None,
            table_stream_duration: Default::default(),
            deletion_protection_enabled: false,
        }),
    }
}

/// A deadline clock that sleeps until the test moves Tokio's paused time past
/// the sleep's end. It starts no Tokio timer, so paused time never jumps.
#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    all(feature = "postgres", feature = "fault-injection")
))]
#[derive(Default)]
pub(crate) struct ManualClock {
    tick: tokio::sync::Notify,
    sleeps: std::sync::atomic::AtomicUsize,
}

#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    all(feature = "postgres", feature = "fault-injection")
))]
impl ManualClock {
    /// Sleeps started so far.
    pub(crate) fn sleeps(&self) -> usize {
        self.sleeps.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Moves paused time just past `until` and wakes every sleeper.
    pub(crate) async fn pass(&self, until: tokio::time::Instant) {
        let target = until + std::time::Duration::from_millis(1);
        let now = tokio::time::Instant::now();
        if target > now {
            tokio::time::advance(target - now).await;
        }
        self.tick.notify_waiters();
    }
}

#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    all(feature = "postgres", feature = "fault-injection")
))]
impl storage_types::Clock for ManualClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::now()
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = storage_types::StorageResult<()>> + Send + '_>,
    > {
        let end = tokio::time::Instant::now() + duration;
        self.sleeps
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            loop {
                let tick = self.tick.notified();
                let mut tick = std::pin::pin!(tick);
                tick.as_mut().enable();
                if tokio::time::Instant::now() >= end {
                    return Ok(());
                }
                tick.await;
            }
        })
    }
}
