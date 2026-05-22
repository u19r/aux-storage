use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use storage_types::{StorageError, TimestampMillis};
use tokio::sync::Mutex;
use tracing_test::traced_test;

use crate::{
    BackfillBatchOutcome, BackfillConfig, BackfillCoordinator, BackfillDriver, BackfillResult,
    BackfillState, BackfillStatus, GsiBackfillDescriptor,
};

fn global_log_lines() -> Vec<String> {
    let buf = tracing_test::internal::global_buf()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let logs = String::from_utf8_lossy(&buf);
    logs.lines().map(|line| line.to_string()).collect()
}

#[derive(Clone)]
struct RecordingDriver {
    descriptor: GsiBackfillDescriptor,
    state: Arc<Mutex<BackfillState>>,
    batches: Arc<Mutex<VecDeque<BackfillBatchOutcome>>>,
    execute_calls: Arc<AtomicUsize>,
}

impl RecordingDriver {
    fn new(
        descriptor: GsiBackfillDescriptor,
        initial_state: BackfillState,
        batches: VecDeque<BackfillBatchOutcome>,
    ) -> Self {
        Self {
            descriptor,
            state: Arc::new(Mutex::new(initial_state)),
            batches: Arc::new(Mutex::new(batches)),
            execute_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn state(&self) -> BackfillState {
        self.state.lock().await.clone()
    }

    async fn update_state<F>(&self, mut update: F)
    where F: FnMut(&mut BackfillState) {
        let mut guard = self.state.lock().await;
        update(&mut guard);
    }
}

#[async_trait::async_trait]
impl BackfillDriver for RecordingDriver {
    async fn enumerate_states(
        &self,
    ) -> Result<Vec<(GsiBackfillDescriptor, BackfillState)>, StorageError> {
        let state = self.state.lock().await.clone();
        Ok(vec![(self.descriptor.clone(), state)])
    }

    async fn persist_state(
        &self,
        _descriptor: &GsiBackfillDescriptor,
        state: &BackfillState,
    ) -> Result<(), StorageError> {
        *self.state.lock().await = state.clone();
        Ok(())
    }

    async fn reload_state(
        &self,
        _descriptor: &GsiBackfillDescriptor,
    ) -> Result<Option<BackfillState>, StorageError> {
        Ok(Some(self.state.lock().await.clone()))
    }

    async fn execute_batch(
        &self,
        _descriptor: &GsiBackfillDescriptor,
        _state: &BackfillState,
        _batch_size: usize,
    ) -> Result<BackfillBatchOutcome, StorageError> {
        self.execute_calls.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.batches.lock().await;
        Ok(guard.pop_front().unwrap_or_default())
    }
}

#[tokio::test]
#[traced_test]
async fn backfill_coordinator_resumes_from_checkpoint() {
    let descriptor = GsiBackfillDescriptor::new("tenant#t", "__ttl#gsi");
    let now = TimestampMillis::now();
    let initial_state = BackfillState {
        status: BackfillStatus::Pending,
        scan_lek: None,
        captured_stream_tail: None,
        lock: None,
        checkpoint: None,
        created_at: now,
        updated_at: now,
    };

    let batches = VecDeque::from([
        BackfillBatchOutcome {
            items_processed: 3,
            next_token: Some("cursor-1".to_string()),
            done: false,
        },
        BackfillBatchOutcome {
            items_processed: 2,
            next_token: None,
            done: true,
        },
    ]);

    let driver = Arc::new(RecordingDriver::new(
        descriptor.clone(),
        initial_state,
        batches,
    ));
    let config = BackfillConfig::default();
    let coordinator = BackfillCoordinator::new(Arc::clone(&driver), config.clone());

    let first_run = coordinator.run_once().await.unwrap();
    assert_eq!(first_run, BackfillResult::DidWork);

    let state_after_first = driver.state().await;
    assert_eq!(state_after_first.scan_lek.as_deref(), Some("cursor-1"));
    assert_eq!(state_after_first.status, BackfillStatus::Backfilling);
    assert!(state_after_first.lock.is_some());

    // Simulate crash by expiring lock
    driver
        .update_state(|state| {
            if let Some(lock) = state.lock.as_mut() {
                lock.expires_at = TimestampMillis::now() - 1;
            }
        })
        .await;

    let resumed = BackfillCoordinator::new(Arc::clone(&driver), config);
    let second_run = resumed.run_once().await.unwrap();
    assert_eq!(second_run, BackfillResult::DidWork);

    let final_state = driver.state().await;
    assert_eq!(final_state.status, BackfillStatus::Done);
    assert!(final_state.scan_lek.is_none());
    assert!(final_state.lock.is_none());

    assert_eq!(driver.execute_calls.load(Ordering::Relaxed), 2);
    let lines = global_log_lines();
    let table_fragment = "table=tenant#t";
    let has_batch = lines
        .iter()
        .any(|line| line.contains("backfill.batch") && line.contains(table_fragment));
    let has_lock = lines.iter().any(|line| {
        line.contains("backfill.lock.expired_claimed") && line.contains(table_fragment)
    });
    let has_acquired = lines
        .iter()
        .any(|line| line.contains("backfill.lock.acquired") && line.contains(table_fragment));
    let has_schedule = lines
        .iter()
        .any(|line| line.contains("backfill.schedule") && line.contains(table_fragment));
    assert!(has_batch, "missing backfill.batch trace");
    assert!(has_lock, "missing backfill.lock.expired_claimed trace");
    assert!(has_acquired, "missing backfill.lock.acquired trace");
    assert!(has_schedule, "missing backfill.schedule trace");
}
