//! One real-provider probe for known aborts and lost durable acknowledgements.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use futures::future::BoxFuture;
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NativeCommitBoundary {
    BeforeCommit,
    #[default]
    AfterDurableAcknowledgement,
}

#[derive(Default)]
pub struct NativeCommitFault {
    boundary: NativeCommitBoundary,
    remaining: AtomicU64,
    observations: AtomicU64,
    interruptions: AtomicU64,
    sustained: AtomicBool,
}

impl NativeCommitFault {
    pub fn new(boundary: NativeCommitBoundary) -> Self {
        Self {
            boundary,
            ..Self::default()
        }
    }

    /// Keep rejecting real native settlement after the selected first fault.
    /// Selection still occurs through arm_publication; setup stays unaffected.
    pub fn sustain_faults(&self) {
        self.sustained.store(true, Ordering::Release);
    }

    pub fn release_faults(&self) {
        self.sustained.store(false, Ordering::Release);
        self.remaining.store(0, Ordering::Release);
    }

    pub fn arm(&self) {
        self.interruptions.store(0, Ordering::Release);
        self.remaining.store(1, Ordering::Release);
    }

    /// Some engines settle read-only inspections at their native commit
    /// boundary. Calibrate that existing inspection, then interrupt the
    /// publication attempt. Fixtures have one exclusive proposer while this
    /// probe is armed.
    pub async fn arm_publication<P: DynamoPort + 'static>(&self, port: &P) -> StorageResult<()> {
        let before = self.observations.load(Ordering::Acquire);
        port.with_dynamo_attempt(AttemptMode::ReadOnly, |_| Box::pin(async { Ok(()) }))
            .await?;
        let inspections = self.observations.load(Ordering::Acquire) - before;
        assert!(inspections <= 1);
        self.interruptions.store(0, Ordering::Release);
        self.remaining.store(inspections + 1, Ordering::Release);
        Ok(())
    }

    pub fn was_observed(&self) -> bool {
        self.interruptions.load(Ordering::Acquire) != 0
    }

    fn interrupt_commit(&self, boundary: NativeCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary != self.boundary {
                return Ok(());
            }
            self.observations.fetch_add(1, Ordering::AcqRel);
            if self
                .remaining
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                == Ok(1)
                || (self.sustained.load(Ordering::Acquire)
                    && self.interruptions.load(Ordering::Acquire) != 0)
            {
                self.interruptions.fetch_add(1, Ordering::AcqRel);
                return Err(StorageError::internal(
                    "test interrupted native publication settlement",
                ));
            }
            Ok(())
        })
    }
}

#[cfg(any(
    feature = "native-sqlite",
    feature = "native-postgres",
    feature = "native-turso"
))]
impl sql_backend::SqlFaultProbe for NativeCommitFault {
    fn fail(&self, _call: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
    ) -> BoxFuture<'_, StorageResult<()>> {
        self.interrupt_commit(match boundary {
            sql_backend::SqlCommitBoundary::BeforeCommit => NativeCommitBoundary::BeforeCommit,
            sql_backend::SqlCommitBoundary::AfterDurableAcknowledgement => {
                NativeCommitBoundary::AfterDurableAcknowledgement
            }
        })
    }
}

#[cfg(any(feature = "native-rocksdb", feature = "native-foundationdb"))]
impl kv_backend::KvFaultProbe for NativeCommitFault {
    fn fail(&self, _call: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
    ) -> BoxFuture<'_, StorageResult<()>> {
        self.interrupt_commit(match boundary {
            kv_backend::KvCommitBoundary::BeforeCommit => NativeCommitBoundary::BeforeCommit,
            kv_backend::KvCommitBoundary::AfterDurableAcknowledgement => {
                NativeCommitBoundary::AfterDurableAcknowledgement
            }
        })
    }
}
