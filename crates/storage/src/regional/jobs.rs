use std::sync::Arc;

use bg_jobs_tokio::JobRuntime;
use bytes::Bytes;
use scheduled_bg_jobs::{EnqueueOptions, FairnessClass, JobHandle, JobId, ValidationError};
use serde::{Deserialize, Serialize};
use storage_types::{PairId, StorageError, StorageResult, TableName};

use crate::ScheduledJobStore;

pub(crate) const REGIONAL_PAIR_JOB_KIND: &str = "regional-pair";
pub(crate) const REGIONAL_QUEUE: &str = "regional";
/// Fixed namespace of pair job ids (uuid v5 of the pair bytes).
const PAIR_JOB_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x6a1f_2c55_93e4_4d0b_9b7e_52c1_0f3a_8d21);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PairJobPayload {
    pub table: TableName,
    pub pair: PairId,
}

/// Job state between turns. REG-10/REG-11 add fields with `#[serde(default)]`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct PairJobCheckpoint {
    #[serde(default)]
    pub next_lane: u32,
    #[serde(default)]
    pub prepared: bool,
    #[serde(default)]
    pub scan_after: Option<String>,
    #[serde(default)]
    pub scan_done: bool,
    #[serde(default)]
    pub sweep_after: Option<Vec<u8>>,
    #[serde(default)]
    pub swept: bool,
    /// Leaving region: the history heads frozen when the first Finish was sent.
    #[serde(default)]
    pub leave_last: Option<Vec<regional_replication::message::LineageHead>>,
    #[serde(default)]
    pub lag_alerting: bool,
    /// Consecutive failed peer calls (transport error or `Unavailable`); drives
    /// the retry backoff.
    #[serde(default)]
    pub peer_failures: u32,
}

/// Payload codec of the pair job: the JSON of [`PairJobPayload`], the bytes the
/// handler decodes (unchanged since REG-9).
pub(crate) fn encode_pair_job_payload(payload: &PairJobPayload) -> Result<Bytes, ValidationError> {
    serde_json::to_vec(payload)
        .map(Bytes::from)
        .map_err(|_| ValidationError::InvalidValue {
            field: "regional pair job payload",
        })
}

/// The pair-job capability of one connection: the handle its catalog
/// registered and the runtime built from that catalog. Complete from
/// construction; its one operation ensures a stable seed.
#[derive(Clone)]
pub(crate) struct PairJobs {
    pub(crate) handle: JobHandle<PairJobPayload>,
    pub(crate) runtime: Arc<JobRuntime<ScheduledJobStore>>,
}

impl PairJobs {
    pub(crate) fn new(
        handle: JobHandle<PairJobPayload>,
        runtime: Arc<JobRuntime<ScheduledJobStore>>,
    ) -> Self {
        Self { handle, runtime }
    }

    pub(crate) fn job_id(pair: PairId) -> JobId {
        JobId::from(uuid::Uuid::new_v5(&PAIR_JOB_NAMESPACE, pair.as_bytes()).into_bytes())
    }

    /// Idempotent: the job id is a function of the pair, and a stable seed
    /// keeps an existing job with this id and payload whatever its schedule. (A
    /// plain enqueue compares the schedule too and fails every repeat with
    /// `JobIdConflict`: run b4.)
    pub(crate) async fn ensure(&self, table: &TableName, pair: PairId) -> StorageResult<()> {
        self.runtime
            .ensure_seed_typed(
                &self.handle,
                PairJobPayload {
                    table: table.clone(),
                    pair,
                },
                EnqueueOptions::new(FairnessClass::low_volume()).job_id(Self::job_id(pair)),
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                StorageError::internal(&format!("regional job enqueue failed: {error:?}"))
            })
    }
}
