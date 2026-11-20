#![allow(non_snake_case)]

//! Replays `quint/durable_ack.qnt` (module `durable_ack_impl`) against
//! `DurabilityBatch`. The driver owns a commit ledger (the native commit
//! order, also the native version for the second-process clause) and a
//! gated fake barrier, so the model decides when each barrier begins and
//! ends. After every step the attempt phases, answers, barrier counts,
//! registrations and the stopped state of the real type must equal the model.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{bail, ensure};
use quint_connect::{Config, Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{AttemptDurability, StorageEnum, StorageError, StorageResult};
use tokio::{runtime::Runtime, task::JoinHandle};

use crate::{
    durability::{
        AttemptCompletion, CommitRecorder, DurabilityBatch, NativeVersionMemo, PendingDurableWrite,
    },
    durability_tests::{FakeBarrier, quiesce},
};

/// The model state of `durable_ack_impl` (with a version probe, SQLite) and
/// `durable_ack_impl_local` (one process, RocksDB), and their action records.
const PROBE_STATE: &[&str] = &["durable_ack_impl::durable_ack::s"];
const PROBE_ACT: &[&str] = &["durable_ack_impl::durable_ack::s", "act"];
const LOCAL_STATE: &[&str] = &["durable_ack_impl_local::durable_ack::s"];
const LOCAL_ACT: &[&str] = &["durable_ack_impl_local::durable_ack::s", "act"];

/// The compared projection of the model state `s`.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(from = "RawState")]
struct DurabilityModelState {
    /// Per attempt: kind, phase, and whether it answered with data (success,
    /// or an error that reveals what it observed).
    ops: BTreeMap<i64, (String, String, bool)>,
    barrier_starts: i64,
    barrier_completions: i64,
    stopped: bool,
    /// Registrations and holds; not compared once the store stopped.
    pending: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawState {
    ops: BTreeMap<i64, RawOp>,
    holds: Vec<serde::de::IgnoredAny>,
    seq_holds: Vec<serde::de::IgnoredAny>,
    barrier_holds: Vec<serde::de::IgnoredAny>,
    epoch: i64,
    barrier_starts: i64,
    barrier_completions: i64,
    stopped: bool,
}

#[derive(Deserialize)]
struct RawOp {
    kind: String,
    phase: String,
    success: bool,
    revealed: bool,
    registered: bool,
    epoch: i64,
}

impl From<RawState> for DurabilityModelState {
    fn from(raw: RawState) -> Self {
        let registered = raw
            .ops
            .values()
            .filter(|op| op.registered && op.epoch == raw.epoch)
            .count();
        let holds = raw.holds.len() + raw.seq_holds.len() + raw.barrier_holds.len();
        let pending = i64::try_from(registered + holds).unwrap_or(i64::MAX);
        Self {
            ops: raw
                .ops
                .into_iter()
                .map(|(id, op)| {
                    let answered = op.phase == "responded" && (op.success || op.revealed);
                    (id, (op.kind, op.phase, answered))
                })
                .collect(),
            barrier_starts: raw.barrier_starts,
            barrier_completions: raw.barrier_completions,
            stopped: raw.stopped,
            pending: (!raw.stopped).then_some(pending),
        }
    }
}

impl<const PROBE: bool> State<DurabilityDriver<PROBE>> for DurabilityModelState {
    fn from_driver(driver: &DurabilityDriver<PROBE>) -> Result<Self> {
        let stats = driver.batch.stats();
        Ok(Self {
            ops: driver
                .ops
                .iter()
                .map(|(id, op)| {
                    let answered = op.phase == "responded" && (op.success || op.revealed);
                    (*id, (op.kind.clone(), op.phase.clone(), answered))
                })
                .collect(),
            barrier_starts: driver.retired_starts + to_i64(stats.barriers_started)
                - driver.startup_offset,
            barrier_completions: driver.retired_completions + to_i64(stats.barriers_completed)
                - driver.startup_offset,
            stopped: stats.stopped,
            pending: (!stats.stopped).then(|| i64::try_from(stats.pending).unwrap_or(i64::MAX)),
        })
    }
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[derive(Default)]
struct DriverOp {
    kind: String,
    phase: String,
    success: bool,
    revealed: bool,
    abandoned: bool,
    /// The ledger length when it observed: its connection's native version.
    observed_len: u64,
    write: Option<PendingDurableWrite>,
    /// The writer thread's side of the registration.
    recorder: Option<CommitRecorder>,
    result: Option<StorageResult<()>>,
    task: Option<JoinHandle<StorageResult<()>>>,
}

struct DurabilityDriver<const PROBE: bool> {
    runtime: Runtime,
    batch: Arc<DurabilityBatch>,
    fake: Arc<FakeBarrier>,
    /// Native commit order: one entry per applied commit, own or external.
    ledger: Arc<AtomicU64>,
    /// Longest ledger prefix a completed, trustworthy barrier synced.
    synced: u64,
    capture: u64,
    poisoned: bool,
    ops: BTreeMap<i64, DriverOp>,
    /// The one connection memo that model readers with `viaMemo` use.
    memo: NativeVersionMemo,
    retired_starts: i64,
    retired_completions: i64,
    /// The first batch's startup barrier runs before the model's `init`.
    startup_offset: i64,
}

impl<const PROBE: bool> Default for DurabilityDriver<PROBE> {
    fn default() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("test runtime");
        let ledger = Arc::new(AtomicU64::new(0));
        let fake = FakeBarrier::new(Arc::clone(&ledger));
        let batch = runtime.block_on(async {
            let batch = Arc::new(Self::new_batch(&fake));
            fake.allow_start(1);
            fake.allow_finish(1);
            quiesce().await;
            batch
        });
        Self {
            runtime,
            batch,
            fake,
            ledger,
            synced: 0,
            capture: 0,
            poisoned: false,
            ops: (1..=4)
                .map(|id| {
                    (
                        id,
                        DriverOp {
                            kind: "none".into(),
                            phase: "none".into(),
                            ..DriverOp::default()
                        },
                    )
                })
                .collect(),
            memo: NativeVersionMemo::default(),
            retired_starts: 0,
            retired_completions: 0,
            startup_offset: 1,
        }
    }
}

impl<const PROBE: bool> Driver for DurabilityDriver<PROBE> {
    type State = DurabilityModelState;

    fn config() -> Config {
        Config {
            state: if PROBE { PROBE_STATE } else { LOCAL_STATE },
            nondet: if PROBE { PROBE_ACT } else { LOCAL_ACT },
        }
    }

    fn step(&mut self, step: &Step) -> Result {
        self.dispatch(step)?;
        self.observe_answers();
        Ok(())
    }
}

impl<const PROBE: bool> DurabilityDriver<PROBE> {
    fn dispatch(&mut self, step: &Step) -> Result {
        switch!(step {
            DoInit => {
                *self = Self::default();
                ensure!(self.batch.stats().barriers_completed == 1, "startup barrier");
            },
            DoStart(id: i64, kind: String) => self.start(id, kind)?,
            DoObserve(id: i64) => {
                let observed_len = self.ledger.load(Ordering::SeqCst);
                let op = self.op(id)?;
                op.phase = "observed".into();
                op.observed_len = observed_len;
            },
            DoStage(id: i64) => self.stage(id)?,
            DoCommit(id: i64, outcome: String) => self.commit(id, &outcome)?,
            DoRecord(id: i64) => self.record(id)?,
            DoSettle(id: i64) => self.settle(id)?,
            DoComplete(id: i64, dataError: bool, viaMemo: bool, probeOk: bool) => {
                self.complete(id, dataError, viaMemo, probeOk)?
            },
            DoAbort(id: i64) => self.abort(id)?,
            DoClose => self.batch.close(),
            DoBarrierStart(probeOk: bool) => self.barrier_start(probeOk)?,
            DoBarrierComplete => self.barrier_finish(false)?,
            DoBarrierFail => self.barrier_finish(true)?,
            DoPowerCrash => self.restart(true),
            DoProcessRestart => self.restart(false),
            DoExternalCommit => {
                self.ledger.fetch_add(1, Ordering::SeqCst);
            },
        })
    }

    fn new_batch(fake: &Arc<FakeBarrier>) -> DurabilityBatch {
        if PROBE {
            fake.batch_with_probe()
        } else {
            fake.batch()
        }
    }

    fn op(&mut self, id: i64) -> Result<&mut DriverOp> {
        match self.ops.get_mut(&id) {
            Some(op) => Ok(op),
            None => bail!("unknown attempt {id}"),
        }
    }

    fn start(&mut self, id: i64, kind: String) -> Result {
        ensure!(
            self.batch.check_available().is_ok(),
            "model started an attempt on a stopped store"
        );
        let op = self.op(id)?;
        op.kind = kind;
        op.phase = "active".into();
        Ok(())
    }

    fn stage(&mut self, id: i64) -> Result {
        let write = self.batch.begin_write();
        let op = self.op(id)?;
        op.recorder = Some(write.recorder());
        op.write = Some(write);
        op.phase = "staged".into();
        Ok(())
    }

    fn commit(&mut self, id: i64, outcome: &str) -> Result {
        if matches!(outcome, "ok" | "unknownApplied" | "postCommitError") {
            self.ledger.fetch_add(1, Ordering::SeqCst);
        }
        let batch = Arc::clone(&self.batch);
        let runtime = self.runtime.handle().clone();
        let op = self.op(id)?;
        op.success = outcome == "ok";
        match outcome {
            // Visible now; the writer thread records the ticket in `Record`.
            "ok" | "postCommitError" => {
                op.result = Some(if outcome == "ok" {
                    Ok(())
                } else {
                    Err(StorageError::internal("worker join failed"))
                });
                op.phase = "visible".into();
            }
            // The writer thread is done without a ticket.
            "unknownApplied" | "unknownLost" | "rollback" => {
                op.recorder = None;
                if op.abandoned {
                    op.phase = "dropped".into();
                } else if outcome == "rollback" {
                    // A known abort settles at once and leaves nothing.
                    let completion =
                        AttemptCompletion::new(AttemptDurability::Durable, op.write.take());
                    op.task = Some(runtime.spawn(async move {
                        batch
                            .settle(
                                Err(
                                    StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                                        storage_types::ConflictCause::Logical("rolled back"),
                                    ))
                                    .into(),
                                ),
                                completion,
                            )
                            .await
                    }));
                    op.phase = "waiting".into();
                } else {
                    op.result = Some(Err(StorageError::commit_unknown("commit outcome unknown")));
                    op.phase = "committed".into();
                }
            }
            other => bail!("unknown outcome {other}"),
        }
        Ok(())
    }

    fn record(&mut self, id: i64) -> Result {
        let op = self.op(id)?;
        let Some(recorder) = op.recorder.take() else {
            bail!("attempt {id} records without a writer thread");
        };
        recorder.record();
        drop(recorder);
        op.phase = if op.abandoned { "dropped" } else { "committed" }.into();
        Ok(())
    }

    /// The attempt's task ends without settling; its writer thread goes on.
    fn abort(&mut self, id: i64) -> Result {
        let op = self.op(id)?;
        op.abandoned = true;
        if let Some(task) = op.task.take() {
            task.abort();
        }
        op.write = None;
        op.result = None;
        if op.phase == "committed" || op.phase == "waiting" {
            op.phase = "dropped".into();
        }
        Ok(())
    }

    fn settle(&mut self, id: i64) -> Result {
        let batch = Arc::clone(&self.batch);
        let runtime = self.runtime.handle().clone();
        let op = self.op(id)?;
        let durability = if op.kind == "relaxed" {
            AttemptDurability::Relaxed
        } else {
            AttemptDurability::Durable
        };
        let result = op.result.take().unwrap_or(Ok(()));
        let completion = AttemptCompletion::new(durability, op.write.take());
        op.task = Some(runtime.spawn(async move { batch.settle(result, completion).await }));
        op.phase = "waiting".into();
        Ok(())
    }

    fn complete(&mut self, id: i64, data_error: bool, via_memo: bool, probe_ok: bool) -> Result {
        let batch = Arc::clone(&self.batch);
        let runtime = self.runtime.handle().clone();
        let memo = self.memo.clone();
        let op = self.op(id)?;
        op.success = !data_error;
        op.revealed = data_error;
        let result = if data_error {
            Err(StorageEnum::ConditionalCheckFailed.into())
        } else {
            Ok(())
        };
        let observed = (PROBE && via_memo).then(|| memo.observe(op.observed_len));
        op.task = Some(runtime.spawn(async move {
            batch
                .settle(
                    result,
                    AttemptCompletion::new(AttemptDurability::Durable, None)
                        .with_observed(observed),
                )
                .await
        }));
        op.phase = "waiting".into();
        if !probe_ok {
            self.fake.probe_fails.store(true, Ordering::SeqCst);
            self.runtime.block_on(quiesce());
            self.fake.probe_fails.store(false, Ordering::SeqCst);
        }
        Ok(())
    }

    /// The real sync task must already be in the barrier (or in its bounded
    /// window before it); the model decides the moment the sync begins.
    fn barrier_start(&mut self, probe_ok: bool) -> Result {
        let begun = self.fake.begun();
        let invoked = self.runtime.block_on(async {
            for _ in 0..4 {
                quiesce().await;
                if self.fake.invoked() > begun {
                    return true;
                }
                tokio::time::advance(Duration::from_millis(1)).await;
            }
            false
        });
        ensure!(
            invoked,
            "the model starts a barrier that the batch does not need"
        );
        self.capture = self.ledger.load(Ordering::SeqCst);
        self.fake.probe_fails.store(!probe_ok, Ordering::SeqCst);
        self.fake.allow_start(1);
        self.runtime.block_on(quiesce());
        self.fake.probe_fails.store(false, Ordering::SeqCst);
        ensure!(self.fake.begun() == begun + 1, "barrier did not begin");
        Ok(())
    }

    fn barrier_finish(&mut self, fail: bool) -> Result {
        if fail {
            self.fake.fail_from_now();
            self.poisoned = true;
        } else if !self.poisoned {
            self.synced = self.synced.max(self.capture);
        }
        self.fake.allow_finish(1);
        Ok(())
    }

    fn restart(&mut self, power_loss: bool) {
        for op in self.ops.values_mut() {
            if !matches!(op.phase.as_str(), "none" | "responded" | "dropped") {
                if let Some(task) = op.task.take() {
                    task.abort();
                }
                op.write = None;
                op.recorder = None;
                op.result = None;
                op.phase = "dead".into();
            }
        }
        if power_loss {
            self.ledger.store(self.synced, Ordering::SeqCst);
        }
        let stats = self.batch.stats();
        self.retired_starts += to_i64(stats.barriers_started);
        self.retired_completions += to_i64(stats.barriers_completed);
        // The old process ends: its parked barrier fails and its task exits.
        self.fake.fail_from_now();
        self.fake.allow_start(u64::MAX / 2);
        self.fake.allow_finish(u64::MAX / 2);
        self.fake = FakeBarrier::new(Arc::clone(&self.ledger));
        self.batch = self
            .runtime
            .block_on(async { Arc::new(Self::new_batch(&self.fake)) });
        self.poisoned = false;
        self.capture = 0;
    }

    /// Lets every task run, then records the answers of finished attempts.
    fn observe_answers(&mut self) {
        self.runtime.block_on(quiesce());
        for op in self.ops.values_mut() {
            if op.task.as_ref().is_some_and(JoinHandle::is_finished)
                && let Some(task) = op.task.take()
            {
                let answer = self
                    .runtime
                    .block_on(task)
                    .unwrap_or_else(|_| Err(StorageError::internal("task stopped")));
                op.success = answer.is_ok();
                op.revealed = answer.as_ref().is_err_and(|error| {
                    matches!(error.as_ref(), StorageEnum::ConditionalCheckFailed)
                });
                op.phase = "responded".into();
            }
        }
    }
}

#[quint_run(
    spec = "../../quint/durable_ack.qnt",
    main = "durable_ack_impl",
    init = "init",
    step = "step",
    max_samples = 300,
    max_steps = 30,
    seed = "0xd0ab1e"
)]
fn given_durable_ack_model_trace_when_batch_with_version_probe_settles_then_answers_and_barriers_match()
-> impl Driver {
    DurabilityDriver::<true>::default()
}

#[quint_run(
    spec = "../../quint/durable_ack.qnt",
    main = "durable_ack_impl_local",
    init = "init",
    step = "step",
    max_samples = 300,
    max_steps = 30,
    seed = "0xd0ab1e"
)]
fn given_durable_ack_model_trace_when_local_batch_settles_then_answers_and_barriers_match()
-> impl Driver {
    DurabilityDriver::<false>::default()
}
