//! Replays `quint/queue_claim_mbt.qnt` (OWN-C) against the real queue
//! operations on one store. Each step is one customer operation through
//! `QueueOperations` on a node that owns its clock; one read attempt then
//! observes the catalog and the rows of the current incarnations, which must
//! equal the model's.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use anyhow::{anyhow, bail, ensure};
use queue_provider::{
    MessageId, Queue, QueueAttempt as _, QueueError, QueueIncarnation, QueueMessageRecord,
    QueueOperations as _, QueuePort, QueueValidationKind, ReceiptHandle, SendMessageRequest,
    queue_message_for_send,
};
use quint_connect::{Driver, Result, State, Step};
use serde::Deserialize;
use storage_types::{
    AttemptDeadlineSource, AttemptRequest, Clock, DurationSeconds, ReadProtection, StorageResult,
    TimestampMillis,
};
use tokio::runtime::Runtime;

use crate::queue_test_support_tests::{TestStore, selected_native_backend_case, unique_suffix};

/// One model time unit. Model VIS = 2 units = 20 s, DELTA = 1 unit = 10 s,
/// retention 9 and 6 units = 90 s and 60 s (SQS minimum 60 s), delay 1 unit.
const UNIT_MS: i64 = 10_000;
/// Model time 0.
const BASE_MS: i64 = 1_800_000_000_000;
const SOURCE: i64 = 1;
const TARGET: i64 = 2;
const VIS_SECONDS: u32 = 20;
const SPEC: &str = "../../quint/queue_claim_mbt.qnt";
const SEED: &str = "0x5e5f";
const MAX_STEPS: usize = 25;
const HEAD: &str = "mbtInitHead";
const SLOT: &str = "mbtInitSlot";

/// The KV slot choice of the next claim (model nondet `rotation`).
static ROTATION: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct RowView {
    q: i64,
    inc: i64,
    id: i64,
    due: i64,
    receipt: i64,
    count: i64,
    created: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct Answer {
    kind: String,
    ids: BTreeSet<i64>,
}

impl Answer {
    fn of(kind: &str) -> Self {
        Self {
            kind: kind.to_owned(),
            ids: BTreeSet::new(),
        }
    }
}

/// Catalog (`1` source, `2` target → incarnation number, `0` none), the rows
/// of current incarnations, and the step's answer.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(from = "RawView")]
struct ClaimView {
    cur: BTreeMap<i64, i64>,
    rows: BTreeSet<RowView>,
    answer: Answer,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawView {
    mbt_state: RawState,
    mbt_answer: Answer,
}

#[derive(Deserialize)]
struct RawState {
    store: RawStore,
}

#[derive(Deserialize)]
struct RawStore {
    cur: BTreeMap<i64, i64>,
    rows: BTreeSet<RowView>,
}

impl From<RawView> for ClaimView {
    fn from(raw: RawView) -> Self {
        let current = raw
            .mbt_state
            .store
            .cur
            .values()
            .copied()
            .filter(|inc| *inc != 0)
            .collect::<BTreeSet<_>>();
        Self {
            rows: raw
                .mbt_state
                .store
                .rows
                .into_iter()
                .filter(|row| current.contains(&row.inc))
                .collect(),
            cur: raw.mbt_state.store.cur,
            answer: raw.mbt_answer,
        }
    }
}

impl<P: QueuePort + 'static> State<ClaimDriver<P>> for ClaimView {
    fn from_driver(driver: &ClaimDriver<P>) -> Result<Self> {
        Ok(driver.observed.clone())
    }
}

/// The shared store seen through one node's clock.
struct MbtNode<P> {
    store: Arc<P>,
    now_ms: Arc<AtomicI64>,
    ahead_ms: i64,
}

impl<P: QueuePort> Clock for MbtNode<P> {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(self.now_ms.load(Ordering::SeqCst) + self.ahead_ms)
    }
}

impl<P: QueuePort> AttemptDeadlineSource for MbtNode<P> {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.store.attempt_deadlines()
    }
}

impl<P: QueuePort + 'static> QueuePort for MbtNode<P> {
    type Attempt<'native>
        = P::Attempt<'native>
    where Self: 'native;

    /// Model DELTA: node 2 runs one unit ahead.
    fn max_clock_skew_millis(&self) -> i64 {
        UNIT_MS
    }

    async fn with_attempt<R, F>(&self, request: AttemptRequest, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> Pin<
                Box<dyn Future<Output = StorageResult<R>> + Send + 'borrow>,
            > + Send
            + 'static,
    {
        self.store.with_attempt(request, operation).await
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
static DRAWS: AtomicU64 = AtomicU64::new(0);

/// Real time; every random draw is `ROTATION + 6 * k` with a fresh 28-bit
/// `k`, so `rotation % slots` equals the model pick for 1, 2 or 3 eligible
/// rows, and `as u32` keeps the value (below 2^31). Ids stay unique: a v7
/// UUID takes its millisecond and counter from here, a v4 UUID two draws.
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
struct RotationSource;

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl storage_types::ambient::AmbientSource for RotationSource {
    fn now_millis(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
            })
    }

    fn next_u64(&self) -> u64 {
        let draw = DRAWS
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(0x9e37_79b9_7f4a_7c15)
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
        ROTATION.load(Ordering::SeqCst) + 6 * (draw >> 36)
    }
}

/// Process-global: nextest runs each test in its own process (`just test`).
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
fn install_rotation_source() {
    assert!(
        storage_types::ambient::install_ambient_source(Box::new(RotationSource)),
        "an ambient source is already installed; run this test under nextest (just test)"
    );
}

struct ClaimDriver<P: QueuePort + 'static> {
    /// Node 1 (clock `now`) and node 2 (clock `now + UNIT_MS`); both share one
    /// store.
    nodes: Vec<MbtNode<P>>,
    now_ms: Arc<AtomicI64>,
    runtime: Runtime,
    /// `mbtInitHead` (SQL) or `mbtInitSlot` (ordered-key stores).
    init: &'static str,
    prefix: String,
    trace: u32,
    /// Queue names of this trace: `[source, target]`.
    names: [String; 2],
    /// Model id → real id, given in send order.
    ids: BTreeMap<i64, MessageId>,
    next_id: i64,
    /// Real receipt text → model receipt, given in answer order.
    receipts: Vec<(String, i64)>,
    next_receipt: i64,
    /// Real incarnation → model number, given in order of first sight.
    incarnations: Vec<(QueueIncarnation, i64)>,
    next_inc: i64,
    observed: ClaimView,
    coverage: Rc<RefCell<BTreeSet<&'static str>>>,
}

impl<P: QueuePort + 'static> Drop for ClaimDriver<P> {
    /// The store may need the runtime while it closes.
    fn drop(&mut self) {
        let _entered = self.runtime.enter();
        self.nodes.clear();
    }
}

#[derive(Clone, Copy)]
enum Catalog {
    Purge,
    Delete,
    Create,
}

impl<P: QueuePort + 'static> ClaimDriver<P> {
    fn new(runtime: Runtime, store: P, init: &'static str) -> Self {
        let store = Arc::new(store);
        let now_ms = Arc::new(AtomicI64::new(BASE_MS));
        let node = |ahead_ms| MbtNode {
            store: Arc::clone(&store),
            now_ms: Arc::clone(&now_ms),
            ahead_ms,
        };
        Self {
            nodes: vec![node(0), node(UNIT_MS)],
            now_ms: Arc::clone(&now_ms),
            runtime,
            init,
            prefix: unique_suffix("qc"),
            trace: 0,
            names: [String::new(), String::new()],
            ids: BTreeMap::new(),
            next_id: 1,
            receipts: Vec::new(),
            next_receipt: 1,
            incarnations: Vec::new(),
            next_inc: 1,
            observed: ClaimView {
                cur: BTreeMap::new(),
                rows: BTreeSet::new(),
                answer: Answer::of("none"),
            },
            coverage: Rc::default(),
        }
    }

    fn node(&self, node: i64) -> Result<&MbtNode<P>> {
        match node {
            1 | 2 => Ok(&self.nodes[(node - 1) as usize]),
            other => bail!("node {other} is not 1 or 2"),
        }
    }

    fn url(&self, queue: i64) -> String {
        format!(
            "http://localhost/queue/{}",
            self.names[(queue - 1) as usize]
        )
    }

    fn model_id(&self, id: MessageId) -> Result<i64> {
        self.ids
            .iter()
            .find(|(_, real)| **real == id)
            .map(|(model, _)| *model)
            .ok_or_else(|| anyhow!("a message the model never sent"))
    }

    fn receipt_number(&self, handle: &str) -> Result<i64> {
        self.receipts
            .iter()
            .find(|(text, _)| text == handle)
            .map(|(_, number)| *number)
            .ok_or_else(|| anyhow!("a row holds a receipt that no receive answered (I3)"))
    }

    fn incarnation_number(&mut self, inc: QueueIncarnation) -> i64 {
        if let Some((_, number)) = self.incarnations.iter().find(|(known, _)| *known == inc) {
            return *number;
        }
        let number = self.next_inc;
        self.incarnations.push((inc, number));
        self.next_inc += 1;
        number
    }

    fn queue_definition(&self, queue: i64) -> Queue {
        let name = self.names[(queue - 1) as usize].clone();
        let retention = if queue == SOURCE { "90" } else { "60" };
        let mut attributes =
            HashMap::from([("MessageRetentionPeriod".to_owned(), retention.to_owned())]);
        if queue == SOURCE {
            attributes.insert(
                "RedrivePolicy".to_owned(),
                format!(
                    r#"{{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:{}","maxReceiveCount":1}}"#,
                    self.names[1]
                ),
            );
        }
        Queue {
            queue_url: format!("http://localhost/queue/{name}"),
            queue_name: name,
            attributes,
            created_at: self.nodes[0].now(),
            ..Default::default()
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.trace += 1;
        self.names = [
            format!("{}-s{}", self.prefix, self.trace),
            format!("{}-t{}", self.prefix, self.trace),
        ];
        self.now_ms.store(BASE_MS, Ordering::SeqCst);
        self.ids.clear();
        self.receipts.clear();
        self.incarnations.clear();
        self.next_id = 1;
        self.next_receipt = 1;
        self.next_inc = 1;
        for queue in [TARGET, SOURCE] {
            self.runtime
                .block_on(self.nodes[0].create_queue(self.queue_definition(queue)))
                .map_err(|error| anyhow!("create: {error}"))?;
        }
        Ok(())
    }

    fn send(&mut self, node: i64, queue: i64, delay: i64) -> Result<Answer> {
        let node_ref = self.node(node)?;
        let message = queue_message_for_send(
            SendMessageRequest {
                queue_url: self.url(queue),
                message_body: format!("m{}", self.next_id),
                delay_seconds: Some(delay * 10),
                ..Default::default()
            },
            node_ref.now(),
        );
        match self.runtime.block_on(node_ref.send_message(message)) {
            Ok(id) => {
                let model = self.next_id;
                self.ids.insert(model, id);
                self.next_id += 1;
                Ok(Answer {
                    kind: "sent".to_owned(),
                    ids: BTreeSet::from([model]),
                })
            }
            Err(QueueError::ResourceNotFound { .. }) => Ok(Answer::of("missing")),
            Err(error) => bail!("send: {error}"),
        }
    }

    fn claim(&mut self, node: i64, queue: i64, rotation: i64) -> Result<Answer> {
        ROTATION.store(u64::try_from(rotation)?, Ordering::SeqCst);
        let node_ref = self.node(node)?;
        let received = match self.runtime.block_on(node_ref.receive_messages(
            &self.url(queue),
            1,
            Some(DurationSeconds::from(VIS_SECONDS)),
            None,
        )) {
            Ok(received) => received,
            Err(QueueError::ResourceNotFound { .. }) => return Ok(Answer::of("missing")),
            Err(error) => bail!("receive: {error}"),
        };
        let mut ids = BTreeSet::new();
        for message in &received.messages {
            let real = message
                .message_id
                .parse::<MessageId>()
                .map_err(|_| anyhow!("unparsable message id"))?;
            ids.insert(self.model_id(real)?);
            self.receipts
                .push((message.receipt_handle.clone(), self.next_receipt));
            self.next_receipt += 1;
        }
        if received.moved_to.is_some() {
            self.coverage.borrow_mut().insert("moved");
        }
        Ok(Answer {
            kind: "received".to_owned(),
            ids,
        })
    }

    fn receipt(
        &mut self,
        node: i64,
        kind: &str,
        queue: i64,
        receipt: i64,
        timeout: i64,
    ) -> Result<Answer> {
        let handle = ReceiptHandle(
            self.receipts
                .iter()
                .find(|(_, number)| *number == receipt)
                .map(|(text, _)| text.clone())
                .ok_or_else(|| anyhow!("model receipt {receipt} was never answered"))?,
        );
        let node_ref = self.node(node)?;
        let url = self.url(queue);
        let result = if kind == "delete" {
            self.runtime.block_on(node_ref.delete_message(&url, handle))
        } else {
            let seconds = u32::try_from(timeout * 10)?;
            self.runtime.block_on(node_ref.change_message_visibility(
                &url,
                handle,
                DurationSeconds::from(seconds),
            ))
        };
        match result {
            Ok(()) => Ok(Answer::of("ok")),
            Err(QueueError::ResourceNotFound { .. }) => Ok(Answer::of("missing")),
            Err(QueueError::Validation {
                kind: QueueValidationKind::InvalidParameterValue,
                ..
            }) if kind == "visibility" => Ok(Answer::of("stale_error")),
            Err(error) => bail!("{kind}: {error}"),
        }
    }

    fn catalog(&mut self, which: Catalog, queue: i64) -> Result<Answer> {
        let node_ref = &self.nodes[0];
        let url = self.url(queue);
        let result = match which {
            Catalog::Purge => self.runtime.block_on(node_ref.purge_queue(&url)),
            Catalog::Delete => self.runtime.block_on(node_ref.delete_queue(&url)),
            Catalog::Create => self
                .runtime
                .block_on(node_ref.create_queue(self.queue_definition(queue)))
                .map(|_| ()),
        };
        match (which, result) {
            (_, Ok(())) => Ok(Answer::of("ok")),
            (
                Catalog::Create,
                Err(QueueError::Validation { .. } | QueueError::ResourceNotFound { .. }),
            ) => Ok(Answer::of("invalid")),
            (Catalog::Purge | Catalog::Delete, Err(QueueError::ResourceNotFound { .. })) => {
                Ok(Answer::of("missing"))
            }
            (_, Err(error)) => bail!("catalog: {error}"),
        }
    }

    fn observe(&mut self, answer: Answer) -> Result<ClaimView> {
        let store = Arc::clone(&self.nodes[0].store);
        let names = self.names.to_vec();
        let reals = self.ids.values().copied().collect::<Vec<_>>();
        let models = self.ids.keys().copied().collect::<Vec<_>>();
        let found = self
            .runtime
            .block_on(read_rows(store, names, reals))
            .map_err(|error| anyhow!("observe: {error}"))?;
        let mut cur = BTreeMap::new();
        let mut rows = BTreeSet::new();
        for (queue, found) in (SOURCE..=TARGET).zip(found) {
            let Some((incarnation, records)) = found else {
                cur.insert(queue, 0);
                continue;
            };
            let inc = self.incarnation_number(incarnation);
            cur.insert(queue, inc);
            for (id, record) in models.iter().zip(records) {
                let Some(record) = record else { continue };
                rows.insert(RowView {
                    q: queue,
                    inc,
                    id: *id,
                    due: units(record.visible_at())?,
                    receipt: match &record.message.receipt_handle {
                        Some(handle) => self.receipt_number(&handle.0)?,
                        None => 0,
                    },
                    count: i64::from(record.message.receive_count),
                    created: units(record.message.created_at)?,
                });
            }
        }
        Ok(ClaimView { cur, rows, answer })
    }

    fn record_coverage(&self, action: &str, step: &Step, before: &ClaimView) -> Result<()> {
        let now = &self.observed;
        let mut tags = Vec::new();
        let queue_rows =
            |view: &ClaimView, queue: i64| view.rows.iter().filter(|row| row.q == queue).count();
        match action {
            "MbtClaim" => {
                let queue: i64 = pick(step, "queue")?;
                let node: i64 = pick(step, "node")?;
                if let Some(id) = now.answer.ids.first().copied() {
                    tags.push("received");
                    if queue == SOURCE
                        && now
                            .rows
                            .iter()
                            .any(|row| row.q == SOURCE && row.id == id && row.count >= 2)
                    {
                        tags.push("redrive_skipped");
                    }
                } else if now.answer.kind == "received" {
                    let other = if queue == SOURCE { TARGET } else { SOURCE };
                    if queue_rows(now, queue) < queue_rows(before, queue)
                        && queue_rows(now, other) <= queue_rows(before, other)
                    {
                        tags.push("dropped_on_claim");
                    }
                    let now2 = (self.now_ms.load(Ordering::SeqCst) - BASE_MS) / UNIT_MS + 1;
                    if node == 2
                        && before
                            .rows
                            .iter()
                            .any(|row| row.q == queue && row.receipt != 0 && row.due == now2)
                    {
                        tags.push("skew_guarded");
                    }
                }
            }
            "MbtReceipt" => {
                let kind: String = pick(step, "kind")?;
                let timeout: i64 = pick(step, "timeout")?;
                if kind == "delete" && now.answer.kind == "ok" && now.rows == before.rows {
                    tags.push("stale_delete");
                }
                if kind == "visibility" && timeout == 0 && now.answer.kind == "ok" {
                    tags.push("visibility_zero");
                }
            }
            "MbtPurge" if now.answer.kind == "ok" => tags.push("purge"),
            _ => {}
        }
        match now.answer.kind.as_str() {
            "stale_error" => tags.push("stale_error"),
            "missing" => tags.push("missing"),
            "invalid" => tags.push("invalid"),
            _ => {}
        }
        self.coverage.borrow_mut().extend(tags);
        Ok(())
    }
}

impl<P: QueuePort + 'static> Driver for ClaimDriver<P> {
    type State = ClaimView;

    fn step(&mut self, step: &Step) -> Result {
        let before = self.observed.clone();
        let action = step.action_taken.as_str();
        let answer = match action {
            HEAD | SLOT => {
                ensure!(
                    action == self.init,
                    "trace starts with {action}, driver runs {}",
                    self.init
                );
                self.reset()?;
                Answer::of("none")
            }
            "MbtSend" => self.send(
                pick(step, "node")?,
                pick(step, "queue")?,
                pick(step, "delay")?,
            )?,
            "MbtClaim" => self.claim(
                pick(step, "node")?,
                pick(step, "queue")?,
                pick(step, "rotation")?,
            )?,
            "MbtReceipt" => {
                let (queue, receipt): (i64, i64) = pick(step, "held")?;
                let kind: String = pick(step, "kind")?;
                self.receipt(
                    pick(step, "node")?,
                    &kind,
                    queue,
                    receipt,
                    pick(step, "timeout")?,
                )?
            }
            "MbtPurge" => self.catalog(Catalog::Purge, pick(step, "queue")?)?,
            "MbtDeleteQueue" => self.catalog(Catalog::Delete, pick(step, "queue")?)?,
            "MbtCreateQueue" => self.catalog(Catalog::Create, pick(step, "queue")?)?,
            "MbtTick" => {
                let k: i64 = pick(step, "k")?;
                self.now_ms.fetch_add(k * UNIT_MS, Ordering::SeqCst);
                Answer::of("none")
            }
            name => bail!("unhandled queue claim action {name}"),
        };
        self.observed = self.observe(answer)?;
        self.record_coverage(action, step, &before)?;
        Ok(())
    }
}

fn units(at: TimestampMillis) -> Result<i64> {
    let offset = at.timestamp_millis() - BASE_MS;
    ensure!(
        offset % UNIT_MS == 0,
        "time {offset} ms is off the model grid"
    );
    Ok(offset.div_euclid(UNIT_MS))
}

type Found = Vec<Option<(QueueIncarnation, Vec<Option<QueueMessageRecord>>)>>;

async fn read_rows<P: QueuePort + 'static>(
    store: Arc<P>,
    names: Vec<String>,
    ids: Vec<MessageId>,
) -> StorageResult<Found> {
    let request = storage_types::AttemptPlan::new(
        storage_types::AttemptMode::ReadWrite,
        storage_types::DeadlineClass::Write,
    )
    .start(store.attempt_deadlines());
    store
        .with_attempt(request, move |attempt| {
            Box::pin(async move {
                let mut found = Vec::with_capacity(names.len());
                for name in &names {
                    let Some(record) = attempt
                        .read_queue_by_name(name, ReadProtection::Snapshot)
                        .await?
                    else {
                        found.push(None);
                        continue;
                    };
                    let rows = if ids.is_empty() {
                        Vec::new()
                    } else {
                        attempt
                            .read_messages_by_id(record.incarnation, ids.clone())
                            .await?
                            .into_vec()
                    };
                    found.push(Some((record.incarnation, rows)));
                }
                Ok(found)
            })
        })
        .await
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("queue claim replay runtime")
}

fn pick<T: serde::de::DeserializeOwned>(step: &Step, name: &str) -> Result<T> {
    let value = step
        .nondet_picks
        .get(name)
        .ok_or_else(|| anyhow!("missing queue claim choice {name}"))?;
    T::deserialize(value.clone()).map_err(Into::into)
}

/// Replays `samples` traces of the boundary from `init` on `store`; returns the
/// coverage.
fn replay<P: QueuePort + 'static>(
    runtime: Runtime,
    store: P,
    init: &'static str,
    samples: usize,
    label: &str,
) -> BTreeSet<&'static str> {
    let driver = ClaimDriver::new(runtime, store, init);
    let coverage = Rc::clone(&driver.coverage);
    quint_connect::runner::run_test(
        driver,
        quint_connect::runner::Config {
            test_name: format!("queue_claim_mbt_{label}"),
            gen_config: quint_connect::runner::RunConfig {
                spec: SPEC.into(),
                main: Some("queue_claim_mbt".into()),
                init: Some(init.into()),
                step: Some("mbtStep".into()),
                max_samples: Some(samples),
                max_steps: Some(MAX_STEPS),
                seed: SEED.into(),
            },
        },
    )
    .expect("model traces match the real queue operations");
    coverage.take()
}

#[cfg(feature = "sqlite")]
#[test]
fn given_queue_claim_model_when_traces_replay_on_sqlite_then_catalog_rows_and_answers_match() {
    let runtime = runtime();
    let store = runtime.block_on(crate::queue_test_support_tests::open_sqlite_store(
        ":memory:".into(),
    ));
    let coverage = replay(runtime, store, HEAD, 60, "sqlite");
    eprintln!("queue claim replay coverage: {coverage:?}");
    for tag in [
        "received",
        "moved",
        "stale_delete",
        "stale_error",
        "missing",
    ] {
        assert!(
            coverage.contains(tag),
            "fixed-seed traces must reach {tag}: {coverage:?}"
        );
    }
}

#[test]
#[ignore = "provider gate: set STORAGE_TEST_BACKEND (just test-native-ignored)"]
fn given_queue_claim_model_when_traces_replay_on_selected_backend_then_catalog_rows_and_answers_match()
 {
    let case = selected_native_backend_case("quint-claim");
    let runtime = runtime();
    let coverage = match case.store {
        #[cfg(feature = "sqlite")]
        TestStore::Sqlite { path } => {
            let store = runtime.block_on(crate::queue_test_support_tests::open_sqlite_store(path));
            replay(runtime, store, HEAD, 30, case.name)
        }
        #[cfg(feature = "turso")]
        TestStore::Turso { path } => {
            let store = runtime.block_on(crate::queue_test_support_tests::open_turso_store(path));
            replay(runtime, store, HEAD, 30, case.name)
        }
        #[cfg(feature = "postgres")]
        TestStore::Postgres { dsn, schema } => {
            let store = runtime.block_on(crate::queue_test_support_tests::open_postgres_store(
                dsn, schema,
            ));
            replay(runtime, store, HEAD, 30, case.name)
        }
        #[cfg(feature = "rocksdb")]
        TestStore::RocksDb { path } => {
            install_rotation_source();
            let store = runtime.block_on(crate::queue_test_support_tests::open_rocksdb_store(path));
            replay(runtime, store, SLOT, 30, case.name)
        }
        #[cfg(feature = "foundationdb")]
        TestStore::FoundationDb {
            cluster_file,
            subspace_prefix,
        } => {
            install_rotation_source();
            let store = runtime.block_on(crate::queue_test_support_tests::open_foundationdb_store(
                cluster_file,
                subspace_prefix,
            ));
            replay(runtime, store, SLOT, 30, case.name)
        }
    };
    eprintln!("queue claim replay coverage on {}: {coverage:?}", case.name);
    assert!(
        coverage.contains("received"),
        "traces must receive: {coverage:?}"
    );
}
