//! Replays `quint/pubsub_delivery_mbt.qnt` (OWN-C-c) against the real SNS
//! delivery calls on one store (spec OWN-C-d). Each step is one provider call
//! on a node over a shared fake clock; the observation then reads every
//! delivery record and the two queue counts, which must equal the model's.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, ensure};
use pubsub_provider::{
    AskMode, ClaimTerms, ClaimedDelivery, CreateTopicRequest, DeliveryAttemptResult,
    DeliveryClaimRequest, DeliveryFailureCode, DeliveryRecordId, DestinationAsk, DestinationKey,
    LocalDelivery, LocalDeliveryItem, LocalOutcome, PublishRequest, PubsubMessageId, PubsubPort,
    PubsubProvider, RenderedQueueMessage, SettleInput, SettleOutcome, SubscribeRequest,
    Subscription, SubscriptionArn, SubscriptionConfirmation, SubscriptionProtocol, Topic, TopicArn,
    TopicName,
};
use queue_provider::{Queue, QueueArn, QueueOperations, QueuePort};
use quint_connect::{Driver, Result, State, Step};
use serde::Deserialize;
use storage_types::{
    AttemptDeadlineSource, AttemptRequest, Clock, MessagingIdentity, StorageResult, TimestampMillis,
};
use tokio::runtime::Runtime;

/// One model unit: 20 minutes. HTTP retry lifetime 1 h = 3 units.
const UNIT_MS: i64 = 1_200_000;
const BASE_MS: i64 = 1_800_000_000_000;
const SPEC: &str = "../../quint/pubsub_delivery_mbt.qnt";
const SEED: &str = "0x5b5";
const MAX_STEPS: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct KeyView {
    m: i64,
    s: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct DRecView {
    present: bool,
    /// `gen` is a reserved word in edition 2024.
    #[serde(rename = "gen")]
    generation: i64,
    owner: i64,
    nonce: i64,
    due: i64,
    attempts: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct AnswerView {
    kind: String,
    keys: BTreeSet<KeyView>,
}

impl AnswerView {
    fn of(kind: &str) -> Self {
        Self {
            kind: kind.to_owned(),
            keys: BTreeSet::new(),
        }
    }

    fn with_keys(kind: &str, keys: BTreeSet<KeyView>) -> Self {
        Self {
            kind: kind.to_owned(),
            keys,
        }
    }
}

/// Present records by key, the number of messages in the "q" queue and in
/// the "h" dead-letter queue (-1 once deleted), and the step's answer.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(from = "RawView")]
struct DeliveryView {
    recs: BTreeMap<KeyView, DRecView>,
    queued: i64,
    dead_lettered: i64,
    answer: AnswerView,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawView {
    d_state: RawState,
    d_answer: AnswerView,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawState {
    recs: BTreeMap<KeyView, DRecView>,
    queue: BTreeMap<KeyView, i64>,
    dlq: BTreeMap<KeyView, i64>,
    dlq_exists: BTreeMap<String, bool>,
}

impl From<RawView> for DeliveryView {
    fn from(raw: RawView) -> Self {
        let state = raw.d_state;
        let total = |counts: &BTreeMap<KeyView, i64>, sub: &str| {
            counts
                .iter()
                .filter(|(key, _)| key.s == sub)
                .map(|(_, count)| count)
                .sum::<i64>()
        };
        Self {
            recs: state
                .recs
                .into_iter()
                .filter(|(_, rec)| rec.present)
                .collect(),
            queued: total(&state.queue, "q"),
            dead_lettered: if state.dlq_exists.get("h").copied().unwrap_or(false) {
                total(&state.dlq, "h")
            } else {
                -1
            },
            answer: raw.d_answer,
        }
    }
}

impl<P: MbtStore> State<DeliveryDriver<P>> for DeliveryView {
    fn from_driver(driver: &DeliveryDriver<P>) -> Result<Self> {
        Ok(driver.observed.clone())
    }
}

/// A store that serves SNS and SQS on one database.
trait MbtStore: PubsubPort + QueuePort + 'static {}
impl<P: PubsubPort + QueuePort + 'static> MbtStore for P {}

/// The shared store seen through the fake clock; nodes differ only by owner.
struct MbtNode<P> {
    store: Arc<P>,
    now_ms: Arc<AtomicI64>,
}

impl<P: MbtStore> Clock for MbtNode<P> {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(self.now_ms.load(Ordering::SeqCst))
    }
}

impl<P: MbtStore> AttemptDeadlineSource for MbtNode<P> {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        AttemptDeadlineSource::attempt_deadlines(&*self.store)
    }
}

impl<P: MbtStore> PubsubPort for MbtNode<P> {
    type Attempt<'native>
        = <P as PubsubPort>::Attempt<'native>
    where Self: 'native;

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
        PubsubPort::with_attempt(&*self.store, request, operation).await
    }
}

impl<P: MbtStore> QueuePort for MbtNode<P> {
    type Attempt<'native>
        = <P as QueuePort>::Attempt<'native>
    where Self: 'native;

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
        QueuePort::with_attempt(&*self.store, request, operation).await
    }
}

#[derive(Deserialize)]
struct FlightPick {
    nonce: i64,
}

struct DeliveryDriver<P: MbtStore> {
    node: Option<MbtNode<P>>,
    now_ms: Arc<AtomicI64>,
    runtime: Runtime,
    prefix: String,
    trace: u32,
    identity: MessagingIdentity,
    topic: Option<TopicArn>,
    /// Subscription ARNs and destinations: index 0 "q", index 1 "h".
    subscriptions: Vec<(SubscriptionArn, DestinationKey)>,
    queue_name: String,
    dlq_name: String,
    /// Model message -> publish message id.
    messages: BTreeMap<i64, PubsubMessageId>,
    /// The message waiting for materialization (model `pending`).
    pending: i64,
    /// Real nonce -> model nonce, and model nonce -> claimed delivery and its
    /// key.
    nonces: Vec<(uuid::Uuid, i64)>,
    flights: BTreeMap<i64, (ClaimedDelivery, i64, &'static str)>,
    results: BTreeMap<i64, DeliveryAttemptResult>,
    next_nonce: i64,
    /// Subscriptions deleted in this trace.
    unsubscribed: BTreeSet<&'static str>,
    observed: DeliveryView,
    coverage: Rc<RefCell<BTreeSet<&'static str>>>,
}

impl<P: MbtStore> Drop for DeliveryDriver<P> {
    /// The store may need the runtime while it closes.
    fn drop(&mut self) {
        let _entered = self.runtime.enter();
        self.node.take();
    }
}

impl<P: MbtStore> DeliveryDriver<P> {
    fn new(runtime: Runtime, store: P) -> Self {
        let now_ms = Arc::new(AtomicI64::new(BASE_MS));
        Self {
            node: Some(MbtNode {
                store: Arc::new(store),
                now_ms: Arc::clone(&now_ms),
            }),
            now_ms,
            runtime,
            prefix: unique_prefix(),
            trace: 0,
            identity: MessagingIdentity::default(),
            topic: None,
            subscriptions: Vec::new(),
            queue_name: String::new(),
            dlq_name: String::new(),
            messages: BTreeMap::new(),
            pending: 0,
            nonces: Vec::new(),
            flights: BTreeMap::new(),
            results: BTreeMap::new(),
            next_nonce: 1,
            unsubscribed: BTreeSet::new(),
            observed: DeliveryView {
                recs: BTreeMap::new(),
                queued: 0,
                dead_lettered: 0,
                answer: AnswerView::of("none"),
            },
            coverage: Rc::default(),
        }
    }

    fn node(&self) -> &MbtNode<P> {
        self.node.as_ref().expect("node while the driver lives")
    }

    fn queue_url(name: &str) -> String {
        format!("http://localhost/queue/{name}")
    }

    fn key_of(&self, message: &PubsubMessageId) -> Result<i64> {
        self.messages
            .iter()
            .find(|(_, real)| *real == message)
            .map(|(model, _)| *model)
            .ok_or_else(|| anyhow!("a message the model never published"))
    }

    fn model_nonce(&self, nonce: uuid::Uuid) -> Result<i64> {
        self.nonces
            .iter()
            .find(|(real, _)| *real == nonce)
            .map(|(_, model)| *model)
            .ok_or_else(|| anyhow!("a record holds a lease nonce that no claim gave"))
    }

    /// A trace may end with a published, unmaterialized message. Its intent
    /// would be the next materialization's oldest candidate in the following
    /// trace, so drain every intent of earlier traces at a time past all of
    /// them before the clock returns to the base.
    fn drain_intents(&self) -> Result<()> {
        self.now_ms
            .store(BASE_MS + 1_000 * UNIT_MS, Ordering::SeqCst);
        loop {
            let report = self
                .runtime
                .block_on(PubsubProvider::materialize_publish_intents(self.node(), 16))
                .map_err(|error| anyhow!("drain materialize_publish_intents: {error}"))?;
            ensure!(report.failed_chunks == 0, "drain: a chunk failed");
            if report.chunks == 0 {
                return Ok(());
            }
        }
    }

    fn reset(&mut self, chosen: &BTreeSet<KeyView>) -> Result<()> {
        self.drain_intents()?;
        self.trace += 1;
        self.messages.clear();
        self.nonces.clear();
        self.flights.clear();
        self.results.clear();
        self.unsubscribed.clear();
        self.pending = 0;
        self.next_nonce = 1;
        self.now_ms.store(BASE_MS, Ordering::SeqCst);
        self.queue_name = format!("{}-q{}", self.prefix, self.trace);
        self.dlq_name = format!("{}-d{}", self.prefix, self.trace);
        self.subscriptions.clear();
        let identity = self.identity.clone();
        let node = self.node();
        let created_at = Clock::now(node);
        for name in [&self.queue_name, &self.dlq_name] {
            self.runtime
                .block_on(QueueOperations::create_queue(
                    node,
                    Queue {
                        queue_name: name.clone(),
                        queue_url: Self::queue_url(name),
                        created_at,
                        ..Default::default()
                    },
                ))
                .map_err(|error| anyhow!("create_queue: {error}"))?;
        }
        let topic = Topic::try_new(
            CreateTopicRequest {
                name: TopicName::new(format!("{}-t{}", self.prefix, self.trace))?,
                attributes: HashMap::new(),
                tags: BTreeMap::new(),
            },
            &identity,
        )?;
        let topic = self
            .runtime
            .block_on(PubsubProvider::create_topic(node, topic))
            .map_err(|error| anyhow!("create_topic: {error}"))?;
        let topic_arn = topic.topic_arn;
        let queue_arn = QueueArn::of(&identity, &self.queue_name);
        let dlq_arn = QueueArn::of(&identity, &self.dlq_name);
        let requests = [
            SubscribeRequest {
                topic_arn: topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: queue_arn.to_string(),
                attributes: HashMap::from([(
                    "FilterPolicy".to_owned(),
                    filter_policy(chosen, "q"),
                )]),
                extra_json: serde_json::Value::Null,
            },
            SubscribeRequest {
                topic_arn: topic_arn.clone(),
                protocol: SubscriptionProtocol::Https,
                endpoint: format!("https://h{}.example.invalid/hook", self.trace),
                attributes: HashMap::from([
                    ("FilterPolicy".to_owned(), filter_policy(chosen, "h")),
                    (
                        "RedrivePolicy".to_owned(),
                        format!(r#"{{"deadLetterTargetArn":"{dlq_arn}"}}"#),
                    ),
                ]),
                extra_json: serde_json::Value::Null,
            },
        ];
        let mut subscriptions = Vec::new();
        for request in requests {
            let protocol = request.protocol;
            let endpoint = request.endpoint.clone();
            let mut subscription = Subscription::try_new(request, &identity)?;
            subscription.confirmation = SubscriptionConfirmation::Confirmed;
            let created = self
                .runtime
                .block_on(PubsubProvider::create_subscription(node, subscription))
                .map_err(|error| anyhow!("create_subscription: {error}"))?;
            subscriptions.push((
                created.subscription_arn,
                DestinationKey::of_endpoint(protocol, &endpoint),
            ));
        }
        self.subscriptions = subscriptions;
        self.topic = Some(topic_arn);
        Ok(())
    }

    fn publish(&mut self, m: i64) -> Result<AnswerView> {
        let id = PubsubMessageId::new();
        let request = PublishRequest {
            topic_arn: self.topic.clone().ok_or_else(|| anyhow!("no topic"))?,
            message: format!("m{m}"),
            subject: None,
            message_attributes: HashMap::from([("n".to_owned(), format!("m{m}").into())]),
        };
        self.runtime
            .block_on(PubsubProvider::accept_publish(
                self.node(),
                request,
                id.clone(),
                false,
            ))
            .map_err(|error| anyhow!("accept_publish: {error}"))?;
        self.messages.insert(m, id);
        self.pending = m;
        Ok(AnswerView::of("published"))
    }

    fn materialize(&mut self) -> Result<AnswerView> {
        let report = self
            .runtime
            .block_on(PubsubProvider::materialize_publish_intents(self.node(), 1))
            .map_err(|error| anyhow!("materialize_publish_intents: {error}"))?;
        ensure!(
            report.failed_chunks == 0 && report.chunks == 1,
            "materialize: {} chunks, {} failed",
            report.chunks,
            report.failed_chunks
        );
        let mut keys = BTreeSet::new();
        for (destination, _) in &report.written {
            for (index, (_, known)) in self.subscriptions.iter().enumerate() {
                if known == destination {
                    keys.insert(KeyView {
                        m: self.pending,
                        s: sub_name(index).to_owned(),
                    });
                }
            }
        }
        self.pending = 0;
        Ok(AnswerView::with_keys("materialized", keys))
    }

    /// The answer and whether a record was taken over from the other node.
    fn claim(&mut self, node: i64, sub: &str) -> Result<(AnswerView, bool)> {
        let index = sub_index(sub)?;
        let now = Clock::now(self.node());
        let request = DeliveryClaimRequest {
            terms: ClaimTerms {
                owner: format!("n{node}"),
                now,
                lease_until: TimestampMillis::from_timestamp(now.timestamp_millis() + UNIT_MS),
            },
            asks: vec![DestinationAsk {
                destination: self.subscriptions[index].1.clone(),
                take: NonZeroU32::new(2).expect("two is not zero"),
                mode: AskMode::Due,
            }],
        };
        let claims = self
            .runtime
            .block_on(PubsubProvider::claim_deliveries(self.node(), request))
            .map_err(|error| anyhow!("claim_deliveries: {error}"))?;
        let mut taken = Vec::new();
        for claim in claims {
            for claimed in claim.claimed {
                let m = self.key_of(&claimed.payload.message_id)?;
                taken.push((m, claimed));
            }
        }
        taken.sort_by_key(|(m, _)| *m);
        let owner = format!("n{node}");
        let mut keys = BTreeSet::new();
        let mut takeover = false;
        for (m, claimed) in taken {
            takeover |= self.flights.values().any(|(held, held_m, held_sub)| {
                *held_m == m
                    && *held_sub == sub_name(index)
                    && held
                        .state
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.owner != owner)
            });
            self.nonces.push((claimed.fence.nonce, self.next_nonce));
            self.flights
                .insert(self.next_nonce, (claimed, m, sub_name(index)));
            self.next_nonce += 1;
            keys.insert(KeyView {
                m,
                s: sub_name(index).to_owned(),
            });
        }
        Ok((AnswerView::with_keys("claimed", keys), takeover))
    }

    fn take_flight(&mut self, nonce: i64) -> Result<(ClaimedDelivery, i64, &'static str)> {
        self.flights
            .remove(&nonce)
            .ok_or_else(|| anyhow!("model nonce {nonce} has no flight"))
    }

    fn deliver_local(&mut self, nonce: i64) -> Result<AnswerView> {
        let (claimed, m, sub) = self.take_flight(nonce)?;
        let delivery = LocalDelivery {
            queue: QueueArn::of(&self.identity, &self.queue_name),
            items: vec![LocalDeliveryItem {
                claimed: claimed.state,
                fence: claimed.fence,
                message_id: claimed.payload.message_id,
                message: RenderedQueueMessage {
                    body: format!("m{m}"),
                    attributes: None,
                },
            }],
            jitter: 0,
            identity: self.identity.clone(),
        };
        let outcome = self
            .runtime
            .block_on(PubsubProvider::deliver_to_local_queue(
                self.node(),
                delivery,
            ))
            .map_err(|error| anyhow!("DDeliverLocal: {error}"))?;
        let LocalOutcome::Settled(reports) = outcome else {
            bail!("DDeliverLocal: the batch was too large");
        };
        ensure!(
            reports.len() == 1,
            "DDeliverLocal: {} reports",
            reports.len()
        );
        let kind = match reports[0].outcome {
            SettleOutcome::Delivered => "delivered",
            SettleOutcome::ClaimLost => "claim_lost",
            other => bail!("DDeliverLocal: unexpected outcome {other:?}"),
        };
        Ok(AnswerView::with_keys(
            kind,
            BTreeSet::from([KeyView {
                m,
                s: sub.to_owned(),
            }]),
        ))
    }

    fn settle(&mut self, nonce: i64) -> Result<AnswerView> {
        let (claimed, m, sub) = self.take_flight(nonce)?;
        let result = self
            .results
            .remove(&nonce)
            .ok_or_else(|| anyhow!("model nonce {nonce} has no HTTP result"))?;
        let input = SettleInput {
            claimed: claimed.state,
            fence: claimed.fence,
            message_id: claimed.payload.message_id,
            result,
            dead_letter: Some(RenderedQueueMessage {
                body: format!("m{m}"),
                attributes: None,
            }),
            jitter: 0,
            identity: self.identity.clone(),
        };
        let report = self
            .runtime
            .block_on(PubsubProvider::settle_delivery(self.node(), input))
            .map_err(|error| anyhow!("DSettle: {error}"))?;
        let kind = match report.outcome {
            SettleOutcome::Delivered => "delivered".to_owned(),
            SettleOutcome::Retried => "retried".to_owned(),
            SettleOutcome::DeadLettered => "dead_lettered".to_owned(),
            SettleOutcome::Dropped(reason) => format!("dropped_{}", reason.label()),
            SettleOutcome::ClaimLost => "claim_lost".to_owned(),
        };
        Ok(AnswerView::with_keys(
            &kind,
            BTreeSet::from([KeyView {
                m,
                s: sub.to_owned(),
            }]),
        ))
    }

    fn count(&self, name: &str) -> Result<Option<i64>> {
        let found = self
            .runtime
            .block_on(QueueOperations::get_queue_with_message_counts(
                self.node(),
                &Self::queue_url(name),
            ))
            .map_err(|error| anyhow!("get_queue_with_message_counts: {error}"))?;
        Ok(found.map(|(_, counts)| (counts.visible + counts.not_visible + counts.delayed) as i64))
    }

    fn observe(&self, answer: AnswerView) -> Result<DeliveryView> {
        let mut recs = BTreeMap::new();
        for (m, message) in &self.messages {
            for (index, (arn, _)) in self.subscriptions.iter().enumerate() {
                let id = DeliveryRecordId(format!("{arn}:{message}"));
                let found = self
                    .runtime
                    .block_on(PubsubProvider::get_delivery(self.node(), &id))
                    .map_err(|error| anyhow!("get_delivery: {error}"))?;
                let Some((state, _)) = found else { continue };
                let (owner, nonce) = match &state.lease {
                    Some(lease) => (
                        match lease.owner.as_str() {
                            "n1" => 1,
                            "n2" => 2,
                            other => bail!("unknown lease owner {other}"),
                        },
                        self.model_nonce(lease.nonce)?,
                    ),
                    None => (0, 0),
                };
                recs.insert(
                    KeyView {
                        m: *m,
                        s: sub_name(index).to_owned(),
                    },
                    DRecView {
                        present: true,
                        generation: state.generation as i64,
                        owner,
                        nonce,
                        due: due_units(state.due_at),
                        attempts: i64::from(state.attempts),
                    },
                );
            }
        }
        Ok(DeliveryView {
            recs,
            queued: self
                .count(&self.queue_name)?
                .ok_or_else(|| anyhow!("the \"q\" queue is missing"))?,
            dead_lettered: self.count(&self.dlq_name)?.unwrap_or(-1),
            answer,
        })
    }

    fn record_coverage(&self, action: &str, answer: &AnswerView, takeover: bool) {
        let mut tags = Vec::new();
        match (action, answer.kind.as_str()) {
            ("DDeliverLocal", "delivered") => tags.push("delivered_local"),
            ("DSettle", "delivered") => tags.push("delivered_http"),
            (_, "claim_lost") => tags.push("claim_lost"),
            (_, "retried") => tags.push("retried"),
            (_, "dead_lettered") => tags.push("dead_lettered"),
            (_, "dropped_subscription_deleted") => tags.push("dropped_subscription_deleted"),
            (_, "dropped_dead_letter_missing") => tags.push("dropped_dead_letter_missing"),
            ("DMaterialize", "materialized") if answer.keys.len() < 2 => tags.push("filtered"),
            _ => {}
        }
        if answer.kind == "materialized"
            && answer
                .keys
                .iter()
                .any(|key| self.unsubscribed.contains(key.s.as_str()))
        {
            tags.push("unsubscribed_then_materialized");
        }
        if takeover {
            tags.push("takeover");
        }
        self.coverage.borrow_mut().extend(tags);
    }
}

impl<P: MbtStore> Driver for DeliveryDriver<P> {
    type State = DeliveryView;

    fn step(&mut self, step: &Step) -> Result {
        let action = step.action_taken.as_str();
        let mut takeover = false;
        let answer = match action {
            "dInit" | "dInitNoCatalog" => {
                let chosen: BTreeSet<KeyView> = pick(step, "chosen")?;
                self.reset(&chosen)?;
                AnswerView::of("none")
            }
            "DPublish" => self.publish(pick(step, "m")?)?,
            "DMaterialize" => self.materialize()?,
            "DClaim" => {
                let node: i64 = pick(step, "node")?;
                let sub: String = pick(step, "sub")?;
                let (answer, taken_over) = self.claim(node, &sub)?;
                takeover = taken_over;
                answer
            }
            "DDeliverLocal" => {
                let flight: FlightPick = pick(step, "f")?;
                self.deliver_local(flight.nonce)?
            }
            "DHttpSend" => {
                let flight: FlightPick = pick(step, "f")?;
                let result: String = pick(step, "result")?;
                let result = match result.as_str() {
                    "delivered" => DeliveryAttemptResult::Delivered,
                    "transient" => {
                        DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(503))
                    }
                    "permanent" => {
                        DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400))
                    }
                    other => bail!("unknown HTTP result {other}"),
                };
                self.results.insert(flight.nonce, result);
                AnswerView::of("none")
            }
            "DSettle" => {
                let flight: FlightPick = pick(step, "f")?;
                self.settle(flight.nonce)?
            }
            "DUnsubscribe" => {
                let sub: String = pick(step, "sub")?;
                let index = sub_index(&sub)?;
                self.unsubscribed.insert(sub_name(index));
                let arn = self.subscriptions[index].0.clone();
                self.runtime
                    .block_on(PubsubProvider::delete_subscription(self.node(), &arn))
                    .map_err(|error| anyhow!("{action}: {error}"))?;
                AnswerView::of("ok")
            }
            "DDeleteDlq" => {
                self.runtime
                    .block_on(QueueOperations::delete_queue(
                        self.node(),
                        &Self::queue_url(&self.dlq_name),
                    ))
                    .map_err(|error| anyhow!("{action}: {error}"))?;
                AnswerView::of("ok")
            }
            "DTick" => {
                self.now_ms.fetch_add(UNIT_MS, Ordering::SeqCst);
                AnswerView::of("none")
            }
            name => bail!("unhandled pubsub delivery action {name}"),
        };
        self.observed = self.observe(answer)?;
        self.record_coverage(action, &self.observed.answer, takeover);
        Ok(())
    }
}

fn sub_index(sub: &str) -> Result<usize> {
    match sub {
        "q" => Ok(0),
        "h" => Ok(1),
        other => bail!("unknown subscription {other}"),
    }
}

fn sub_name(index: usize) -> &'static str {
    if index == 0 { "q" } else { "h" }
}

/// The ceiling in model units: a retry 0.5 s after unit `t` reads as `t + 1`.
fn due_units(at: TimestampMillis) -> i64 {
    (at.timestamp_millis() - BASE_MS + UNIT_MS - 1).div_euclid(UNIT_MS)
}

/// The `"none"` value keeps the array non-empty; no published message carries
/// it.
fn filter_policy(chosen: &BTreeSet<KeyView>, sub: &str) -> String {
    let mut values = vec!["\"none\"".to_owned()];
    for m in [1, 2] {
        if chosen.contains(&KeyView {
            m,
            s: sub.to_owned(),
        }) {
            values.push(format!("\"m{m}\""));
        }
    }
    format!(r#"{{"n":[{}]}}"#, values.join(","))
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let mut prefix = format!("pd{}{nanos}", std::process::id());
    prefix.truncate(40);
    prefix
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("pubsub delivery replay runtime")
}

fn pick<T: serde::de::DeserializeOwned>(step: &Step, name: &str) -> Result<T> {
    let value = step
        .nondet_picks
        .get(name)
        .ok_or_else(|| anyhow!("missing pubsub delivery choice {name}"))?;
    T::deserialize(value.clone()).map_err(Into::into)
}

#[cfg(feature = "sqlite")]
async fn open_sqlite() -> sql_backend::SqlStore<sql_backend::sqlite::SqliteBackend> {
    let driver = sql_backend::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .expect("open SQLite");
    let store = sql_backend::SqlStore::initialize_schema(
        driver,
        storage_types::InitialStreamLineages::ONE,
        false,
    )
    .await
    .expect("SQLite schema");
    PubsubProvider::initialize(&store)
        .await
        .expect("pubsub initialize");
    store
}

/// Replays `samples` traces from `dInit` on `store`; returns the coverage.
fn replay<P: MbtStore>(
    runtime: Runtime,
    store: P,
    samples: usize,
    label: &str,
) -> BTreeSet<&'static str> {
    replay_from(runtime, store, "dInit", samples, label)
}

/// `dInit` allows catalog changes; `dInitNoCatalog` keeps the dead-letter
/// path reachable in sampled traces.
fn replay_from<P: MbtStore>(
    runtime: Runtime,
    store: P,
    init: &str,
    samples: usize,
    label: &str,
) -> BTreeSet<&'static str> {
    let driver = DeliveryDriver::new(runtime, store);
    let coverage = Rc::clone(&driver.coverage);
    quint_connect::runner::run_test(
        driver,
        quint_connect::runner::Config {
            test_name: format!("pubsub_delivery_mbt_{label}"),
            gen_config: quint_connect::runner::RunConfig {
                spec: SPEC.into(),
                main: Some("pubsub_delivery_mbt".into()),
                init: Some(init.into()),
                step: Some("dStep".into()),
                max_samples: Some(samples),
                max_steps: Some(MAX_STEPS),
                seed: SEED.into(),
            },
        },
    )
    .expect("model traces match the real delivery calls");
    coverage.take()
}

#[cfg(feature = "sqlite")]
#[test]
fn given_delivery_model_when_traces_replay_on_sqlite_then_records_queues_and_answers_match() {
    // Two inits from one seed, one store each: 40 traces with catalog
    // changes and 40 without (the dead-letter path needs both subscription
    // and dead-letter queue to survive until the settle).
    let runtime = runtime();
    let store = runtime.block_on(open_sqlite());
    let mut coverage = replay_from(runtime, store, "dInit", 40, "sqlite");
    let runtime = self::runtime();
    let store = runtime.block_on(open_sqlite());
    coverage.extend(replay_from(
        runtime,
        store,
        "dInitNoCatalog",
        40,
        "sqlite_no_catalog",
    ));
    eprintln!("pubsub delivery replay coverage: {coverage:?}");
    for tag in [
        "delivered_local",
        "delivered_http",
        "claim_lost",
        "retried",
        "dead_lettered",
    ] {
        assert!(
            coverage.contains(tag),
            "fixed-seed traces must reach {tag}: {coverage:?}"
        );
    }
}

#[test]
#[ignore = "provider gate: set STORAGE_TEST_BACKEND (just test-native-ignored)"]
fn given_delivery_model_when_traces_replay_on_selected_backend_then_records_queues_and_answers_match()
 {
    let runtime = runtime();
    let prefix = unique_prefix();
    let backend = std::env::var("STORAGE_TEST_BACKEND").unwrap_or_else(|_| "sqlite".to_owned());
    let deadlines = storage_types::AttemptDeadlines::default();
    let coverage = match backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let store = runtime.block_on(open_sqlite());
            replay(runtime, store, 30, "sqlite")
        }
        #[cfg(feature = "turso")]
        "turso" => {
            let store = runtime.block_on(async {
                let driver = sql_backend::turso::TursoBackend::initialize(":memory:", deadlines)
                    .await
                    .expect("open Turso");
                let store = sql_backend::SqlStore::initialize_schema(
                    driver,
                    storage_types::InitialStreamLineages::ONE,
                    false,
                )
                .await
                .expect("Turso schema");
                PubsubProvider::initialize(&store)
                    .await
                    .expect("pubsub initialize");
                store
            });
            replay(runtime, store, 30, "turso")
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            let store = runtime.block_on(async {
                let dsn = std::env::var("TEST_POSTGRES_DSN").expect("set TEST_POSTGRES_DSN");
                let (dsn, schema) =
                    sql_backend::postgres::test_schema::isolated_test_dsn(&dsn, "pubsub_mbt");
                sql_backend::postgres::test_schema::create_test_schema(&dsn, &schema)
                    .await
                    .expect("create Postgres test schema");
                let driver =
                    sql_backend::postgres::PostgresBackend::initialize(&dsn, 16, false, deadlines)
                        .await
                        .expect("open Postgres");
                let store = sql_backend::SqlStore::initialize_schema(
                    driver,
                    storage_types::InitialStreamLineages::ONE,
                    false,
                )
                .await
                .expect("Postgres schema");
                PubsubProvider::initialize(&store)
                    .await
                    .expect("pubsub initialize");
                store
            });
            replay(runtime, store, 30, "postgres")
        }
        #[cfg(feature = "rocksdb")]
        "rocksdb" => {
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(std::path::Path::parent)
                .expect("workspace root")
                .join("target")
                .join("pubsub-test-data")
                .join(&prefix);
            let store = runtime.block_on(async {
                // storage_provider::ROCKSDB_DEFAULT_WRITER_QUEUE_CAPACITY; the
                // pubsub crate has no storage-provider dependency.
                let driver = kv_backend::rocksdb::RocksDbBackend::initialize(path, deadlines, 256)
                    .expect("open RocksDB");
                let store = kv_backend::KvStore::initialize_format(
                    driver,
                    storage_types::InitialStreamLineages::ONE,
                )
                .await
                .expect("RocksDB format");
                PubsubProvider::initialize(&store)
                    .await
                    .expect("pubsub initialize");
                store
            });
            replay(runtime, store, 30, "rocksdb")
        }
        #[cfg(feature = "foundationdb")]
        "foundationdb" => {
            let store = runtime.block_on(async {
                let cluster_file =
                    std::env::var("TEST_FDB_CLUSTER_FILE").expect("set TEST_FDB_CLUSTER_FILE");
                let subspace_prefix = format!("tests/pubsub-mbt/{prefix}/");
                let driver = kv_backend::foundationdb::FoundationDbBackend::initialize(
                    kv_backend::foundationdb::FoundationDbOptions {
                        cluster_file: Some(cluster_file.as_str()),
                        subspace_prefix: Some(subspace_prefix.as_bytes()),
                        ..kv_backend::foundationdb::FoundationDbOptions::default()
                    },
                )
                .await
                .expect("open FoundationDB");
                let store = kv_backend::KvStore::initialize_format(
                    driver,
                    storage_types::InitialStreamLineages::ONE,
                )
                .await
                .expect("FoundationDB format");
                PubsubProvider::initialize(&store)
                    .await
                    .expect("pubsub initialize");
                store
            });
            replay(runtime, store, 30, "foundationdb")
        }
        other => {
            let compiled: Vec<&str> = vec![
                #[cfg(feature = "sqlite")]
                "sqlite",
                #[cfg(feature = "turso")]
                "turso",
                #[cfg(feature = "postgres")]
                "postgres",
                #[cfg(feature = "rocksdb")]
                "rocksdb",
                #[cfg(feature = "foundationdb")]
                "foundationdb",
            ];
            let _ = (&prefix, deadlines);
            panic!("STORAGE_TEST_BACKEND {other} is not compiled; compiled: {compiled:?}");
        }
    };
    eprintln!("pubsub delivery replay coverage on {backend}: {coverage:?}");
    assert!(
        coverage.contains("delivered_local"),
        "traces must deliver locally: {coverage:?}"
    );
}
