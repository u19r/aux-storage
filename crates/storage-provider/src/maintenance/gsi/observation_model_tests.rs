//! Full observation-contract correspondence. Decoded facts are controlled
//! inputs; build state and projection effects come only from the production
//! executor.
use std::collections::BTreeMap;

use anyhow::{anyhow, ensure};
use quint_connect::{Driver, Result, State, Step, quint_run};
use serde::Deserialize;
use storage_backfill::{
    GsiBuild, GsiBuildProjectionObservation, GsiChange, GsiFailureReason, GsiProjectedRow,
    GsiProjection, GsiProjectionKey, GsiScanObservation, GsiScanPageEnd, GsiSourceProjection,
    GsiState, GsiStreamPageEnd,
};
use storage_types::{AttributeValue, ItemRevision, KeyAttributes, WireItem};

use super::observation_test_support::{
    Observations, Read, checkpoint, offset, scan, stream, target,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct Row {
    version: i64,
    live: bool,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct Source {
    version: i64,
    index: i64,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct Effect {
    entity: i64,
    before: i64,
    after: i64,
    version: i64,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct Observation {
    key: i64,
    version: i64,
    live: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Contract {
    phase: String,
    cursor: u64,
    high: u64,
    pending: bool,
    scan: u64,
    hold: i64,
    rows: BTreeMap<i64, Row>,
    sources: BTreeMap<i64, Source>,
    history: Vec<Effect>,
    history_available: bool,
    current_target: bool,
    before_rows: BTreeMap<i64, Row>,
    observations: Vec<Observation>,
    before_cursor: u64,
    writes: Vec<Observation>,
    event: String,
    outcome: String,
    last_mutation: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ModelState {
    state: Contract,
}

struct GsiDriver {
    build: Option<GsiBuild<u64>>,
    state: Contract,
}

impl Default for GsiDriver {
    fn default() -> Self {
        let rows: BTreeMap<_, _> = (0..4)
            .map(|key| {
                (
                    key,
                    Row {
                        version: 0,
                        live: false,
                    },
                )
            })
            .collect();
        Self {
            build: None,
            state: Contract {
                phase: "absent".into(),
                cursor: 0,
                high: 0,
                pending: false,
                scan: 0,
                hold: -1,
                before_rows: rows.clone(),
                rows,
                sources: (0..2)
                    .map(|entity| {
                        (
                            entity,
                            Source {
                                version: 0,
                                index: -1,
                            },
                        )
                    })
                    .collect(),
                history: vec![],
                history_available: true,
                current_target: true,
                observations: vec![],
                before_cursor: 0,
                writes: vec![],
                event: "init".into(),
                outcome: "idle".into(),
                last_mutation: "none".into(),
            },
        }
    }
}

impl State<GsiDriver> for ModelState {
    fn from_driver(driver: &GsiDriver) -> Result<Self> {
        let mut state = driver.state.clone();
        (
            state.phase,
            state.cursor,
            state.high,
            state.pending,
            state.scan,
            state.hold,
        ) = match &driver.build {
            None => ("absent".into(), 0, 0, false, 0, -1),
            Some(build) => {
                let retained = offset(build.state.required_stream_history());
                let (phase, high, pending, scan) = match &build.state {
                    GsiState::Scanning(s) => ("scanning", 0, false, s.after.unwrap_or(0)),
                    GsiState::CatchingUp(s) => ("catching_up", offset(&s.high), false, 0),
                    GsiState::Active(s) => (
                        "active",
                        s.pending_high.as_ref().map(offset).unwrap_or(retained),
                        s.pending_high.is_some(),
                        0,
                    ),
                    GsiState::Blocked(_) => ("blocked", 0, false, 0),
                };
                (
                    phase.into(),
                    retained,
                    high,
                    pending,
                    scan,
                    i64::try_from(retained)?,
                )
            }
        };
        Ok(Self { state })
    }
}

impl Driver for GsiDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        // Refresh only the observed production state, never the Quint expected
        // state.
        self.state = ModelState::from_driver(self)?.state;
        match step.action_taken.as_str() {
            "init" => *self = Self::default(),
            "ConcurrentPut" => self.mutate(pick(step, "entity")?, pick(step, "index")?)?,
            "ConcurrentDelete" => self.mutate(pick(step, "entity")?, -1)?,
            "KeyMove" => {
                let entity = pick(step, "entity")?;
                self.mutate(entity, 1 - self.state.sources[&entity].index)?;
            }
            "AbsentDelete" => {
                ensure!(self.state.sources[&pick(step, "entity")?].index == -1);
                self.begin("absent_delete");
            }
            "CaptureBoundary" => self.capture(),
            "ScanObservation" => self.scan_source()?,
            "MarkScanComplete" => self.finish_scan()?,
            "StreamObservation" => self.read_stream(1)?,
            "FinalDrain" => self.read_stream(2)?,
            "Activate" => self.activate()?,
            "MarkMissingHistory" | "TrimRequiredHistory" => {
                self.begin("lose_history");
                self.state.history_available = false;
            }
            "RetainRequiredHistory" => self.begin("retain_history"),
            "RecreateTarget" => {
                self.begin("recreate_target");
                self.state.current_target = false;
            }
            "ReleaseBoundary" => {
                self.begin("delete_build");
                self.build = None;
                self.state.rows = Self::default().state.rows;
            }
            "Restart" => {
                self.begin("restart");
                if let Some(build) = self.build.take() {
                    let bytes = storage_types::storage_serde::to_bytes(&build)?;
                    self.build = Some(storage_types::storage_serde::from_bytes(&bytes)?);
                }
            }
            "ConflictingObservation" => self.adversarial(step)?,
            name => return Err(anyhow!("unhandled GSI model action {name}")),
        }
        Ok(())
    }
}

impl GsiDriver {
    fn begin(&mut self, event: &str) {
        self.state.before_rows = self.state.rows.clone();
        self.state.before_cursor = self.state.cursor;
        self.state.observations.clear();
        self.state.writes.clear();
        self.state.event = event.into();
        self.state.outcome = "idle".into();
    }

    fn mutate(&mut self, entity: i64, index: i64) -> Result {
        self.begin("source");
        let source = self
            .state
            .sources
            .get_mut(&entity)
            .ok_or_else(|| anyhow!("unknown source"))?;
        self.state.last_mutation = if index < 0 {
            "delete"
        } else if source.index >= 0 && source.index != index {
            "move"
        } else {
            "put"
        }
        .into();
        source.version += 2;
        self.state.history.push(Effect {
            entity,
            before: source.index,
            after: index,
            version: source.version,
        });
        source.index = index;
        Ok(())
    }

    fn capture(&mut self) {
        self.begin("capture");
        self.build = Some(GsiBuild::new(
            target(),
            checkpoint(self.state.history.len() as u64),
        ));
        self.state.rows = Self::default().state.rows;
        self.state.current_target = true;
        self.state.history_available = true;
        self.state.outcome = "persist".into();
    }

    fn scan_source(&mut self) -> Result {
        self.begin("scan");
        let source = &self.state.sources[&(self.state.scan as i64)];
        if source.index >= 0 {
            self.state.observations.push(Observation {
                key: self.state.scan as i64 * 2 + source.index,
                version: source.version,
                live: true,
            });
        }
        self.scan_observations()
    }

    fn scan_observations(&mut self) -> Result {
        let after = (self.state.scan > 0).then_some(self.state.scan);
        let mut port = Observations::new([Read::Scan(after)]);
        let projections = self.projections()?;
        port.scan = Some(if self.state.history_available {
            Ok(GsiScanObservation {
                expected_after: after,
                end: GsiScanPageEnd::Continue(self.state.scan + 1),
                projections,
            })
        } else {
            Err(storage_types::StorageEnum::HistoryUnavailable.into())
        });
        self.evaluate(port)
    }

    fn finish_scan(&mut self) -> Result {
        self.begin("finish_scan");
        let high = self.state.history.len() as u64;
        let mut port = Observations::new([
            Read::Scan(Some(self.state.scan)),
            Read::Frontier(self.state.cursor, high),
        ]);
        port.scan = Some(scan(
            Some(self.state.scan),
            GsiScanPageEnd::Complete(checkpoint(high)),
        ));
        port.history_available = self.state.history_available;
        self.evaluate(port)
    }

    fn read_stream(&mut self, limit: u64) -> Result {
        self.begin("stream");
        let high = if self.state.phase == "catching_up" || self.state.pending {
            self.state.high
        } else {
            self.state.history.len() as u64
        };
        let through = (self.state.cursor + limit).min(high);
        for effect in &self.state.history[self.state.cursor as usize..through as usize] {
            if effect.before >= 0 && effect.before != effect.after {
                self.state.observations.push(Observation {
                    key: effect.entity * 2 + effect.before,
                    version: effect.version,
                    live: false,
                });
            }
            if effect.after >= 0 {
                self.state.observations.push(Observation {
                    key: effect.entity * 2 + effect.after,
                    version: effect.version,
                    live: true,
                });
            }
        }
        let mut reads = vec![];
        if self.state.phase == "active" && !self.state.pending {
            reads.push(Read::Tail);
        }
        reads.push(Read::Stream(self.state.cursor, high));
        let mut port = Observations::new(reads);
        port.tail = self.state.history.len() as u64;
        let end = if through < high {
            GsiStreamPageEnd::Continue(checkpoint(through))
        } else {
            GsiStreamPageEnd::ReachedThrough
        };
        let mut page =
            stream(self.state.cursor, high, end).map_err(|e| anyhow!("stream fixture: {e:?}"))?;
        page.projections = self.projections()?;
        port.stream = Some(if self.state.history_available {
            Ok(page)
        } else {
            Err(storage_types::StorageEnum::HistoryUnavailable.into())
        });
        self.evaluate(port)
    }

    fn activate(&mut self) -> Result {
        self.begin("activate");
        let mut reads = vec![Read::Frontier(self.state.cursor, self.state.high)];
        if self.state.history_available {
            reads.push(Read::Activation(self.state.high));
        }
        let mut port = Observations::new(reads);
        port.history_available = self.state.history_available;
        if !self.state.current_target {
            port.incarnation = [2; 16].into();
        }
        self.evaluate(port)
    }

    fn adversarial(&mut self, step: &Step) -> Result {
        self.begin("adversarial");
        let key = pick(step, "key")?;
        let row = &self.state.rows[&key];
        self.state.observations.push(Observation {
            key,
            version: row.version,
            live: !row.live,
        });
        self.scan_observations()
    }

    fn projections(&self) -> Result<Vec<GsiBuildProjectionObservation>> {
        self.state.observations.iter().map(|input| {
            let key = projection_key(input.key);
            let item_version = ItemRevision::try_from(input.version)?;
            let source = if input.live {
                let wire = WireItem::dynamo_json(serde_json::to_vec(&serde_json::json!({"pk": {"S": (input.key / 2).to_string()}, "group": {"S": (input.key % 2).to_string()}}))?);
                GsiSourceProjection::Present { item_version, row: Box::new(GsiProjectedRow { key: key.clone(), source_item: wire, projection: storage_types::FrozenImageProjection::All, indexers: storage_types::IndexerDeclaration::try_new(vec![], storage_types::MaxIndexers::ZERO)? }) }
            } else { GsiSourceProjection::Missing { item_version, previous_key: Some(Box::new(key.clone())) } };
            let row = &self.state.rows[&input.key];
            let current = if row.version == 0 { None } else { Some(GsiProjection { item_version: ItemRevision::try_from(row.version)?, projection: row.live.then_some(key) }) };
            Ok(GsiBuildProjectionObservation { source, current })
        }).collect()
    }

    fn evaluate(&mut self, mut port: Observations) -> Result {
        let build = self
            .build
            .as_mut()
            .ok_or_else(|| anyhow!("build missing"))?;
        let change = futures::executor::block_on(port.evaluate(build))
            .map_err(|e| anyhow!("GSI evaluation: {e:?}"))?;
        match change {
            GsiChange::Idle => self.state.outcome = "idle".into(),
            GsiChange::Superseded => self.state.outcome = "superseded".into(),
            GsiChange::Persist {
                projections,
                next_state,
            } => {
                self.state.outcome = match &next_state {
                    GsiState::Blocked(s) if s.reason == GsiFailureReason::HistoryUnavailable => {
                        "history_blocked"
                    }
                    GsiState::Blocked(_) => "projection_blocked",
                    _ => "persist",
                }
                .into();
                for write in projections {
                    let key = (0..4)
                        .find(|key| projection_key(*key) == write.key)
                        .ok_or_else(|| anyhow!("unknown projection key"))?;
                    let version = i64::try_from(write.item_version)?;
                    let live = write.row.is_some();
                    self.state.writes.push(Observation { key, version, live });
                    self.state.rows.insert(key, Row { version, live });
                }
                build.state = next_state;
            }
        }
        Ok(())
    }
}

fn projection_key(key: i64) -> GsiProjectionKey {
    let mut index_key = KeyAttributes::new();
    index_key.insert("group", AttributeValue::S((key % 2).to_string()));
    let mut table_item_key = KeyAttributes::new();
    table_item_key.insert("pk", AttributeValue::S((key / 2).to_string()));
    GsiProjectionKey {
        index_name: target().index_name,
        index_key,
        table_item_key,
        ttl_index_key: None,
    }
}

fn pick(step: &Step, name: &str) -> Result<i64> {
    let value = step
        .nondet_picks
        .get(name)
        .ok_or_else(|| anyhow!("missing GSI choice {name}"))?;
    i64::deserialize(value.clone()).map_err(Into::into)
}

#[quint_run(
    spec = "../../quint/gsi_backfill_catchup.qnt",
    main = "gsi_backfill_catchup",
    init = "init",
    step = "step",
    max_samples = 500,
    max_steps = 32,
    seed = "0x11503"
)]
fn given_gsi_model_trace_when_shared_executor_runs_then_phase_frontier_and_effects_match()
-> impl Driver {
    GsiDriver::default()
}
