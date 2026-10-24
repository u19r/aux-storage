//! Reconcile traces execute the shared controller and topology owner. The test
//! port assigns monotonic split boundaries, as native commit and subsequent
//! reads do; native FDB tests separately prove versionstamp publication.
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use anyhow::{anyhow, ensure};
use quint_connect::{Driver, Result, State, Step, quint_run};
use serde::{Deserialize, Deserializer};
use storage_common::partition::{
    PartitionFamilyKind, PartitionLoadObservation, PartitionLoadSample, PartitionParentChildren,
    PartitionState, PartitionTopologyChange, ResolvedPartitionFamily,
};
use storage_types::StreamItemId;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    family: Family,
    pressure: u64,
    routing_bucket_count: u32,
    cooldown_ticks: u8,
    split_count: u64,
    observed_samples: u32,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Family {
    partitions: BTreeMap<u16, Partition>,
    live_ids: BTreeSet<u16>,
    next_id: u16,
    next_slot: u16,
    config: Config,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Partition {
    id: u16,
    slot: u16,
    state: String,
    opened_after: i64,
    sealed_after: i64,
    start: u64,
    end: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Config {
    autoscale_enabled: bool,
    freeze: bool,
    max_open: u16,
    min_open: u16,
    partition_count: u16,
    cooldown_active: bool,
    epoch: u64,
    controller: Controller,
}

#[derive(Debug)]
struct Controller {
    pressure: f64,
    integral: f64,
    high_streak: u32,
    low_streak: u32,
}

impl PartialEq for Controller {
    fn eq(&self, other: &Self) -> bool {
        // The model uses exact rationals; production uses IEEE-754 arithmetic.
        (self.pressure - other.pressure).abs() < 1e-12
            && (self.integral - other.integral).abs() < 1e-12
            && self.high_streak == other.high_streak
            && self.low_streak == other.low_streak
    }
}

impl<'de> Deserialize<'de> for Controller {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RationalController {
            pressure: i64,
            integral: i64,
            scale: i64,
            high_streak: u32,
            low_streak: u32,
        }
        let raw = RationalController::deserialize(deserializer)?;
        if raw.scale <= 0 {
            return Err(serde::de::Error::custom(
                "controller scale must be positive",
            ));
        }
        Ok(Self {
            pressure: raw.pressure as f64 / raw.scale as f64 / 10.0,
            integral: raw.integral as f64 / raw.scale as f64 / 10.0,
            high_streak: raw.high_streak,
            low_streak: raw.low_streak,
        })
    }
}

struct PartitionDriver {
    family: ResolvedPartitionFamily,
    memberships: Vec<PartitionParentChildren>,
    now: i64,
    pressure: u64,
    routing_buckets: u32,
    split_count: u64,
    observed_samples: u32,
    coverage: Rc<RefCell<BTreeSet<&'static str>>>,
}

impl Default for PartitionDriver {
    fn default() -> Self {
        let mut family = ResolvedPartitionFamily::new(
            PartitionFamilyKind::OrderedLog,
            std::num::NonZeroU16::new(2).expect("nonzero count"),
        );
        family.config.max_open_partitions = 8;
        Self {
            family,
            memberships: vec![],
            now: 0,
            pressure: 0,
            routing_buckets: 0,
            split_count: 0,
            observed_samples: 0,
            coverage: Rc::default(),
        }
    }
}

impl Driver for PartitionDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        match step.action_taken.as_str() {
            "init" => {
                let coverage = Rc::clone(&self.coverage);
                *self = Self {
                    coverage,
                    ..Self::default()
                };
            }
            "observeLoad" => self.observe(step)?,
            "expireCooldown" => {
                self.now = self
                    .family
                    .config
                    .cooldown_until_ms
                    .ok_or_else(|| anyhow!("cannot expire absent cooldown"))?;
            }
            "setPolicy" => {
                self.family.config.freeze = pick(step, "freeze")?;
                self.family.config.autoscale_enabled = pick(step, "autoscale")?;
            }
            name => return Err(anyhow!("unhandled partition action {name}")),
        }
        Ok(())
    }
}

impl PartitionDriver {
    fn observe(&mut self, step: &Step) -> Result {
        self.pressure = pick(step, "nextPressure")?;
        self.routing_buckets = pick(step, "nextRoutingBucketCount")?;
        let mut observations = vec![PartitionLoadObservation {
            partition_id: pick(step, "parentId")?,
            sample: PartitionLoadSample {
                writes: self.pressure * self.family.config.target_writes_per_second / 10,
                routing_key_bucket_bitmap: (1_u64 << self.routing_buckets) - 1,
                ..Default::default()
            },
            queue_empty: false,
        }];
        let config = &self.family.config;
        // Record only sustained hot observations, so rejection coverage cannot
        // be satisfied by an idle family which could not split anyway.
        if self.pressure >= 20 && config.controller.high_streak >= 2 {
            let mut coverage = self.coverage.borrow_mut();
            if config.freeze {
                coverage.insert("frozen");
            }
            if !config.autoscale_enabled {
                coverage.insert("disabled");
            }
            if self.routing_buckets < 2 {
                coverage.insert("single_bucket");
            }
            if config.cooldown_until_ms.is_some_and(|end| end > self.now) {
                coverage.insert("cooldown");
            }
        }
        let before_epoch = self.family.config.family_epoch;
        let result = self.family.clone().reconcile(&mut observations, self.now)?;
        ensure!(
            result.expected_epoch == before_epoch,
            "wrong observed epoch"
        );
        self.family = result.family;
        if let Some(change) = result.topology_change {
            ensure!(
                result.write_required,
                "topology effects require persistence"
            );
            self.settle_split(change)?;
        }
        self.observed_samples += 1;
        self.family.validate()?;
        Ok(())
    }

    fn settle_split(&mut self, change: PartitionTopologyChange) -> Result {
        let PartitionTopologyChange::SplitOrderedLog {
            parent_partition_id,
            left_child_partition_id,
            right_child_partition_id,
        } = change
        else {
            return Err(anyhow!("ordered log produced queue transition"));
        };
        let mut bytes = [0; 12];
        bytes[4..].copy_from_slice(&(10 + self.split_count).to_be_bytes());
        let boundary = StreamItemId::from(bytes);
        self.family
            .partition_mut(parent_partition_id)
            .ok_or_else(|| anyhow!("missing parent"))?
            .sealed_after_id = Some(boundary);
        for child in [left_child_partition_id, right_child_partition_id] {
            self.family
                .partition_mut(child)
                .ok_or_else(|| anyhow!("missing child"))?
                .opened_after_id = Some(boundary);
        }
        self.family
            .memberships
            .iter_mut()
            .find(|membership| membership.parent_partition_id == parent_partition_id)
            .ok_or_else(|| anyhow!("missing durable parent membership"))?
            .boundary = Some(boundary);
        self.family.validate()?;
        self.memberships.push(PartitionParentChildren {
            parent_partition_id,
            left_child_partition_id,
            right_child_partition_id,
            boundary: Some(boundary),
        });
        self.split_count += 1;
        self.coverage.borrow_mut().insert("split");
        Ok(())
    }
}

impl State<PartitionDriver> for ModelState {
    fn from_driver(driver: &PartitionDriver) -> Result<Self> {
        let mut partitions: BTreeMap<_, _> = (0..16)
            .map(|id| {
                (
                    id,
                    Partition {
                        id,
                        slot: id,
                        state: "retired".into(),
                        opened_after: -1,
                        sealed_after: -1,
                        start: 0,
                        end: 0,
                    },
                )
            })
            .collect();
        for member in &driver.family.partitions {
            // Every modeled split is aligned to one sixteenth of the u64 hash
            // space.
            ensure!(member.hash_start_inclusive % (1_u64 << 60) == 0);
            if let Some(end) = member.hash_end_exclusive {
                ensure!(end % (1_u64 << 60) == 0);
            }
            partitions.insert(
                member.partition_id,
                Partition {
                    id: member.partition_id,
                    slot: member.placement_slot,
                    state: match member.state {
                        PartitionState::Open => "open",
                        PartitionState::WriteClosed => "write_closed",
                        _ => return Err(anyhow!("queue state in ordered log")),
                    }
                    .into(),
                    opened_after: boundary_number(member.opened_after_id),
                    sealed_after: boundary_number(member.sealed_after_id),
                    start: member.hash_start_inclusive >> 60,
                    end: member.hash_end_exclusive.map_or(16, |end| end >> 60),
                },
            );
        }
        let config = &driver.family.config;
        let cooldown_active = config.cooldown_until_ms.is_some_and(|end| end > driver.now);
        Ok(Self {
            family: Family {
                partitions,
                live_ids: driver
                    .family
                    .partitions
                    .iter()
                    .map(|m| m.partition_id)
                    .collect(),
                next_id: driver
                    .family
                    .partitions
                    .iter()
                    .map(|m| m.partition_id)
                    .max()
                    .expect("nonempty")
                    + 1,
                next_slot: driver
                    .family
                    .partitions
                    .iter()
                    .map(|m| m.placement_slot)
                    .max()
                    .expect("nonempty")
                    + 1,
                config: Config {
                    autoscale_enabled: config.autoscale_enabled,
                    freeze: config.freeze,
                    max_open: config.max_open_partitions,
                    min_open: config.min_open_partitions,
                    partition_count: config.partition_count,
                    cooldown_active,
                    epoch: config.family_epoch,
                    controller: Controller {
                        pressure: config.controller.ewma_pressure,
                        integral: config.controller.integral,
                        high_streak: config.controller.high_streak,
                        low_streak: config.controller.low_streak,
                    },
                },
            },
            pressure: driver.pressure,
            routing_bucket_count: driver.routing_buckets,
            cooldown_ticks: u8::from(cooldown_active),
            split_count: driver.split_count,
            observed_samples: driver.observed_samples,
        })
    }
}

fn boundary_number(boundary: Option<StreamItemId>) -> i64 {
    boundary.map_or(-1, |id| {
        i64::from_be_bytes(id.as_bytes()[4..].try_into().expect("boundary width"))
    })
}

fn pick<T: serde::de::DeserializeOwned>(step: &Step, name: &str) -> Result<T> {
    let value = step
        .nondet_picks
        .get(name)
        .ok_or_else(|| anyhow!("missing partition choice {name}"))?;
    T::deserialize(value.clone()).map_err(Into::into)
}

#[test]
fn given_partition_model_trace_when_replayed_then_required_transitions_are_covered() {
    let driver = PartitionDriver::default();
    let coverage = Rc::clone(&driver.coverage);
    quint_connect::runner::run_test(
        driver,
        quint_connect::runner::Config {
            test_name: "partition_owner_correspondence".into(),
            gen_config: quint_connect::runner::RunConfig {
                spec: "../../quint/stream_partition_split.qnt".into(),
                main: Some("stream_partition_split".into()),
                init: Some("init".into()),
                step: Some("step".into()),
                max_samples: Some(500),
                max_steps: Some(16),
                seed: "0x11505".into(),
            },
        },
    )
    .expect("model traces match the shared partition owner");
    assert_eq!(
        *coverage.borrow(),
        BTreeSet::from(["split", "frozen", "disabled", "single_bucket", "cooldown",]),
        "fixed-seed traces must exercise splitting and every admission gate"
    );
}

#[quint_run(
    spec = "../../quint/stream_partition_split.qnt",
    main = "stream_partition_split",
    init = "init",
    step = "step",
    max_samples = 500,
    max_steps = 16,
    seed = "0x11505"
)]
fn given_partition_model_trace_when_shared_reconcile_runs_then_topology_and_controller_match()
-> impl Driver {
    PartitionDriver::default()
}
