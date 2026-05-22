use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use storage_types::ReplicationApplyRequest;

#[derive(Debug)]
pub(super) struct SimulationNetworkState {
    pub(super) links: HashMap<(String, String), SimulatedLinkState>,
    seed: u64,
    decision_counter: u64,
}

impl SimulationNetworkState {
    pub(super) fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            ..Self::default()
        }
    }

    pub(super) fn apply_decision(
        &mut self,
        source_region: &str,
        destination_region: &str,
        service_token: &str,
        request: ReplicationApplyRequest,
    ) -> SimulatedDecision {
        let roll = self.next_roll(source_region, destination_region, "apply");
        let link = self
            .links
            .entry((source_region.to_string(), destination_region.to_string()))
            .or_default();
        if !link.accepted_tokens.contains(service_token) {
            return SimulatedDecision::RejectToken;
        }
        if link.blocked {
            return SimulatedDecision::Blocked;
        }
        if link.drop_next_apply {
            link.drop_next_apply = false;
            return SimulatedDecision::Drop {
                delay: link.profile.apply_delay(roll),
            };
        }
        if link.apply_queue_mode == ApplyQueueMode::ManualReorderQueue {
            link.queued_applies.push(request);
            return SimulatedDecision::ManualReorderQueued {
                delay: link.profile.apply_delay(roll),
            };
        }
        let queue_roll = (roll >> 16) % 10_000;
        if queue_roll < u64::from(link.profile.queue_probability_per_10k) {
            return SimulatedDecision::ProbabilisticDelay {
                delay: link.profile.apply_delay(roll),
            };
        }
        let drop_roll = (roll >> 32) % 10_000;
        if drop_roll < u64::from(link.profile.drop_probability_per_10k) {
            return SimulatedDecision::Drop {
                delay: link.profile.apply_delay(roll),
            };
        }

        let duplicate = if link.duplicate_next_apply {
            link.duplicate_next_apply = false;
            true
        } else {
            let duplicate_roll = (roll >> 48) % 10_000;
            duplicate_roll < u64::from(link.profile.duplicate_probability_per_10k)
        };
        SimulatedDecision::Deliver {
            duplicate,
            delay: link.profile.apply_delay(roll),
        }
    }

    pub(super) fn heartbeat_decision(
        &mut self,
        source_region: &str,
        destination_region: &str,
        service_token: &str,
    ) -> SimulatedDecision {
        let roll = self.next_roll(source_region, destination_region, "heartbeat");
        let link = self
            .links
            .entry((source_region.to_string(), destination_region.to_string()))
            .or_default();
        if !link.accepted_tokens.contains(service_token) {
            return SimulatedDecision::RejectToken;
        }
        if link.blocked {
            return SimulatedDecision::Blocked;
        }
        SimulatedDecision::Deliver {
            duplicate: false,
            delay: link.profile.heartbeat_delay(roll),
        }
    }

    fn next_roll(&mut self, source_region: &str, destination_region: &str, salt: &str) -> u64 {
        self.decision_counter = self.decision_counter.saturating_add(1);
        mix64(
            self.seed
                ^ hash_str(source_region)
                ^ hash_str(destination_region)
                ^ hash_str(salt)
                ^ self.decision_counter,
        )
    }
}

impl Default for SimulationNetworkState {
    fn default() -> Self {
        Self {
            links: HashMap::new(),
            seed: 1,
            decision_counter: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SimulatedLinkState {
    pub(super) blocked: bool,
    pub(super) apply_queue_mode: ApplyQueueMode,
    pub(super) drop_next_apply: bool,
    pub(super) duplicate_next_apply: bool,
    pub(super) queued_applies: Vec<ReplicationApplyRequest>,
    pub(super) accepted_tokens: HashSet<String>,
    pub(super) profile: LinkFaultProfile,
}

impl Default for SimulatedLinkState {
    fn default() -> Self {
        Self {
            blocked: false,
            apply_queue_mode: ApplyQueueMode::None,
            drop_next_apply: false,
            duplicate_next_apply: false,
            queued_applies: Vec::new(),
            accepted_tokens: HashSet::new(),
            profile: LinkFaultProfile::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum ApplyQueueMode {
    #[default]
    None,
    ManualReorderQueue,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LinkFaultProfile {
    pub(super) apply_latency: Duration,
    pub(super) apply_latency_jitter: Duration,
    pub(super) heartbeat_latency: Duration,
    pub(super) heartbeat_latency_jitter: Duration,
    pub(super) drop_probability_per_10k: u16,
    pub(super) duplicate_probability_per_10k: u16,
    pub(super) queue_probability_per_10k: u16,
}

impl Default for LinkFaultProfile {
    fn default() -> Self {
        Self {
            apply_latency: Duration::ZERO,
            apply_latency_jitter: Duration::ZERO,
            heartbeat_latency: Duration::ZERO,
            heartbeat_latency_jitter: Duration::ZERO,
            drop_probability_per_10k: 0,
            duplicate_probability_per_10k: 0,
            queue_probability_per_10k: 0,
        }
    }
}

impl LinkFaultProfile {
    fn apply_delay(self, roll: u64) -> Duration {
        self.apply_latency + jitter_duration(self.apply_latency_jitter, roll)
    }

    fn heartbeat_delay(self, roll: u64) -> Duration {
        self.heartbeat_latency + jitter_duration(self.heartbeat_latency_jitter, roll)
    }
}

pub(super) enum SimulatedDecision {
    Deliver { duplicate: bool, delay: Duration },
    Drop { delay: Duration },
    ManualReorderQueued { delay: Duration },
    ProbabilisticDelay { delay: Duration },
    RejectToken,
    Blocked,
}

pub(super) fn lock_simulation_network(
    network: &Arc<Mutex<SimulationNetworkState>>,
) -> MutexGuard<'_, SimulationNetworkState> {
    network
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) async fn sleep_if_needed(delay: Duration) {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

fn jitter_duration(max_jitter: Duration, roll: u64) -> Duration {
    if max_jitter.is_zero() {
        return Duration::ZERO;
    }
    let jitter_ms = roll % (max_jitter.as_millis() as u64 + 1);
    Duration::from_millis(jitter_ms)
}

fn hash_str(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn mix64(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}
