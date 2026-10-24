use std::collections::BTreeMap;

use replication_types::event::identity::{CanonicalEventId, CanonicalItemKey};

use super::{
    event::{HarnessEvent, ItemMutation},
    replica::RegionalReplica,
};
use crate::HarnessResult;
#[derive(serde::Serialize)]
pub struct ConvergenceReport {
    pub seed: u64,
    pub events: usize,
    pub keys: usize,
    pub deliveries: usize,
}
/// Reproducible operation schedule, independent max-per-key and event-union
/// oracle. This checks convergence of accepted events, not equality of regional
/// DynamoDB stream delivery orders.
pub fn exercise(seed: u64, operations: usize) -> HarnessResult<ConvergenceReport> {
    if operations == 0 {
        return Err("empty convergence schedule".into());
    }
    let mut replicas = ["us-east-1", "us-east-2", "eu-central-1"]
        .into_iter()
        .map(RegionalReplica::new)
        .collect::<HarnessResult<Vec<_>>>()?;
    let mut random = Schedule(seed);
    let mut expected = BTreeMap::new();
    let mut deliveries = 0;
    for operation in 0..operations {
        let source = random.choose(replicas.len());
        let event = issue_mutation(&mut replicas[source], operation, &mut random)?;
        record_event(&mut expected, event.clone())?;
        if operation % 11 == 0 {
            let expired = event.expiration()?;
            replicas[source].accept(expired.clone())?;
            record_event(&mut expected, expired)?;
        }
        if operation % 3 == 0 {
            let target = random.choose(replicas.len());
            replicas[target].accept(event)?;
            deliveries += 1;
        }
        if operation % 31 == 0 {
            let replica = replicas.remove(source).restart()?;
            replicas.insert(source, replica);
        }
    }
    let winners = maximum_winners(&expected.values().cloned().collect::<Vec<_>>());
    deliveries += heal(&mut replicas, &mut random, &expected, &winners)?;
    Ok(ConvergenceReport {
        seed,
        events: expected.len(),
        keys: winners.len(),
        deliveries,
    })
}
fn record_event(
    events: &mut BTreeMap<CanonicalEventId, HarnessEvent>,
    event: HarnessEvent,
) -> HarnessResult<()> {
    let id = event.winner.event.clone().ok_or("event lacks identity")?;
    if let Some(old) = events.insert(id, event.clone())
        && old != event
    {
        return Err("oracle identity collision".into());
    }
    Ok(())
}
fn maximum_winners(events: &[HarnessEvent]) -> BTreeMap<CanonicalItemKey, HarnessEvent> {
    let mut winners: BTreeMap<CanonicalItemKey, HarnessEvent> = BTreeMap::new();
    for event in events {
        let entry = winners
            .entry(event.winner.key.clone())
            .or_insert_with(|| event.clone());
        if event.winner.order > entry.winner.order {
            *entry = event.clone();
        }
    }
    winners
}
struct Schedule(u64);
impl Schedule {
    fn choose(&mut self, count: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 32) as usize) % count
    }
    fn shuffle<T>(&mut self, values: &mut [T]) {
        for end in (1..values.len()).rev() {
            let other = self.choose(end + 1);
            values.swap(end, other);
        }
    }
}

fn issue_mutation(
    replica: &mut RegionalReplica,
    operation: usize,
    random: &mut Schedule,
) -> HarnessResult<HarnessEvent> {
    let key = (random.choose(32) as u64).to_be_bytes();
    let payload = (operation as u64).to_be_bytes();
    let wall = match operation % 4 {
        0 => 1_000_000,
        1 => 0,
        2 => operation as u64,
        _ => 500,
    };
    let event = replica.write_item(
        wall,
        ItemMutation {
            key: &key,
            value: (!operation.is_multiple_of(7)).then_some(payload.as_slice()),
        },
    )?;
    Ok(event)
}

fn heal(
    replicas: &mut [RegionalReplica],
    random: &mut Schedule,
    expected: &BTreeMap<CanonicalEventId, HarnessEvent>,
    winners: &BTreeMap<CanonicalItemKey, HarnessEvent>,
) -> HarnessResult<usize> {
    let mut deliveries = 0;
    let mut events = expected.values().cloned().collect::<Vec<_>>();
    for replica in replicas {
        random.shuffle(&mut events);
        for event in &events {
            for _ in 0..1 + random.choose(3) {
                replica.accept(event.clone())?;
                deliveries += 1;
            }
        }
        if replica.history() != expected || replica.winners() != winners {
            return Err("max/union convergence oracle failed".into());
        }
    }
    Ok(deliveries)
}
