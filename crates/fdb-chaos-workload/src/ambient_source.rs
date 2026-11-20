//! Simulated ambient time and randomness for the storage crates.
//!
//! Storage code that reads `TimestampMillis::now` or generates ids and scan
//! rotations uses `storage_types::ambient`. The first workload instance of the
//! process installs a source backed by simulated time and a generator seeded
//! from the simulator, so a seed replays the same run.

use std::sync::Once;

use foundationdb::env::{Clock as _, Rng as _, SeededRng};
use foundationdb_simulation::{SimClock, WorkloadContext};
use storage_types::ambient::{AmbientSource, install_ambient_source};

/// Simulated second zero is 2026-01-01T00:00:00Z, so stored timestamps have
/// the width and order of real ones.
const SIMULATION_EPOCH_MILLIS: i64 = 1_767_225_600_000;

struct SimulationAmbient {
    /// Holds a copy of the first workload context. The simulator keeps every
    /// workload instance until the test ends, and storage code runs only
    /// inside workload phases.
    clock: SimClock,
    rng: SeededRng,
}

impl AmbientSource for SimulationAmbient {
    fn now_millis(&self) -> i64 {
        let elapsed = i64::try_from(self.clock.monotonic().as_millis()).unwrap_or(i64::MAX);
        SIMULATION_EPOCH_MILLIS.saturating_add(elapsed)
    }

    fn next_u64(&self) -> u64 {
        self.rng.next_u64()
    }
}

pub(crate) fn install_simulation_ambient(context: &WorkloadContext) {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let seed = (u64::from(context.rnd()) << 32) | u64::from(context.rnd());
        install_ambient_source(Box::new(SimulationAmbient {
            clock: SimClock::new(context),
            rng: SeededRng::new(seed),
        }));
    });
}
