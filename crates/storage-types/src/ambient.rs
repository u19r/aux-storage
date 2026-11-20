//! Ambient time and randomness for code that has no injected [`crate::Clock`].
//!
//! Production installs nothing and reads the machine clock and the operating
//! system generator. The FoundationDB simulation workload installs one
//! deterministic source per process before its first storage call, so that
//! timestamps, generated ids, scan rotations and retry jitter come from the
//! simulator and a seed replays the same run.

use std::{
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use uuid::{Builder, Uuid};

/// Time and randomness of a deterministic simulator.
pub trait AmbientSource: Send + Sync {
    /// Milliseconds since the UNIX epoch.
    fn now_millis(&self) -> i64;

    fn next_u64(&self) -> u64;
}

static SOURCE: OnceLock<Box<dyn AmbientSource>> = OnceLock::new();

/// Machine instant and source millisecond of the first simulated instant.
static INSTANT_BASE: OnceLock<(Instant, i64)> = OnceLock::new();

/// Last millisecond and counter of simulated version 7 UUIDs.
static V7_STATE: Mutex<(u64, u64)> = Mutex::new((0, 0));

/// Largest counter of a simulated version 7 UUID (42 bits, as `uuid` uses).
const V7_COUNTER_MAX: u64 = (1 << 42) - 1;

/// Install the process source. Returns `false` when one is installed already.
pub fn install_ambient_source(source: Box<dyn AmbientSource>) -> bool {
    SOURCE.set(source).is_ok()
}

#[must_use]
pub fn now_millis() -> i64 {
    match SOURCE.get() {
        Some(source) => source.now_millis(),
        None => jiff::Timestamp::now().as_millisecond(),
    }
}

/// A monotonic instant that follows the installed source, or `None` when no
/// source is installed and the caller keeps its own clock (for example Tokio
/// time, which tests can pause).
#[must_use]
pub fn source_instant() -> Option<Instant> {
    let source = SOURCE.get()?;
    let now = source.now_millis();
    let (base, base_millis) = *INSTANT_BASE.get_or_init(|| (Instant::now(), now));
    let elapsed = u64::try_from(now.saturating_sub(base_millis)).unwrap_or(0);
    Some(base + Duration::from_millis(elapsed))
}

#[must_use]
pub fn random_u64() -> u64 {
    match SOURCE.get() {
        Some(source) => source.next_u64(),
        None => rand::random(),
    }
}

#[must_use]
pub fn uuid_v4() -> Uuid {
    match SOURCE.get() {
        Some(source) => Builder::from_random_bytes(random_bytes(source.as_ref())).into_uuid(),
        None => Uuid::new_v4(),
    }
}

/// A time-ordered UUID. Under a source, ids of one millisecond keep their
/// creation order through a 42-bit counter, as `Uuid::now_v7` does.
#[must_use]
pub fn uuid_now_v7() -> Uuid {
    let Some(source) = SOURCE.get() else {
        return Uuid::now_v7();
    };
    let now = u64::try_from(source.now_millis()).unwrap_or(0);
    let (millis, counter) = {
        let mut state = V7_STATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (last, counter) = *state;
        *state = if now > last || counter >= V7_COUNTER_MAX {
            (
                now.max(last + u64::from(now <= last)),
                source.next_u64() >> 23,
            )
        } else {
            (last, counter + 1)
        };
        *state
    };
    let tail = source.next_u64().to_be_bytes();
    let mut bytes = [0_u8; 10];
    bytes[..2].copy_from_slice(&((counter >> 30) as u16).to_be_bytes());
    bytes[2..6].copy_from_slice(&((counter & 0x3fff_ffff) as u32).to_be_bytes());
    bytes[6..].copy_from_slice(&tail[..4]);
    Builder::from_unix_timestamp_millis(millis, &bytes).into_uuid()
}

fn random_bytes(source: &dyn AmbientSource) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&source.next_u64().to_be_bytes());
    bytes[8..].copy_from_slice(&source.next_u64().to_be_bytes());
    bytes
}
