//! The regional wiring of one connection, built once and shared by the
//! pair-job handler and the peer-facing service.
use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use regional_replication::{RegionalConfig, RegionalPeerClient};
use storage_provider::RegionalOperations;

use super::lag::PairLag;
use crate::constants::{REGIONAL_BACKOFF_CAP, REGIONAL_LAG_WINDOW_TURNS};

pub(crate) struct RegionalPair {
    pub(crate) ops: Arc<dyn RegionalOperations>,
    pub(crate) client: RegionalPeerClient,
    pub(crate) config: Arc<RegionalConfig>,
    pub(crate) lag: PairLag,
    /// Consecutive failed local turns on this connection, any pair. Drives the
    /// local backoff and its log; reset by any successful turn. In process
    /// only.
    pub(crate) local_failures: AtomicU32,
}

impl RegionalPair {
    pub(crate) fn new(
        connection_id: &str,
        config: Arc<RegionalConfig>,
        ops: Arc<dyn RegionalOperations>,
    ) -> Arc<Self> {
        // A pair whose turns run here is observed again within its longest
        // retry delay plus one turn (at most one lease).
        let window = config
            .send_interval
            .max(REGIONAL_BACKOFF_CAP)
            .saturating_mul(REGIONAL_LAG_WINDOW_TURNS)
            .saturating_add(turn_lease(&config));
        Arc::new(Self {
            ops,
            client: RegionalPeerClient::new(Arc::clone(&config)),
            lag: PairLag::new(connection_id, window),
            local_failures: AtomicU32::new(0),
            config,
        })
    }

    /// Claim lease of one pair-job turn.
    pub(crate) fn lease(&self) -> Duration {
        turn_lease(&self.config)
    }
}

fn turn_lease(config: &RegionalConfig) -> Duration {
    config
        .request_timeout
        .saturating_mul(4)
        .max(Duration::from_secs(1))
}
