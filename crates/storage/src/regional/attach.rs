//! Regional wiring of one native connection: the pair wiring, the peer-facing
//! service and its listener. Built only when the connection has a regional
//! file (regional off: none of this runs).
use std::sync::Arc;

use regional_replication::RegionalConfig;
use storage_provider::RegionalOperations;
use storage_types::{StorageError, StorageResult};

use super::{RegionalPair, RegionalService};
use crate::maintenance_runtime::MaintenanceRuntime;

pub(crate) struct RegionalAttachment {
    pub(crate) service: Arc<RegionalService>,
}

/// The one pair wiring of a connection, shared by its pair-job handler and its
/// service. `None` when the connection has no regional file.
pub(crate) fn regional_pair(
    connection_id: &str,
    config: Option<Arc<RegionalConfig>>,
    ops: &Arc<dyn RegionalOperations>,
) -> Option<Arc<RegionalPair>> {
    Some(RegionalPair::new(connection_id, config?, Arc::clone(ops)))
}

/// The peer-facing service over `pair`, with the pair-job capability that
/// `runtime` registered for it.
pub(crate) fn regional_attachment(
    pair: Option<Arc<RegionalPair>>,
    runtime: &MaintenanceRuntime,
) -> StorageResult<Option<RegionalAttachment>> {
    let Some(pair) = pair else {
        return Ok(None);
    };
    let jobs = runtime
        .pair_jobs()
        .ok_or_else(|| StorageError::internal("regional pair jobs are not registered"))?;
    Ok(Some(RegionalAttachment {
        service: Arc::new(RegionalService { pair, jobs }),
    }))
}

/// Bind the regional listener of one connection and start its server.
pub(crate) async fn start_regional_server(
    connection_id: &str,
    attachment: &RegionalAttachment,
) -> StorageResult<tokio::task::JoinHandle<()>> {
    let config = &attachment.service.pair.config;
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|error| {
            StorageError::validation(format!(
                "regional listener for '{connection_id}' on {}: {error}",
                config.listen
            ))
        })?;
    Ok(regional_replication::spawn_server(
        listener,
        Arc::clone(config),
        attachment.service.clone(),
    ))
}
