use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use storage::DatabaseManager;
use storage_types::{
    ReplicationApplyRequest, ReplicationApplyResponse, ReplicationHeartbeatRequest,
    ReplicationHeartbeatResponse, StorageEnum, StorageError, StorageResult, TimestampMillis,
};

use crate::{
    multi_region_harness::simulation_network::{
        SimulatedDecision, SimulationNetworkState, lock_simulation_network, sleep_if_needed,
    },
    replication_logical_import::enforce_logical_backfill_import_preflight,
    replication_runtime::{ReplicationPeerClient, ReplicationPeerConfig},
    types::{ReplicationLogicalBackfillImportRequest, ReplicationLogicalBackfillImportResponse},
};

pub(super) struct SimulationPeerClient {
    pub(super) origin_region: String,
    pub(super) regions: HashMap<String, Arc<DatabaseManager>>,
    pub(super) network: Arc<std::sync::Mutex<SimulationNetworkState>>,
}

#[async_trait]
impl ReplicationPeerClient for SimulationPeerClient {
    async fn apply(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationApplyRequest,
    ) -> StorageResult<ReplicationApplyResponse> {
        let decision = {
            let mut network = lock_simulation_network(&self.network);
            network.apply_decision(
                &self.origin_region,
                &peer.region_name,
                &peer.service_token,
                request.clone(),
            )
        };

        match decision {
            SimulatedDecision::Deliver { duplicate, delay } => {
                sleep_if_needed(delay).await;
                let destination = self.destination(peer)?;
                let response =
                    apply_request_to_region(destination.clone(), request.clone()).await?;
                if duplicate {
                    let _ = apply_request_to_region(destination, request.clone()).await?;
                }
                Ok(response)
            }
            SimulatedDecision::Drop { delay } => {
                sleep_if_needed(delay).await;
                Err(StorageError::internal("simulation dropped apply"))
            }
            SimulatedDecision::ManualReorderQueued { delay } => {
                sleep_if_needed(delay).await;
                Err(StorageError::internal("simulation queued apply"))
            }
            SimulatedDecision::ProbabilisticDelay { delay } => {
                sleep_if_needed(delay).await;
                let destination = self.destination(peer)?;
                apply_request_to_region(destination, request.clone()).await
            }
            SimulatedDecision::RejectToken => {
                Err(StorageError::Base(StorageEnum::Authentication {
                    message: "simulation token rejected".to_string(),
                }))
            }
            SimulatedDecision::Blocked => Err(StorageError::internal("simulation partition")),
        }
    }

    async fn heartbeat(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationHeartbeatRequest,
    ) -> StorageResult<ReplicationHeartbeatResponse> {
        let decision = {
            let mut network = lock_simulation_network(&self.network);
            network.heartbeat_decision(&self.origin_region, &peer.region_name, &peer.service_token)
        };

        match decision {
            SimulatedDecision::Deliver { delay, .. } => {
                let received_at = TimestampMillis::now();
                sleep_if_needed(delay).await;
                let destination = self.destination(peer)?;
                let last_applied_commit_ts = destination
                    .get_peer_replication_status(&request.source_region)
                    .await?
                    .and_then(|status| status.last_received_commit_ts);
                Ok(ReplicationHeartbeatResponse {
                    region_name: peer.region_name.clone(),
                    received_at,
                    acknowledged_at: TimestampMillis::now(),
                    last_applied_commit_ts,
                })
            }
            SimulatedDecision::Drop { delay }
            | SimulatedDecision::ManualReorderQueued { delay }
            | SimulatedDecision::ProbabilisticDelay { delay } => {
                sleep_if_needed(delay).await;
                Err(StorageError::internal("simulation heartbeat fault"))
            }
            SimulatedDecision::RejectToken => {
                Err(StorageError::Base(StorageEnum::Authentication {
                    message: "simulation token rejected".to_string(),
                }))
            }
            SimulatedDecision::Blocked => Err(StorageError::internal("simulation partition")),
        }
    }

    async fn import_logical_backfill(
        &self,
        peer: &ReplicationPeerConfig,
        request: &ReplicationLogicalBackfillImportRequest,
    ) -> StorageResult<ReplicationLogicalBackfillImportResponse> {
        let destination = self.destination(peer)?;
        enforce_logical_backfill_import_preflight(destination.as_ref(), request).await?;
        let result = destination
            .import_logical_backfill_chunk(&request.manifest, request.chunk.clone())
            .await?;
        Ok(ReplicationLogicalBackfillImportResponse { result })
    }
}

impl SimulationPeerClient {
    fn destination(&self, peer: &ReplicationPeerConfig) -> StorageResult<Arc<DatabaseManager>> {
        self.regions
            .get(&peer.region_name)
            .cloned()
            .ok_or_else(|| StorageError::internal("simulation destination region missing"))
    }
}

pub(super) async fn apply_request_to_region(
    destination: Arc<DatabaseManager>,
    request: ReplicationApplyRequest,
) -> StorageResult<ReplicationApplyResponse> {
    let last_received_commit_ts = request
        .mutations
        .iter()
        .map(|mutation| mutation.metadata.origin_commit_ts)
        .max();
    let source_region = request.source_region.clone();
    let outcomes = destination
        .apply_replication_mutations_with_outcomes(request.mutations)
        .await?;
    let applied_mutations = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, storage::ReplicationMutationApplyOutcome::Applied))
        .count();
    let observed_at = TimestampMillis::now();
    let _ = destination
        .update_peer_replication_status(&source_region, |status| {
            status.last_received_commit_ts = last_received_commit_ts;
            status.last_inbound_apply_at = Some(observed_at);
        })
        .await?;
    Ok(ReplicationApplyResponse {
        received_mutations: outcomes.len(),
        applied_mutations,
        skipped_mutations: outcomes.len().saturating_sub(applied_mutations),
    })
}
