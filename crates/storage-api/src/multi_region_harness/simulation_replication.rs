use std::sync::Arc;

use async_trait::async_trait;
use http_error::HttpApiError;
use storage_types::{
    StorageEnum, StorageError, StorageResult, TimestampMillis, context::WrappedError as _,
};
use tokio::task::JoinSet;

use crate::{
    manager::SyncHealthReporter,
    multi_region_harness::{
        simulation::SimulationHarness,
        simulation_network::{ApplyQueueMode, SimulatedLinkState, lock_simulation_network},
        simulation_peer::apply_request_to_region,
    },
    replication_runtime::StorageReplicationRuntime,
};

impl SimulationHarness {
    pub async fn all_replica_regions_applied_commit(
        &self,
        source_region: &str,
        source_commit_ts: TimestampMillis,
    ) -> StorageResult<bool> {
        for region_name in &self.region_order {
            if region_name == source_region {
                continue;
            }
            let Some(status) = self
                .region(region_name)?
                .db
                .get_peer_replication_status(source_region)
                .await?
            else {
                return Ok(false);
            };
            let Some(last_received_commit_ts) = status.last_received_commit_ts else {
                return Ok(false);
            };
            if last_received_commit_ts < source_commit_ts {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn step_all_regions(&self, send_heartbeat: bool) -> StorageResult<bool> {
        let mut region_steps = JoinSet::new();
        for region_name in &self.region_order {
            let harness = self.clone();
            let region_name = region_name.clone();
            region_steps
                .spawn(async move { harness.step_region(&region_name, send_heartbeat).await });
        }

        let mut made_progress = false;
        while let Some(step_result) = region_steps.join_next().await {
            made_progress |= step_result
                .map_err(|error| StorageError::internal(&format!("join region step: {error}")))??;
        }
        Ok(made_progress)
    }

    pub async fn step_region(
        &self,
        region_name: &str,
        send_heartbeat: bool,
    ) -> StorageResult<bool> {
        let region = self.region(region_name)?;
        let mut runtime = StorageReplicationRuntime::new(
            Arc::clone(&region.db),
            region.config.clone(),
            Arc::clone(&region.client),
        );
        if let Some(role) = region.sync_role.clone() {
            runtime =
                runtime.with_sync_health_reporter(Arc::new(SimulationSyncHealthReporter { role }));
        }
        let mut made_progress = false;
        for peer in &region.config.peers {
            match runtime.run_peer_once(peer, send_heartbeat).await {
                Ok(progressed) => made_progress |= progressed,
                Err(error) if is_transient_simulation_fault(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(made_progress)
    }

    pub async fn run_until_idle(&self, max_rounds: usize) -> StorageResult<()> {
        for _ in 0..max_rounds {
            if !self.step_all_regions(true).await? {
                break;
            }
            self.flush_all_queued_applies(false).await?;
        }
        Ok(())
    }

    pub async fn flush_queued_applies(
        &self,
        source_region: &str,
        destination_region: &str,
        reverse: bool,
    ) -> StorageResult<()> {
        let queued = {
            let mut network = lock_simulation_network(&self.network);
            let link = network
                .links
                .entry((source_region.to_string(), destination_region.to_string()))
                .or_default();
            let mut queued = std::mem::take(&mut link.queued_applies);
            if reverse {
                queued.reverse();
            }
            queued
        };

        let destination = self.region(destination_region)?.db.clone();
        for request in queued {
            let _ = apply_request_to_region(destination.clone(), request).await?;
        }
        Ok(())
    }

    pub async fn flush_all_queued_applies(&self, reverse: bool) -> StorageResult<()> {
        for source_region in &self.region_order {
            for destination_region in &self.region_order {
                if source_region == destination_region {
                    continue;
                }
                self.flush_queued_applies(source_region, destination_region, reverse)
                    .await?;
            }
        }
        Ok(())
    }

    pub fn block_link(&self, source_region: &str, destination_region: &str, blocked: bool) {
        self.with_link(source_region, destination_region, |link| {
            link.blocked = blocked;
        });
    }

    pub fn queue_link(&self, source_region: &str, destination_region: &str, queue_applies: bool) {
        self.with_link(source_region, destination_region, |link| {
            link.apply_queue_mode = if queue_applies {
                ApplyQueueMode::ManualReorderQueue
            } else {
                ApplyQueueMode::None
            };
        });
    }

    pub fn drop_next_apply(&self, source_region: &str, destination_region: &str) {
        self.with_link(source_region, destination_region, |link| {
            link.drop_next_apply = true;
        });
    }

    pub fn duplicate_next_apply(&self, source_region: &str, destination_region: &str) {
        self.with_link(source_region, destination_region, |link| {
            link.duplicate_next_apply = true;
        });
    }

    pub fn accept_token(&self, source_region: &str, destination_region: &str, token: &str) {
        self.with_link(source_region, destination_region, |link| {
            link.accepted_tokens.insert(token.to_string());
        });
    }

    pub fn revoke_token(&self, source_region: &str, destination_region: &str, token: &str) {
        self.with_link(source_region, destination_region, |link| {
            link.accepted_tokens.remove(token);
        });
    }

    pub fn rotate_outbound_token(
        &mut self,
        source_region: &str,
        destination_region: &str,
        token: &str,
    ) {
        let Some(region) = self.regions.get_mut(source_region) else {
            return;
        };
        if let Some(peer) = region
            .config
            .peers
            .iter_mut()
            .find(|peer| peer.region_name == destination_region)
        {
            peer.service_token = token.to_string();
        }
    }

    fn with_link(
        &self,
        source_region: &str,
        destination_region: &str,
        update: impl FnOnce(&mut SimulatedLinkState),
    ) {
        let mut network = lock_simulation_network(&self.network);
        let link = network
            .links
            .entry((source_region.to_string(), destination_region.to_string()))
            .or_default();
        update(link);
    }
}

fn is_transient_simulation_fault(error: &StorageError) -> bool {
    let StorageEnum::InternalServerError { message } = error.to_enum() else {
        return false;
    };
    matches!(
        message.as_str(),
        "simulation dropped apply"
            | "simulation queued apply"
            | "simulation partition"
            | "simulation heartbeat fault"
    )
}

struct SimulationSyncHealthReporter {
    role: storage_sync::SyncRaftRole,
}

#[async_trait]
impl SyncHealthReporter for SimulationSyncHealthReporter {
    async fn sync_health(&self) -> Result<storage_sync::SyncHealthResponse, HttpApiError> {
        let mut health = storage_sync::SyncHealthResponse::disabled();
        health.role = self.role.clone();
        Ok(health)
    }
}
