use std::collections::{BTreeSet, HashMap};

use http_error::HttpApiError;
use storage::{
    PeerReplicationStatusRecord, increment_multi_region_apply_total,
    record_multi_region_heartbeat_staleness, record_multi_region_replication_lag,
};
use storage_backfill::validate_logical_chunk_for_manifest;
use storage_types::{
    ReplicationApplyRequest, ReplicationApplyResponse, ReplicationHealthResponse,
    ReplicationHeartbeatRequest, ReplicationHeartbeatResponse, ReplicationPeerHealth,
    TimestampMillis,
};

use crate::{
    manager::StorageApiManagerImpl,
    replication_logical_import::enforce_logical_backfill_import_preflight,
    types::{ReplicationLogicalBackfillImportRequest, Response},
};

impl StorageApiManagerImpl {
    pub(super) async fn apply_replication_internal(
        &self,
        request: ReplicationApplyRequest,
    ) -> Result<Response, HttpApiError> {
        let source_region = self.validate_replication_source_region(&request.source_region)?;
        if request.mutations.is_empty() {
            return Err(HttpApiError::validation_error(
                "Mutations must not be empty for replication apply requests",
            ));
        }
        if request.mutations.len() > 1_000 {
            return Err(HttpApiError::validation_error(
                "Mutations must not exceed 1000 entries per replication apply request",
            ));
        }

        let mut last_received_commit_ts: Option<TimestampMillis> = None;
        let mutations = request.mutations;
        for mutation in &mutations {
            if mutation.metadata.origin_region != source_region {
                return Err(HttpApiError::validation_error(format!(
                    "Mutation OriginRegion '{}' does not match SourceRegion '{}'",
                    mutation.metadata.origin_region, source_region
                )));
            }
            last_received_commit_ts = Some(match last_received_commit_ts {
                Some(existing) => existing.max(mutation.metadata.origin_commit_ts),
                None => mutation.metadata.origin_commit_ts,
            });
        }
        let outcomes = self
            .db()
            .apply_replication_mutations_with_outcomes(mutations)
            .await?;
        let applied_mutations = outcomes
            .iter()
            .filter(|outcome| self.is_applied_replication_outcome(**outcome))
            .count();
        let skipped_mutations = outcomes.len().saturating_sub(applied_mutations);
        let observed_at = TimestampMillis::now();
        let _ = self
            .db()
            .update_peer_replication_status(&source_region, |status| {
                status.last_received_commit_ts = last_received_commit_ts;
                status.last_inbound_apply_at = Some(observed_at);
            })
            .await?;
        increment_multi_region_apply_total(&source_region, "applied", applied_mutations as u64);
        increment_multi_region_apply_total(&source_region, "skipped", skipped_mutations as u64);

        Ok(Response::ReplicationApply(ReplicationApplyResponse {
            received_mutations: applied_mutations + skipped_mutations,
            applied_mutations,
            skipped_mutations,
        }))
    }

    pub(super) async fn heartbeat_replication_internal(
        &self,
        request: ReplicationHeartbeatRequest,
    ) -> Result<Response, HttpApiError> {
        let _ = request.sent_at;
        let source_region = self.validate_replication_source_region(&request.source_region)?;
        let observed_at = TimestampMillis::now();
        let status = self
            .db()
            .update_peer_replication_status(&source_region, |status| {
                status.last_inbound_heartbeat_at = Some(observed_at);
                status.last_received_source_commit_ts = request.source_latest_commit_ts;
            })
            .await?;
        let region_name = self
            .replication_self_region_name()
            .unwrap_or_else(|| "unknown".to_string());
        Ok(Response::ReplicationHeartbeat(
            ReplicationHeartbeatResponse {
                region_name,
                received_at: observed_at,
                acknowledged_at: TimestampMillis::now(),
                last_applied_commit_ts: status.last_received_commit_ts,
            },
        ))
    }

    pub(super) async fn import_replication_logical_backfill_internal(
        &self,
        request: ReplicationLogicalBackfillImportRequest,
    ) -> Result<Response, HttpApiError> {
        let _source_region = self.validate_replication_source_region(&request.source_region)?;
        validate_logical_chunk_for_manifest(&request.manifest, &request.chunk)
            .map_err(|error| HttpApiError::validation_error(error.to_string()))?;
        enforce_logical_backfill_import_preflight(self.db(), &request).await?;
        let result = self
            .db()
            .import_logical_backfill_chunk(&request.manifest, request.chunk)
            .await?;
        let response =
            serde_json::to_value(crate::types::ReplicationLogicalBackfillImportResponse { result })
                .map_err(|error| HttpApiError::internal_server_error(error.to_string()))?;
        Ok(Response::Raw(response))
    }

    pub(super) async fn replication_health_internal(&self) -> Result<Response, HttpApiError> {
        Ok(Response::ReplicationHealth(
            self.replication_health_snapshot().await?,
        ))
    }
}

impl StorageApiManagerImpl {
    async fn replication_health_snapshot(&self) -> Result<ReplicationHealthResponse, HttpApiError> {
        let now = TimestampMillis::now();
        let statuses = self.db().list_peer_replication_statuses().await?;
        let mut peer_regions = BTreeSet::new();
        for status in &statuses {
            peer_regions.insert(status.peer_region.clone());
        }
        for config in self.db().list_table_replication_configs().await? {
            for replica in config.replicas {
                peer_regions.insert(replica.region_name);
            }
        }

        let mut status_by_peer = HashMap::new();
        for status in statuses {
            status_by_peer.insert(status.peer_region.clone(), status);
        }

        let peers = peer_regions
            .into_iter()
            .map(|peer_region| {
                build_replication_peer_health(&peer_region, status_by_peer.get(&peer_region), now)
            })
            .collect::<Vec<_>>();

        Ok(ReplicationHealthResponse {
            self_region: self.replication_self_region_name(),
            peers,
        })
    }
}

fn build_replication_peer_health(
    peer_region: &str,
    status: Option<&PeerReplicationStatusRecord>,
    now: TimestampMillis,
) -> ReplicationPeerHealth {
    let last_heartbeat_at = status.and_then(|record| record.last_inbound_heartbeat_at);
    let heartbeat_staleness_ms =
        last_heartbeat_at.map(|timestamp| timestamp_delta_ms(now, timestamp));
    if let Some(staleness_ms) = heartbeat_staleness_ms {
        record_multi_region_heartbeat_staleness(peer_region, staleness_ms);
    }

    let source_latest_commit_ts = status.and_then(|record| {
        record
            .last_received_source_commit_ts
            .or(record.last_outbound_commit_ts)
    });
    let last_received_commit_ts = status.and_then(|record| record.last_received_commit_ts);
    let applied_commit_ts = last_received_commit_ts
        .or_else(|| status.and_then(|record| record.last_remote_applied_commit_ts));
    let replication_lag_ms = match (source_latest_commit_ts, applied_commit_ts) {
        (Some(source_commit_ts), Some(applied_commit_ts)) => Some(
            source_commit_ts
                .timestamp_millis()
                .saturating_sub(applied_commit_ts.timestamp_millis()) as u64,
        ),
        _ => None,
    };
    if let Some(lag_ms) = replication_lag_ms {
        record_multi_region_replication_lag(peer_region, lag_ms);
    }

    let healthy = heartbeat_staleness_ms
        .map(|staleness_ms| {
            staleness_ms <= crate::constants::STORAGE_REPLICATION_HEARTBEAT_MISS_THRESHOLD_MS
        })
        .unwrap_or(false);

    ReplicationPeerHealth {
        region_name: peer_region.to_string(),
        healthy,
        last_heartbeat_at,
        last_heartbeat_rtt_ms: status.and_then(|record| record.last_heartbeat_rtt_ms),
        clock_offset_estimate_ms: status.and_then(|record| record.clock_offset_estimate_ms),
        clock_offset_uncertainty_ms: status.and_then(|record| record.clock_offset_uncertainty_ms),
        heartbeat_staleness_ms,
        source_latest_commit_ts,
        last_received_commit_ts,
        replication_lag_ms,
        sender_queue_depth: status.and_then(|record| record.sender_queue_depth),
        last_auth_failure_at: status.and_then(|record| record.last_auth_failure_at),
    }
}

fn timestamp_delta_ms(now: TimestampMillis, earlier: TimestampMillis) -> u64 {
    now.timestamp_millis()
        .saturating_sub(earlier.timestamp_millis())
        .unsigned_abs()
}
