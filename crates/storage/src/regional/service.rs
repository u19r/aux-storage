use std::sync::Arc;

use async_trait::async_trait;
use regional_replication::{
    RegionalRequestHandler,
    message::{PairTable, PrepareTarget, RegionalRejection, RegionalRequest, RegionalResponse},
};
use storage_types::{PairState, RegionId, StorageError, StorageResult, TableName};

use super::{PairJobs, RegionalPair};

/// The peer-facing service of one connection; shares the connection's
/// [`RegionalPair`] with its pair-job handler.
pub(crate) struct RegionalService {
    pub(crate) pair: Arc<RegionalPair>,
    pub(crate) jobs: PairJobs,
}

/// Logs a failed peer request and answers `Unavailable`: the source repeats the
/// request, and every handled request is idempotent. The data steps decode
/// items, so their line prints the class, the label and the attempt failure,
/// never the error; the catalog and job steps print the error's Display (code
/// text). The Debug form is never logged.
pub(crate) fn unavailable(step: &'static str, error: StorageError) -> RegionalResponse {
    let class = error.class().as_str();
    if !matches!(
        step,
        "prepare_target"
            | "activate_target"
            | "finish_pair"
            | "force_unpair"
            | "begin_leave"
            | "history_heads"
            | "ensure_pair_job"
    ) {
        match error.attempt_failure() {
            Some(failure) => tracing::warn!(step, class, %failure, "regional request failed"),
            None => {
                tracing::warn!(step, class, kind = error.label(), "regional request failed");
            }
        }
    } else {
        tracing::warn!(step, class, %error, "regional request failed");
    }
    RegionalResponse::Rejected(RegionalRejection::Unavailable)
}

#[async_trait]
impl RegionalRequestHandler for RegionalService {
    async fn handle(&self, _from: RegionId, request: RegionalRequest) -> RegionalResponse {
        match request {
            RegionalRequest::Page(page) => self
                .pair
                .ops
                .receive_page(page)
                .await
                .unwrap_or_else(|e| unavailable("receive_page", e)),
            RegionalRequest::Prepare(request) => self.prepare(request).await,
            RegionalRequest::Scan(page) => self
                .pair
                .ops
                .apply_scan_page(page)
                .await
                .unwrap_or_else(|e| unavailable("apply_scan_page", e)),
            RegionalRequest::Activate(target) => self
                .pair
                .ops
                .activate_target(target)
                .await
                .unwrap_or_else(|e| unavailable("activate_target", e)),
            RegionalRequest::Leave(target) => self.leave(target).await,
            RegionalRequest::Finish(request) => self
                .pair
                .ops
                .finish_pair(request)
                .await
                .unwrap_or_else(|e| unavailable("finish_pair", e)),
            RegionalRequest::Force(target) => match self
                .pair
                .ops
                .force_unpair(&target.table, Some(target.pair))
                .await
            {
                Ok(true) => RegionalResponse::Forced,
                Ok(false) => RegionalResponse::Rejected(RegionalRejection::UnknownPair),
                Err(error) => unavailable("force_unpair", error),
            },
        }
    }
}

impl RegionalService {
    async fn prepare(&self, request: PrepareTarget) -> RegionalResponse {
        let (table, pair) = (request.table.table_name.clone(), request.pair);
        let response = match self
            .pair
            .ops
            .prepare_target(
                request,
                self.pair.config.local.id,
                self.pair.config.peer.id,
                &self.pair.config.peer.name,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => return unavailable("prepare_target", error),
        };
        if matches!(response, RegionalResponse::Prepared)
            && let Err(error) = self.jobs.ensure(&table, pair).await
        {
            return unavailable("ensure_pair_job", error); // the source repeats Prepare; both steps are idempotent
        }
        response
    }

    async fn leave(&self, target: PairTable) -> RegionalResponse {
        match self.pair.ops.begin_leave(&target).await {
            Ok(Some(PairState::Leaving)) => {}
            Ok(Some(_)) => return RegionalResponse::Rejected(RegionalRejection::Conflict),
            Ok(None) => return RegionalResponse::Rejected(RegionalRejection::UnknownPair),
            Err(error) => return unavailable("begin_leave", error),
        }
        if let Err(error) = self.jobs.ensure(&target.table, target.pair).await {
            return unavailable("ensure_pair_job", error);
        }
        match self
            .pair
            .ops
            .history_heads(&target.table, target.pair)
            .await
        {
            Ok(Some(last)) => RegionalResponse::Left { last },
            Ok(None) => RegionalResponse::Rejected(RegionalRejection::UnknownPair),
            Err(error) => unavailable("history_heads", error),
        }
    }

    /// UpdateTable ReplicaUpdates Delete. The peer's name: graceful removal of
    /// the peer. The local name: force (end here now; tell the peer best
    /// effort).
    pub(crate) async fn delete_replica(
        &self,
        table: &TableName,
        region_name: &str,
    ) -> StorageResult<()> {
        let binding = self
            .pair
            .ops
            .current_binding(table)
            .await?
            .ok_or_else(|| StorageError::validation("the table has no replica"))?;
        let target = PairTable {
            pair: binding.pair,
            table: table.clone(),
        };
        if region_name == self.pair.config.local.name {
            self.pair
                .ops
                .force_unpair(table, Some(binding.pair))
                .await?;
            match self.pair.client.call(RegionalRequest::Force(target)).await {
                Ok(
                    RegionalResponse::Forced
                    | RegionalResponse::Rejected(RegionalRejection::UnknownPair),
                ) => {}
                other => {
                    tracing::warn!(%table, response = ?other.as_ref().ok(), "regional force not delivered to the peer")
                }
            }
            return Ok(());
        }
        if region_name != self.pair.config.peer.name {
            return Err(StorageError::validation(format!(
                "unknown replica region '{region_name}'"
            )));
        }
        match self.pair.client.call(RegionalRequest::Leave(target)).await {
            Ok(RegionalResponse::Left { .. }) => Ok(()),
            Ok(RegionalResponse::Rejected(RegionalRejection::Conflict)) => {
                Err(StorageError::validation(
                    "the replica is not active; delete the local region's replica to force the end",
                ))
            }
            Ok(RegionalResponse::Rejected(RegionalRejection::UnknownPair)) => {
                Err(StorageError::validation(
                    "the replica region does not carry this pair; delete the local region's \
                     replica to end it here",
                ))
            }
            other => {
                tracing::warn!(%table, response = ?other.as_ref().ok(), "regional leave failed");
                Err(StorageError::validation(format!(
                    "region '{region_name}' is unreachable; delete the local region's replica to \
                     force the end"
                )))
            }
        }
    }

    /// UpdateTable ReplicaUpdates Create at the source.
    pub(crate) async fn create_replica(
        &self,
        table: &TableName,
        region_name: &str,
    ) -> StorageResult<()> {
        if region_name != self.pair.config.peer.name {
            return Err(StorageError::validation(format!(
                "unknown replica region '{region_name}'"
            )));
        }
        let pair = self
            .pair
            .ops
            .begin_pairing(
                table,
                self.pair.config.local.id,
                self.pair.config.peer.id,
                &self.pair.config.peer.name,
            )
            .await?;
        let request = self
            .pair
            .ops
            .prepare_request(table, pair)
            .await?
            .ok_or_else(|| StorageError::validation("the table's replica changed concurrently"))?;
        match self
            .pair
            .client
            .call(RegionalRequest::Prepare(request))
            .await
        {
            Ok(RegionalResponse::Prepared) => {}
            Ok(RegionalResponse::Rejected(rejection))
                if rejection != RegionalRejection::Unavailable =>
            {
                self.pair.ops.abandon_pairing(table, pair).await?;
                return Err(StorageError::validation(match rejection {
                    RegionalRejection::TableExists => {
                        format!("table '{table}' already exists in region '{region_name}'")
                    }
                    other => format!("region '{region_name}' refused the replica: {other:?}"),
                }));
            }
            // Unknown at the target: the pair job repeats Prepare.
            other => {
                tracing::warn!(%table, response = ?other.as_ref().ok(), "regional prepare deferred to the pair job")
            }
        }
        self.jobs.ensure(table, pair).await
    }
}
