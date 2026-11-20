//! One erased maintenance service over a family store. Each method runs one
//! native attempt per try through `DynamoPort`.

use std::{num::NonZeroU32, sync::Arc};

use async_trait::async_trait;
use storage_common::{TtlSweepPageCommand, TtlSweepPageResult, TtlSweepPolicy, TtlSweepRequest};
use storage_types::{StorageResult, TableName, TimestampMillis, maintenance::StepResult};
use stream_provider::{SystemStreamTrimPageRequest, SystemStreamTrimPageResult};

use super::{
    DirectExpirationTransaction, ExpirationAttempt, ExpirationPort, ExpirationRequest,
    ExpirationTransaction, GsiBuildRequest, GsiPort, StreamDurationTrimStats, StreamRetentionPort,
    StreamRetentionWave, StreamTrimDueMarker, StreamTrimScope, StreamTrimState,
    read_direct_expiration_policy_in_attempt,
};
use crate::{
    DynamoPort, ReclaimAttempt, TableReclaimStep,
    constants::{
        BACKGROUND_ITEM_PAGE, BACKGROUND_PAGE, BACKGROUND_READ_PAGE, BACKGROUND_SNAPSHOT_READ,
        SCHEMA_CHANGE,
    },
};

#[async_trait]
pub trait MaintenanceOperations: Send + Sync {
    async fn advance_expiration(&self, request: ExpirationRequest)
    -> StorageResult<StepResult<()>>;

    /// Trim one page of expired transact-write receipts; `None` ends the pass.
    async fn trim_transact_receipts_page(
        &self,
        after: Option<String>,
        limit: NonZeroU32,
    ) -> StorageResult<Option<String>>;

    async fn advance_gsi_build(&self, request: GsiBuildRequest) -> StorageResult<StepResult<()>>;

    /// Observe policy, reclaim rows, and finish markers in one native attempt.
    async fn trim_stream_marker_wave(
        &self,
        wave: StreamRetentionWave,
    ) -> StorageResult<StreamDurationTrimStats>;

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>>;

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>>;

    async fn trim_system_stream_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult>;

    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep>;

    /// The deterministic TTL page command used by native replication and the
    /// internal test helper.
    async fn read_ttl_sweep_policy(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlSweepPolicy>>;

    async fn execute_ttl_sweep_page(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult>;

    async fn execute_ttl_sweep_request(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageResult>;
}

/// One expiration page request and the bindings every try shares.
struct ExpirationInput {
    request: ExpirationRequest,
    bindings: crate::RequestTableBindings,
}

/// One receipt-trim page request.
struct TrimReceiptsInput {
    after: Option<String>,
    limit: NonZeroU32,
    at: TimestampMillis,
}

/// The page bounds of one due-marker listing.
struct DueMarkersInput {
    due_before: TimestampMillis,
    limit: usize,
}

/// One TTL sweep request and the bindings every try shares.
struct TtlSweepInput {
    request: TtlSweepRequest,
    bindings: crate::RequestTableBindings,
}

#[async_trait]
impl<P: DynamoPort> MaintenanceOperations for P
where for<'native> P::Attempt<'native>: ExpirationPort
        + GsiPort
        + StreamRetentionPort
        + stream_provider::StreamAttempt
        + ReclaimAttempt
{
    async fn advance_expiration(
        &self,
        request: ExpirationRequest,
    ) -> StorageResult<StepResult<()>> {
        let input = Arc::new(ExpirationInput {
            request,
            bindings: crate::RequestTableBindings::default(),
        });
        DynamoPort::run(self, BACKGROUND_ITEM_PAGE, input, |attempt, input, _| {
            Box::pin(async move {
                ExpirationAttempt::new(attempt, input.bindings.clone())
                    .advance_expiration(&input.request)
                    .await
            })
        })
        .await
    }

    async fn trim_transact_receipts_page(
        &self,
        after: Option<String>,
        limit: NonZeroU32,
    ) -> StorageResult<Option<String>> {
        let at = self.now();
        let input = Arc::new(TrimReceiptsInput { after, limit, at });
        DynamoPort::run(self, BACKGROUND_PAGE, input, |attempt, input, _| {
            Box::pin(async move {
                super::trim_transact_receipts_page(
                    attempt,
                    input.after.as_deref(),
                    input.limit,
                    input.at,
                )
                .await
            })
        })
        .await
    }

    async fn advance_gsi_build(&self, request: GsiBuildRequest) -> StorageResult<StepResult<()>> {
        let advance = DynamoPort::run(
            self,
            BACKGROUND_ITEM_PAGE,
            Arc::new(request),
            |attempt, request, attempt_try| {
                Box::pin(super::gsi::gsi_build_in_attempt(
                    attempt,
                    request,
                    attempt_try,
                ))
            },
        )
        .await?;
        if let Some(name) = advance.ttl_activation.map(|policy| policy.table_name) {
            self.metadata_cache().invalidate(&name);
        }
        Ok(advance.result)
    }

    async fn trim_stream_marker_wave(
        &self,
        wave: StreamRetentionWave,
    ) -> StorageResult<StreamDurationTrimStats> {
        DynamoPort::run(self, BACKGROUND_PAGE, Arc::new(wave), |attempt, wave, _| {
            Box::pin(wave.execute(attempt))
        })
        .await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let input = Arc::new(DueMarkersInput { due_before, limit });
        DynamoPort::run(self, BACKGROUND_READ_PAGE, input, |attempt, input, _| {
            Box::pin(StreamRetentionPort::list_due_stream_trim_markers(
                attempt,
                input.due_before,
                input.limit,
            ))
        })
        .await
    }

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        DynamoPort::run(
            self,
            BACKGROUND_READ_PAGE,
            Arc::new(scopes.to_vec()),
            |attempt, scopes, _| {
                Box::pin(StreamRetentionPort::load_stream_trim_states(
                    attempt, scopes,
                ))
            },
        )
        .await
    }

    async fn trim_system_stream_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        DynamoPort::run(
            self,
            BACKGROUND_PAGE,
            Arc::new(request),
            |attempt, request, _| {
                Box::pin(stream_provider::execute_trim_page(attempt, request.clone()))
            },
        )
        .await
    }

    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep> {
        storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            SCHEMA_CHANGE,
            |attempt_request, _| {
                self.with_attempt(
                    attempt_request.scoped(storage_types::AttemptScope::Schema),
                    move |attempt| Box::pin(crate::reclaim_table_page(attempt)),
                )
            },
        )
        .await
    }

    async fn read_ttl_sweep_policy(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlSweepPolicy>> {
        let table_name = table_name.clone();
        DynamoPort::with_attempt(
            self,
            BACKGROUND_SNAPSHOT_READ.start(self.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    read_direct_expiration_policy_in_attempt(
                        attempt,
                        &table_name,
                        &Default::default(),
                    )
                    .await
                    .map(|policy| {
                        policy.map(|(config, table)| TtlSweepPolicy {
                            target: table.as_ref().into(),
                            config,
                        })
                    })
                })
            },
        )
        .await
    }

    async fn execute_ttl_sweep_page(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult> {
        DynamoPort::run(
            self,
            BACKGROUND_ITEM_PAGE,
            Arc::new(command),
            |attempt, command, _| {
                Box::pin(async move {
                    ExpirationAttempt::new(attempt, Default::default())
                        .execute_direct_expiration(command)
                        .await
                })
            },
        )
        .await
    }

    async fn execute_ttl_sweep_request(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageResult> {
        let input = Arc::new(TtlSweepInput {
            request,
            bindings: crate::RequestTableBindings::default(),
        });
        DynamoPort::run(self, BACKGROUND_ITEM_PAGE, input, |attempt, input, _| {
            Box::pin(async move {
                ExpirationAttempt::new(attempt, input.bindings.clone())
                    .execute_direct_request(&input.request)
                    .await
            })
        })
        .await
    }
}
