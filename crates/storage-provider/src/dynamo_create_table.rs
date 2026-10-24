//! One create-table decision across SQL and KV native attempts.

use std::future::Future;

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_types::{
    CreateTableResponse, IndexName, StorageError, StorageResult, StoredTableInfo,
    SystemStreamCheckpoint, TableDescription, TableStatus, maintenance::TargetIncarnation,
};

use crate::{
    SemanticDecision, StreamTrimStateWrite, name_reservation::TableNameReservation,
    plan_table_stream_duration,
};

/// One physical read result. `seed` carries any backend state read alongside
/// the reservation, such as the KV table-ID allocator, into the insert step.
pub struct CreateTableLayout<Seed> {
    pub reservation: TableNameReservation,
    pub seed: Seed,
}

/// Family lowerers read and persist physical effects on the native attempt
/// already lent by their backend. The shared executor owns name availability.
pub trait CreateTableAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    type Identity: Send;
    type LayoutSeed: Send;

    fn read_layout(
        &mut self,
        table: &StoredTableInfo,
    ) -> impl Future<Output = StorageResult<CreateTableLayout<Self::LayoutSeed>>> + Send;

    /// Return `None` when a conditional physical insert lost the name race.
    fn insert_layout(
        &mut self,
        table: &StoredTableInfo,
        seed: Self::LayoutSeed,
    ) -> impl Future<Output = StorageResult<Option<Self::Identity>>> + Send;

    fn retention_scope_id(&self, table: &StoredTableInfo, identity: &Self::Identity) -> String;

    fn write_retention(
        &mut self,
        identity: &Self::Identity,
        write: StreamTrimStateWrite,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn capture_high(
        &mut self,
    ) -> impl Future<Output = StorageResult<SystemStreamCheckpoint>> + Send;

    fn index_incarnation(
        &self,
        identity: &Self::Identity,
        index: &IndexName,
    ) -> StorageResult<TargetIncarnation>;

    fn register_gsis(
        &mut self,
        identity: &Self::Identity,
        builds: Vec<GsiBuild<()>>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

/// Execute all create-table effects before the outer driver settles the
/// attempt. The caller samples `created_at` once before retries and
/// invalidates its metadata cache after acknowledged commit. Each attempt
/// generates its own physical identity; an unknown commit outcome is never
/// retried. Request validation has produced this table.
pub async fn create_table_in_attempt<A: CreateTableAttempt>(
    attempt: &mut A,
    table: StoredTableInfo,
) -> StorageResult<SemanticDecision<CreateTableResponse>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let layout = attempt.read_layout(&table).await?;
        if !layout.reservation.is_available(table.created_at) {
            return Ok(SemanticDecision::Rejected(
                StorageError::table_already_exists(&table.table_name),
            ));
        }
        let identity = attempt
            .insert_layout(&table, layout.seed)
            .await?
            .ok_or_else(|| StorageError::table_already_exists(&table.table_name))?;
        let scope = attempt.retention_scope_id(&table, &identity);
        let retention = plan_table_stream_duration(
            table.table_name.clone(),
            scope,
            1,
            table.table_stream_duration,
            table.default_item_stream_duration,
            table.created_at,
        );
        let retention_write = StreamTrimStateWrite {
            state: retention.trim_state,
            next_marker: retention.due_marker,
        };
        // These effects share a mutable native attempt. KV staging is local;
        // SQL uses one connection, so concurrent calls cannot reduce latency.
        attempt.write_retention(&identity, retention_write).await?;
        register_initial_gsis(attempt, &table, identity).await?;
        Ok(SemanticDecision::Accepted(CreateTableResponse {
            table_description: TableDescription::from_stored_table_info(table, TableStatus::Active),
        }))
    }
    .await;
    settlement.complete(result)
}

async fn register_initial_gsis<A: CreateTableAttempt>(
    attempt: &mut A,
    table: &StoredTableInfo,
    identity: A::Identity,
) -> StorageResult<()> {
    let Some(indexes) = table
        .global_secondary_indexes
        .as_ref()
        .filter(|indexes| !indexes.is_empty())
    else {
        return Ok(());
    };
    let frontier = attempt.capture_high().await?;
    let builds = indexes
        .iter()
        .map(|index| {
            Ok(GsiBuild::new_active(
                GsiBuildTarget {
                    table_name: table.table_name.clone(),
                    index_name: index.index_name.clone(),
                    incarnation: attempt.index_incarnation(&identity, &index.index_name)?,
                },
                frontier.clone(),
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    attempt.register_gsis(&identity, builds).await
}
