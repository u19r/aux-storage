use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::identity::resource::TableIdentity;
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageResult};

use super::AdditionSourceObservation;
use crate::journal::JournalStore;

pub async fn observe_addition_source<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    table: TableIdentity,
) -> StorageResult<AdditionSourceObservation>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move { journal.observe_addition_source(attempt, table).await })
            },
        )
        .await
}

/// Reserve one actual hidden incarnation through the configured TCP peer.
/// The resulting proposal is descriptive until all native history proofs exist.
pub async fn reserve_addition_target(
    transport: &crate::transport::TcpTransport,
    plan: Arc<super::AddRegionPlan>,
) -> Result<
    replication_types::configuration::proposal::ControlProposal,
    crate::transport::TransportError,
> {
    plan.require_deployment(transport.configuration().as_ref().as_ref())?;
    let peer = transport.destination(plan.destination()?)?;
    let reservation = peer.reserve_target(Arc::new(plan.target().clone())).await?;
    Ok(plan.bind_remote_reservation(&reservation)?)
}
