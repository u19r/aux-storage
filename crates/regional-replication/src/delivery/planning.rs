use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::error::DeliveryError,
    runtime_configuration::limits::WorkLimits,
};
use replication_types::{event::interval::ClosedSourceInterval, regional_import::RegionalImport};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageResult};

use super::DeliveryScope;
use crate::{
    journal::JournalStore,
    source_frame::{SourceFrameRequest, select_source_frame},
};

/// Plan from the native checkpoint and original journal in one fresh snapshot.
/// Pages cover complete original positions and shrink only for configured
/// bounds.
pub async fn plan_delivery<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: DeliveryScope,
    configuration: (Arc<CanonicalJsonCodec>, Arc<WorkLimits>),
) -> StorageResult<Option<RegionalImport>>
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
                Box::pin(async move {
                    let progress = journal.load_delivery_progress(attempt, &scope).await?;
                    let head = journal.load_delivery_head(attempt, &scope).await?;
                    if progress.through() >= head {
                        return Ok(None);
                    }
                    let range = ClosedSourceInterval::try_from((progress.through(), head))
                        .map_err(|_| crate::native_record::corrupt_record())?;
                    let request = scope.source_request(range);
                    let frame = load_bounded_frame(
                        &journal,
                        attempt,
                        request,
                        (&configuration.0, &configuration.1),
                    )
                    .await
                    .map_err(storage_types::StorageError::from)?;
                    let (manifest, events) = frame.into_parts();
                    RegionalImport::try_new(
                        (vec![scope.binding()], manifest, events),
                        &*configuration.0,
                    )
                    .map(Some)
                    .map_err(|_| crate::native_record::corrupt_record())
                })
            },
        )
        .await
}

pub(crate) async fn load_bounded_frame<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    mut request: SourceFrameRequest,
    configuration: (&CanonicalJsonCodec, &WorkLimits),
) -> Result<replication_provider::delivery::frame::SourceFrame, DeliveryError> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (codec, limits) = configuration;
        let after = u64::from(request.range.after());
        request.range = journal.bound_source_range(request.range, limits.frame_events)?;
        let mut width = u64::from(request.range.through()) - after;
        loop {
            request.range = ClosedSourceInterval::try_from((after.into(), (after + width).into()))?;
            match select_source_frame(journal, attempt, (&request, limits), codec).await? {
                Some(frame) => return Ok(frame),
                None if width > 1 => width /= 2,
                None => {
                    return Err(
                        replication_types::error::ReplicationProtocolError::InvalidRange {
                            context: "source frame configured bound",
                        }
                        .into(),
                    );
                }
            }
        }
    }
    .await;
    settlement.complete(result)
}
