mod admission;
mod completion;
mod decision;
mod history;
mod history_predecessor;
mod installation;
mod metadata;
mod observation;
mod preparation;
mod removal_transfer;
mod ttl;

use storage_types::StorageResult;

use crate::{
    control::{ControlRecord, ControlRequest},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn execute_control<
        A: stream_provider::retained_image::RetainedOwnershipAttempt
            + storage_provider::UpdateTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        input: (
            &ControlRequest,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<ControlRecord> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, codec) = input;
            let operation = request.proposal();
            replication_provider::control::validate_preparation(operation)
                .map_err(|_| corrupt_record())?;
            if !operation.operation().participants.contains(&self.region) {
                return Err(corrupt_record());
            }
            let record = match request {
                ControlRequest::Prepare(_) => self
                    .prepare_control(attempt, operation, observed_at)
                    .await
                    .map(|(record, _)| record),
                ControlRequest::Commit { grants, .. } => {
                    let record = self
                        .decide_control(attempt, (operation, Some(grants)))
                        .await?;
                    self.capture_control_history(attempt, &record, (codec, observed_at))
                        .await?;
                    Ok(record)
                }
                ControlRequest::Cancel(_) => self.decide_control(attempt, (operation, None)).await,
                ControlRequest::Install(certificate) => {
                    self.install_control(attempt, certificate, observed_at)
                        .await
                }
                ControlRequest::Finish { installations, .. } => {
                    self.finish_control(attempt, (operation, installations))
                        .await
                }
                ControlRequest::Release(certificate) => {
                    self.complete_control(attempt, certificate).await
                }
                ControlRequest::ReleaseCancelled(certificate) => {
                    self.release_cancelled_control(attempt, certificate).await
                }
            }?;
            self.require_control_history(
                attempt,
                &record,
                (codec, storage_types::ReadProtection::Shared),
            )
            .await?;
            Ok(record)
        }
        .await;
        settlement.complete(result)
    }
}

mod addition;
mod bootstrap;

mod acceptance;
mod configuration;
mod description;
mod recovery;
