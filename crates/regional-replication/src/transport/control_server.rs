use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    connection::TransportConnection,
    control::require_participant,
    control_message::{ControlCommand, ControlMessage, ControlReply},
    error::{ImportNack, TransportError, TransportResult},
    server::{TcpImportServer, classify_native_error},
};
use crate::{
    constants::TRANSPORT_REPLY_KIND,
    control::{
        CancelledControl, CommittedControl, CompletedControl, ControlRecord, ControlRequest,
        execute_control, reconcile_control,
    },
};

impl<P: DynamoPort + 'static> TcpImportServer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub(super) async fn receive_control(
        &self,
        socket: &mut TransportConnection,
        message: ControlMessage,
    ) -> TransportResult<()> {
        self.require_peer(&message.from, &message.to)?;
        message.command.validate()?;
        let proposal = message.command.proposal();
        require_participant(proposal, &message.from)?;
        require_participant(proposal, &message.to)?;
        if message.from.region != proposal.operation().home_region {
            return Err(TransportError::Peer);
        }
        let to = message.from.clone();
        socket.require_live()?;
        let result = self.apply_control(message.command).await;
        super::chunk_exchange::write_serialized(
            socket,
            (
                TRANSPORT_REPLY_KIND,
                ControlReply {
                    from: self.resources.identity.clone(),
                    to: to.clone(),
                    result,
                },
            ),
            &self.resources,
            (self.resources.identity.clone(), to),
        )
        .await
    }

    async fn apply_control(&self, command: ControlCommand) -> Result<ControlRecord, ImportNack> {
        let proposal = Arc::clone(command.proposal());
        let request = match command {
            ControlCommand::Observe { .. } => {
                let record = self.reconcile_control(proposal).await?;
                return self.finish_control_cleanup(record).await;
            }
            ControlCommand::Prepare { .. } => {
                self.advance_control_seal(Arc::clone(&proposal)).await?;
                ControlRequest::Prepare(Arc::clone(&proposal))
            }
            ControlCommand::Install {
                bootstrap,
                retirement,
                ..
            } => ControlRequest::Install(CommittedControl {
                proposal: proposal.as_ref().clone(),
                bootstrap,
                retirement,
            }),
            ControlCommand::Release {
                bootstrap,
                retirement,
                ..
            } => ControlRequest::Release(CompletedControl {
                proposal: proposal.as_ref().clone(),
                bootstrap,
                retirement,
            }),
            ControlCommand::ReleaseCancelled { bootstrap, .. } => {
                ControlRequest::ReleaseCancelled(CancelledControl {
                    proposal: proposal.as_ref().clone(),
                    bootstrap,
                })
            }
        };
        match execute_control(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            Arc::new(request),
        )
        .await
        {
            Ok(outcome) => self.finish_control_cleanup(outcome.into_record()).await,
            Err(error) if error.is_commit_unknown() => {
                let record = self
                    .reconcile_control(proposal)
                    .await
                    .map_err(|_| ImportNack::UnresolvedCommit)?;
                self.finish_control_cleanup(record).await
            }
            Err(error) => Err(classify_native_error(&error)),
        }
    }

    async fn finish_control_cleanup(
        &self,
        record: ControlRecord,
    ) -> Result<ControlRecord, ImportNack> {
        let record = self.finish_removal_cancellation(record).await?;
        let record = self.finish_removal_retirement(record).await?;
        let record = self.finish_target_cancellation(record).await?;
        if !matches!(
            record.decision,
            crate::control::ControlDecision::Completed | crate::control::ControlDecision::Cancelled
        ) || !matches!(
            record.proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. }
        ) {
            return Ok(record);
        }
        let complete = storage_common::retry::retry_native_conflicts(
            self.port.as_ref(),
            storage_provider::DYNAMO_WRITE_RETRY,
            || {
                let journal = Arc::clone(&self.journal);
                let proposal = record.proposal.clone();
                async move {
                    Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                self.port.as_ref(),
                                storage_provider::native_admission::NativeWork::Recovery,
                            ),
                            crate::journal::NativeAttempt::Items(
                                storage_types::AttemptMode::ReadWriteItems,
                            ),
                            move |attempt, _| {
                                Box::pin(async move {
                                    journal
                                        .cleanup_target_verification(attempt, &proposal)
                                        .await
                                })
                            },
                        )
                        .await
                }
            },
        )
        .await
        .map_err(|error| classify_native_error(&error))?;
        if complete {
            Ok(record)
        } else {
            Err(ImportNack::Unavailable)
        }
    }

    async fn finish_target_cancellation(
        &self,
        record: ControlRecord,
    ) -> Result<ControlRecord, ImportNack> {
        if record.decision != crate::control::ControlDecision::Cancelled
            || !matches!(
                record.proposal.operation().change,
                replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. }
            )
        {
            return Ok(record);
        }
        let proposal = record.proposal.clone();
        let journal = Arc::clone(&self.journal);
        let request = Arc::clone(&journal)
            .with_admitted_attempt(
                (
                    self.port.as_ref(),
                    storage_provider::native_admission::NativeWork::Recovery,
                ),
                crate::journal::NativeAttempt::Items(storage_types::AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(async move {
                        journal
                            .load_cancelled_target_request(attempt, &proposal)
                            .await
                    })
                },
            )
            .await
            .map_err(|error| classify_native_error(&error))?;
        let Some(request) = request else {
            return Ok(record);
        };
        let request = Arc::new(request);
        match crate::bootstrap::cancel_hidden_target(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&request),
        )
        .await
        {
            Ok(()) => Ok(record),
            Err(error) if error.is_commit_unknown() => {
                let cancelled = crate::bootstrap::reconcile_hidden_cancellation(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                )
                .await
                .map_err(|_| ImportNack::UnresolvedCommit)?;
                if cancelled {
                    Ok(record)
                } else {
                    Err(ImportNack::UnresolvedCommit)
                }
            }
            Err(error) => Err(classify_native_error(&error)),
        }
    }

    async fn reconcile_control(
        &self,
        proposal: Arc<replication_types::configuration::proposal::ControlProposal>,
    ) -> Result<ControlRecord, ImportNack> {
        match reconcile_control(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            proposal,
        )
        .await
        {
            Ok(Some(outcome)) => Ok(outcome.into_record()),
            Ok(None) => Err(ImportNack::UnresolvedCommit),
            Err(_) => Err(ImportNack::Unavailable),
        }
    }

    async fn advance_control_seal(
        &self,
        proposal: Arc<replication_types::configuration::proposal::ControlProposal>,
    ) -> Result<(), ImportNack> {
        if !matches!(
            proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::RemoveRegion { .. }
                | replication_types::configuration::control::ConfigurationChange::DisableGlobal
        ) {
            return Ok(());
        }
        let retiring = replication_provider::control::retiring_regions(&proposal)
            .map_err(|_| ImportNack::InvalidPage)?;
        if !retiring.contains(self.journal.region()) {
            return Ok(());
        }
        let request = Arc::new(
            crate::lifecycle::SealRequest::try_new(
                proposal.as_ref().clone(),
                self.journal.region().clone(),
            )
            .map_err(|error| classify_native_error(&error))?,
        );
        let status = crate::lifecycle::advance_table_seal(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            request,
        )
        .await
        .map_err(|error| classify_native_error(&error))?;
        match status {
            crate::lifecycle::SealStatus::Ready(_) | crate::lifecycle::SealStatus::Removed => {
                Ok(())
            }
            _ => Err(ImportNack::Unavailable),
        }
    }

    async fn finish_removal_cancellation(
        &self,
        record: ControlRecord,
    ) -> Result<ControlRecord, ImportNack> {
        if record.decision != crate::control::ControlDecision::Cancelled
            || !matches!(
                record.proposal.operation().change,
                replication_types::configuration::control::ConfigurationChange::RemoveRegion { .. }
                    | replication_types::configuration::control::ConfigurationChange::DisableGlobal
            )
        {
            return Ok(record);
        }
        let retiring = replication_provider::control::retiring_regions(&record.proposal)
            .map_err(|_| ImportNack::InvalidPage)?;
        if !retiring.contains(self.journal.region()) {
            return Ok(record);
        }
        let request = Arc::new(
            crate::lifecycle::SealRequest::try_new(
                record.proposal.clone(),
                self.journal.region().clone(),
            )
            .map_err(|error| classify_native_error(&error))?,
        );
        let status = crate::lifecycle::observe_table_seal(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&request),
        )
        .await
        .map_err(|error| classify_native_error(&error))?;
        if status.is_some() {
            crate::lifecycle::cancel_table_seal(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await
            .map_err(|error| classify_native_error(&error))?;
        }
        Ok(record)
    }
    async fn finish_removal_retirement(
        &self,
        record: ControlRecord,
    ) -> Result<ControlRecord, ImportNack> {
        if record.decision != crate::control::ControlDecision::Completed
            || !matches!(
                record.proposal.operation().change,
                replication_types::configuration::control::ConfigurationChange::RemoveRegion { .. }
                    | replication_types::configuration::control::ConfigurationChange::DisableGlobal
            )
        {
            return Ok(record);
        }
        let retiring = replication_provider::control::retiring_regions(&record.proposal)
            .map_err(|_| ImportNack::InvalidPage)?;
        if !retiring.contains(self.journal.region()) {
            return Ok(record);
        }
        crate::lifecycle::retire_removed_table(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::new(record.proposal.clone()),
        )
        .await
        .map_err(|error| classify_native_error(&error))?;
        Ok(record)
    }
}
