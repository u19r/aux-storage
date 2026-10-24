use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    bootstrap_client::require_bindings,
    bootstrap_message::{BootstrapAction, BootstrapMessage, BootstrapOutcome, BootstrapReply},
    connection::TransportConnection,
    error::{ImportNack, TransportResult},
    server::{TcpImportServer, classify_native_error},
};
use crate::{
    bootstrap::{
        BaselineCopyExecution, HiddenTargetRequest, cancel_hidden_target, copy_baseline_page,
        reconcile_baseline_page, reconcile_hidden_cancellation, reconcile_hidden_target,
        reserve_hidden_target,
    },
    constants::TRANSPORT_REPLY_KIND,
};

impl<P: DynamoPort + 'static> TcpImportServer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub(super) async fn receive_bootstrap(
        &self,
        socket: &mut TransportConnection,
        message: BootstrapMessage,
    ) -> TransportResult<()> {
        self.require_peer(&message.from, &message.to)?;
        require_bindings(message.action.request(), (&message.from, &message.to))?;
        let operation = message.action.request().operation.id;
        let content = message.action.request().content()?;
        socket.require_live()?;
        let result = self.apply_bootstrap(message.action).await;
        super::chunk_exchange::write_serialized(
            socket,
            (
                TRANSPORT_REPLY_KIND,
                BootstrapReply {
                    from: self.resources.identity.clone(),
                    to: message.from.clone(),
                    operation,
                    content,
                    result,
                },
            ),
            &self.resources,
            (self.resources.identity.clone(), message.from),
        )
        .await
    }

    async fn apply_bootstrap(
        &self,
        action: BootstrapAction,
    ) -> Result<BootstrapOutcome, ImportNack> {
        match action {
            BootstrapAction::RetainedBaselines {
                target,
                ordinal,
                page,
            } => crate::bootstrap::reenable_baseline_executor::accept_retained_baselines(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                (target, ordinal, page),
                (
                    Arc::clone(&self.codec),
                    Arc::new(
                        self.resources
                            .configuration
                            .as_ref()
                            .as_ref()
                            .limits
                            .clone(),
                    ),
                ),
            )
            .await
            .map(BootstrapOutcome::RetainedBaselines)
            .map_err(|error| classify_native_error(&error)),
            BootstrapAction::BeginRetained { target, definition } => {
                self.apply_retained_declaration(
                    target,
                    definition,
                    crate::bootstrap::reenable_executor::RetainedDeclaration::Begin,
                )
                .await
            }
            BootstrapAction::ObserveRetained { target, definition } => {
                self.apply_retained_declaration(
                    target,
                    definition,
                    crate::bootstrap::reenable_executor::RetainedDeclaration::Observe,
                )
                .await
            }
            BootstrapAction::AcceptRetained {
                target,
                definition,
                page,
            } => {
                self.apply_retained_declaration(
                    target,
                    definition,
                    crate::bootstrap::reenable_executor::RetainedDeclaration::Page(page),
                )
                .await
            }
            BootstrapAction::BeginAdoption { target, definition } => {
                self.begin_adoption(target, definition).await
            }
            BootstrapAction::ObserveAdoption { target, definition } => {
                self.observe_adoption(target, definition).await
            }
            BootstrapAction::AcceptAdoption {
                target,
                definition,
                revision,
                change,
            } => {
                self.accept_adoption(
                    crate::history_collection::adoption_executor::AdoptionAcceptance {
                        request: target,
                        definition,
                        revision,
                        change: *change,
                    },
                )
                .await
            }
            BootstrapAction::AdvanceVerification { target, definition } => {
                crate::bootstrap::advance_hidden_verification(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    (
                        target,
                        definition,
                        Arc::clone(&self.codec),
                        Arc::new(
                            self.resources
                                .configuration
                                .as_ref()
                                .as_ref()
                                .limits
                                .clone(),
                        ),
                    ),
                )
                .await
                .map(BootstrapOutcome::Verification)
                .map_err(|error| classify_native_error(&error))
            }
            BootstrapAction::DeclareCut {
                target,
                definition,
                cut,
            } => crate::bootstrap::declare_bootstrap_cut(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                (target, definition, cut),
            )
            .await
            .map(BootstrapOutcome::Cut)
            .map_err(|error| classify_native_error(&error)),
            BootstrapAction::Reserve(request) => {
                self.apply_target_reservation(request, false).await
            }
            BootstrapAction::Observe(request) => self.apply_target_reservation(request, true).await,
            BootstrapAction::Cancel(request) => {
                self.apply_target_cancellation(request, false).await
            }
            BootstrapAction::ObserveCancellation(request) => {
                self.apply_target_cancellation(request, true).await
            }
            BootstrapAction::Copy {
                target,
                definition,
                page,
            } => {
                self.apply_baseline_copy(
                    Arc::new(BaselineCopyExecution {
                        target,
                        definition,
                        page,
                        codec: Arc::clone(&self.codec),
                    }),
                    false,
                )
                .await
            }
            BootstrapAction::ObserveCopy {
                target,
                definition,
                page,
            } => {
                self.apply_baseline_copy(
                    Arc::new(BaselineCopyExecution {
                        target,
                        definition,
                        page,
                        codec: Arc::clone(&self.codec),
                    }),
                    true,
                )
                .await
            }
        }
    }

    async fn apply_retained_declaration(
        &self,
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::bootstrap::ReenableHistoryDefinition>,
        action: crate::bootstrap::reenable_executor::RetainedDeclaration,
    ) -> Result<BootstrapOutcome, ImportNack> {
        crate::bootstrap::reenable_executor::execute_retained_declaration(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            (target, definition),
            action,
        )
        .await
        .map(BootstrapOutcome::Retained)
        .map_err(|error| classify_native_error(&error))
    }

    async fn apply_target_reservation(
        &self,
        request: Arc<HiddenTargetRequest>,
        observe: bool,
    ) -> Result<BootstrapOutcome, ImportNack> {
        if !observe {
            match reserve_hidden_target(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                Arc::clone(&request),
            )
            .await
            {
                Ok(reservation) => {
                    return Ok(BootstrapOutcome::Reservation(reservation.proposed()));
                }
                Err(error) if error.is_commit_unknown() => {}
                Err(error) => return Err(classify_native_error(&error)),
            }
        }
        match reconcile_hidden_target(self.port.as_ref(), Arc::clone(&self.journal), request).await
        {
            Ok(Some(reservation)) => Ok(BootstrapOutcome::Reservation(reservation.proposed())),
            Ok(None) if observe => Ok(BootstrapOutcome::Missing),
            Ok(None) => Err(ImportNack::UnresolvedCommit),
            Err(_) if !observe => Err(ImportNack::UnresolvedCommit),
            Err(error) => Err(classify_native_error(&error)),
        }
    }

    async fn apply_target_cancellation(
        &self,
        request: Arc<HiddenTargetRequest>,
        observe: bool,
    ) -> Result<BootstrapOutcome, ImportNack> {
        if !observe {
            match cancel_hidden_target(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                Arc::clone(&request),
            )
            .await
            {
                Ok(()) => return Ok(BootstrapOutcome::Cancellation(true)),
                Err(error) if error.is_commit_unknown() => {}
                Err(error) => return Err(classify_native_error(&error)),
            }
        }
        match reconcile_hidden_cancellation(self.port.as_ref(), Arc::clone(&self.journal), request)
            .await
        {
            Ok(known) if observe || known => Ok(BootstrapOutcome::Cancellation(known)),
            Ok(_) => Err(ImportNack::UnresolvedCommit),
            Err(_) if !observe => Err(ImportNack::UnresolvedCommit),
            Err(error) => Err(classify_native_error(&error)),
        }
    }

    async fn apply_baseline_copy(
        &self,
        execution: Arc<BaselineCopyExecution>,
        observe: bool,
    ) -> Result<BootstrapOutcome, ImportNack> {
        if !observe {
            match copy_baseline_page(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                Arc::clone(&execution),
            )
            .await
            {
                Ok(receipt) => return Ok(BootstrapOutcome::Baseline(receipt)),
                Err(error) if error.is_commit_unknown() => {}
                Err(error) => return Err(classify_native_error(&error)),
            }
        }
        match reconcile_baseline_page(self.port.as_ref(), Arc::clone(&self.journal), execution)
            .await
        {
            Ok(Some(receipt)) => Ok(BootstrapOutcome::Baseline(receipt)),
            Ok(None) if observe => Ok(BootstrapOutcome::Missing),
            Ok(None) => Err(ImportNack::UnresolvedCommit),
            Err(_) if !observe => Err(ImportNack::UnresolvedCommit),
            Err(error) => Err(classify_native_error(&error)),
        }
    }
}
