use std::sync::Arc;

use replication_types::configuration::installed::{InstalledTableState, RegionalTablePhase};

use super::{
    bootstrap_message::{BootstrapAction, BootstrapMessage, BootstrapOutcome, BootstrapReply},
    client::TcpPeerClient,
    error::{TransportError, TransportResult},
    message::WireIdentity,
};
use crate::{
    bootstrap::{
        BaselineCopyPage, CommittedBaselinePage, HiddenTargetRequest, SourceCaptureDefinition,
        TargetBootstrapDefinition,
    },
    constants::{TRANSPORT_BOOTSTRAP_KIND, TRANSPORT_REPLY_KIND},
};

pub struct RemoteTargetReservation {
    proposed: InstalledTableState,
    operation: replication_types::identity::resource::ControlOperationId,
    content: replication_types::protocol::CanonicalDigest,
}
impl RemoteTargetReservation {
    pub fn proposed(&self) -> &InstalledTableState {
        &self.proposed
    }
    pub fn operation(&self) -> replication_types::identity::resource::ControlOperationId {
        self.operation
    }
    pub fn content(&self) -> replication_types::protocol::CanonicalDigest {
        self.content
    }
}

impl TcpPeerClient {
    pub(crate) async fn accept_target_retained_baselines(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            u64,
            Arc<crate::bootstrap::ReenableBaselinePage>,
        ),
    ) -> TransportResult<crate::bootstrap::ReenableBaselineReceipt> {
        let action = BootstrapAction::RetainedBaselines {
            target: input.0,
            ordinal: input.1,
            page: Arc::clone(&input.2),
        };
        let BootstrapOutcome::RetainedBaselines(receipt) = self.bootstrap(action).await? else {
            return Err(TransportError::Peer);
        };
        receipt
            .require_page(&input.2)
            .map_err(|_| TransportError::Peer)?;
        Ok(receipt)
    }

    pub(crate) fn retained_baseline_capacity(
        &self,
        target: &HiddenTargetRequest,
        ordinal: u64,
    ) -> TransportResult<crate::bootstrap::copy_scope::BaselineCopyBudget> {
        let from = self.resources.identity.clone();
        let to = self.destination_identity();
        require_bindings(target, (&from, &to))?;
        let page = crate::bootstrap::ReenableBaselinePage {
            definition: [0; 32].into(),
            sequence: u64::MAX,
            items: Vec::new(),
            complete: false,
            content: [0; 32].into(),
        };
        let empty = BootstrapMessage {
            from,
            to,
            action: BootstrapAction::RetainedBaselines {
                target: Arc::new(target.clone()),
                ordinal,
                page: Arc::new(page),
            },
        };
        self.baseline_message_capacity(&empty)
    }

    pub async fn begin_target_retained_history(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            Arc<crate::bootstrap::ReenableHistoryDefinition>,
        ),
    ) -> TransportResult<crate::bootstrap::TargetRetainedHistoryObservation> {
        let action = BootstrapAction::BeginRetained {
            target: input.0,
            definition: Arc::clone(&input.1),
        };
        self.require_retained_observation(&input.1, self.bootstrap(action).await?)?
            .ok_or(TransportError::Peer)
    }

    pub async fn observe_target_retained_history(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            Arc<crate::bootstrap::ReenableHistoryDefinition>,
        ),
    ) -> TransportResult<Option<crate::bootstrap::TargetRetainedHistoryObservation>> {
        let action = BootstrapAction::ObserveRetained {
            target: input.0,
            definition: Arc::clone(&input.1),
        };
        self.require_retained_observation(&input.1, self.bootstrap(action).await?)
    }

    pub async fn accept_target_retained_page(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            Arc<crate::bootstrap::ReenableHistoryDefinition>,
            Arc<crate::bootstrap::ReenableHistoryPage>,
        ),
    ) -> TransportResult<crate::bootstrap::TargetRetainedHistoryObservation> {
        let action = BootstrapAction::AcceptRetained {
            target: input.0,
            definition: Arc::clone(&input.1),
            page: Arc::clone(&input.2),
        };
        let observation = self
            .require_retained_observation(&input.1, self.bootstrap(action).await?)?
            .ok_or(TransportError::Peer)?;
        if observation.next_ordinal <= input.2.cut.ordinal {
            return Err(TransportError::Peer);
        }
        Ok(observation)
    }

    fn require_retained_observation(
        &self,
        definition: &crate::bootstrap::ReenableHistoryDefinition,
        outcome: BootstrapOutcome,
    ) -> TransportResult<Option<crate::bootstrap::TargetRetainedHistoryObservation>> {
        let BootstrapOutcome::Retained(observation) = outcome else {
            return Err(TransportError::Peer);
        };
        if observation.as_ref().is_some_and(|observation| {
            observation.definition != definition.content()
                || observation.next_ordinal > definition.cuts()
                || (observation.next_ordinal == definition.cuts()
                    && observation.vector != definition.inventory())
        }) {
            return Err(TransportError::Peer);
        }
        Ok(observation)
    }

    pub async fn advance_target_verification(
        &self,
        input: (Arc<HiddenTargetRequest>, Arc<TargetBootstrapDefinition>),
    ) -> TransportResult<crate::bootstrap::HiddenVerificationStatus> {
        let action = BootstrapAction::AdvanceVerification {
            target: input.0,
            definition: Arc::clone(&input.1),
        };
        match self.bootstrap(action).await? {
            BootstrapOutcome::Verification(status) => Ok(status),
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) async fn declare_bootstrap_cut(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            Arc<SourceCaptureDefinition>,
            Arc<crate::bootstrap::SourceCaptureCut>,
        ),
    ) -> TransportResult<()> {
        let (target, definition, cut) = input;
        let action = BootstrapAction::DeclareCut {
            target,
            definition,
            cut: Arc::clone(&cut),
        };
        match self.bootstrap(action).await? {
            BootstrapOutcome::Cut(received) if received == *cut => Ok(()),
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) fn baseline_copy_capacity(
        &self,
        target: &HiddenTargetRequest,
        definition: &TargetBootstrapDefinition,
    ) -> TransportResult<crate::bootstrap::copy_scope::BaselineCopyBudget> {
        let from = self.resources.identity.clone();
        let to = self.destination_identity();
        require_bindings(target, (&from, &to))?;
        let empty = BootstrapMessage {
            from,
            to,
            action: BootstrapAction::Copy {
                target: Arc::new(target.clone()),
                definition: Arc::new(definition.clone()),
                page: Arc::new(BaselineCopyPage {
                    sequence: u64::MAX,
                    items: Vec::new(),
                    complete: false,
                }),
            },
        };
        self.baseline_message_capacity(&empty)
    }

    fn baseline_message_capacity(
        &self,
        empty: &BootstrapMessage,
    ) -> TransportResult<crate::bootstrap::copy_scope::BaselineCopyBudget> {
        let overhead = super::codec::serialized_length(empty, self.resources.limit)?;
        let limits = &self.resources.configuration.as_ref().as_ref().limits;
        let records = limits.import_events.get().saturating_sub(2).max(1);
        // JSON byte arrays use at most four bytes per original byte. Each array
        // adds two brackets and a separator; reserve those before a native
        // scan.
        let framing = records
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(overhead))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(TransportError::Frame)?;
        let bytes = self
            .resources
            .limit
            .checked_sub(framing)
            .ok_or(TransportError::Frame)?
            / 4;
        Ok(crate::bootstrap::copy_scope::BaselineCopyBudget {
            records: std::num::NonZeroUsize::new(records).ok_or(TransportError::Configuration)?,
            bytes: std::num::NonZeroUsize::new(bytes.min(limits.import_bytes.get()))
                .ok_or(TransportError::Frame)?,
        })
    }

    pub async fn reserve_target(
        &self,
        request: Arc<HiddenTargetRequest>,
    ) -> TransportResult<RemoteTargetReservation> {
        let outcome = self
            .bootstrap(BootstrapAction::Reserve(Arc::clone(&request)))
            .await?;
        require_reservation(&request, outcome)
    }

    pub async fn observe_target(
        &self,
        request: Arc<HiddenTargetRequest>,
    ) -> TransportResult<Option<RemoteTargetReservation>> {
        let outcome = self
            .bootstrap(BootstrapAction::Observe(Arc::clone(&request)))
            .await?;
        if matches!(outcome, BootstrapOutcome::Missing) {
            return Ok(None);
        }
        require_reservation(&request, outcome).map(Some)
    }

    pub async fn cancel_target(&self, request: Arc<HiddenTargetRequest>) -> TransportResult<()> {
        match self
            .bootstrap(BootstrapAction::Cancel(Arc::clone(&request)))
            .await?
        {
            BootstrapOutcome::Cancellation(true) => Ok(()),
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn observe_target_cancellation(
        &self,
        request: Arc<HiddenTargetRequest>,
    ) -> TransportResult<bool> {
        match self
            .bootstrap(BootstrapAction::ObserveCancellation(Arc::clone(&request)))
            .await?
        {
            BootstrapOutcome::Cancellation(known) => Ok(known),
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn copy_target_baselines(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            Arc<TargetBootstrapDefinition>,
            Arc<BaselineCopyPage>,
        ),
    ) -> TransportResult<CommittedBaselinePage> {
        let (target, definition, page) = input;
        let action = BootstrapAction::Copy {
            target,
            definition: Arc::clone(&definition),
            page: Arc::clone(&page),
        };
        require_page(&definition, &page, self.bootstrap(action).await?)
    }

    pub async fn observe_target_baselines(
        &self,
        input: (
            Arc<HiddenTargetRequest>,
            Arc<TargetBootstrapDefinition>,
            Arc<BaselineCopyPage>,
        ),
    ) -> TransportResult<Option<CommittedBaselinePage>> {
        let (target, definition, page) = input;
        let action = BootstrapAction::ObserveCopy {
            target,
            definition: Arc::clone(&definition),
            page: Arc::clone(&page),
        };
        let outcome = self.bootstrap(action).await?;
        if matches!(outcome, BootstrapOutcome::Missing) {
            return Ok(None);
        }
        require_page(&definition, &page, outcome).map(Some)
    }

    pub(super) async fn bootstrap(
        &self,
        action: BootstrapAction,
    ) -> TransportResult<BootstrapOutcome> {
        let from = self.resources.identity.clone();
        let to = self.destination_identity();
        require_bindings(action.request(), (&from, &to))?;
        let operation = action.request().operation.id;
        let content = action.request().content()?;
        self.settle_exchange(move |peer, deadline| async move {
            let mut socket = peer.connect(deadline).await?;
            super::chunk_exchange::write_serialized(
                &mut socket,
                (
                    TRANSPORT_BOOTSTRAP_KIND,
                    BootstrapMessage {
                        from: from.clone(),
                        to: to.clone(),
                        action,
                    },
                ),
                &peer.resources,
                (from.clone(), to.clone()),
            )
            .await?;
            let reply: BootstrapReply = super::chunk_exchange::read_message(
                &mut socket,
                TRANSPORT_REPLY_KIND,
                &peer.resources,
                (to.clone(), from.clone()),
            )
            .await?;
            if reply.from != to
                || reply.to != from
                || reply.operation != operation
                || reply.content != content
            {
                return Err(TransportError::Peer);
            }
            reply.result.map_err(TransportError::Nack)
        })
        .await
    }
}

pub(super) fn require_bindings(
    request: &HiddenTargetRequest,
    identities: (&WireIdentity, &WireIdentity),
) -> TransportResult<()> {
    let (from, to) = identities;
    replication_provider::control::validate_joining_definition(
        &request.operation,
        (&request.source, &request.target),
        &to.region,
    )?;
    request.schema.validate(&request.target.name)?;
    if request.operation.home_region != from.region
        || !request.target.sources.iter().any(|source| {
            source.region == to.region
                && source.database_group == to.group
                && source.generation == to.generation
        })
        || !request.source.sources.iter().any(|source| {
            source.region == from.region
                && source.database_group == from.group
                && source.generation == from.generation
        })
    {
        return Err(TransportError::Peer);
    }
    Ok(())
}

fn require_reservation(
    request: &HiddenTargetRequest,
    outcome: BootstrapOutcome,
) -> TransportResult<RemoteTargetReservation> {
    let BootstrapOutcome::Reservation(proposed) = outcome else {
        return Err(TransportError::Peer);
    };
    let mut expected = request.target.bind_physical(proposed.physical);
    expected.phase = RegionalTablePhase::Active;
    if proposed != expected {
        return Err(TransportError::Peer);
    }
    Ok(RemoteTargetReservation {
        proposed,
        operation: request.operation.id,
        content: request.content()?,
    })
}

fn require_page(
    definition: &TargetBootstrapDefinition,
    page: &BaselineCopyPage,
    outcome: BootstrapOutcome,
) -> TransportResult<CommittedBaselinePage> {
    let BootstrapOutcome::Baseline(receipt) = outcome else {
        return Err(TransportError::Peer);
    };
    let content = crate::bootstrap::baseline_page_content(definition.content(), page)?;
    if receipt.sequence != page.sequence
        || receipt.content != content
        || receipt.baselines_complete != page.complete
    {
        return Err(TransportError::Peer);
    }
    Ok(receipt)
}
