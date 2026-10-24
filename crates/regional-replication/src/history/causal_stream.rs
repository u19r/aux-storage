//! Native paginated ancestor-before-descendant delivery over two held cuts.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::event::identity::CanonicalEventId;
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageError, StorageResult};

use super::{
    checkpoint::CanonicalCheckpointId,
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    selection::CanonicalViewCursor,
    view::{CanonicalViewRequest, DeliveryPosition},
};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "uuid::Uuid", into = "uuid::Uuid")]
pub struct CanonicalCausalStreamId(uuid::Uuid);
impl TryFrom<uuid::Uuid> for CanonicalCausalStreamId {
    type Error = StorageError;
    fn try_from(value: uuid::Uuid) -> StorageResult<Self> {
        if value.is_nil() {
            return Err(StorageError::validation(
                "Causal stream operation must be nonzero",
            ));
        }
        Ok(Self(value))
    }
}
impl From<CanonicalCausalStreamId> for uuid::Uuid {
    fn from(value: CanonicalCausalStreamId) -> Self {
        value.0
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalCausalStreamCursor {
    operation: CanonicalCausalStreamId,
    scope: CanonicalViewCursor,
    portable: CanonicalCheckpointId,
    ancestry: replication_types::protocol::CanonicalDigest,
    step: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CanonicalCausalStreamAction {
    Start,
    Continue(Box<CanonicalCausalStreamCursor>),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalCausalStreamRequest {
    pub view: CanonicalViewRequest,
    pub portable_checkpoint: CanonicalCheckpointId,
    pub causal_stream: CanonicalCausalStreamId,
    pub action: CanonicalCausalStreamAction,
    #[serde(default)]
    pub ancestry_scopes: super::ancestry_scope::CanonicalAncestryScopes,
}
#[derive(Serialize)]
pub struct CanonicalCausalStreamEvent {
    pub identity: CanonicalEventId,
    pub original: String,
}
#[derive(Serialize)]
pub struct CanonicalCausalStreamPage {
    pub through: CanonicalCausalStreamCursor,
    pub selected_through: DeliveryPosition,
    pub finished: bool,
    pub event: Option<CanonicalCausalStreamEvent>,
    #[serde(skip_serializing)]
    pub(crate) _source: Option<crate::transport::SourceReservation>,
}
impl CanonicalCausalStreamRequest {
    pub(crate) fn requested_step(&self, scope: &CanonicalViewCursor) -> StorageResult<Option<u64>> {
        let CanonicalCausalStreamAction::Continue(cursor) = &self.action else {
            return Ok(None);
        };
        if cursor.operation != self.causal_stream
            || cursor.scope != *scope
            || cursor.portable != self.portable_checkpoint
            || cursor.ancestry != self.ancestry_scopes.binding()?
        {
            return Err(StorageError::validation(
                "Causal stream cursor does not match its selection",
            ));
        }
        Ok(Some(cursor.step))
    }
    pub(crate) fn page(
        &self,
        scope: CanonicalViewCursor,
        progress: (u64, DeliveryPosition, bool),
        event: Option<CanonicalCausalStreamEvent>,
    ) -> StorageResult<CanonicalCausalStreamPage> {
        Ok(CanonicalCausalStreamPage {
            through: CanonicalCausalStreamCursor {
                operation: self.causal_stream,
                scope,
                portable: self.portable_checkpoint,
                ancestry: self.ancestry_scopes.binding()?,
                step: progress.0,
            },
            selected_through: progress.1,
            finished: progress.2,
            event,
            _source: None,
        })
    }
}
impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn read_causal_stream(
        &self,
        request: CanonicalCausalStreamRequest,
    ) -> StorageResult<CanonicalCausalStreamPage> {
        if !self.matches((&request.view.serving_region, &request.view.serving_group)) {
            return Err(StorageError::validation(
                "History serving database does not match",
            ));
        }
        if request.view.checkpoint.is_none() || request.view.after.is_some() {
            return Err(StorageError::validation(
                "Causal stream requires an initial held receiver view",
            ));
        }
        let source = self
            .transport
            .reserve_history_source()
            .await
            .map_err(|_| StorageError::from(storage_types::StorageEnum::HistoryUnavailable))?;
        let port = Arc::clone(&self.port);
        let journal = Arc::clone(&self.journal);
        let codec = Arc::clone(&self.codec);
        let limits = self.transport.source_limits();
        let request = Arc::new(request);
        crate::transport::settle_source_with(source, move |source| async move {
            let mut page = retry_native_conflicts(port.as_ref(), DYNAMO_WRITE_RETRY, || {
                let request = Arc::clone(&request);
                let journal = Arc::clone(&journal);
                let codec = Arc::clone(&codec);
                let port = Arc::clone(&port);
                let limits = Arc::clone(&limits);
                async move {
                    std::sync::Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                port.as_ref(),
                                storage_provider::native_admission::NativeWork::Write,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, observed_at| {
                                Box::pin(async move {
                                    super::retention::settle_expiry(
                                        journal
                                            .advance_causal_stream(
                                                attempt,
                                                (&request, observed_at),
                                                (&limits, &codec),
                                            )
                                            .await,
                                    )
                                })
                            },
                        )
                        .await
                }
            })
            .await??;
            page._source = Some(source);
            Ok(page)
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native causal stream failed"))
        })
    }
}
