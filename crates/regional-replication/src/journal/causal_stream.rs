//! Bounded native advancement, replay and response budgeting for causal
//! streams.
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::event::{content::CanonicalEvent, identity::CanonicalEventId};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult, TimestampMillis};

use super::{
    JournalStore, causal_scope::HeldCausalScope, causal_stream_record::CausalStreamRecord,
};
use crate::{
    history::{
        ancestry_scope::CanonicalAncestryScope,
        causal_stream::{
            CanonicalCausalStreamEvent, CanonicalCausalStreamPage, CanonicalCausalStreamRequest,
        },
    },
    native_record::corrupt_record,
};

pub(super) struct CausalStreamScope<'a> {
    pub request: &'a CanonicalCausalStreamRequest,
    pub scopes: Vec<HeldCausalScope>,
    pub receiver_authority: super::public_trim_authority::PublicTrimAuthority,
    pub limits: &'a WorkLimits,
    pub codec: &'a CanonicalJsonCodec,
}
impl JournalStore {
    pub(crate) async fn advance_causal_stream<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&CanonicalCausalStreamRequest, TimestampMillis),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<CanonicalCausalStreamPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, observed_at) = input;
            self.require_view_receiver(&request.view)?;
            let bounds = self
                .load_receiver_checkpoint(attempt, &request.view, observed_at)
                .await?
                .ok_or_else(corrupt_record)?;
            let root = CanonicalAncestryScope {
                table: request.view.table.clone(),
                lineage: request.view.lineage,
                membership: request.view.membership,
                checkpoint: request.portable_checkpoint,
            };
            let scopes = self
                .load_held_causal_scopes(
                    attempt,
                    (&root, &request.ancestry_scopes, observed_at),
                    encoding,
                )
                .await?;
            let key = self.identity_key(
                crate::constants::CAUSAL_STREAM_SUFFIX,
                &request.causal_stream,
            )?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let requested = request.requested_step(&bounds.scope)?;
            let mut stream: CausalStreamRecord = match &row {
                Some(row) if row.key == key => self.records.decode(row)?,
                Some(_) => return Err(corrupt_record()),
                None if requested.is_none() => CausalStreamRecord::try_new(request, &bounds)?,
                None => return Err(storage_types::StorageEnum::HistoryUnavailable.into()),
            };
            stream.require_request(request, &bounds)?;
            let scope = CausalStreamScope {
                request,
                scopes,
                receiver_authority: bounds.authority,
                limits: encoding.0,
                codec: encoding.1,
            };
            let prior_step = stream.step;
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let event = self
                .advance_or_replay_causal_stream(attempt, &scope, &mut stream, requested)
                .await?;
            let page = request.page(
                stream.scope.clone(),
                (stream.step, stream.position, stream.finished),
                event,
            )?;
            crate::history::response_budget::encoded_length(&page, scope.limits.frame_bytes.get())?;
            if row.is_none() || stream.step != prior_step {
                self.records
                    .store(
                        attempt,
                        &[self.records.prepare(
                            key,
                            row.as_ref().map(|row| row.revision),
                            &stream,
                        )?],
                    )
                    .await?;
                self.records
                    .require_discretionary_capacity(attempt, capacity)
                    .await?;
            }
            Ok(page)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }
    async fn advance_or_replay_causal_stream<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &CausalStreamScope<'_>,
        stream: &mut CausalStreamRecord,
        requested: Option<u64>,
    ) -> StorageResult<Option<CanonicalCausalStreamEvent>> {
        let Some(step) = requested else {
            return Ok(None);
        };
        if step == stream.step {
            if stream.finished {
                return Ok(None);
            }
            let event = self.advance_causal_step(attempt, scope, stream).await?;
            stream.step = stream.step.checked_add(1).ok_or_else(corrupt_record)?;
            stream.last = event.as_ref().map(|event| event.identity.clone());
            return Ok(event);
        }
        if step.checked_add(1) != Some(stream.step) {
            return Err(StorageError::validation(
                "Causal stream cursor is stale or future",
            ));
        }
        let Some(identity) = &stream.last else {
            return Ok(None);
        };
        let event = self
            .load_causal_stream_event(attempt, scope, identity)
            .await?;
        let visit = self
            .load_causal_visit(attempt, (stream.operation, identity, event.as_ref().digest))
            .await?
            .ok_or_else(corrupt_record)?
            .1;
        if visit.state != super::causal_stream_record::CausalVisitState::Complete {
            return Err(corrupt_record());
        }
        self.render_causal_stream_event(event).map(Some)
    }
    pub(super) async fn load_causal_stream_event<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &CausalStreamScope<'_>,
        identity: &CanonicalEventId,
    ) -> StorageResult<CanonicalEvent> {
        self.load_verified_causal_identity(
            attempt,
            (identity, &scope.scopes),
            (scope.limits, scope.codec),
        )
        .await
    }
    pub(super) fn render_causal_stream_event(
        &self,
        event: CanonicalEvent,
    ) -> StorageResult<CanonicalCausalStreamEvent> {
        Ok(CanonicalCausalStreamEvent {
            identity: event.as_ref().id.clone(),
            original: String::from_utf8(event.as_ref().bytes.as_ref().to_vec())
                .map_err(|_| corrupt_record())?,
        })
    }
}
