use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, DurableRecordKey, RecordChange};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::content::CanonicalEvent;
use storage_types::{Revision, StorageResult, TableHistoryId};
use stream_provider::retained_image::{
    RetainedCausalBody, RetainedImageManifest, RetainedOwnerKind, RetainedPayload,
};

use crate::{
    canonical_original::CanonicalOriginalRef, constants::EVENT_SUFFIX, journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Default)]
pub(in crate::journal) struct PreparedOriginalBatch<'a> {
    keys: Vec<DurableRecordKey>,
    originals: stream_provider::retained_image::RetainedOriginalBatch<'a>,
}

impl<'a> PreparedOriginalBatch<'a> {
    pub(in crate::journal) fn include(
        &mut self,
        journal: &JournalStore,
        event: CanonicalOriginalRef<'a>,
        history: Option<TableHistoryId>,
    ) -> StorageResult<()> {
        let (key, manifest, images) = declare_original(journal, event, history)?;
        self.keys
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.originals.include(manifest, &images)?;
        self.keys.push(key);
        Ok(())
    }

    pub(in crate::journal) async fn prepare<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        journal: &JournalStore,
        attempt: &mut A,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self.originals.as_ref().len() != self.keys.len() {
                return Err(corrupt_record());
            }
            let mut keys = Vec::new();
            let mut owners = Vec::new();
            let mut bindings = Vec::new();
            let mut changes = Vec::new();
            keys.try_reserve_exact(self.keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            owners
                .try_reserve_exact(self.keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            bindings
                .try_reserve_exact(self.keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            changes
                .try_reserve_exact(self.keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for key in &self.keys {
                keys.push((
                    DurableRecordKey::try_from(key)?,
                    DurableRecordKey::try_from(key)?,
                ));
            }
            let encoded = self
                .originals
                .prepare_with_custody(attempt, journal.records.limits, &journal.records)
                .await?;
            if encoded.len() != self.keys.len() {
                return Err(corrupt_record());
            }
            for ((manifest, (key, owner_key)), (locator, encoded)) in self
                .originals
                .as_ref()
                .iter()
                .zip(keys)
                .zip(self.keys.iter().zip(encoded))
            {
                if let Some(owner) =
                    manifest.prepare_owner_record(RetainedOwnerKind::Event, owner_key, &encoded)?
                {
                    owners.push((owner, locator));
                }
                changes.push(journal.records.prepare_bytes(key, None, encoded.into())?);
            }
            bindings.extend(owners.iter().map(|(owner, key)| (owner, *key)));
            journal.retain_original_owners(attempt, &bindings).await?;
            Ok(changes)
        }
        .await;
        settlement.complete(result)
    }
}

impl PreparedOriginalBatch<'static> {
    pub(in crate::journal) fn include_owned(
        &mut self,
        journal: &JournalStore,
        event: CanonicalOriginalRef<'_>,
        history: Option<TableHistoryId>,
    ) -> StorageResult<()> {
        let (key, manifest, images) = declare_original(journal, event, history)?;
        self.keys
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.originals.include_owned(manifest, &images)?;
        self.keys.push(key);
        Ok(())
    }
}

fn declare_original<'a>(
    journal: &JournalStore,
    event: CanonicalOriginalRef<'a>,
    history: Option<TableHistoryId>,
) -> StorageResult<(DurableRecordKey, RetainedImageManifest, Vec<&'a [u8]>)> {
    let key = journal.identity_key(EVENT_SUFFIX, event.identity())?;
    journal.records.limits.validate_key(&key)?;
    let (images, causal) = event_body_spans(event, &key)?;
    let manifest = RetainedImageManifest::split_event(
        event.bytes(),
        &images,
        history.filter(|_| !images.is_empty()),
        causal,
    )?;
    Ok((key, manifest, images))
}

impl JournalStore {
    pub(in crate::journal) fn decode_retained_event(
        &self,
        row: &DurableRecord,
        bytes: &[u8],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<CanonicalEvent> {
        let event = codec.decode_event(bytes).map_err(|_| corrupt_record())?;
        if row.revision != Revision::INITIAL
            || row.key != self.identity_key(EVENT_SUFFIX, &event.as_ref().id)?
        {
            return Err(corrupt_record());
        }
        Ok(event)
    }

    pub(in crate::journal) async fn prepare_original_batch<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: &[(&CanonicalEvent, Option<TableHistoryId>)],
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let mut originals = PreparedOriginalBatch::default();
            for (event, history) in input {
                originals.include(self, (*event).into(), *history)?;
            }
            originals.prepare(self, settlement.as_mut()).await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn prepare_baseline_manifest<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&[u8], TableHistoryId, DurableRecordKey),
    ) -> StorageResult<Arc<[u8]>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let images = baseline_body_spans(input.0)?;
            let manifest = RetainedImageManifest::split(
                input.0,
                &images,
                (!images.is_empty()).then_some(input.1),
            )?;
            self.prepare_image_manifest(
                attempt,
                (&manifest, &images),
                (RetainedOwnerKind::Baseline, input.2),
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_image_manifest<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&RetainedImageManifest, &[&[u8]]),
        owner: (RetainedOwnerKind, DurableRecordKey),
    ) -> StorageResult<Arc<[u8]>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (manifest, images) = input;
            self.records.limits.validate_key(&owner.1)?;
            let locator = DurableRecordKey::try_from(&owner.1)?;
            let encoded = manifest
                .prepare_with_custody(attempt, images, self.records.limits, &self.records)
                .await?;
            if let Some(receipt) = manifest.prepare_owner_record(owner.0, locator, &encoded)? {
                self.retain_original_owner(attempt, (&receipt, &owner.1))
                    .await?;
            }
            Ok(encoded.into())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_retained_bytes<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Arc<[u8]>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let manifest = self.decode_retained_manifest(row, codec)?;
            let maximum = manifest.original_length();
            let mut bytes = self
                .load_retained_manifests(
                    attempt,
                    std::slice::from_ref(row),
                    (vec![manifest], maximum),
                )
                .await
                .map_err(storage_types::StorageError::from)?;
            if bytes.len() != 1 {
                return Err(corrupt_record());
            }
            bytes.pop().ok_or_else(corrupt_record)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn decode_retained_manifest(
        &self,
        row: &DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<RetainedImageManifest> {
        let encoded_length = RetainedPayload::decoded_length(&row.bytes)?;
        codec
            .require_event_length(
                encoded_length.saturating_sub(RetainedImageManifest::metadata_length_bound(0)?),
            )
            .map_err(|_| corrupt_record())?;
        let payload = RetainedPayload::decode(&row.bytes, encoded_length)?;
        let length = RetainedImageManifest::declared_length(&payload)?;
        codec
            .require_event_length(length)
            .map_err(|_| corrupt_record())?;
        let manifest = RetainedImageManifest::decode(&payload, length)?;
        manifest.require_causal_scope(&row.key)?;
        Ok(manifest)
    }
}

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Deserialize)]
struct Images<'a> {
    #[serde(borrow)]
    old: Option<&'a RawValue>,
    #[serde(borrow)]
    new: Option<&'a RawValue>,
    #[serde(borrow)]
    old_indexers: Option<&'a RawValue>,
    #[serde(borrow)]
    new_indexers: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct ImageContainer<'a> {
    #[serde(borrow)]
    key: &'a RawValue,
    #[serde(borrow)]
    canonical_key: &'a RawValue,
    #[serde(borrow)]
    images: Images<'a>,
}

#[derive(Deserialize)]
struct ItemBodySpans<'a> {
    #[serde(borrow)]
    key: &'a RawValue,
    #[serde(borrow)]
    canonical_key: &'a RawValue,
    #[serde(borrow)]
    images: Images<'a>,
    #[serde(borrow)]
    observations: &'a RawValue,
    #[serde(borrow)]
    parents: &'a RawValue,
}

#[derive(Deserialize)]
struct ObservedEventSpan<'a> {
    #[serde(borrow)]
    event: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct EventEnvelope<'a> {
    #[serde(borrow)]
    payload: ItemPayload<'a>,
}

#[derive(Deserialize)]
enum ItemPayload<'a> {
    Item(#[serde(borrow)] ItemBodySpans<'a>),
}

#[derive(Deserialize)]
struct BaselineEnvelope<'a> {
    #[serde(borrow)]
    baseline: ImageContainer<'a>,
}

fn event_body_spans<'a>(
    event: CanonicalOriginalRef<'a>,
    locator: &DurableRecordKey,
) -> StorageResult<(Vec<&'a [u8]>, Option<RetainedCausalBody<'a>>)> {
    let Some(has_observations) = event.item_observations() else {
        return Ok((Vec::new(), None));
    };
    let envelope: EventEnvelope<'_> =
        serde_json::from_slice(event.bytes()).map_err(|_| corrupt_record())?;
    let ItemPayload::Item(item) = envelope.payload;
    let mut bodies = retained_item_body_spans(item.images, item.key, item.canonical_key)?;
    if !has_observations {
        return Ok((bodies, None));
    }
    let observations = item.observations.get().as_bytes();
    let causal = event_causal_body((observations, item.parents.get().as_bytes()), locator)?;
    bodies.push(observations);
    Ok((bodies, Some(causal)))
}

fn event_causal_body<'a>(
    input: (&'a [u8], &'a [u8]),
    locator: &DurableRecordKey,
) -> StorageResult<RetainedCausalBody<'a>> {
    let (bytes, parents) = input;
    let parents: Vec<&RawValue> = serde_json::from_slice(parents).map_err(|_| corrupt_record())?;
    let observations: Vec<ObservedEventSpan<'_>> =
        serde_json::from_slice(bytes).map_err(|_| corrupt_record())?;
    let mut aliases = Vec::new();
    aliases
        .try_reserve_exact(parents.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    aliases.extend(parents.into_iter().filter_map(|parent| {
        let original = parent.get().as_bytes();
        observations
            .iter()
            .filter_map(|observation| observation.event)
            .find(|observed| observed.get().as_bytes() == original)
            .map(|observed| (original, observed.get().as_bytes()))
    }));
    RetainedCausalBody::try_new(bytes, locator, aliases)
}

pub(super) fn baseline_body_spans(bytes: &[u8]) -> StorageResult<Vec<&[u8]>> {
    let envelope: BaselineEnvelope<'_> =
        serde_json::from_slice(bytes).map_err(|_| corrupt_record())?;
    let item = envelope.baseline;
    retained_item_body_spans(item.images, item.key, item.canonical_key)
}

fn retained_item_body_spans<'a>(
    images: Images<'a>,
    key: &'a RawValue,
    canonical_key: &'a RawValue,
) -> StorageResult<Vec<&'a [u8]>> {
    let spans = [
        Some(key),
        Some(canonical_key),
        images.old,
        images.new,
        images.old_indexers,
        images.new_indexers,
    ];
    let mut bodies = Vec::new();
    // The optional causal observations body joins these item spans below.
    bodies
        .try_reserve_exact(spans.len() + 1)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    bodies.extend(
        spans
            .into_iter()
            .flatten()
            .map(|body| body.get().as_bytes()),
    );
    Ok(bodies)
}
