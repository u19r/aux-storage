use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::{
    content::{CanonicalEvent, CanonicalPayload, SourceImagesRef},
    order::WinnerProvenance,
};
use storage_types::{KeyAttributes, StorageResult, WireItem};

use crate::{
    journal::{BaselineAuthority, JournalStore},
    native_record::corrupt_record,
};

pub(in crate::journal) enum WinnerAuthority {
    Baseline(Box<BaselineAuthority>),
    Event(CanonicalEvent),
}

impl WinnerAuthority {
    pub(in crate::journal) fn require_materialized_images(
        &self,
        observed: (Option<&WireItem>, Option<&[String]>),
    ) -> StorageResult<()> {
        let images = self.images()?;
        let matches = match (images.new, observed.0) {
            (Some(expected), Some(actual)) => {
                stream_provider::retained_image::require_materialized_image(expected, actual)?;
                true
            }
            (None, None) => true,
            _ => false,
        };
        if !matches || images.new_indexers.unwrap_or_default() != observed.1.unwrap_or_default() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn from_event(event: CanonicalEvent) -> Option<Self> {
        matches!(&event.as_ref().payload, CanonicalPayload::Item(_)).then_some(Self::Event(event))
    }

    pub(in crate::journal) fn images(&self) -> StorageResult<SourceImagesRef<'_>> {
        match self {
            Self::Baseline(baseline) => Ok((&baseline.item().images).into()),
            Self::Event(event) => match &event.as_ref().payload {
                CanonicalPayload::Item(item) => Ok((&item.images).into()),
                _ => Err(corrupt_record()),
            },
        }
    }

    pub(in crate::journal) fn key(&self) -> StorageResult<&KeyAttributes> {
        match self {
            Self::Baseline(baseline) => Ok(&baseline.item().key),
            Self::Event(event) => match &event.as_ref().payload {
                CanonicalPayload::Item(item) => Ok(&item.key),
                _ => Err(corrupt_record()),
            },
        }
    }

    pub(in crate::journal) fn provenance(&self) -> StorageResult<WinnerProvenance> {
        match self {
            Self::Baseline(baseline) => Ok(baseline.provenance()),
            Self::Event(event) => {
                let CanonicalPayload::Item(item) = &event.as_ref().payload else {
                    return Err(corrupt_record());
                };
                Ok(WinnerProvenance {
                    lineage: item.lineage,
                    key: item.canonical_key.clone(),
                    version: item.version.clone(),
                    event: Some(event.as_ref().id.clone()),
                    order: item.order.clone(),
                    digest: event.as_ref().digest,
                })
            }
        }
    }
}

/// A known winner requires exact key/provenance evidence; an optional baseline
/// lookup preserves absence rather than synthesizing a winner or Genesis.
pub(in crate::journal) enum RetainedAuthorityRequest<'expected> {
    Winner(&'expected KeyAttributes, &'expected WinnerProvenance),
    Baseline(&'expected replication_types::event::identity::VersionIdentity),
}

impl JournalStore {
    pub(in crate::journal) async fn load_winner_authority<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        expected: (&KeyAttributes, &WinnerProvenance),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<WinnerAuthority> {
        self.load_retained_authorities(
            attempt,
            &[RetainedAuthorityRequest::Winner(expected.0, expected.1)],
            codec,
        )
        .await?
        .pop()
        .flatten()
        .ok_or_else(corrupt_record)
    }

    /// Admit the complete mixed metadata inventory before shared original-body
    /// hydration; each returned authority still binds its own caller's exact
    /// slot.
    pub(in crate::journal) async fn load_retained_authorities<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        requests: &[RetainedAuthorityRequest<'_>],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Vec<Option<WinnerAuthority>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let keys = requests
                .iter()
                .map(|request| request.prepare_record_keys(self))
                .collect::<StorageResult<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let mut rows = self
                .load_immutable_waves(
                    settlement.as_mut(),
                    &keys,
                    storage_types::ReadProtection::Snapshot,
                )
                .await?
                .into_iter();
            let metadata = requests
                .iter()
                .map(|request| request.decode_metadata(self, &mut rows))
                .collect::<StorageResult<Vec<_>>>()?;
            if rows.next().is_some() {
                return Err(corrupt_record());
            }
            let originals = metadata
                .iter()
                .flatten()
                .map(|entry| entry.original().clone())
                .collect::<Vec<_>>();
            let manifests = originals
                .iter()
                .map(|row| self.decode_retained_manifest(row, codec))
                .collect::<StorageResult<Vec<_>>>()?;
            let maximum = manifests.iter().try_fold(0usize, |bytes, manifest| {
                bytes
                    .checked_add(manifest.original_length())
                    .ok_or_else(corrupt_record)
            })?;
            let mut bytes = self
                .load_retained_manifests(settlement.as_mut(), &originals, (manifests, maximum))
                .await
                .map_err(storage_types::StorageError::from)?
                .into_iter();
            let authorities = requests
                .iter()
                .zip(metadata)
                .map(|(request, metadata)| {
                    let Some(metadata) = metadata else {
                        return Ok(None);
                    };
                    let authority = metadata.decode_authority(
                        self,
                        bytes.next().ok_or_else(corrupt_record)?,
                        codec,
                    )?;
                    request.require_authority(&authority)?;
                    Ok(Some(authority))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            if bytes.next().is_some() {
                return Err(corrupt_record());
            }
            Ok(authorities)
        }
        .await;
        settlement.complete(result)
    }
}

impl RetainedAuthorityRequest<'_> {
    fn prepare_record_keys(
        &self,
        journal: &JournalStore,
    ) -> StorageResult<Vec<infrastructure_provider::record::DurableRecordKey>> {
        match self {
            Self::Winner(_, winner) => match &winner.event {
                Some(event) => Ok(vec![
                    journal.identity_key(crate::constants::ACCEPTANCE_SUFFIX, event)?,
                    journal.identity_key(crate::constants::EVENT_SUFFIX, event)?,
                ]),
                None => Ok(vec![journal.baseline_key(&winner.version)?]),
            },
            Self::Baseline(identity) => Ok(vec![journal.baseline_key(identity)?]),
        }
    }

    fn decode_metadata(
        &self,
        journal: &JournalStore,
        rows: &mut impl Iterator<Item = Option<infrastructure_provider::record::DurableRecord>>,
    ) -> StorageResult<Option<WinnerAuthorityMetadata>> {
        let row = rows.next().ok_or_else(corrupt_record)?;
        let Self::Winner(_, winner) = self else {
            return Ok(row.map(WinnerAuthorityMetadata::Baseline));
        };
        let row = row.ok_or_else(corrupt_record)?;
        let Some(identity) = &winner.event else {
            return Ok(Some(WinnerAuthorityMetadata::Baseline(row)));
        };
        let acceptance = journal.decode_native_acceptance(&row)?;
        if &acceptance.receipt.event != identity || acceptance.receipt.digest != winner.digest {
            return Err(corrupt_record());
        }
        let original = rows.next().flatten().ok_or_else(corrupt_record)?;
        Ok(Some(WinnerAuthorityMetadata::Event {
            original,
            acceptance: Box::new(acceptance),
        }))
    }

    fn require_authority(&self, authority: &WinnerAuthority) -> StorageResult<()> {
        let provenance = authority.provenance()?;
        match self {
            Self::Winner(key, winner) if &provenance == *winner && authority.key()? == *key => {
                Ok(())
            }
            Self::Baseline(identity) if &provenance.version == *identity => Ok(()),
            _ => Err(corrupt_record()),
        }
    }
}

/// Retain each exact descriptor and receipt through shared body hydration.
enum WinnerAuthorityMetadata {
    Baseline(infrastructure_provider::record::DurableRecord),
    Event {
        original: infrastructure_provider::record::DurableRecord,
        acceptance: Box<super::super::history_view::NativeAcceptanceRecord>,
    },
}

impl WinnerAuthorityMetadata {
    fn original(&self) -> &infrastructure_provider::record::DurableRecord {
        match self {
            Self::Baseline(row) => row,
            Self::Event { original, .. } => original,
        }
    }

    fn decode_authority(
        self,
        journal: &JournalStore,
        bytes: std::sync::Arc<[u8]>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<WinnerAuthority> {
        match self {
            Self::Baseline(row) => journal
                .decode_retained_baseline(&row, bytes, codec)
                .map(|baseline| WinnerAuthority::Baseline(Box::new(baseline))),
            Self::Event {
                original,
                acceptance,
            } => {
                let event = journal.decode_retained_event(&original, &bytes, codec)?;
                journal.require_acceptance_binding(
                    (&acceptance.receipt, &acceptance.delivery),
                    (&acceptance.receipt.event, &event),
                )?;
                Ok(WinnerAuthority::Event(event))
            }
        }
    }
}
