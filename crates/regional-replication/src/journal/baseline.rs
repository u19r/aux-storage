use infrastructure_provider::record::{DurableRecord, RecordAddress};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::{
        baseline::BaselineItem, content::SourceImagesRef, identity::VersionIdentity,
        order::WinnerProvenance,
    },
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{constants::BASELINE_SUFFIX, native_record::corrupt_record};

/// Native immutable baseline bytes, verified against their deterministic key.
/// There is no canonical origin event for a baseline.
pub struct BaselineAuthority {
    item: BaselineItem,
    digest: CanonicalDigest,
    bytes: std::sync::Arc<[u8]>,
}

impl BaselineAuthority {
    pub(crate) fn decode(
        bytes: std::sync::Arc<[u8]>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Self> {
        let item = codec
            .decode_baseline(&bytes)
            .map_err(|_| corrupt_record())?;
        let digest = CanonicalDigest::of(&bytes);
        Ok(Self {
            item,
            digest,
            bytes,
        })
    }
    pub(crate) fn into_bytes(self) -> std::sync::Arc<[u8]> {
        self.bytes
    }
    pub fn item(&self) -> &BaselineItem {
        &self.item
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn provenance(&self) -> WinnerProvenance {
        self.item.provenance(self.digest)
    }
}

impl JournalStore {
    pub(in crate::journal) fn decode_retained_baseline(
        &self,
        row: &DurableRecord,
        bytes: std::sync::Arc<[u8]>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<BaselineAuthority> {
        let authority = BaselineAuthority::decode(bytes, codec)?;
        if row.revision != Revision::INITIAL
            || row.key != self.baseline_key(&authority.provenance().version)?
        {
            return Err(corrupt_record());
        }
        Ok(authority)
    }

    pub(in crate::journal) fn baseline_key(
        &self,
        identity: &VersionIdentity,
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        let VersionIdentity::Baseline { lineage, key } = identity else {
            return Err(corrupt_record());
        };
        let mut address: RecordAddress = self.identity_key(BASELINE_SUFFIX, lineage)?.into();
        address
            .suffix
            .extend_from_slice(CanonicalDigest::of(key.as_ref()).as_ref());
        address.try_into()
    }

    pub(crate) async fn load_baseline_page<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: (
            replication_types::identity::resource::TableLineage,
            Option<infrastructure_provider::record::DurableRecordKey>,
        ),
        input: (
            &CanonicalJsonCodec,
            crate::bootstrap::copy_scope::BaselineCopyBudget,
        ),
    ) -> StorageResult<BaselineAuthorityPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (lineage, after) = scope;
            let (codec, budget) = input;
            let first = self.identity_key(BASELINE_SUFFIX, &lineage)?;
            let original = self.record_prefix_range(&first)?;
            let (first, last, _) = original.bounds();
            let range = infrastructure_provider::page::BoundedRecordRange::try_new(
                first.clone(),
                last.clone(),
                budget.records,
            )?;
            let request = infrastructure_provider::page::RecordPageRequest {
                range,
                after,
                max_bytes: self.records.limits.max_bytes(),
                protection: ReadProtection::Snapshot,
            };
            let page = self.scan_immutable_records(attempt, request).await?;
            let mut selected = Vec::with_capacity(page.records.len());
            let mut manifests = Vec::with_capacity(page.records.len());
            let mut used = 0usize;
            let mut last = None;
            let mut continuation = page.continuation;
            for row in page.records {
                let manifest = self.decode_retained_manifest(&row, codec)?;
                let size = self
                    .records
                    .limits
                    .validate_key(&row.key)?
                    .checked_add(manifest.original_length())
                    .ok_or_else(corrupt_record)?;
                let next = used.checked_add(size).ok_or_else(corrupt_record)?;
                if next > budget.bytes.get() {
                    continuation = Some(last.ok_or(
                        storage_types::InfrastructureResultKind::InvalidBound {
                            context: storage_types::InfrastructureContext::RecordPage,
                        },
                    )?);
                    break;
                }
                used = next;
                last = Some(row.key.clone());
                selected.push(row);
                manifests.push(manifest);
            }
            let bytes = self
                .load_retained_manifests(attempt, &selected, (manifests, budget.bytes.get()))
                .await
                .map_err(storage_types::StorageError::from)?;
            let authorities = selected
                .into_iter()
                .zip(bytes)
                .map(|(row, bytes)| {
                    let authority = self.decode_retained_baseline(&row, bytes, codec)?;
                    if authority.item.lineage != lineage {
                        return Err(corrupt_record());
                    }
                    Ok(authority)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(BaselineAuthorityPage {
                authorities,
                continuation,
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub async fn load_baseline<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        identity: &VersionIdentity,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<BaselineAuthority>> {
        use super::reconstruction::{RetainedAuthorityRequest, WinnerAuthority};
        let mut authorities = self
            .load_retained_authorities(
                attempt,
                &[RetainedAuthorityRequest::Baseline(identity)],
                codec,
            )
            .await?;
        match authorities.pop().ok_or_else(corrupt_record)? {
            Some(WinnerAuthority::Baseline(baseline)) => Ok(Some(*baseline)),
            None => Ok(None),
            Some(WinnerAuthority::Event(_)) => Err(corrupt_record()),
        }
    }

    pub(crate) async fn store_baseline<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &BaselineItem<SourceImagesRef<'_>>,
            storage_types::TableHistoryId,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<BaselineAuthority> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (baseline, history) = input;
            let bytes = codec
                .encode_baseline(baseline)
                .map_err(|_| corrupt_record())?;
            let authority = BaselineAuthority::decode(bytes.into(), codec)?;
            self.store_baseline_bytes(
                attempt,
                (
                    &authority.provenance(),
                    std::sync::Arc::clone(&authority.bytes),
                    history,
                ),
                codec,
            )
            .await?;
            Ok(authority)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn load_baseline_image<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            storage_types::TableHistoryId,
            Option<&storage_types::WireItem>,
            Option<storage_types::StoredImageSource>,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<storage_types::WireItem>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (history, observed, source) = input;
            let Some(observed) = observed else {
                if source.is_some() {
                    return Err(corrupt_record());
                }
                return Ok(None);
            };
            let source = source.ok_or_else(corrupt_record)?;
            if source.address.history() != history {
                return Err(corrupt_record());
            }
            codec
                .require_event_length(source.raw_bytes.get())
                .map_err(|_| corrupt_record())?;
            let exact = stream_provider::retained_image::load_stored_image(attempt, source).await?;
            stream_provider::retained_image::require_materialized_image(&exact, observed)?;
            Ok(Some(exact))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn retain_received_baseline<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&BaselineAuthority, storage_types::TableHistoryId),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (authority, history) = input;
            self.store_baseline_bytes(
                attempt,
                (
                    &authority.provenance(),
                    std::sync::Arc::clone(&authority.bytes),
                    history,
                ),
                codec,
            )
            .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn store_baseline_bytes<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        input: (
            &WinnerProvenance,
            std::sync::Arc<[u8]>,
            storage_types::TableHistoryId,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let (provenance, bytes, history) = input;
        let key = self.baseline_key(&provenance.version)?;
        let rows = self
            .load_immutable_waves(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?;
        if let Some(row) = &rows[0] {
            let old = self.decode_retained_baseline(
                row,
                self.load_retained_bytes(attempt, row, codec).await?,
                codec,
            )?;
            if old.bytes() != bytes.as_ref() {
                return Err(corrupt_record());
            }
        } else {
            let manifest = self
                .prepare_baseline_manifest(attempt, (&bytes, history, key.clone()))
                .await?;
            let change = self.records.prepare_bytes(key, None, manifest)?;
            self.records.store(attempt, &[change]).await?;
        }
        Ok(())
    }
}

impl JournalStore {
    /// Bind a captured or received baseline without inventing an origin or
    /// stream event.
    pub(in crate::journal) async fn apply_baseline_effects<A: storage_provider::DynamoAttempt>(
        &self,
        attempt: &mut A,
        input: (
            std::sync::Arc<storage_types::BoundTableInfo>,
            &BaselineAuthority,
            storage_types::ItemProvenance,
        ),
        observed: (
            storage_provider::ItemWriteObservation,
            storage_backfill::GsiProjectionLifecycle,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        use replication_types::event::content::CanonicalEventCodec;
        use storage_provider::{
            BoundDeleteItem, PutItemShape, prepare_baseline_delete, prepare_baseline_upsert,
        };
        let (table, baseline, provenance) = input;
        let item = baseline.item();
        match &item.images.new {
            Some(image) => {
                let request = storage_types::PutItemRequest::builder()
                    .table_name(table.table_name.clone())
                    .item(image.clone())
                    .indexers(item.images.new_indexers.clone())
                    .build();
                let policy = attempt.read_ttl_config(&table).await?;
                let shape =
                    PutItemShape::try_from_request(request)?.bind_table(table, policy.as_ref())?;
                if codec.encode_key(&shape.key).map_err(|_| corrupt_record())? != item.canonical_key
                {
                    return Err(corrupt_record());
                }
                let (accepted, _) = shape.accept_old(observed.0, observed.1)?.into_result()?;
                prepare_baseline_upsert(attempt, accepted, provenance).await?;
            }
            None => {
                let shape = BoundDeleteItem::for_maintenance(table, item.key.clone())?;
                let accepted = shape.accept_old(observed.0, observed.1)?.into_result()?;
                prepare_baseline_delete(attempt, &accepted, provenance).await?;
            }
        }
        attempt.flush_dynamo_write().await
    }
}

pub struct BaselineAuthorityPage {
    pub authorities: Vec<BaselineAuthority>,
    pub continuation: Option<infrastructure_provider::record::DurableRecordKey>,
}
