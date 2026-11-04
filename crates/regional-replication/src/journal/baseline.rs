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
            || row.key
                != self.baseline_key(
                    self.decode_original_payload(row, codec)?.history,
                    &authority.provenance().version,
                )?
        {
            return Err(corrupt_record());
        }
        Ok(authority)
    }

    pub(in crate::journal) fn baseline_key(
        &self,
        history: storage_types::TableHistoryId,
        identity: &VersionIdentity,
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        let VersionIdentity::Baseline { lineage, key } = identity else {
            return Err(corrupt_record());
        };
        let mut address: RecordAddress = self
            .table_identity_key(BASELINE_SUFFIX, history, lineage)?
            .into();
        address
            .suffix
            .extend_from_slice(CanonicalDigest::of(key.as_ref()).as_ref());
        address.try_into()
    }

    pub(crate) async fn load_baseline_page<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: (
            storage_types::TableHistoryId,
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
            let (history, lineage, after) = scope;
            let (codec, budget) = input;
            let first = self.table_identity_key(BASELINE_SUFFIX, history, &lineage)?;
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
            let mut used = 0usize;
            let mut last = None;
            let mut continuation = page.continuation;
            for row in page.records {
                let length = self
                    .decode_original_payload(&row, codec)?
                    .original_bytes()?
                    .len();
                let size = self
                    .records
                    .limits
                    .validate_key(&row.key)?
                    .checked_add(length)
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
            }
            let bytes = selected
                .iter()
                .map(|row| self.decode_original_bytes(row, codec))
                .collect::<StorageResult<Vec<_>>>()?;
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

    pub async fn load_baseline<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (storage_types::TableHistoryId, &VersionIdentity),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<BaselineAuthority>> {
        use super::reconstruction::{RetainedAuthorityRequest, WinnerAuthority};
        let mut authorities = self
            .load_retained_authorities(
                attempt,
                &[RetainedAuthorityRequest::Baseline(input.0, input.1)],
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
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
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

    pub(in crate::journal) fn prepare_baseline_image<'image>(
        &self,
        observed: Option<&'image storage_types::WireItem>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<&'image storage_types::WireItem>> {
        if let Some(image) = observed {
            codec
                .require_event_length(image.dynamo_json_bytes()?.len())
                .map_err(|_| corrupt_record())?;
        }
        Ok(observed)
    }

    pub(in crate::journal) async fn retain_received_baseline<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
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

    async fn store_baseline_bytes<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
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
        let key = self.baseline_key(history, &provenance.version)?;
        let rows = self
            .load_immutable_waves(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?;
        if let Some(row) = &rows[0] {
            let old =
                self.decode_retained_baseline(row, self.decode_original_bytes(row, codec)?, codec)?;
            if old.bytes() != bytes.as_ref() {
                return Err(corrupt_record());
            }
        } else {
            let change = self.records.prepare_original(key, &bytes, history)?;
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
