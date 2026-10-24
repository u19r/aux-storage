use std::collections::BTreeMap;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange},
};
use replication_types::{
    event::interval::CanonicalEventLink,
    identity::{origin::OriginId, resource::TableLineage},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::RETAINED_LINK_SUFFIX, history::publication_scope::SourcePublicationScope,
    native_record::corrupt_record,
};

/// Preserve the original publication address independently of winner order.
/// In particular, expiration order names its victim, not its publication.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct RetainedSourceLink {
    pub origin: OriginId,
    pub link: CanonicalEventLink,
}

pub(in crate::journal) struct RetainedLinksPage {
    pub links: Vec<RetainedSourceLink>,
    pub continuation: Option<DurableRecordKey>,
}

#[derive(Clone, Copy)]
struct RetainedPublicationInput<'link> {
    origin: &'link OriginId,
    link: &'link CanonicalEventLink,
}

struct RetainedPublicationInventory<'link> {
    links: BTreeMap<DurableRecordKey, RetainedPublicationInput<'link>>,
    lookups: BTreeMap<DurableRecordKey, RetainedPublicationInput<'link>>,
}

impl JournalStore {
    pub(in crate::journal) fn decode_retained_source_link(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<RetainedSourceLink> {
        let retained: RetainedSourceLink = self.records.decode(row)?;
        if row.revision != Revision::INITIAL
            || row.key != self.retained_link_key(&retained.origin, &retained.link)?
        {
            return Err(corrupt_record());
        }
        self.require_publication_origin(&retained.origin, &retained.link)?;
        Ok(retained)
    }

    pub(in crate::journal) fn require_publication_origin(
        &self,
        origin: &OriginId,
        link: &CanonicalEventLink,
    ) -> StorageResult<()> {
        link.require_source(origin).map_err(|_| corrupt_record())?;
        if self.is_local_origin(origin) {
            self.partitions
                .require_partition(origin.journal_partition)
                .map_err(|_| corrupt_record())?;
        }
        Ok(())
    }

    pub(in crate::journal) fn retained_link_key(
        &self,
        origin: &OriginId,
        link: &CanonicalEventLink,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self
            .identity_key(RETAINED_LINK_SUFFIX, &link.lineage)?
            .into();
        let serialized_digest = CanonicalDigest::of_json(&(origin, link.position, link.ordinal))
            .map_err(|_| corrupt_record())?;
        key.suffix.extend_from_slice(serialized_digest.as_ref());
        key.try_into()
    }

    pub(in crate::journal) fn prepare_retained_publications<'attempt, A: PhysicalRecordAttempt>(
        &'attempt self,
        attempt: &'attempt mut A,
        links: impl Iterator<Item = (&'attempt OriginId, &'attempt CanonicalEventLink)>,
    ) -> impl Future<Output = StorageResult<Vec<RecordChange>>> + Send + 'attempt {
        let inventory = self.prepare_retained_publication_inventory(links);
        self.load_retained_publication_changes(attempt, inventory)
    }

    async fn load_retained_publication_changes<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        inventory: StorageResult<RetainedPublicationInventory<'_>>,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let inventory = inventory?;
            let keys = inventory
                .links
                .keys()
                .chain(inventory.lookups.keys())
                .cloned()
                .collect::<Vec<_>>();
            let rows = self
                .load_immutable_waves(attempt, &keys, ReadProtection::Conflict)
                .await?;
            let observed = keys.into_iter().zip(rows).collect::<BTreeMap<_, _>>();
            self.prepare_retained_publication_changes(inventory, &observed)
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_retained_publication_inventory<'link>(
        &self,
        links: impl Iterator<Item = (&'link OriginId, &'link CanonicalEventLink)>,
    ) -> StorageResult<RetainedPublicationInventory<'link>> {
        let mut inventory = RetainedPublicationInventory {
            links: BTreeMap::new(),
            lookups: BTreeMap::new(),
        };
        for (origin, link) in links {
            self.require_publication_origin(origin, link)?;
            let input = RetainedPublicationInput { origin, link };
            let key = self.retained_link_key(origin, link)?;
            if inventory
                .links
                .insert(key, input)
                .is_some_and(|existing| existing.origin != origin || existing.link != link)
            {
                return Err(corrupt_record());
            }
            let lookup = self.publication_lookup_key(
                &link.event,
                SourcePublicationScope::new(link.lineage, origin, link.table),
            )?;
            match inventory.lookups.entry(lookup) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(input);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let previous = entry.get();
                    if previous.origin != origin {
                        return Err(corrupt_record());
                    }
                    if self.is_earlier_publication(previous.link, link)? {
                        entry.insert(input);
                    }
                }
            }
        }
        Ok(inventory)
    }

    fn prepare_retained_publication_changes(
        &self,
        inventory: RetainedPublicationInventory<'_>,
        observed: &BTreeMap<DurableRecordKey, Option<DurableRecord>>,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut changes = Vec::with_capacity(observed.len());
        for (key, input) in inventory.links {
            let RetainedPublicationInput { origin, link } = input;
            let row = observed.get(&key).ok_or_else(corrupt_record)?.as_ref();
            if let Some(row) = row {
                let existing = self.decode_retained_source_link(row)?;
                if existing.origin != *origin || existing.link != *link {
                    return Err(corrupt_record());
                }
            } else {
                let publication = RetainedSourceLink {
                    origin: origin.clone(),
                    link: link.clone(),
                };
                changes.push(self.records.prepare(key, None, &publication)?);
            }
        }
        for (key, input) in inventory.lookups {
            let RetainedPublicationInput { origin, link } = input;
            let publication = RetainedSourceLink {
                origin: origin.clone(),
                link: link.clone(),
            };
            let row = observed.get(&key).ok_or_else(corrupt_record)?.as_ref();
            if let Some(change) = self.prepare_publication_lookup(row, publication)? {
                changes.push(change);
            }
        }
        Ok(changes)
    }

    pub(in crate::journal) async fn load_retained_links<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        lineage: TableLineage,
        page: (Option<DurableRecordKey>, std::num::NonZeroUsize),
    ) -> StorageResult<RetainedLinksPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let first = self.identity_key(RETAINED_LINK_SUFFIX, &lineage)?;
            let original = self.record_prefix_range(&first)?;
            let (first, last, _) = original.bounds();
            let count = std::num::NonZeroUsize::new(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .saturating_sub(1)
                    .max(1),
            )
            .ok_or_else(corrupt_record)?;
            let range = infrastructure_provider::page::BoundedRecordRange::try_new(
                first.clone(),
                last.clone(),
                count.min(page.1),
            )?;
            let request = infrastructure_provider::page::RecordPageRequest {
                range,
                after: page.0,
                max_bytes: self.records.limits.max_bytes(),
                protection: ReadProtection::Snapshot,
            };
            let page = self.scan_immutable_records(attempt, request).await?;
            let mut links = Vec::with_capacity(page.records.len());
            for row in page.records {
                let retained = self.decode_retained_source_link(&row)?;
                if retained.link.lineage != lineage {
                    return Err(corrupt_record());
                }
                links.push(retained);
            }
            Ok(RetainedLinksPage {
                links,
                continuation: page.continuation,
            })
        }
        .await;
        settlement.complete(result)
    }
}
