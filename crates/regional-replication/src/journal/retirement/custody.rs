//! Native retired-source custody independent of a bootstrap capture request.

use std::borrow::Cow;

use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
};
use replication_types::{
    configuration::installed::{InstalledTableState, RegionalSourceBinding},
    identity::origin::OriginId,
};
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::{catalog::RetiredSourceReference, record::RetiredOrigin};
use crate::{
    constants::{RETIRED_ORIGIN_SUFFIX, RETIRED_SOURCE_CATALOG_SUFFIX},
    journal::JournalStore,
    lifecycle::{SealRequest, SealedOriginCut},
    native_record::corrupt_record,
};

pub(in crate::journal) struct RetiredSourceAuthority<'scope> {
    removal: Cow<'scope, SealRequest>,
    custodian: Cow<'scope, InstalledTableState>,
    location: RetiredSourceLocation,
}

enum RetiredSourceLocation {
    Survivor,
    Adopted,
}

impl<'scope> RetiredSourceAuthority<'scope> {
    pub(in crate::journal) fn source(&self) -> StorageResult<&RegionalSourceBinding> {
        self.removal.source()
    }

    pub(super) fn from_capture(
        definition: &'scope crate::history_capture::RetiredHistoryDefinition,
        custodian: &'scope InstalledTableState,
    ) -> Self {
        let location = if definition.is_adopted() {
            RetiredSourceLocation::Adopted
        } else {
            RetiredSourceLocation::Survivor
        };
        Self {
            removal: Cow::Borrowed(definition.removal_scope()),
            custodian: Cow::Borrowed(custodian),
            location,
        }
    }

    fn require_table(
        &self,
        journal: &JournalStore,
        current: &InstalledTableState,
    ) -> StorageResult<()> {
        let custodian = &self.custodian;
        let installed = (
            current.configuration.table,
            current.physical,
            current.configuration.tenant,
            current.configuration.lineage,
        );
        let retained = (
            custodian.configuration.table,
            custodian.physical,
            custodian.configuration.tenant,
            custodian.configuration.lineage,
        );
        let before = self.removal.before()?;
        if retained != installed
            || custodian.membership > current.membership
            || (before.configuration.lineage, before.configuration.tenant)
                != (current.configuration.lineage, current.configuration.tenant)
        {
            return Err(corrupt_record());
        }
        let sealed = self.source()?;
        if current.sources.iter().any(|source| {
            journal.is_current_history_source(current, source)
                && (
                    &source.region,
                    source.database_group,
                    source.generation,
                    source.table,
                ) == (
                    &sealed.region,
                    sealed.database_group,
                    sealed.generation,
                    sealed.table,
                )
        }) {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    pub(in crate::journal) async fn load_retired_source_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        current: &InstalledTableState,
        source: &RegionalSourceBinding,
        protection: ReadProtection,
    ) -> StorageResult<RetiredSourceAuthority<'static>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.retired_source_catalog_key(
                (current.configuration.table, current.configuration.lineage),
                source,
            )?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(|| {
                    StorageError::validation("History source is not retained for the table")
                })?;
            let reference = self.decode_retired_source_reference(&row)?;
            let authority = self
                .load_retired_custody(attempt, &reference, protection)
                .await?;
            authority.require_table(self, current)?;
            if authority.source()? != source {
                return Err(StorageError::validation(
                    "History source binding does not match retained authority",
                ));
            }
            Ok(authority)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn find_next_retired_source<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        current: &InstalledTableState,
        after: Option<&RegionalSourceBinding>,
        protection: ReadProtection,
    ) -> StorageResult<Option<RetiredSourceAuthority<'static>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local = (current.configuration.table, current.configuration.lineage);
            let first = self.identity_key(RETIRED_SOURCE_CATALOG_SUFFIX, &local)?;
            let original = self.record_prefix_range(&first)?;
            let bounds = original.bounds();
            let range = BoundedRecordRange::try_new(
                bounds.0.clone(),
                bounds.1.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            let after = after
                .map(|source| self.retired_source_catalog_key(local, source))
                .transpose()?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after,
                        max_bytes: self.records.limits.max_bytes(),
                        protection,
                    },
                )
                .await?;
            let Some(row) = page.records.into_iter().next() else {
                return Ok(None);
            };
            let reference = self.decode_retired_source_reference(&row)?;
            let authority = self
                .load_retired_custody(attempt, &reference, protection)
                .await?;
            authority.require_table(self, current)?;
            if row.key != self.retired_source_catalog_key(local, authority.source()?)? {
                return Err(corrupt_record());
            }
            Ok(Some(authority))
        }
        .await;
        settlement.complete(result)
    }

    async fn load_retired_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &RetiredSourceReference,
        protection: ReadProtection,
    ) -> StorageResult<RetiredSourceAuthority<'static>> {
        let (removal, custodian, location) = match reference {
            RetiredSourceReference::Removal { .. } => {
                let (removal, _) = self
                    .load_received_retirement(attempt, reference, protection)
                    .await?;
                let custodian = removal
                    .proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    .clone();
                (removal, custodian, RetiredSourceLocation::Survivor)
            }
            RetiredSourceReference::Adoption { .. } => {
                let (original, proposal, _) = self
                    .load_adopted_retirement(attempt, reference, protection)
                    .await?;
                let custodian = proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    .clone();
                (
                    original.into_original_seal(),
                    custodian,
                    RetiredSourceLocation::Adopted,
                )
            }
        };
        Ok(RetiredSourceAuthority {
            removal: Cow::Owned(removal),
            custodian: Cow::Owned(custodian),
            location,
        })
    }

    pub(in crate::journal) async fn load_retired_source_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        current: &InstalledTableState,
        scope: (&RetiredSourceAuthority<'_>, &[OriginId]),
        protection: ReadProtection,
    ) -> StorageResult<Vec<SealedOriginCut>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (authority, origins) = scope;
            authority.require_table(self, current)?;
            let source = authority.source()?;
            let local = (
                current.configuration.table,
                source.table,
                current.configuration.lineage,
            );
            let keys = origins
                .iter()
                .map(|origin| match authority.location {
                    RetiredSourceLocation::Adopted => self.adopted_retired_cut_key(local, origin),
                    RetiredSourceLocation::Survivor => self
                        .identity_key(RETIRED_ORIGIN_SUFFIX, &(local.0, local.1, local.2, origin)),
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self.records.load(attempt, &keys, protection).await?;
            let coverage = self
                .load_received_coverages(attempt, local, origins, protection)
                .await?;
            let survivors = authority.removal.survivors()?;
            let content = authority.removal.content()?;
            rows.into_iter()
                .zip(origins)
                .zip(coverage)
                .map(|((row, origin), coverage)| {
                    let row = row.as_ref().ok_or_else(corrupt_record)?;
                    if matches!(authority.location, RetiredSourceLocation::Adopted)
                        && row.revision != storage_types::Revision::INITIAL
                    {
                        return Err(corrupt_record());
                    }
                    let retired: RetiredOrigin = self.records.decode(row)?;
                    if retired.operation != authority.removal.proposal.operation().id
                        || retired.region != source.region
                        || retired.content != content
                    {
                        return Err(corrupt_record());
                    }
                    authority.removal.require_history_cut(
                        (
                            &retired.cut.origin,
                            retired.cut.through,
                            &retired.cut.receipts,
                        ),
                        u32::from(u16::from(origin.journal_partition)),
                        survivors,
                    )?;
                    if retired.cut.origin != *origin {
                        return Err(corrupt_record());
                    }
                    if coverage.through() < retired.cut.through {
                        return Err(storage_types::InfrastructureResultKind::Unavailable {
                            context: storage_types::InfrastructureContext::RegionalSource,
                        }
                        .into());
                    }
                    Ok(retired.cut)
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }
}
