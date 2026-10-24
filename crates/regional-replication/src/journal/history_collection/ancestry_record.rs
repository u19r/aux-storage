//! Collected ancestry indexes retain the request and exact native page address.
use infrastructure_provider::record::{DurableRecord, RecordChange};
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

use crate::{
    history_capture::{HistoryCaptureRequest, RetiredHistoryCatalogPage, RetiredHistoryPage},
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectedCatalogPage {
    request: HistoryCaptureRequest,
    page: RetiredHistoryCatalogPage,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectedRetiredPage {
    partition: u32,
    page: RetiredHistoryPage,
}

impl JournalStore {
    pub(super) fn prepare_collected_catalog_page(
        &self,
        request: &HistoryCaptureRequest,
        page: &RetiredHistoryCatalogPage,
    ) -> StorageResult<RecordChange> {
        self.require_collection_request(request)?;
        page.require_scope(request, &self.collection_catalog_scope(page.after.clone()))?;
        let key = self.identity_key(
            crate::constants::COLLECTED_CATALOG_SUFFIX,
            &(
                request.proposal.operation().id,
                &request.region,
                &page.after,
            ),
        )?;
        self.records.prepare(
            key,
            None,
            &CollectedCatalogPage {
                request: request.clone(),
                page: page.clone(),
            },
        )
    }

    pub(in crate::journal) fn decode_collected_catalog_page(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<RetiredHistoryCatalogPage> {
        let fact: CollectedCatalogPage = self.records.decode(row)?;
        self.require_collection_request(&fact.request)?;
        fact.page.require_scope(
            &fact.request,
            &self.collection_catalog_scope(fact.page.after.clone()),
        )?;
        let key = self.identity_key(
            crate::constants::COLLECTED_CATALOG_SUFFIX,
            &(
                fact.request.proposal.operation().id,
                &fact.request.region,
                &fact.page.after,
            ),
        )?;
        if row.revision != Revision::INITIAL || row.key != key {
            return Err(corrupt_record());
        }
        Ok(fact.page)
    }

    pub(super) fn prepare_collected_retired_index(
        &self,
        page: &RetiredHistoryPage,
        partition: u32,
    ) -> StorageResult<RecordChange> {
        let request = page.definition.request();
        self.require_collection_request(request)?;
        page.definition.require_page(page, partition)?;
        let key = self.identity_key(
            crate::constants::COLLECTED_RETIRED_PAGE_SUFFIX,
            &(
                request.proposal.operation().id,
                &request.region,
                page.definition.content(),
                partition,
            ),
        )?;
        self.records.prepare(
            key,
            None,
            &CollectedRetiredPage {
                partition,
                page: page.clone(),
            },
        )
    }

    pub(in crate::journal) fn decode_collected_retired_index(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<RetiredHistoryPage> {
        let fact: CollectedRetiredPage = self.records.decode(row)?;
        let request = fact.page.definition.request();
        self.require_collection_request(request)?;
        fact.page
            .definition
            .require_page(&fact.page, fact.partition)?;
        let key = self.identity_key(
            crate::constants::COLLECTED_RETIRED_PAGE_SUFFIX,
            &(
                request.proposal.operation().id,
                &request.region,
                fact.page.definition.content(),
                fact.partition,
            ),
        )?;
        if row.revision != Revision::INITIAL || row.key != key {
            return Err(corrupt_record());
        }
        Ok(fact.page)
    }

    pub(in crate::journal) fn require_collection_request(
        &self,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<()> {
        request.validate()?;
        let local = request
            .proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?;
        if local.proposed.configuration.tenant != self.tenant {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
