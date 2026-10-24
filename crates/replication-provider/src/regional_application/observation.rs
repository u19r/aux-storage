use replication_types::event::{disposition::CanonicalAcceptanceReceipt, order::WinnerProvenance};
/// Loaded inside the operation attempt, including any earlier effects in that
/// attempt.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EventApplication {
    Materialize,
    RetainHistory,
}

pub struct EventObservation<W> {
    pub application: EventApplication,
    pub schema: Option<replication_types::configuration::table::SchemaRevision>,
    pub accepted: Option<StoredAcceptance>,
    pub winner: Option<WinnerProvenance>,
    pub native: W,
}
#[derive(Clone)]
pub struct StoredAcceptance {
    pub receipt: CanonicalAcceptanceReceipt,
    pub event: replication_types::event::content::CanonicalEvent,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePageAcceptance {
    pub page: replication_types::event::interval::TablePageId,
    pub events: Vec<CanonicalAcceptanceReceipt>,
    pub selected_tables: Option<replication_types::event::interval::ImportTableSelection>,
}

/// Receipt authority for the selected fragment or its complete source
/// declaration.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ImportPageScope {
    Selected,
    Complete,
}

impl ImportPageScope {
    pub fn page_id(
        self,
        import: &replication_types::regional_import::RegionalImport,
        binding: &replication_types::configuration::membership::RegionalTableBinding,
    ) -> Result<
        replication_types::event::interval::TablePageId,
        replication_types::error::ReplicationProtocolError,
    > {
        let mut page = import.table_page_id(binding)?;
        if self == Self::Complete {
            page.fragment = None;
            page.selection = None;
        }
        Ok(page)
    }

    pub fn require_pages(
        self,
        import: &replication_types::regional_import::RegionalImport,
        pages: &[TablePageAcceptance],
    ) -> Result<(), replication_types::error::ReplicationProtocolError> {
        if pages.len() != import.bindings().len() {
            return Err(page_mismatch());
        }
        for (binding, page) in import.bindings().iter().zip(pages) {
            page.require_binding(import, binding, self)?;
        }
        Ok(())
    }
}

impl TablePageAcceptance {
    pub fn require_selection(
        &self,
    ) -> Result<(), replication_types::error::ReplicationProtocolError> {
        match (
            &self.selected_tables,
            self.page.fragment,
            self.page.selection,
        ) {
            (None, None, None) => Ok(()),
            (Some(selection), Some(_), Some(digest)) => {
                selection.require_table(self.page.table, self.page.lineage)?;
                if selection.digest()? != digest {
                    return Err(page_mismatch());
                }
                Ok(())
            }
            _ => Err(page_mismatch()),
        }
    }

    pub fn require_manifest<'manifest>(
        &self,
        manifest: &'manifest replication_types::event::interval::SourceIntervalManifest,
    ) -> Result<
        Vec<&'manifest replication_types::event::interval::CanonicalEventLink>,
        replication_types::error::ReplicationProtocolError,
    > {
        self.require_selection()?;
        manifest.validate_shape()?;
        if manifest
            .links
            .iter()
            .any(|link| link.table == self.page.table && link.lineage != self.page.lineage)
        {
            return Err(page_mismatch());
        }
        if self.page.origin != manifest.origin
            || self.page.range != manifest.range
            || self.page.manifest != manifest.digest
        {
            return Err(page_mismatch());
        }
        let links = match (
            &self.selected_tables,
            self.page.fragment,
            self.page.selection,
        ) {
            (None, None, None) => manifest
                .links
                .iter()
                .filter(|link| link.table == self.page.table && link.lineage == self.page.lineage)
                .collect::<Vec<_>>(),
            (Some(selection), Some(fragment), Some(_)) => {
                let selected = selection.select_links(manifest)?;
                selected
                    .get(fragment.after()..fragment.through())
                    .ok_or_else(page_mismatch)?
                    .iter()
                    .copied()
                    .filter(|link| link.table == self.page.table)
                    .collect()
            }
            _ => return Err(page_mismatch()),
        };
        if links.len() != self.events.len()
            || links
                .iter()
                .zip(&self.events)
                .any(|(link, receipt)| link.event != receipt.event || link.digest != receipt.digest)
        {
            return Err(page_mismatch());
        }
        Ok(links)
    }

    fn require_binding(
        &self,
        import: &replication_types::regional_import::RegionalImport,
        binding: &replication_types::configuration::membership::RegionalTableBinding,
        scope: ImportPageScope,
    ) -> Result<(), replication_types::error::ReplicationProtocolError> {
        let expected = scope.page_id(import, binding)?;
        let selection = match scope {
            ImportPageScope::Selected => import.fragment_selection()?,
            ImportPageScope::Complete => None,
        };
        if self.page != expected || self.selected_tables != selection {
            return Err(page_mismatch());
        }
        self.require_manifest(import.manifest())?;
        Ok(())
    }
}

fn page_mismatch() -> replication_types::error::ReplicationProtocolError {
    replication_types::error::ReplicationProtocolError::IdentityContentMismatch {
        context: "complete import page receipt",
    }
}
