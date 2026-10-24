use std::sync::Arc;

use crate::{
    configuration::membership::RegionalTableBinding,
    event::{
        content::CanonicalEvent,
        interval::{ImportFragment, SourceIntervalManifest},
    },
};

/// Immutable import whose constructors qualify bindings, manifest and
/// originals.
pub struct RegionalImport {
    bindings: Vec<RegionalTableBinding>,
    manifest: Arc<SourceIntervalManifest>,
    events: Vec<CanonicalEvent>,
    fragment: Option<ImportFragment>,
}

impl RegionalImport {
    pub fn try_new(
        input: (
            Vec<RegionalTableBinding>,
            SourceIntervalManifest,
            Vec<CanonicalEvent>,
        ),
        codec: &impl crate::event::interval::IntervalCodec,
    ) -> Result<Self, crate::error::ReplicationProtocolError> {
        Self::validate_import(input, None, codec)
    }

    pub fn try_fragment(
        input: (
            Vec<RegionalTableBinding>,
            SourceIntervalManifest,
            Vec<CanonicalEvent>,
        ),
        fragment: ImportFragment,
        codec: &impl crate::event::interval::IntervalCodec,
    ) -> Result<Self, crate::error::ReplicationProtocolError> {
        Self::validate_import(input, Some(fragment), codec)
    }

    pub fn select_table(
        &self,
        table: crate::identity::resource::TableIdentity,
    ) -> Result<Self, crate::error::ReplicationProtocolError> {
        if self.fragment.is_some() {
            return Err(crate::error::ReplicationProtocolError::InvalidScope {
                context: "table selection requires complete import",
            });
        }
        let binding = self
            .bindings
            .iter()
            .find(|binding| binding.source_table == table)
            .ok_or(crate::error::ReplicationProtocolError::InvalidScope {
                context: "unbound import table",
            })?;
        let events = self
            .event_bindings()
            .filter(|(binding, _)| binding.source_table == table)
            .map(|(_, event)| event.clone())
            .collect();
        let selected = Self {
            bindings: vec![binding.clone()],
            manifest: Arc::clone(&self.manifest),
            events,
            fragment: None,
        };
        selected.validate_bindings()?;
        Ok(selected)
    }

    pub fn select_fragment(
        &self,
        fragment: ImportFragment,
    ) -> Result<Self, crate::error::ReplicationProtocolError> {
        if self.fragment.is_some() {
            return Err(crate::error::ReplicationProtocolError::InvalidScope {
                context: "fragment selection requires complete import",
            });
        }
        let events = self
            .events
            .get(fragment.after()..fragment.through())
            .ok_or(crate::error::ReplicationProtocolError::InvalidRange {
                context: "fragment outside complete import",
            })?
            .to_vec();
        let selected = Self {
            bindings: self.bindings.clone(),
            manifest: Arc::clone(&self.manifest),
            events,
            fragment: Some(fragment),
        };
        selected.validate_bindings()?;
        Ok(selected)
    }

    pub fn fragment_selection(
        &self,
    ) -> Result<
        Option<crate::event::interval::ImportTableSelection>,
        crate::error::ReplicationProtocolError,
    > {
        self.fragment
            .map(|_| {
                self.bindings
                    .iter()
                    .map(|binding| (binding.source_table, binding.lineage))
                    .collect::<Vec<_>>()
                    .try_into()
            })
            .transpose()
    }

    pub fn table_page_id(
        &self,
        binding: &RegionalTableBinding,
    ) -> Result<crate::event::interval::TablePageId, crate::error::ReplicationProtocolError> {
        Ok(crate::event::interval::TablePageId {
            origin: self.manifest.origin.clone(),
            range: self.manifest.range,
            table: binding.source_table,
            lineage: binding.lineage,
            manifest: self.manifest.digest,
            fragment: self.fragment,
            selection: self
                .fragment_selection()?
                .map(|selection| selection.digest())
                .transpose()?,
        })
    }

    pub fn fragment(&self) -> Option<ImportFragment> {
        self.fragment
    }

    pub fn selected_links(
        &self,
    ) -> impl Iterator<Item = &crate::event::interval::CanonicalEventLink> {
        let (after, through) = self
            .fragment
            .map(|span| (span.after(), span.through()))
            .unwrap_or((0, usize::MAX));
        self.manifest
            .links
            .iter()
            .filter(|link| {
                self.bindings
                    .binary_search_by_key(&link.table, |binding| binding.source_table)
                    .is_ok()
            })
            .skip(after)
            .take(through - after)
    }

    pub fn bindings(&self) -> &[RegionalTableBinding] {
        &self.bindings
    }
    pub fn manifest(&self) -> &SourceIntervalManifest {
        &self.manifest
    }
    pub fn events(&self) -> &[CanonicalEvent] {
        &self.events
    }

    /// Events follow the selected original manifest links. Group receipts and
    /// effects by the publishing table, not an expiration victim's source.
    pub fn event_bindings(&self) -> impl Iterator<Item = (&RegionalTableBinding, &CanonicalEvent)> {
        self.selected_links()
            .filter_map(|link| {
                self.bindings
                    .binary_search_by_key(&link.table, |binding| binding.source_table)
                    .ok()
                    .map(|index| &self.bindings[index])
            })
            .zip(self.events.iter())
    }

    pub fn maximum_clock(&self) -> Option<crate::event::order::HybridLogicalClock> {
        self.events
            .iter()
            .filter_map(|event| {
                let crate::event::content::CanonicalPayload::Item(item) = &event.as_ref().payload
                else {
                    return None;
                };
                match &item.order {
                    crate::event::order::WinnerOrder::Mutation { clock, .. } => Some(*clock),
                    crate::event::order::WinnerOrder::Baseline { .. } => None,
                }
            })
            .max()
    }

    fn validate_bindings(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        use crate::error::ReplicationProtocolError as E;
        self.manifest.validate_shape()?;
        self.validate_binding_set()?;
        self.validate_fragment_declaration()?;
        let mut events = self.events.iter();
        for link in self.selected_links() {
            let event = events.next().ok_or(E::SourceGap {
                context: "selected import event missing",
            })?;
            link.require_event(event, &self.manifest.origin)?;
        }
        if events.next().is_some() {
            return Err(E::SourceGap {
                context: "import includes an unselected event",
            });
        }
        Ok(())
    }

    fn validate_import(
        input: (
            Vec<RegionalTableBinding>,
            SourceIntervalManifest,
            Vec<CanonicalEvent>,
        ),
        fragment: Option<ImportFragment>,
        codec: &impl crate::event::interval::IntervalCodec,
    ) -> Result<Self, crate::error::ReplicationProtocolError> {
        let (bindings, manifest, events) = input;
        manifest
            .digest
            .verify(codec.encode_interval(&manifest)?.as_ref())?;
        let import = Self {
            bindings,
            manifest: Arc::new(manifest),
            events,
            fragment,
        };
        import.validate_bindings()?;
        Ok(import)
    }

    fn validate_binding_set(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        if self.bindings.is_empty()
            || self
                .bindings
                .windows(2)
                .any(|pair| pair[0].source_table >= pair[1].source_table)
            || self.bindings.iter().any(|binding| {
                binding.source_region != self.manifest.origin.region
                    || binding.membership != self.manifest.membership
            })
        {
            return Err(crate::error::ReplicationProtocolError::InvalidScope {
                context: "execution.validate_bindings",
            });
        }
        Ok(())
    }

    fn validate_fragment_declaration(&self) -> Result<(), crate::error::ReplicationProtocolError> {
        use crate::error::ReplicationProtocolError as E;
        let mut count = 0usize;
        for link in &self.manifest.links {
            let Ok(index) = self
                .bindings
                .binary_search_by_key(&link.table, |binding| binding.source_table)
            else {
                continue;
            };
            if self.bindings[index].lineage != link.lineage {
                return Err(E::LineageMismatch {
                    context: "complete fragment declaration",
                });
            }
            count += 1;
        }
        if self.fragment.is_some_and(|span| span.through() > count) {
            return Err(E::InvalidRange {
                context: "fragment exceeds selected links",
            });
        }
        Ok(())
    }
}
