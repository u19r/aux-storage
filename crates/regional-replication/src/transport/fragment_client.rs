use std::sync::Arc;

use replication_provider::regional_application::observation::{
    ImportPageScope, TablePageAcceptance,
};
use replication_types::{event::interval::ImportFragment, regional_import::RegionalImport};

use super::{
    client::{RemoteImportReceipt, TcpPeerClient, encode_page},
    codec::EncodedFrame,
    error::{TransportError, TransportResult},
    message::{ImportAcknowledgement, WireIdentity},
    resources::TransportResources,
};

impl TcpPeerClient {
    /// Each table advances independently through stable native fragment
    /// receipts. Only complete durable table pages can construct a delivery
    /// acknowledgement.
    pub async fn deliver(
        &self,
        import: Arc<RegionalImport>,
    ) -> TransportResult<RemoteImportReceipt> {
        if import.fragment().is_some() {
            return Err(TransportError::Frame);
        }
        let maximum = self
            .resources
            .configuration
            .as_ref()
            .as_ref()
            .limits
            .frame_events
            .get();
        if import
            .manifest()
            .links
            .len()
            .max(import.manifest().coverage.len())
            .max(import.bindings().len())
            > maximum
        {
            return Err(TransportError::Frame);
        }
        tokio::time::timeout(
            self.resources.configuration.delivery_budgets().transfer(),
            self.deliver_original_pages(import),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
    }

    async fn deliver_original_pages(
        &self,
        import: Arc<RegionalImport>,
    ) -> TransportResult<RemoteImportReceipt> {
        let mut pages = Vec::with_capacity(import.bindings().len());
        for binding in import.bindings() {
            let table = Arc::new(import.select_table(binding.source_table)?);
            pages.extend(self.deliver_table(table).await?);
        }
        ImportPageScope::Selected.require_pages(&import, &pages)?;
        Ok(RemoteImportReceipt {
            destination: self.destination_identity(),
            bindings: import.bindings().to_vec(),
            pages,
        })
    }

    async fn deliver_table(
        &self,
        import: Arc<RegionalImport>,
    ) -> TransportResult<Vec<TablePageAcceptance>> {
        if import.events().is_empty() {
            let acknowledgement = self.deliver_fragment(Arc::clone(&import)).await?;
            let pages = acknowledgement.completed.ok_or(TransportError::Peer)?;
            ImportPageScope::Selected.require_pages(&import, &pages)?;
            return Ok(pages);
        }
        let mut after = 0;
        while after < import.events().len() {
            let (through, acknowledgement) = self
                .deliver_selected_fragment(Arc::clone(&import), after)
                .await?;
            if let Some(pages) = acknowledgement.completed {
                ImportPageScope::Selected.require_pages(&import, &pages)?;
                return Ok(pages);
            }
            after = through;
        }
        Err(TransportError::Nack(
            super::error::ImportNack::UnresolvedCommit,
        ))
    }

    async fn deliver_selected_fragment(
        &self,
        import: Arc<RegionalImport>,
        after: usize,
    ) -> TransportResult<(usize, ImportAcknowledgement)> {
        self.settle_exchange(move |peer, deadline| async move {
            let resources = Arc::clone(&peer.resources);
            let destination = peer.destination_identity();
            let (fragment, frame) = tokio::task::spawn_blocking(move || {
                plan_native_fragment(&import, &resources, &destination, after)
            })
            .await
            .map_err(|_| TransportError::Worker)??;
            let through = fragment.fragment().ok_or(TransportError::Frame)?.through();
            let acknowledgement = peer.exchange_import(&fragment, frame, deadline).await?;
            Ok((through, acknowledgement))
        })
        .await
    }
}

fn plan_native_fragment(
    import: &RegionalImport,
    resources: &TransportResources,
    destination: &WireIdentity,
    after: usize,
) -> TransportResult<(RegionalImport, EncodedFrame)> {
    let limits = &resources.configuration.as_ref().as_ref().limits;
    let mut through = after;
    let mut bytes = 0usize;
    for event in import
        .events()
        .iter()
        .skip(after)
        .take(limits.import_events.get())
    {
        let next = bytes
            .checked_add(event.as_ref().bytes.as_ref().len())
            .ok_or(TransportError::Frame)?;
        if next > limits.import_bytes.get() {
            break;
        }
        bytes = next;
        through += 1;
    }
    if through == after {
        return Err(TransportError::FrameTooLarge);
    }
    loop {
        let span = ImportFragment::try_from((
            u32::try_from(after).map_err(|_| TransportError::Frame)?,
            u32::try_from(through).map_err(|_| TransportError::Frame)?,
        ))?;
        let fragment = import.select_fragment(span)?;
        match encode_page(
            &fragment,
            (resources.identity.clone(), destination.clone()),
            resources.semantic_limit,
        ) {
            Ok(frame) => return Ok((fragment, frame)),
            Err(TransportError::FrameTooLarge) if through - after > 1 => {
                through = after + (through - after) / 2;
            }
            Err(error) => return Err(error),
        }
    }
}
