use std::sync::Arc;

use infrastructure_provider::record::DurableRecord;
use replication_provider::delivery::error::DeliveryError;
use storage_types::StorageResult;
use stream_provider::retained_image::{ImageHydrationBatch, RetainedImageManifest};

use super::JournalStore;
use crate::native_record::corrupt_record;

impl JournalStore {
    /// Admit all declared originals before hot hydration or archive I/O.
    pub(in crate::journal) async fn load_retained_manifests<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        rows: &[DurableRecord],
        input: (Vec<RetainedImageManifest>, usize),
    ) -> Result<Vec<Arc<[u8]>>, DeliveryError> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (manifests, maximum) = input;
            if rows.len() != manifests.len() {
                return Err(DeliveryError::from(corrupt_record()));
            }
            manifests.iter().try_fold(maximum, |remaining, manifest| {
                remaining
                    .checked_sub(manifest.original_length())
                    .ok_or_else(frame_bound)
            })?;
            let pointers = self
                .qualify_original_owners(attempt, rows, &manifests)
                .await?;
            let mut hot = Vec::new();
            let mut hot_slots = Vec::new();
            let mut archived = Vec::new();
            for (slot, (manifest, pointer)) in manifests.into_iter().zip(pointers).enumerate() {
                match pointer {
                    Some(pointer) => archived.push((slot, pointer, manifest)),
                    None => {
                        hot_slots.push(slot);
                        hot.push(manifest);
                    }
                }
            }
            let bytes = ImageHydrationBatch::try_new(hot, maximum)?
                .load(attempt, self.records.limits)
                .await?;
            let mut originals = (0..rows.len()).map(|_| None).collect::<Vec<_>>();
            if bytes.len() != hot_slots.len() {
                return Err(corrupt_record().into());
            }
            for (slot, bytes) in hot_slots.into_iter().zip(bytes) {
                originals[slot] = Some(bytes.into());
            }
            self.fetch_owned_archive_originals(originals, archived)
                .await
                .map_err(DeliveryError::from)
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn fetch_owned_archive_originals(
        &self,
        mut originals: Vec<Option<Arc<[u8]>>>,
        archived: Vec<(usize, crate::archive::ArchivePointer, RetainedImageManifest)>,
    ) -> StorageResult<Vec<Arc<[u8]>>> {
        if !archived.is_empty() {
            let archive = self.archive.get().ok_or_else(corrupt_record)?;
            // Settle all admitted archive reads before returning a failed
            // frame.
            let fetched = futures::future::join_all(archived.into_iter().map(
                |(slot, pointer, manifest)| async move {
                    Self::fetch_archive_original(archive, &pointer, &manifest)
                        .await
                        .map(|bytes| (slot, bytes))
                },
            ))
            .await;
            for result in fetched {
                let (slot, bytes) = result?;
                originals[slot] = Some(bytes);
            }
        }
        originals
            .into_iter()
            .map(|bytes| bytes.ok_or_else(corrupt_record))
            .collect()
    }
}

fn frame_bound() -> DeliveryError {
    DeliveryError::from(
        replication_types::error::ReplicationProtocolError::InvalidRange {
            context: "source frame configured bound",
        },
    )
}
