//! A projection effect batch verifies and stores each immutable address once.

use serde::Deserialize;
use storage_types::{SourceOrdinalProjection, StorageResult};

use super::{
    RetainedImageManifest,
    batch::RetainedImageBatch,
    error::{capacity_rejected, invalid_image},
    preparation::PreparedImageManifest,
    prepared_bodies::PreparedImageBodies,
    projection::{AcceptedProjectionPlan, RetainedProjectionRecord},
    read_work::RetainedReadWork,
};

#[derive(Deserialize)]
struct ProjectionSpans<'a>(
    serde::de::IgnoredAny,
    #[serde(borrow)] &'a serde_json::value::RawValue,
    #[serde(borrow)] &'a serde_json::value::RawValue,
);

struct PreparedProjectionManifest {
    manifest: PreparedImageManifest,
    slots: Vec<usize>,
}

impl PreparedProjectionManifest {
    fn capture<'body>(
        plan: &'body AcceptedProjectionPlan,
        batch: &mut RetainedImageBatch<'body>,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<Self> {
        let ProjectionSpans(_, selection, image) = serde_json::from_slice(&plan.bytes)?;
        if selection.get().len() > SourceOrdinalProjection::encoded_length_bound() {
            return Err(invalid_image());
        }
        let spans = [selection.get().as_bytes(), image.get().as_bytes()];
        let manifest = RetainedImageManifest::split(&plan.bytes, &spans, Some(plan.history))?;
        let mut slots = batch.include_manifest(&manifest, &spans)?;
        slots.sort_unstable();
        slots.dedup();
        let references = manifest.references()?;
        let (encoded, metadata_bytes) = manifest.encode_payload(limits.max_record_bytes().get())?;
        if metadata_bytes > crate::constants::RETAINED_TWO_BODY_METADATA_BYTES {
            return Err(invalid_image());
        }
        Ok(Self {
            manifest: PreparedImageManifest {
                encoded,
                metadata_bytes,
                references,
            },
            slots,
        })
    }

    fn bind(
        self,
        plan: AcceptedProjectionPlan,
        bodies: &PreparedImageBodies,
    ) -> StorageResult<RetainedProjectionRecord> {
        let work = bodies.select_work(&self.slots)?;
        let work = RetainedReadWork::prepare(&plan.bytes, &self.manifest, work)?;
        RetainedProjectionRecord::try_new(plan.history, self.manifest.encoded, work, usize::MAX)
    }
}

impl AcceptedProjectionPlan {
    pub async fn prepare_batch<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        plans: Vec<Self>,
    ) -> StorageResult<Vec<RetainedProjectionRecord>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some(maximum) = plans.iter().map(|plan| plan.bytes.len()).max() else {
                return Ok(Vec::new());
            };
            let limits = RetainedImageManifest::image_work_limits(maximum)?;
            let mut batch = RetainedImageBatch::empty();
            let mut prepared = Vec::new();
            let mut records = Vec::new();
            prepared
                .try_reserve_exact(plans.len())
                .map_err(|_| capacity_rejected())?;
            records
                .try_reserve_exact(plans.len())
                .map_err(|_| capacity_rejected())?;
            for plan in &plans {
                prepared.push(PreparedProjectionManifest::capture(
                    plan, &mut batch, limits,
                )?);
            }
            let bodies = batch.prepare(attempt, limits).await?;
            if prepared.len() != plans.len() {
                return Err(invalid_image());
            }
            for (plan, manifest) in plans.into_iter().zip(prepared) {
                records.push(manifest.bind(plan, &bodies)?);
            }
            bodies.store(attempt).await?;
            Ok(records)
        }
        .await;
        settlement_guard.complete(result)
    }
}
