//! Protected graph preparation cannot itself authorize body deletion or
//! refunds.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use sha2::{Digest as _, Sha256};
use storage_types::{ReadProtection, StorageResult};

use super::{
    error::{capacity_rejected, invalid_image},
    hydration_wave::prepare_waves,
    observation::{BodyReadRequest, load_body_wave},
    ownership_batch::{BodyOwnershipChanges, OwnerEdgeTransition, OwnershipNetPlan},
    ownership_graph::{ImageOwnerEdge, ImageOwnershipHead},
    ownership_inventory::OwnershipRecords,
    payload::RetainedPayload,
};

/// These effects remain private until inverse-prefix and charge qualification
/// completes.
pub(super) struct PreparedOwnershipChanges {
    pub(super) changes: Vec<RecordChange>,
    pub(super) releases: Vec<FinalOwnerRelease>,
}

pub(super) struct FinalOwnerRelease {
    pub(super) address: storage_types::ImageAddress,
    pub(super) body_key: DurableRecordKey,
    pub(super) body_revision: storage_types::Revision,
    pub(super) body_bytes: usize,
    pub(super) head: Option<(DurableRecordKey, storage_types::Revision)>,
    pub(super) custody: Option<super::ImageChargeCustody>,
}

impl PreparedOwnershipChanges {
    pub(super) async fn prepare<A: PhysicalRecordAttempt>(
        attempt: &mut A,
        plan: OwnershipNetPlan,
        limits: InfrastructureLimits,
    ) -> StorageResult<Self> {
        let mut requests = Vec::new();
        requests
            .try_reserve(plan.bodies.len())
            .map_err(|_| capacity_rejected())?;
        for (key, body) in plan.bodies.iter() {
            requests.push(BodyReadRequest {
                key: key.try_into()?,
                identity: body.address.identity(),
                length: body.raw_bytes,
                encoded_bytes: body.observation.as_ref().map(|body| body.encoded_bytes),
            });
        }
        let effects = plan
            .bodies
            .values()
            .try_fold(plan.bodies.len(), |count, body| {
                count
                    .checked_add(body.owners.len())
                    .ok_or_else(capacity_rejected)
            })?;
        let mut prepared = Self {
            changes: Vec::new(),
            releases: Vec::new(),
        };
        prepared
            .changes
            .try_reserve(effects)
            .map_err(|_| capacity_rejected())?;
        prepared
            .releases
            .try_reserve(plan.bodies.len())
            .map_err(|_| capacity_rejected())?;
        let waves = prepare_waves(&requests, limits)?;
        let mut bodies = plan.bodies.into_iter();
        for wave in waves {
            let requests = &requests[wave];
            let mut changes = Vec::new();
            changes
                .try_reserve(requests.len())
                .map_err(|_| capacity_rejected())?;
            changes.extend(
                bodies
                    .by_ref()
                    .take(requests.len())
                    .map(|(_, change)| change),
            );
            if changes.len() != requests.len() {
                return Err(invalid_image());
            }
            let mut graph = load_graph_rows(attempt, &changes, limits).await?;
            let rows = load_body_wave(attempt, requests, limits, ReadProtection::Conflict).await?;
            for ((request, row), change) in requests.iter().zip(rows).zip(changes) {
                let body = row.ok_or_else(invalid_image)?;
                require_body_content(request, &body)?;
                prepared.prepare_body(change, body, &mut graph, limits)?;
            }
            if !graph.is_empty() {
                return Err(invalid_image());
            }
        }
        if bodies.next().is_some() {
            return Err(invalid_image());
        }
        Ok(prepared)
    }

    fn prepare_body(
        &mut self,
        change: BodyOwnershipChanges,
        body: DurableRecord,
        graph: &mut OwnershipRecords<Option<DurableRecord>>,
        limits: InfrastructureLimits,
    ) -> StorageResult<()> {
        let row = graph
            .remove(&ImageOwnershipHead::key(change.address)?)
            .ok_or_else(invalid_image)?;
        let head = qualify_head(&change, &body, &row, limits)?;
        let mut count = head.as_ref().map_or(0, |head| head.owners.get());
        let established = change
            .owners
            .values()
            .filter(|transition| !matches!(transition, OwnerEdgeTransition::Attach))
            .count();
        if u64::try_from(established).map_err(|_| invalid_image())? > count {
            return Err(invalid_image());
        }
        // Count removals before additions so an undercount cannot hide behind
        // new owners.
        for (owner, transition) in change.owners.iter() {
            if matches!(transition, OwnerEdgeTransition::Detach) {
                count = count.checked_sub(1).ok_or_else(invalid_image)?;
            }
            self.prepare_edge(&change, owner, *transition, (graph, limits))?;
        }
        let additions = change
            .owners
            .values()
            .filter(|transition| matches!(transition, OwnerEdgeTransition::Attach))
            .count();
        count = count
            .checked_add(u64::try_from(additions).map_err(|_| invalid_image())?)
            .ok_or_else(invalid_image)?;
        let mut custody = head.and_then(|head| head.charge);
        if let Some(staged) = change
            .observation
            .as_ref()
            .and_then(|body| body.charge.as_ref())
        {
            if custody.as_ref().is_some_and(|existing| existing != staged) {
                return Err(invalid_image());
            }
            custody = Some(staged.try_into()?);
        }
        if let Some(custody) = &custody {
            custody.require_body(&body)?;
        }
        if count == 0 {
            self.releases.push(FinalOwnerRelease {
                address: change.address,
                body_bytes: body.bytes.len(),
                body_key: body.key,
                body_revision: body.revision,
                head: row.map(|row| (row.key, row.revision)),
                custody,
            });
        } else {
            let next = ImageOwnershipHead::try_new(
                change.address,
                (change.raw_bytes, body.bytes.len()),
                count,
                custody,
            )?;
            self.changes.push(next.prepare_change(
                row.as_ref().map(|row| row.revision),
                limits.max_record_bytes().get(),
            )?);
        }
        Ok(())
    }

    fn prepare_edge(
        &mut self,
        body: &BodyOwnershipChanges,
        owner: &DurableRecordKey,
        transition: OwnerEdgeTransition,
        observations: (
            &mut OwnershipRecords<Option<DurableRecord>>,
            InfrastructureLimits,
        ),
    ) -> StorageResult<()> {
        let (graph, limits) = observations;
        let edge = ImageOwnerEdge::try_new(body.address, owner.try_into()?)?;
        let row = graph.remove(&edge.key()?).ok_or_else(invalid_image)?;
        match (transition, row) {
            (OwnerEdgeTransition::Attach, None) => self
                .changes
                .push(edge.prepare_change(limits.max_record_bytes().get())?),
            (OwnerEdgeTransition::Detach | OwnerEdgeTransition::Retain, Some(row)) => {
                edge.require_observation(&row, limits.max_record_bytes().get())?;
                if matches!(transition, OwnerEdgeTransition::Detach) {
                    self.changes.push(RecordChange::Delete {
                        key: row.key,
                        expected: row.revision,
                    });
                }
            }
            _ => return Err(invalid_image()),
        }
        Ok(())
    }
}

fn qualify_head(
    change: &BodyOwnershipChanges,
    body: &DurableRecord,
    row: &Option<DurableRecord>,
    limits: InfrastructureLimits,
) -> StorageResult<Option<ImageOwnershipHead>> {
    match row {
        Some(row) => {
            if change.observation.as_ref().is_some_and(|body| body.created) {
                return Err(invalid_image());
            }
            let head =
                ImageOwnershipHead::decode(row, change.address, limits.max_record_bytes().get())?;
            if head.raw_bytes != change.raw_bytes || head.encoded_bytes != body.bytes.len() {
                return Err(invalid_image());
            }
            Ok(Some(head))
        }
        None => {
            if !change.observation.as_ref().is_some_and(|body| body.created)
                || change
                    .owners
                    .values()
                    .any(|transition| !matches!(transition, OwnerEdgeTransition::Attach))
            {
                return Err(invalid_image());
            }
            Ok(None)
        }
    }
}

async fn load_graph_rows<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    changes: &[BodyOwnershipChanges],
    limits: InfrastructureLimits,
) -> StorageResult<OwnershipRecords<Option<DurableRecord>>> {
    let count = changes.iter().try_fold(changes.len(), |count, body| {
        count
            .checked_add(body.owners.len())
            .ok_or_else(capacity_rejected)
    })?;
    let mut keys = Vec::new();
    keys.try_reserve(count).map_err(|_| capacity_rejected())?;
    for body in changes {
        keys.push(ImageOwnershipHead::key(body.address)?);
        for owner in body.owners.keys() {
            keys.push(ImageOwnerEdge::try_new(body.address, owner.try_into()?)?.key()?);
        }
    }
    load_protected_records(attempt, &keys, limits).await
}

pub(super) async fn load_protected_records<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    keys: &[DurableRecordKey],
    limits: InfrastructureLimits,
) -> StorageResult<OwnershipRecords<Option<DurableRecord>>> {
    let waves = prepare_protected_waves(keys, limits)?;
    let mut rows = OwnershipRecords::default();
    rows.try_reserve(keys.len())?;
    for wave in waves {
        let limits = limits.project_read_keys(wave)?;
        let observed = attempt
            .load_records(wave, ReadProtection::Conflict, limits)
            .await?;
        limits.validate_observations(wave, &observed)?;
        for (key, row) in wave.iter().zip(observed) {
            rows.try_insert(key.try_into()?, row)?;
        }
    }
    Ok(rows)
}

fn prepare_protected_waves(
    keys: &[DurableRecordKey],
    limits: InfrastructureLimits,
) -> StorageResult<Vec<&[DurableRecordKey]>> {
    let envelope = limits
        .max_key_bytes()
        .get()
        .checked_add(limits.max_record_bytes().get())
        .ok_or_else(invalid_image)?;
    let count = limits
        .max_records()
        .get()
        .min(limits.max_bytes().get() / envelope);
    if count == 0 {
        return Err(invalid_image());
    }
    let mut unique = Vec::new();
    unique
        .try_reserve(keys.len())
        .map_err(|_| capacity_rejected())?;
    unique.extend(keys.iter());
    unique.sort_unstable();
    if unique.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid_image());
    }
    let mut waves = Vec::new();
    waves
        .try_reserve(keys.len().div_ceil(count))
        .map_err(|_| capacity_rejected())?;
    waves.extend(keys.chunks(count));
    for wave in &waves {
        limits.validate_keys(wave)?;
    }
    Ok(waves)
}

fn require_body_content(request: &BodyReadRequest, row: &DurableRecord) -> StorageResult<()> {
    let mut digest = Sha256::new();
    RetainedPayload::visit_decoded(&row.bytes, request.length, |chunk| {
        digest.update(chunk);
        Ok(())
    })?;
    if <[u8; 32]>::from(digest.finalize()) != *request.identity.as_ref() {
        return Err(invalid_image());
    }
    Ok(())
}
