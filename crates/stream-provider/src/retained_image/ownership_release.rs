//! Last-owner release requires empty inverse scope and exact protected charges.

use std::num::NonZeroUsize;

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{ConditionalWriteResult, RecordChange},
};
use storage_types::{ReadProtection, StorageResult};

use super::{
    ImageChargeAccount,
    error::{capacity_rejected, invalid_image},
    ownership_graph::graph_key,
    ownership_inventory::OwnershipRecords,
    ownership_preparation::{FinalOwnerRelease, PreparedOwnershipChanges, load_protected_records},
};

impl PreparedOwnershipChanges {
    /// The caller must abort its native attempt on any error, including an
    /// error after edge writes.
    pub(super) async fn store<A: PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        limits: InfrastructureLimits,
    ) -> StorageResult<()> {
        let mut releases = prepare_refunds(attempt, &self.releases, limits).await?;
        let deletes = self
            .releases
            .len()
            .checked_mul(2)
            .ok_or_else(capacity_rejected)?;
        releases
            .try_reserve(deletes)
            .map_err(|_| capacity_rejected())?;
        for release in &self.releases {
            releases.push(RecordChange::Delete {
                key: (&release.body_key).try_into()?,
                expected: release.body_revision,
            });
            if let Some((key, expected)) = &release.head {
                releases.push(RecordChange::Delete {
                    key: key.try_into()?,
                    expected: *expected,
                });
            }
        }
        let [graph_waves, release_waves] =
            RecordBatch::plan_waves([self.changes.as_slice(), releases.as_slice()], limits)?;
        store_graph_waves(attempt, graph_waves).await?;
        for release in &self.releases {
            require_empty_inverse(attempt, release, limits).await?;
        }
        store_graph_waves(attempt, release_waves).await
    }
}

async fn require_empty_inverse<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    release: &FinalOwnerRelease,
    limits: InfrastructureLimits,
) -> StorageResult<()> {
    let address = release.address;
    let identity = address.identity();
    let mut first = Vec::new();
    let mut last = Vec::new();
    first
        .try_reserve(identity.as_ref().len() + 1)
        .map_err(|_| capacity_rejected())?;
    last.try_reserve(identity.as_ref().len() + 1)
        .map_err(|_| capacity_rejected())?;
    first.extend_from_slice(identity.as_ref());
    last.extend_from_slice(identity.as_ref());
    first.push(0);
    last.push(1);
    let range = BoundedRecordRange::try_new(
        graph_key(crate::constants::IMAGE_EDGE_NAMESPACE, address, first)?,
        graph_key(crate::constants::IMAGE_EDGE_NAMESPACE, address, last)?,
        NonZeroUsize::MIN,
    )?;
    let page = attempt
        .scan_records(
            RecordPageRequest {
                range,
                after: None,
                max_bytes: limits.max_bytes(),
                protection: ReadProtection::Conflict,
            },
            limits,
        )
        .await?;
    if !page.records.is_empty() || page.continuation.is_some() {
        return Err(invalid_image());
    }
    Ok(())
}

async fn prepare_refunds<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    releases: &[FinalOwnerRelease],
    limits: InfrastructureLimits,
) -> StorageResult<Vec<RecordChange>> {
    let count = releases
        .len()
        .checked_mul(2)
        .ok_or_else(capacity_rejected)?;
    let mut keys = Vec::new();
    keys.try_reserve(count).map_err(|_| capacity_rejected())?;
    for custody in releases
        .iter()
        .filter_map(|release| release.custody.as_ref())
    {
        let (account, receipt) = custody.refund_keys();
        keys.push(account.try_into()?);
        keys.push(receipt.try_into()?);
    }
    keys.sort_unstable();
    keys.dedup();
    let mut totals = OwnershipRecords::<u64>::default();
    totals.try_reserve(releases.len())?;
    let mut changes = Vec::new();
    changes
        .try_reserve(count)
        .map_err(|_| capacity_rejected())?;
    let observed = load_protected_records(attempt, &keys, limits).await?;
    for release in releases {
        let Some(custody) = &release.custody else {
            continue;
        };
        let (account, receipt_key) = custody.refund_keys();
        let receipt = observed
            .get(receipt_key)
            .and_then(Option::as_ref)
            .ok_or_else(invalid_image)?;
        let bytes = custody.require_refund(
            (&release.body_key, release.body_bytes),
            receipt,
            limits.max_record_bytes().get(),
        )?;
        let total = totals.try_get_or_insert(account.try_into()?, || 0)?;
        *total = total.checked_add(bytes).ok_or_else(invalid_image)?;
        changes.push(RecordChange::Delete {
            key: (&receipt.key).try_into()?,
            expected: receipt.revision,
        });
    }
    for (key, total) in totals {
        let row = observed
            .get(&key)
            .and_then(Option::as_ref)
            .ok_or_else(invalid_image)?;
        let account = ImageChargeAccount::decode(&row.bytes, limits.max_record_bytes().get())?;
        account.require_key(&key)?;
        changes.push(
            account
                .adjust(-i128::from(total))?
                .prepare_change(Some(row.revision), limits.max_record_bytes().get())?,
        );
    }
    Ok(changes)
}

async fn store_graph_waves<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    waves: Vec<RecordBatch<'_>>,
) -> StorageResult<()> {
    for batch in waves {
        let result = attempt.store_records(&batch).await?;
        if !matches!(result, ConditionalWriteResult::Applied) {
            return Err(storage_types::StorageEnum::TransactionConflict {
                message: "Image ownership graph changed".into(),
            }
            .into());
        }
    }
    Ok(())
}
