use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{BaselineCopyScope, TargetBootstrapDefinition},
    constants::SOURCE_COPY_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CopyProgress {
    pub scope: CanonicalDigest,
    pub definition: CanonicalDigest,
    pub sequence: u64,
    pub after: Option<DurableRecordKey>,
    pub complete: bool,
    pub last_receipt: Option<CanonicalDigest>,
}

impl CopyProgress {
    pub(super) fn initial(
        scope: &BaselineCopyScope,
        definition: &TargetBootstrapDefinition,
    ) -> StorageResult<Self> {
        Ok(Self {
            scope: scope.content()?,
            definition: definition.content(),
            sequence: 0,
            after: None,
            complete: false,
            last_receipt: None,
        })
    }

    fn require_observation(&self, revision: Option<storage_types::Revision>) -> StorageResult<()> {
        if self
            .sequence
            .checked_sub(1)
            .map(storage_types::Revision::from)
            != revision
        {
            return Err(corrupt_record());
        }
        if revision.is_none() {
            if self.complete || self.after.is_some() || self.last_receipt.is_some() {
                return Err(corrupt_record());
            }
            return Ok(());
        }
        if self.last_receipt.is_none() || self.complete != self.after.is_none() {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    pub(super) fn source_copy_key(
        &self,
        scope: &BaselineCopyScope,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            SOURCE_COPY_SUFFIX,
            &(scope.proposal.operation().id, &scope.destination.region),
        )
    }

    pub(super) async fn load_source_copy<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&BaselineCopyScope, &TargetBootstrapDefinition),
        protection: ReadProtection,
    ) -> StorageResult<(Option<DurableRecord>, CopyProgress)> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (scope, definition) = input;
            scope.require_definition(definition)?;
            let key = self.source_copy_key(scope)?;
            let mut rows = self.records.load(attempt, &[key], protection).await?;
            let row = rows.pop().flatten();
            let progress = row
                .as_ref()
                .map(|row| self.records.decode::<CopyProgress>(row))
                .transpose()?
                .unwrap_or(CopyProgress::initial(scope, definition)?);
            if progress.scope != scope.content()? || progress.definition != definition.content() {
                return Err(corrupt_record());
            }
            progress.require_observation(row.as_ref().map(|row| row.revision))?;
            if row.is_some() {
                self.require_acknowledged_copy(attempt, (scope, &progress), protection)
                    .await?;
            }
            Ok((row, progress))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn require_acknowledged_copy<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&BaselineCopyScope, &CopyProgress),
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let (scope, progress) = input;
        let sequence = progress
            .sequence
            .checked_sub(1)
            .ok_or_else(corrupt_record)?;
        let key = self.source_copy_page_key(scope, sequence)?;
        let row = self
            .records
            .load(attempt, &[key], protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        if row.revision != storage_types::Revision::INITIAL {
            return Err(corrupt_record());
        }
        let mut selected: SelectedCopyPage = self.records.decode(&row)?;
        selected.require_before(&selected.before)?;
        selected.after.last_receipt = Some(selected.content);
        if selected.after != *progress {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedCopyPage {
    pub before: CopyProgress,
    pub after: CopyProgress,
    pub budget: crate::bootstrap::copy_scope::BaselineCopyBudget,
    pub content: CanonicalDigest,
}

impl SelectedCopyPage {
    fn require_before(&self, before: &CopyProgress) -> StorageResult<()> {
        if self.before != *before
            || before.complete
            || self.after.scope != before.scope
            || self.after.definition != before.definition
            || before.sequence.checked_add(1) != Some(self.after.sequence)
            || self.after.last_receipt != before.last_receipt
            || self.after.complete != self.after.after.is_none()
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    pub(super) fn source_copy_page_key(
        &self,
        scope: &BaselineCopyScope,
        sequence: u64,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            crate::constants::SOURCE_COPY_PAGE_SUFFIX,
            &(
                scope.proposal.operation().id,
                &scope.destination.region,
                sequence,
            ),
        )
    }

    pub(super) async fn load_selected_copy<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        before: &CopyProgress,
    ) -> StorageResult<Option<SelectedCopyPage>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.source_copy_page_key(scope, before.sequence)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?;
            let record = rows[0]
                .as_ref()
                .map(|row| {
                    if row.revision != storage_types::Revision::INITIAL {
                        return Err(corrupt_record());
                    }
                    let record = self.records.decode::<SelectedCopyPage>(row)?;
                    record.require_before(before)?;
                    Ok(record)
                })
                .transpose()?;
            Ok(record)
        }
        .await;
        settlement_guard.complete(result)
    }
}
