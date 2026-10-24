use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::CanonicalDigest;
use storage_provider::{CatalogAttempt, DynamoAttempt};
use storage_types::{ReadProtection, StorageResult};

use super::record::{BaselinePageReceipt, TargetRecord};
use crate::{
    bootstrap::{
        BaselineCopyPage, CommittedBaselinePage, HiddenTargetRequest, SourceCaptureDefinition,
        TargetBootstrapDefinition, baseline_page_content,
    },
    constants::{BASELINE_COPY_SUFFIX, TARGET_BOOTSTRAP_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn copy_baseline_page<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        input: (
            &TargetBootstrapDefinition,
            &BaselineCopyPage,
            &CanonicalJsonCodec,
        ),
    ) -> StorageResult<CommittedBaselinePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (definition, page, codec) = input;
            self.require_baseline_page_budget(page)?;
            let (row, mut record) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_target_bootstrap(&record, definition)?;
            let content = baseline_page_content(definition.content(), page)?;
            if let Some(receipt) = self
                .load_baseline_page_receipt(
                    attempt,
                    (request, page.sequence),
                    ReadProtection::Conflict,
                )
                .await?
            {
                self.require_baseline_receipt_state(
                    attempt,
                    &record,
                    definition,
                    ReadProtection::Shared,
                )
                .await?;
                return committed_page(page, content, receipt);
            }
            let table = self
                .require_hidden_table(attempt, &record, ReadProtection::Shared)
                .await?;
            if record.baselines_complete || record.next_page != page.sequence {
                return Err(corrupt_record());
            }
            for bytes in &page.items {
                let baseline =
                    crate::journal::BaselineAuthority::decode(bytes.clone().into(), codec)?;
                let key = CanonicalDigest::of(baseline.item().canonical_key.as_ref());
                if record.last_baseline.is_some_and(|last| last >= key) {
                    return Err(corrupt_record());
                }
                self.materialize_baseline(
                    attempt,
                    (&record, std::sync::Arc::clone(&table), &baseline),
                    codec,
                )
                .await?;
                record.last_baseline = Some(key);
            }
            record.definition = Some(definition.content());
            record.source_vector = Some(definition.vector());
            record.next_page = record.next_page.checked_add(1).ok_or_else(corrupt_record)?;
            record.baselines_complete = page.complete;
            let receipt = BaselinePageReceipt {
                content,
                complete: page.complete,
            };
            let root_key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &request.operation.id)?;
            let receipt_key = self.baseline_page_key(request, page.sequence)?;
            let changes = [
                self.records
                    .prepare(root_key, Some(row.revision), &record)?,
                self.records.prepare(receipt_key, None, &receipt)?,
            ];
            self.records.store(attempt, &changes).await?;
            committed_page(page, content, receipt)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_baseline_page<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        input: (&TargetBootstrapDefinition, &BaselineCopyPage),
    ) -> StorageResult<Option<CommittedBaselinePage>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_baseline_page_budget(input.1)?;
            let (_, record) = self
                .load_hidden_target(attempt, request, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_target_bootstrap(&record, input.0)?;
            self.require_baseline_receipt_state(
                attempt,
                &record,
                input.0,
                ReadProtection::Snapshot,
            )
            .await?;
            let content = baseline_page_content(input.0.content(), input.1)?;
            self.load_baseline_page_receipt(
                attempt,
                (request, input.1.sequence),
                ReadProtection::Snapshot,
            )
            .await?
            .map(|receipt| committed_page(input.1, content, receipt))
            .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn require_baseline_receipt_state<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        record: &TargetRecord,
        definition: &TargetBootstrapDefinition,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        if self
            .reconcile_verified_activation(
                attempt,
                record,
                (definition.proposal(), definition.content()),
                protection,
            )
            .await?
        {
            return Ok(());
        }
        self.require_hidden_table(attempt, record, protection)
            .await?;
        Ok(())
    }

    pub(super) fn require_initial_target_definition(
        &self,
        record: &TargetRecord,
        definition: &SourceCaptureDefinition,
    ) -> StorageResult<()> {
        definition.validate()?;
        if !record.request.matches_definition(definition)?
            || record.definition.is_some_and(|content| {
                content
                    != record
                        .reenable
                        .as_ref()
                        .map_or(definition.content(), |retained| {
                            retained.definition.content()
                        })
            })
            || record
                .source_vector
                .is_some_and(|vector| vector != definition.vector())
            || record.definition.is_some() != record.source_vector.is_some()
        {
            return Err(corrupt_record());
        }
        let participant = definition
            .proposal()
            .participant(&self.region)
            .map_err(|_| corrupt_record())?;
        let mut target = participant.proposed.clone();
        target.phase = replication_types::configuration::installed::RegionalTablePhase::Joining;
        if participant.before.is_some() || target != record.state {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn require_target_bootstrap(
        &self,
        record: &TargetRecord,
        definition: &TargetBootstrapDefinition,
    ) -> StorageResult<()> {
        definition.validate()?;
        match definition {
            TargetBootstrapDefinition::Initial(initial) => {
                if record.adoption.is_some() || record.reenable.is_some() {
                    return Err(corrupt_record());
                }
                self.require_initial_target_definition(record, initial)
            }
            TargetBootstrapDefinition::Addition(adoption) => {
                self.require_target_adoption(record, adoption)
            }
            TargetBootstrapDefinition::Reenable(retained) => {
                self.require_target_retained_history(record, retained)
            }
        }
    }

    fn require_baseline_page_budget(&self, page: &BaselineCopyPage) -> StorageResult<()> {
        if !page.complete && page.items.is_empty() {
            return Err(corrupt_record());
        }
        let bytes = page.items.iter().try_fold(0usize, |total, item| {
            total
                .checked_add(item.as_ref().len())
                .ok_or_else(corrupt_record)
        })?;
        // Root and receipt join separate native waves in this page's one
        // attempt.
        self.records.limits.check_batch(page.items.len(), bytes)
    }

    fn baseline_page_key(
        &self,
        request: &HiddenTargetRequest,
        sequence: u64,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(BASELINE_COPY_SUFFIX, &(request.operation.id, sequence))
    }

    async fn load_baseline_page_receipt<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&HiddenTargetRequest, u64),
        protection: ReadProtection,
    ) -> StorageResult<Option<BaselinePageReceipt>> {
        let key = self.baseline_page_key(input.0, input.1)?;
        let rows = self.records.load(attempt, &[key], protection).await?;
        rows[0]
            .as_ref()
            .map(|row| self.records.decode(row))
            .transpose()
    }
}

fn committed_page(
    page: &BaselineCopyPage,
    content: CanonicalDigest,
    receipt: BaselinePageReceipt,
) -> StorageResult<CommittedBaselinePage> {
    if receipt.content != content || receipt.complete != page.complete {
        return Err(corrupt_record());
    }
    Ok(CommittedBaselinePage {
        sequence: page.sequence,
        content,
        baselines_complete: receipt.complete,
    })
}
