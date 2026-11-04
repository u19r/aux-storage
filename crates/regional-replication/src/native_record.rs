use std::sync::Arc;

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordChange, RecordExpectation,
    },
};
use replication_provider::record::RecordCodec;
use serde::{Deserialize, Serialize};
use storage_types::{InfrastructureContext, InfrastructureResultKind, Revision, StorageResult};

/// Bounded record mechanics. Policy remains with the regional flow that calls
/// it.
#[derive(Clone)]
pub(crate) struct NativeRecords {
    pub(crate) limits: InfrastructureLimits,
    codec: RecordCodec,
    acceptance_codec: std::sync::OnceLock<RecordCodec>,
    pub(crate) origin_receipt_codec: std::sync::OnceLock<(std::num::NonZeroUsize, RecordCodec)>,
    pub(crate) capacity: crate::history_capacity::HistoryCapacity,
}

impl NativeRecords {
    pub(crate) fn try_new(
        limits: InfrastructureLimits,
        capacity: (
            DurableRecordKey,
            replication_types::identity::resource::TenantId,
            replication_provider::runtime_configuration::limits::BacklogBudgets,
        ),
    ) -> StorageResult<Self> {
        Ok(Self {
            limits,
            codec: RecordCodec::try_new(limits.max_record_bytes())?,
            acceptance_codec: std::sync::OnceLock::new(),
            origin_receipt_codec: std::sync::OnceLock::new(),
            capacity: crate::history_capacity::HistoryCapacity::try_new(
                capacity.0, capacity.1, capacity.2,
            )?,
        })
    }

    pub(crate) fn require_original_capacity(
        &self,
        native: replication_provider::runtime_configuration::limits::NativeBudgets,
    ) -> StorageResult<()> {
        let account = self.require_history_account_capacity()?;
        let declaration = self
            .codec
            .encoded_payload_bound(native.maximum_history_declaration_bytes)?;
        let receipt = self
            .codec
            .encoded_payload_bound(native.maximum_acceptance_receipt_bytes)?;
        let declaration_batch = declaration
            .get()
            .checked_add(self.limits.max_key_bytes().get())
            .ok_or_else(corrupt_record)?;
        self.limits.check_batch(1, declaration_batch)?;
        let original = self
            .codec
            .encoded_payload_bound(
                self.original_payload_bound(native.maximum_canonical_event_bytes.get())?,
            )?
            .get();
        // The native receipt embeds the whole delivery pointer; its encoded
        // bound also bounds the separately indexed pointer without another size
        // assertion.
        self.limits.check_batch(
            1,
            original
                .checked_add(self.limits.max_key_bytes().get())
                .ok_or_else(corrupt_record)?,
        )?;
        self.limits.check_batch(
            1,
            receipt
                .get()
                .checked_add(self.limits.max_key_bytes().get())
                .ok_or_else(corrupt_record)?,
        )?;
        // Original, receipt and delivery index own their complete values.
        let acceptance = original
            .checked_add(receipt.get().checked_mul(2).ok_or_else(corrupt_record)?)
            .and_then(|bytes| bytes.checked_add(self.limits.max_key_bytes().get().checked_mul(3)?))
            .and_then(|bytes| bytes.checked_add(account.checked_mul(3)?))
            .ok_or_else(corrupt_record)?;
        self.require_receipt_allowances(native, acceptance)?;
        self.acceptance_codec
            .set(RecordCodec::try_new(receipt)?)
            .map_err(|_| corrupt_record())
    }

    fn require_receipt_allowances(
        &self,
        native: replication_provider::runtime_configuration::limits::NativeBudgets,
        acceptance: usize,
    ) -> StorageResult<()> {
        let receipt = self
            .codec
            .encoded_payload_bound(native.maximum_origin_receipt_bytes)?;
        let receipt = receipt
            .get()
            .checked_add(self.limits.max_key_bytes().get())
            .ok_or_else(corrupt_record)?;
        self.limits.check_batch(1, receipt)?;
        let origin_acceptance = acceptance.checked_add(receipt).ok_or_else(corrupt_record)?;
        if origin_acceptance > native.attempt.data_bytes.get()
            || acceptance > native.attempt.data_bytes.get()
        {
            return Err(storage_types::StorageError::validation(
                "native customer/import component allowance cannot retain its original and \
                 required receipts",
            ));
        }
        Ok(())
    }

    pub(crate) async fn scan<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: infrastructure_provider::page::RecordPageRequest,
    ) -> StorageResult<infrastructure_provider::page::RecordPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            request.validate(self.limits)?;
            let page = attempt
                .scan_records(
                    infrastructure_provider::page::RecordPageRequest {
                        range: request.range.clone(),
                        after: request.after.clone(),
                        max_bytes: request.max_bytes,
                        protection: request.protection,
                    },
                    self.limits,
                )
                .await?;
            let count = page.records.len();
            let expected = page.continuation;
            let checked = infrastructure_provider::page::RecordPage::try_new(
                page.records.into_iter().map(Ok),
                expected.is_some(),
                self.limits,
                &request,
            )?;
            if checked.records.len() != count || checked.continuation != expected {
                return Err(corrupt_record());
            }
            Ok(checked)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) fn decode<'record, T: Deserialize<'record>>(
        &self,
        record: &'record DurableRecord,
    ) -> StorageResult<T> {
        self.codec.decode(&record.bytes).map_err(Into::into)
    }

    pub(crate) fn content_digest<T: Serialize>(
        &self,
        value: &T,
    ) -> StorageResult<replication_types::protocol::CanonicalDigest> {
        Ok(replication_types::protocol::CanonicalDigest::of(
            self.codec.encode(value)?.as_ref(),
        ))
    }

    pub(crate) fn prepare<T: Serialize>(
        &self,
        key: DurableRecordKey,
        expected: Option<Revision>,
        value: &T,
    ) -> StorageResult<RecordChange> {
        let bytes = self.codec.encode(value)?;
        self.prepare_bytes(key, expected, bytes.into())
    }

    pub(crate) fn prepare_acceptance<T: Serialize>(
        &self,
        key: DurableRecordKey,
        value: &T,
    ) -> StorageResult<RecordChange> {
        let codec = self.acceptance_codec.get().ok_or_else(corrupt_record)?;
        let bytes = codec.encode(value)?;
        self.prepare_bytes(key, None, bytes.into())
    }

    pub(crate) fn prepare_bytes(
        &self,
        key: DurableRecordKey,
        expected: Option<Revision>,
        bytes: Arc<[u8]>,
    ) -> StorageResult<RecordChange> {
        let revision = expected.map_or(Ok(Revision::INITIAL), Revision::checked_next)?;
        let record = DurableRecord {
            key,
            revision,
            bytes,
        };
        self.limits.validate_record(&record)?;
        Ok(RecordChange::Store {
            expected: expected.map_or(RecordExpectation::Absent, RecordExpectation::Revision),
            record,
        })
    }

    /// Every wave joins the caller's attempt; qualify all batches before
    /// dispatch.
    pub(crate) async fn store<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        changes: &[RecordChange],
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if changes.is_empty() {
                return Ok(());
            }
            let charge = self.prepare_history_charge(attempt, changes).await?;
            let waves = RecordBatch::plan_waves([charge.as_slice(), changes], self.limits)?;
            for batch in waves.into_iter().flatten() {
                self.store_batch(attempt, &batch).await?;
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn store_batch<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        batch: &RecordBatch<'_>,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = match attempt.store_records(batch).await {
            Ok(ConditionalWriteResult::Applied) => Ok(()),
            Ok(ConditionalWriteResult::Conflict { .. }) => {
                Err(storage_types::StorageEnum::TransactionConflict {
                    message: "Regional state changed during the native attempt".into(),
                }
                .into())
            }
            Err(error) => Err(error),
        };
        settlement_guard.complete(result)
    }
}

pub(crate) fn corrupt_record() -> storage_types::StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::PublicationHistory,
    }
    .into()
}
