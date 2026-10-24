use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{
    constants::OPERATION_SUFFIX,
    native_record::corrupt_record,
    origin::{OriginOperationReceipt, RecoveryOperation},
};

impl JournalStore {
    pub(crate) async fn store_operation_receipt<A: PhysicalRecordAttempt, R: serde::Serialize>(
        &self,
        attempt: &mut A,
        receipt: &OriginOperationReceipt<R>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_operation_events(&receipt.events)?;
            let key = self.identity_key(OPERATION_SUFFIX, &receipt.operation)?;
            let (_, codec) = self.origin_receipt_capacity()?;
            let bytes = codec.encode(receipt)?;
            let change = self.records.prepare_bytes(key, None, bytes.into())?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_operation_receipt<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        recovery: RecoveryOperation,
    ) -> StorageResult<Option<OriginOperationReceipt>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (maximum, codec) = self.origin_receipt_capacity()?;
            let maximum = codec.encoded_payload_bound(*maximum)?;
            let key = self.identity_key(OPERATION_SUFFIX, &recovery.identity())?;
            let records = self
                .records
                .load_bounded(attempt, &[key], ReadProtection::Snapshot, maximum)
                .await?;
            records[0]
                .as_ref()
                .map(|record| {
                    let receipt: OriginOperationReceipt = codec.decode(&record.bytes)?;
                    if record.revision != storage_types::Revision::INITIAL {
                        return Err(corrupt_record());
                    }
                    recovery.require_receipt(&receipt)?;
                    self.require_operation_events(&receipt.events)?;
                    Ok(receipt)
                })
                .transpose()
        }
        .await;
        settlement_guard.complete(result)
    }

    fn require_operation_events(
        &self,
        events: &[replication_types::event::identity::CanonicalEventId],
    ) -> StorageResult<()> {
        if events.is_empty() || events.len() > storage_types::MAX_TRANSACTION_MEMBERS {
            return Err(corrupt_record());
        }
        let mut seen = std::collections::BTreeSet::new();
        for event in events {
            let replication_types::event::identity::CanonicalEventId::Origin(original) = event
            else {
                return Err(corrupt_record());
            };
            if !seen.insert(event)
                || u64::from(original.position) == 0
                || original.origin != self.origin(original.origin.journal_partition)?
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) fn bind_origin_receipt_capacity(
        &self,
        maximum: std::num::NonZeroUsize,
    ) -> StorageResult<()> {
        let physical = replication_provider::record::RecordCodec::try_new(
            self.records.limits.max_record_bytes(),
        )?;
        let encoded = physical.encoded_payload_bound(maximum)?;
        let codec = replication_provider::record::RecordCodec::try_new(encoded)?;
        let (bound, _) = self
            .records
            .origin_receipt_codec
            .get_or_init(|| (maximum, codec));
        if *bound != maximum {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn require_origin_receipt_capacity(
        &self,
        maximum: std::num::NonZeroUsize,
    ) -> StorageResult<()> {
        let Some((bound, _)) = self.records.origin_receipt_codec.get() else {
            return Err(unavailable_receipt_capacity());
        };
        if *bound != maximum {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn origin_receipt_capacity(
        &self,
    ) -> StorageResult<&(
        std::num::NonZeroUsize,
        replication_provider::record::RecordCodec,
    )> {
        self.records
            .origin_receipt_codec
            .get()
            .ok_or_else(unavailable_receipt_capacity)
    }
}

fn unavailable_receipt_capacity() -> storage_types::StorageError {
    storage_types::InfrastructureResultKind::Unavailable {
        context: storage_types::InfrastructureContext::NativeCapacity,
    }
    .into()
}
