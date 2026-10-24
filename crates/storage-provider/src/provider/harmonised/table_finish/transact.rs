//! Shared `TransactWriteItems` finish and cancellation mapping.

use storage_types::StorageResult;

use super::{
    super::{
        models::TableWriteError, table_common::finish_batch_by_ordinal,
        write_preparation::ValidatedWriteObservations,
    },
    FinishedObservedBatch, PreparedTableBatch,
};

/// Validate and consume native evidence before mapping transaction
/// cancellation reasons.  This keeps the ordinal/role gate on the shared
/// finish boundary instead of trusting a separate positional wire vector.
pub fn finish_transact_members_with_observations(
    prepared: PreparedTableBatch,
    observations: ValidatedWriteObservations,
) -> StorageResult<FinishedObservedBatch> {
    let member_total = prepared.puts.len()
        + prepared.updates.len()
        + prepared.deletes.len()
        + prepared.checks.len();
    let mut collector = ReasonCollector::new(member_total);
    let super::super::write_preparation::ObservedWriteFinish { wires, histories } =
        observations.into_finish_inputs();
    let finished = finish_batch_by_ordinal(prepared, wires, |ordinal, error| {
        collector.record(ordinal, error)
    })?;
    collector.cancelled()?;
    let histories = super::super::write_preparation::decode_write_histories(histories)?;
    Ok(FinishedObservedBatch {
        finished,
        histories,
    })
}

struct ReasonCollector {
    reasons: Vec<Option<String>>,
    member_total: usize,
}

impl ReasonCollector {
    fn new(member_total: usize) -> Self {
        Self {
            reasons: Vec::new(),
            member_total,
        }
    }

    fn record(&mut self, ordinal: usize, error: TableWriteError) -> StorageResult<()> {
        match error {
            TableWriteError::ConditionFailed {
                old_item,
                return_old_on_condition_failure,
                ..
            } => {
                let old_map = match (return_old_on_condition_failure, old_item) {
                    (true, Some(wire)) => wire.into_attribute_map().ok(),
                    _ => None,
                };
                pad_reasons(&mut self.reasons, ordinal);
                self.reasons[ordinal] = Some(storage_types::conditional_check_failed_reason(
                    old_map.as_ref(),
                )?);
                Ok(())
            }
            TableWriteError::Failed(error) => {
                Err(storage_types::transaction_canceled_for_item_error_with_len(
                    ordinal,
                    self.member_total,
                    error,
                ))
            }
        }
    }

    fn cancelled(self) -> StorageResult<()> {
        let mut reasons = self.reasons;
        if reasons.len() < self.member_total {
            reasons.resize_with(self.member_total, || None);
        }
        if let Some(error) = storage_types::transaction_canceled_for_indexed_reasons(reasons) {
            return Err(error);
        }
        Ok(())
    }
}

fn pad_reasons(reasons: &mut Vec<Option<String>>, ordinal: usize) {
    if reasons.len() <= ordinal {
        reasons.resize_with(ordinal + 1, || None);
    }
}
