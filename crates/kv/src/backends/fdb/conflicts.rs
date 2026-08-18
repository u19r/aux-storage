use foundationdb::{FdbError, RangeOption, Transaction, options};

use super::{
    constants::{
        CONFLICT_LOG_MAX_KEYS, CONFLICT_LOG_MAX_RANGES, CONFLICTING_KEYS_PREFIX,
        READ_CONFLICT_RANGE_PREFIX, WRITE_CONFLICT_RANGE_PREFIX,
    },
    metrics::record_fdb_conflict_artifacts,
    store::FoundationDbKvStore,
};
use crate::helpers::increment_bytes;

impl FoundationDbKvStore {
    pub(super) async fn log_conflict_details(
        &self,
        trx: &Transaction,
        operation: &'static str,
        attempt: u32,
        retryable: bool,
        error_code: i32,
        candidate_key_count: usize,
    ) {
        if !retryable {
            return;
        }

        if let Err(err) = trx.set_option(options::TransactionOption::ReportConflictingKeys) {
            tracing::debug!(
                operation,
                attempt,
                error = %err,
                "failed to enable conflict key reporting for conflict logging"
            );
            return;
        }
        if let Err(err) = trx.set_option(options::TransactionOption::SpecialKeySpaceRelaxed) {
            tracing::debug!(
                operation,
                attempt,
                error = %err,
                "failed to relax special key space for conflict logging"
            );
            return;
        }

        let conflicting = match read_special_key_prefix(
            trx,
            CONFLICTING_KEYS_PREFIX,
            CONFLICT_LOG_MAX_KEYS,
        )
        .await
        {
            Ok(items) => items,
            Err(read_err) => {
                tracing::debug!(
                    operation,
                    attempt,
                    error = %read_err,
                    "failed to read FoundationDB conflicting keys"
                );
                return;
            }
        };

        let read_ranges =
            read_special_key_prefix(trx, READ_CONFLICT_RANGE_PREFIX, CONFLICT_LOG_MAX_RANGES)
                .await
                .unwrap_or_default();
        let write_ranges =
            read_special_key_prefix(trx, WRITE_CONFLICT_RANGE_PREFIX, CONFLICT_LOG_MAX_RANGES)
                .await
                .unwrap_or_default();

        let conflict_key_count = conflicting.len();
        let candidate_key_count = candidate_key_count.min(CONFLICT_LOG_MAX_KEYS);
        let read_conflict_range_count = read_ranges.len();
        let write_conflict_range_count = write_ranges.len();
        record_fdb_conflict_artifacts(
            operation,
            u64::try_from(conflict_key_count).unwrap_or(u64::MAX),
            u64::try_from(read_conflict_range_count).unwrap_or(u64::MAX),
            u64::try_from(write_conflict_range_count).unwrap_or(u64::MAX),
            u64::try_from(candidate_key_count).unwrap_or(u64::MAX),
        );

        if conflict_key_count == 0
            && read_conflict_range_count == 0
            && write_conflict_range_count == 0
            && candidate_key_count == 0
        {
            return;
        }

        tracing::info!(
            operation,
            attempt,
            error_code,
            conflict_key_count,
            candidate_key_count,
            read_conflict_range_count,
            write_conflict_range_count,
            "FoundationDB transaction conflict detected"
        );
    }
}

async fn read_special_key_prefix(
    trx: &Transaction,
    prefix: &[u8],
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FdbError> {
    let start = prefix.to_vec();
    let end = increment_bytes(prefix.to_vec());
    let mut option = RangeOption::from((start, end));
    option.limit = Some(limit);
    option.mode = options::StreamingMode::WantAll;

    let mut iteration = 1;
    let mut out = Vec::new();

    loop {
        let values = trx.get_range(&option, iteration, true).await?;
        for kv in &values {
            out.push((kv.key().to_vec(), kv.value().to_vec()));
            if out.len() >= limit {
                return Ok(out);
            }
        }

        if let Some(next) = option.next_range(&values) {
            option = next;
            iteration += 1;
        } else {
            break;
        }
    }

    Ok(out)
}
