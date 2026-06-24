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
        candidate_keys: &[Vec<u8>],
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

        let conflict_keys: Vec<String> = conflicting
            .iter()
            .map(|(key, _)| {
                let stripped = key.strip_prefix(CONFLICTING_KEYS_PREFIX).unwrap_or(key);
                self.format_key_with_prefix(stripped)
            })
            .collect();
        let candidate_key_list: Vec<String> = candidate_keys
            .iter()
            .take(CONFLICT_LOG_MAX_KEYS)
            .map(|key| self.format_key_with_prefix(key))
            .collect();
        let read_conflict_ranges: Vec<String> = read_ranges
            .iter()
            .map(|(key, value)| format!("{} -> {}", hex_encode(key), hex_encode(value)))
            .collect();
        let write_conflict_ranges: Vec<String> = write_ranges
            .iter()
            .map(|(key, value)| format!("{} -> {}", hex_encode(key), hex_encode(value)))
            .collect();
        record_fdb_conflict_artifacts(
            operation,
            u64::try_from(conflict_keys.len()).unwrap_or(u64::MAX),
            u64::try_from(read_conflict_ranges.len()).unwrap_or(u64::MAX),
            u64::try_from(write_conflict_ranges.len()).unwrap_or(u64::MAX),
            u64::try_from(candidate_key_list.len()).unwrap_or(u64::MAX),
        );

        if conflict_keys.is_empty()
            && read_conflict_ranges.is_empty()
            && write_conflict_ranges.is_empty()
            && candidate_key_list.is_empty()
        {
            return;
        }

        tracing::info!(
            operation,
            attempt,
            error_code,
            conflict_keys = ?conflict_keys,
            candidate_keys = ?candidate_key_list,
            read_conflict_ranges = ?read_conflict_ranges,
            write_conflict_ranges = ?write_conflict_ranges,
            "FoundationDB transaction conflict detected"
        );
    }

    fn format_key_with_prefix(&self, key: &[u8]) -> String {
        let config = self.config();
        if let Some(prefix) = &config.subspace_prefix
            && key.starts_with(prefix)
        {
            let stripped = &key[prefix.len()..];
            return format!("{} (stripped={})", hex_encode(key), hex_encode(stripped));
        }
        hex_encode(key)
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

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
