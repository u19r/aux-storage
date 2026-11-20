//! Bounded physical deletion of one incarnation's explicit append data.

use storage_provider::TableReclaimStep;
use storage_types::{StorageError, StorageResult, TableHistoryId};

use crate::{SqlAttempt, SqlLayout};

pub(crate) async fn delete_table_appends<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    history: TableHistoryId,
) -> StorageResult<TableReclaimStep> {
    let counts = attempt
        .execute_batch_exact(super::sql_statements::delete_table_appends(engine, history))
        .await?
        .into_array::<4>()?;
    if counts[..3].iter().any(|count| *count > 64) || counts[3] > 1 {
        return Err(StorageError::internal(
            "SQL append reclaim count is out of range",
        ));
    }
    Ok(if counts[..3].contains(&64) {
        TableReclaimStep::Advanced
    } else {
        TableReclaimStep::Complete
    })
}
