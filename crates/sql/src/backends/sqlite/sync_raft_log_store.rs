use openraft::{
    Entry, LogId, StorageIOError, Vote,
    storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage},
};
use rusqlite::OptionalExtension as _;
use storage_sync::{
    SyncCommandDedupeStore, SyncNodeId, SyncProposalId, SyncProposalResponse, SyncTypeConfig,
};
use storage_types::{StorageError, StorageResult};

use super::SQLiteStorageProvider;
use crate::{
    error_handler::map_sqlite_error,
    utils::{SqliteConn, call_sqlite},
};

#[derive(Clone)]
pub struct SqliteSyncRaftLogStore {
    provider: SQLiteStorageProvider,
}

impl SqliteSyncRaftLogStore {
    #[must_use]
    pub const fn new(provider: SQLiteStorageProvider) -> Self {
        Self { provider }
    }

    async fn append_entries(&self, entries: Vec<Entry<SyncTypeConfig>>) -> StorageResult<()> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            ensure_sync_raft_tables(&SqliteConn::Connection(sqlite))?;
            for entry in entries {
                sqlite
                    .execute(
                        r"INSERT INTO sys_sync_raft_log (log_index, log_id_json, entry_json)
                          VALUES (?1, ?2, ?3)
                          ON CONFLICT(log_index)
                          DO UPDATE SET
                            log_id_json = excluded.log_id_json,
                            entry_json = excluded.entry_json",
                        (
                            i64::try_from(entry.log_id.index).map_err(|_| {
                                StorageError::validation(
                                    "raft log index does not fit sqlite integer",
                                )
                            })?,
                            serde_json::to_string(&entry.log_id)?,
                            serde_json::to_string(&entry)?,
                        ),
                    )
                    .map_err(map_sqlite_error)?;
            }
            Ok(())
        })
        .await
    }
}

impl RaftLogReader<SyncTypeConfig> for SqliteSyncRaftLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<SyncTypeConfig>>, openraft::StorageError<SyncNodeId>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        let bounds = SqliteLogRange::from_bounds(&range);
        call_sqlite(&self.provider.connection, move |sqlite| {
            ensure_sync_raft_tables(&SqliteConn::Connection(sqlite))?;
            let mut stmt = sqlite
                .prepare(
                    r"SELECT entry_json FROM sys_sync_raft_log
                      WHERE log_index >= ?1
                        AND (?2 IS NULL OR log_index < ?2)
                      ORDER BY log_index ASC",
                )
                .map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map((bounds.start_inclusive, bounds.end_exclusive), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(map_sqlite_error)?;
            let mut entries = Vec::new();
            for row in rows {
                let entry_json = row.map_err(map_sqlite_error)?;
                entries.push(serde_json::from_str(&entry_json)?);
            }
            Ok(entries)
        })
        .await
        .map_err(read_logs_error)
    }
}

impl RaftLogStorage<SyncTypeConfig> for SqliteSyncRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<SyncTypeConfig>, openraft::StorageError<SyncNodeId>> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            ensure_sync_raft_tables(&SqliteConn::Connection(sqlite))?;
            let last_purged = read_log_id(sqlite, "last_purged")?;
            let last_log_id_json = sqlite
                .query_row(
                    "SELECT log_id_json FROM sys_sync_raft_log ORDER BY log_index DESC LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map(Some)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    error => Err(map_sqlite_error(error)),
                })?;
            let last_log_id = last_log_id_json
                .map(|json| serde_json::from_str(&json))
                .transpose()?
                .or(last_purged);
            Ok(LogState {
                last_purged_log_id: last_purged,
                last_log_id,
            })
        })
        .await
        .map_err(read_logs_error)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<SyncNodeId>,
    ) -> Result<(), openraft::StorageError<SyncNodeId>> {
        let vote = *vote;
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite = SqliteConn::Connection(sqlite);
            ensure_sync_raft_tables(&sqlite)?;
            upsert_metadata(&sqlite, "vote", &serde_json::to_string(&vote)?)
        })
        .await
        .map_err(write_logs_error)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<SyncNodeId>>, openraft::StorageError<SyncNodeId>> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite = SqliteConn::Connection(sqlite);
            ensure_sync_raft_tables(&sqlite)?;
            read_json_metadata(&sqlite, "vote")
        })
        .await
        .map_err(read_logs_error)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<SyncTypeConfig>,
    ) -> Result<(), openraft::StorageError<SyncNodeId>>
    where
        I: IntoIterator<Item = Entry<SyncTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let result = self.append_entries(entries).await;
        callback.log_io_completed(result.as_ref().map(|_| ()).map_err(io_write_error));
        result.map_err(write_logs_error)
    }

    async fn truncate(
        &mut self,
        log_id: LogId<SyncNodeId>,
    ) -> Result<(), openraft::StorageError<SyncNodeId>> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            ensure_sync_raft_tables(&SqliteConn::Connection(sqlite))?;
            sqlite
                .execute(
                    "DELETE FROM sys_sync_raft_log WHERE log_index >= ?1",
                    [i64::try_from(log_id.index).map_err(|_| {
                        StorageError::validation("raft truncate index does not fit sqlite integer")
                    })?],
                )
                .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
        .map_err(write_logs_error)
    }

    async fn purge(
        &mut self,
        log_id: LogId<SyncNodeId>,
    ) -> Result<(), openraft::StorageError<SyncNodeId>> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite_conn = SqliteConn::Connection(sqlite);
            ensure_sync_raft_tables(&sqlite_conn)?;
            sqlite
                .execute(
                    "DELETE FROM sys_sync_raft_log WHERE log_index <= ?1",
                    [i64::try_from(log_id.index).map_err(|_| {
                        StorageError::validation("raft purge index does not fit sqlite integer")
                    })?],
                )
                .map_err(map_sqlite_error)?;
            upsert_metadata(
                &sqlite_conn,
                "last_purged",
                &serde_json::to_string(&log_id)?,
            )
        })
        .await
        .map_err(write_logs_error)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<SyncNodeId>>,
    ) -> Result<(), openraft::StorageError<SyncNodeId>> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite = SqliteConn::Connection(sqlite);
            ensure_sync_raft_tables(&sqlite)?;
            upsert_metadata(&sqlite, "committed", &serde_json::to_string(&committed)?)
        })
        .await
        .map_err(write_logs_error)
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<SyncNodeId>>, openraft::StorageError<SyncNodeId>> {
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite = SqliteConn::Connection(sqlite);
            ensure_sync_raft_tables(&sqlite)?;
            read_json_metadata(&sqlite, "committed").map(|value| value.flatten())
        })
        .await
        .map_err(read_logs_error)
    }
}

#[async_trait::async_trait]
impl SyncCommandDedupeStore for SqliteSyncRaftLogStore {
    async fn load_sync_command_response(
        &self,
        proposal_id: &SyncProposalId,
    ) -> StorageResult<Option<SyncProposalResponse>> {
        let proposal_id = proposal_id.as_str().to_string();
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite = SqliteConn::Connection(sqlite);
            ensure_sync_raft_command_results_table(&sqlite)?;
            let json = sqlite
                .query_row(
                    "SELECT response_json FROM sys_sync_raft_command_results WHERE proposal_id = \
                     ?1",
                    [&proposal_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(map_sqlite_error)?;
            json.map(|json| serde_json::from_str(&json).map_err(Into::into))
                .transpose()
        })
        .await
    }

    async fn save_sync_command_response(
        &self,
        response: &SyncProposalResponse,
    ) -> StorageResult<()> {
        let proposal_id = response.proposal_id.as_str().to_string();
        let response_json = serde_json::to_string(response)?;
        call_sqlite(&self.provider.connection, move |sqlite| {
            let sqlite = SqliteConn::Connection(sqlite);
            ensure_sync_raft_command_results_table(&sqlite)?;
            sqlite
                .execute(
                    r"INSERT INTO sys_sync_raft_command_results (proposal_id, response_json)
                      VALUES (?1, ?2)
                      ON CONFLICT(proposal_id)
                      DO UPDATE SET response_json = excluded.response_json",
                    (&proposal_id, &response_json),
                )
                .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }
}

struct SqliteLogRange {
    start_inclusive: i64,
    end_exclusive: Option<i64>,
}

impl SqliteLogRange {
    fn from_bounds<RB>(range: &RB) -> Self
    where RB: std::ops::RangeBounds<u64> {
        use std::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => Some(value.saturating_add(1)),
            Bound::Excluded(value) => Some(*value),
            Bound::Unbounded => None,
        };
        Self {
            start_inclusive: i64::try_from(start).unwrap_or(i64::MAX),
            end_exclusive: end.and_then(|value| i64::try_from(value).ok()),
        }
    }
}

fn ensure_sync_raft_tables(sqlite: &SqliteConn<'_>) -> StorageResult<()> {
    sqlite
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_raft_log (
                log_index INTEGER PRIMARY KEY,
                log_id_json TEXT NOT NULL,
                entry_json TEXT NOT NULL
            )",
            [],
        )
        .map_err(map_sqlite_error)?;
    sqlite
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_raft_metadata (
                metadata_key TEXT PRIMARY KEY,
                metadata_json TEXT NOT NULL
            )",
            [],
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn ensure_sync_raft_command_results_table(sqlite: &SqliteConn<'_>) -> StorageResult<()> {
    sqlite
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_raft_command_results (
                proposal_id TEXT PRIMARY KEY,
                response_json TEXT NOT NULL
            )",
            [],
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn upsert_metadata(sqlite: &SqliteConn<'_>, key: &str, json: &str) -> StorageResult<()> {
    sqlite
        .execute(
            r"INSERT INTO sys_sync_raft_metadata (metadata_key, metadata_json)
              VALUES (?1, ?2)
              ON CONFLICT(metadata_key)
              DO UPDATE SET metadata_json = excluded.metadata_json",
            (key, json),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn read_json_metadata<T>(sqlite: &SqliteConn<'_>, key: &str) -> StorageResult<Option<T>>
where T: serde::de::DeserializeOwned {
    let json = sqlite
        .query_row(
            "SELECT metadata_json FROM sys_sync_raft_metadata WHERE metadata_key = ?1",
            [key],
            |row| row.get::<_, String>(0),
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            error => Err(map_sqlite_error(error)),
        })?;
    json.map(|json| serde_json::from_str(&json).map_err(Into::into))
        .transpose()
}

fn read_log_id(
    sqlite: &rusqlite::Connection,
    key: &str,
) -> StorageResult<Option<LogId<SyncNodeId>>> {
    let sqlite = SqliteConn::Connection(sqlite);
    read_json_metadata(&sqlite, key)
}

fn write_logs_error(error: StorageError) -> openraft::StorageError<SyncNodeId> {
    StorageIOError::write_logs(openraft::AnyError::error(error.to_string())).into()
}

fn read_logs_error(error: StorageError) -> openraft::StorageError<SyncNodeId> {
    StorageIOError::read_logs(openraft::AnyError::error(error.to_string())).into()
}

fn io_write_error(error: &StorageError) -> std::io::Error {
    std::io::Error::other(StorageIOError::<SyncNodeId>::write_logs(
        openraft::AnyError::error(error.to_string()),
    ))
}
