use openraft::{
    CommittedLeaderId, Entry, LogId, Vote,
    entry::RaftEntry,
    storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt},
};
use storage_provider::SqliteSettings;
use storage_sync::{
    SyncCommandDedupeStore, SyncMutationResponse, SyncProposalId, SyncProposalResponse,
    SyncTypeConfig,
};

use crate::{SQLiteStorageProvider, SqliteSyncRaftLogStore};

#[tokio::test]
async fn vote_log_entries_and_committed_metadata_persist_across_reopen() {
    let tempdir = crate::sql_test_support::temp_dir("sqlite");
    let database_path = tempdir.path().join("sync-raft.db");

    let provider = file_backed_provider(&database_path).await;
    let mut store = SqliteSyncRaftLogStore::new(provider);
    let first_entry = blank_entry(1, 7, 1);
    let second_entry = blank_entry(1, 7, 2);
    let committed = second_entry.log_id;
    let vote = Vote::new(3, 7);

    store.save_vote(&vote).await.expect("save vote");
    append_entries(&mut store, vec![first_entry.clone(), second_entry.clone()]).await;
    store
        .save_committed(Some(committed))
        .await
        .expect("save committed");
    drop(store);

    let provider = file_backed_provider(&database_path).await;
    let mut store = SqliteSyncRaftLogStore::new(provider);

    assert_eq!(Some(vote), store.read_vote().await.expect("read vote"));
    assert_eq!(
        Some(committed),
        store.read_committed().await.expect("read committed")
    );
    assert_eq!(
        vec![first_entry, second_entry],
        store
            .try_get_log_entries(1..3)
            .await
            .expect("read log entries")
    );
}

#[tokio::test]
async fn truncate_and_purge_update_persistent_log_state() {
    let tempdir = crate::sql_test_support::temp_dir("sqlite");
    let database_path = tempdir.path().join("sync-raft.db");

    let provider = file_backed_provider(&database_path).await;
    let mut store = SqliteSyncRaftLogStore::new(provider);
    let first_entry = blank_entry(1, 7, 1);
    let second_entry = blank_entry(1, 7, 2);
    let third_entry = blank_entry(2, 7, 3);

    append_entries(
        &mut store,
        vec![
            first_entry.clone(),
            second_entry.clone(),
            third_entry.clone(),
        ],
    )
    .await;
    store.truncate(second_entry.log_id).await.expect("truncate");
    assert_eq!(
        vec![first_entry],
        store
            .try_get_log_entries(1..4)
            .await
            .expect("read truncated entries")
    );

    append_entries(&mut store, vec![second_entry.clone(), third_entry.clone()]).await;
    store.purge(second_entry.log_id).await.expect("purge");
    drop(store);

    let provider = file_backed_provider(&database_path).await;
    let mut store = SqliteSyncRaftLogStore::new(provider);
    let log_state = store.get_log_state().await.expect("log state");

    assert_eq!(Some(second_entry.log_id), log_state.last_purged_log_id);
    assert_eq!(Some(third_entry.log_id), log_state.last_log_id);
    assert_eq!(
        vec![third_entry],
        store
            .try_get_log_entries(1..4)
            .await
            .expect("read purged entries")
    );
}

#[tokio::test]
async fn command_dedupe_response_persists_across_reopen() {
    let tempdir = crate::sql_test_support::temp_dir("sqlite");
    let database_path = tempdir.path().join("sync-raft.db");

    let provider = file_backed_provider(&database_path).await;
    let store = SqliteSyncRaftLogStore::new(provider);
    let proposal_id = SyncProposalId::new("TransactWriteItems#client_request_token#token-1")
        .expect("proposal id");
    let response = SyncProposalResponse::new(
        proposal_id.clone(),
        vec![SyncMutationResponse {
            response_json: Some(r#"{"ConsumedCapacity":[]}"#.to_string()),
        }],
    );

    store
        .save_sync_command_response(&response)
        .await
        .expect("save command response");
    drop(store);

    let provider = file_backed_provider(&database_path).await;
    let store = SqliteSyncRaftLogStore::new(provider);
    assert_eq!(
        Some(response),
        store
            .load_sync_command_response(&proposal_id)
            .await
            .expect("load command response")
    );
}

async fn file_backed_provider(path: &std::path::Path) -> SQLiteStorageProvider {
    let settings = SqliteSettings {
        force_file_backed_database: true,
        ..SqliteSettings::default()
    };
    SQLiteStorageProvider::new_with_settings(&path.to_string_lossy(), settings)
        .await
        .expect("sqlite provider")
}

fn blank_entry(term: u64, node_id: u64, index: u64) -> Entry<SyncTypeConfig> {
    Entry::new_blank(LogId::new(CommittedLeaderId::new(term, node_id), index))
}

async fn append_entries(store: &mut SqliteSyncRaftLogStore, entries: Vec<Entry<SyncTypeConfig>>) {
    store
        .blocking_append(entries)
        .await
        .expect("append entries");
}
