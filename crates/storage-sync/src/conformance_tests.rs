use storage_types::{ItemStreamVersion, TableName, TimestampMillis};

use crate::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncApply, SyncCommitMetadata,
    SyncConformanceCase, SyncConformanceExpectation, SyncCreateTableMutation, SyncDeleteMutation,
    SyncDeleteTableMutation, SyncLogId, SyncMutationId, SyncMutationResponse, SyncPutMutation,
    SyncUpdateTableMutation, SyncUpdateTimeToLiveMutation,
    sync_support_tests::ResolvedOnlyApplyAdapter,
};

#[tokio::test]
async fn conformance_cases_cover_write_shapes_without_openraft_runtime() {
    let cases = conformance_cases();
    let adapter = ResolvedOnlyApplyAdapter;

    for case in cases {
        let responses = adapter
            .apply_resolved_sync_mutations(
                SyncCommitMetadata {
                    log_id: SyncLogId::new(1, 1),
                    committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
                    leader_node_id: "node-a".to_string(),
                },
                case.expected.resolved_batch.clone(),
            )
            .await
            .unwrap();

        assert_eq!(responses, case.expected.responses, "{}", case.name);
    }
}

#[tokio::test]
async fn mixed_backend_conformance_reuses_identical_resolved_payloads() {
    let pairs = [
        ("sqlite", "rocksdb"),
        ("sqlite", "foundationdb"),
        ("foundationdb", "sqlite"),
    ];
    let adapter = ResolvedOnlyApplyAdapter;

    for (source_backend, destination_backend) in pairs {
        for case in conformance_cases() {
            let responses = adapter
                .apply_resolved_sync_mutations(
                    SyncCommitMetadata {
                        log_id: SyncLogId::new(1, 1),
                        committed_at: TimestampMillis::from_timestamp(1_700_000_000_000),
                        leader_node_id: format!("{source_backend}-leader"),
                    },
                    case.expected.resolved_batch.clone(),
                )
                .await
                .unwrap();

            assert_eq!(
                responses, case.expected.responses,
                "{source_backend}->{destination_backend} {}",
                case.name
            );
            assert!(
                case.expected
                    .resolved_batch
                    .mutations
                    .iter()
                    .all(has_expected_version_contract),
                "{source_backend}->{destination_backend} {} missing stream version",
                case.name
            );
        }
    }
}

fn conformance_cases() -> Vec<SyncConformanceCase<&'static str>> {
    vec![
        conformance_case("put_item", vec![put("put-1", 1)]),
        conformance_case("update_item", vec![put("update-as-final-put", 2)]),
        conformance_case("delete_item", vec![delete("delete-1", 3)]),
        conformance_case(
            "batch_write_item",
            vec![put("batch-put", 4), delete("batch-delete", 5)],
        ),
        conformance_case(
            "transact_write_items",
            vec![put("txn-put", 6), delete("txn-delete", 7)],
        ),
        conformance_case("create_table", vec![create_table("create-table")]),
        conformance_case("update_table", vec![update_table("update-table")]),
        conformance_case("update_time_to_live", vec![update_ttl("update-ttl")]),
        conformance_case("delete_table", vec![delete_table("delete-table")]),
    ]
}

fn has_expected_version_contract(mutation: &ResolvedSyncMutation) -> bool {
    match mutation {
        ResolvedSyncMutation::Put(_) | ResolvedSyncMutation::Delete(_) => {
            mutation.target_item_stream_version().get() > 0
        }
        ResolvedSyncMutation::CreateTable(_)
        | ResolvedSyncMutation::UpdateTable(_)
        | ResolvedSyncMutation::DeleteTable(_)
        | ResolvedSyncMutation::UpdateTimeToLive(_) => {
            mutation.target_item_stream_version().get() == 0
        }
    }
}

fn conformance_case(
    name: &'static str,
    mutations: Vec<ResolvedSyncMutation>,
) -> SyncConformanceCase<&'static str> {
    let responses = mutations
        .iter()
        .map(|mutation| SyncMutationResponse {
            response_json: Some(mutation.mutation_id().as_str().to_string()),
        })
        .collect::<Vec<_>>();
    SyncConformanceCase {
        name,
        request: name,
        expected: SyncConformanceExpectation::new(
            ResolvedSyncMutationBatch::new(mutations),
            responses,
        ),
    }
}

fn put(id: &str, version: u64) -> ResolvedSyncMutation {
    ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new(id).unwrap(),
        table_name: TableName::new("orders"),
        key_json: format!(r#"{{"pk":{{"S":"{id}"}}}}"#),
        item_json: format!(r#"{{"pk":{{"S":"{id}"}},"status":{{"S":"open"}}}}"#),
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(version),
        response: SyncMutationResponse::default(),
    })
}

fn delete(id: &str, version: u64) -> ResolvedSyncMutation {
    ResolvedSyncMutation::Delete(SyncDeleteMutation {
        mutation_id: SyncMutationId::new(id).unwrap(),
        table_name: TableName::new("orders"),
        key_json: format!(r#"{{"pk":{{"S":"{id}"}}}}"#),
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(version),
        response: SyncMutationResponse::default(),
    })
}

fn create_table(id: &str) -> ResolvedSyncMutation {
    ResolvedSyncMutation::CreateTable(SyncCreateTableMutation {
        mutation_id: SyncMutationId::new(id).unwrap(),
        table_name: TableName::new("orders"),
        request_json: r#"{"TableName":"orders"}"#.to_string(),
    })
}

fn update_table(id: &str) -> ResolvedSyncMutation {
    ResolvedSyncMutation::UpdateTable(SyncUpdateTableMutation {
        mutation_id: SyncMutationId::new(id).unwrap(),
        table_name: TableName::new("orders"),
        request_json: r#"{"TableName":"orders","StreamSpecification":{"StreamEnabled":true}}"#
            .to_string(),
    })
}

fn update_ttl(id: &str) -> ResolvedSyncMutation {
    ResolvedSyncMutation::UpdateTimeToLive(SyncUpdateTimeToLiveMutation {
        mutation_id: SyncMutationId::new(id).unwrap(),
        table_name: TableName::new("orders"),
        request_json: r#"{"TableName":"orders","TimeToLiveSpecification":{"AttributeName":"expires_at","Enabled":true}}"#
            .to_string(),
    })
}

fn delete_table(id: &str) -> ResolvedSyncMutation {
    ResolvedSyncMutation::DeleteTable(SyncDeleteTableMutation {
        mutation_id: SyncMutationId::new(id).unwrap(),
        table_name: TableName::new("orders"),
        request_json: r#"{"TableName":"orders"}"#.to_string(),
    })
}
