use std::collections::HashMap;

use serde_json::json;
use storage_types::{
    StreamRecord, SystemStreamCheckpointToken, SystemStreamContractError, SystemStreamPosition,
    SystemStreamScope, TimestampMillis,
};

use crate::{
    SystemStreamIssuedCheckpoint, SystemStreamLineage, SystemStreamLineageCatalog,
    SystemStreamPage, SystemStreamSplitChild, SystemStreamSplitDisposition,
    SystemStreamSplitOperationId, SystemStreamSplitReceipt, SystemStreamSplitRequest,
    SystemStreamSplitResult, SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest,
};

fn scope() -> SystemStreamScope {
    SystemStreamScope::try_new("system", 7).expect("scope")
}

fn lineage(shard: &str, id: &str, parent: Option<&str>, sealed: bool) -> SystemStreamLineage {
    SystemStreamLineage::try_new(shard, id, parent.map(str::to_string), sealed).expect("lineage")
}

fn parent() -> SystemStreamLineage {
    lineage("parent", "lineage-parent", None, false)
}

fn children() -> [SystemStreamSplitChild; 2] {
    [
        SystemStreamSplitChild::try_new(
            lineage("left", "lineage-left", Some("lineage-parent"), false),
            1,
        )
        .expect("left child"),
        SystemStreamSplitChild::try_new(
            lineage("right", "lineage-right", Some("lineage-parent"), false),
            1,
        )
        .expect("right child"),
    ]
}

fn request() -> SystemStreamSplitRequest {
    SystemStreamSplitRequest::try_new(
        scope(),
        parent(),
        children(),
        SystemStreamSplitOperationId::try_new("split-parent-1").expect("operation id"),
    )
    .expect("split request")
}

fn trim_request(page_limit: usize) -> SystemStreamTrimPageRequest {
    SystemStreamTrimPageRequest::try_new(
        scope(),
        TimestampMillis::from_timestamp(1_700_000_001_000),
        page_limit,
        None,
    )
    .expect("trim request")
}

#[test]
fn split_request_accepts_exactly_two_new_children_against_active_parent() {
    let request = request();
    let catalog = SystemStreamLineageCatalog::try_new(scope(), vec![parent()])
        .expect("active parent catalog");

    request
        .validate_against(&catalog)
        .expect("validated split request");
    assert_eq!(request.children().len(), 2);
    assert_eq!(request.parent().lineage_id().as_str(), "lineage-parent");
}

#[test]
fn split_request_rejects_stale_scope_parent_and_partial_retry_before_effects() {
    let request = request();
    let wrong_scope = SystemStreamScope::try_new("system", 8).expect("scope");
    let wrong_catalog =
        SystemStreamLineageCatalog::try_new(wrong_scope, vec![parent()]).expect("catalog");
    assert_eq!(
        request.validate_against(&wrong_catalog),
        Err(SystemStreamContractError::WrongGeneration)
    );

    let wrong_scope = SystemStreamScope::try_new("other-system", 7).expect("scope");
    let wrong_scope_catalog =
        SystemStreamLineageCatalog::try_new(wrong_scope, vec![parent()]).expect("catalog");
    assert_eq!(
        request.validate_against(&wrong_scope_catalog),
        Err(SystemStreamContractError::WrongScope)
    );

    let unknown_catalog = SystemStreamLineageCatalog::try_new(
        scope(),
        vec![lineage("other", "lineage-other", None, false)],
    )
    .expect("catalog");
    assert_eq!(
        request.validate_against(&unknown_catalog),
        Err(SystemStreamContractError::UnknownLineage)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                parent(),
                lineage("left", "lineage-left", Some("lineage-parent"), false)
            ],
        ),
        Err(SystemStreamContractError::BackendContract)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                lineage("parent", "lineage-parent", None, true),
                lineage("left", "lineage-left", Some("lineage-parent"), false),
            ],
        ),
        Err(SystemStreamContractError::BackendContract)
    );
}

#[test]
fn lineage_catalog_rejects_duplicate_ids_shards_missing_parents_and_cycles() {
    assert_eq!(
        SystemStreamLineage::try_new(
            "same",
            "lineage-same",
            Some("lineage-same".to_string()),
            false,
        ),
        Err(SystemStreamContractError::BackendContract)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                lineage("same", "lineage-parent", None, true),
                lineage("same", "lineage-child", Some("lineage-parent"), false),
            ],
        ),
        Err(SystemStreamContractError::BackendContract)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                lineage("left", "lineage-same", None, true),
                lineage("right", "lineage-same", None, false),
            ],
        ),
        Err(SystemStreamContractError::BackendContract)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                lineage("same", "lineage-a", None, false),
                lineage("same", "lineage-b", None, false),
            ],
        ),
        Err(SystemStreamContractError::DuplicateShard)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![lineage(
                "child",
                "lineage-child",
                Some("lineage-missing"),
                false,
            )],
        ),
        Err(SystemStreamContractError::UnknownLineage)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                lineage("parent", "lineage-parent", None, false),
                lineage("child", "lineage-child", Some("lineage-parent"), false),
            ],
        ),
        Err(SystemStreamContractError::BackendContract)
    );

    assert_eq!(
        SystemStreamLineageCatalog::try_new(
            scope(),
            vec![
                lineage("a", "lineage-a", Some("lineage-b"), true),
                lineage("b", "lineage-b", Some("lineage-a"), true),
                lineage("root", "lineage-root", None, false),
            ],
        ),
        Err(SystemStreamContractError::BackendContract)
    );
}

#[test]
fn lineage_catalog_rejects_more_than_the_bounded_lineage_snapshot() {
    let mut entries = Vec::new();
    for index in 0..=storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        let lineage_id = format!("lineage-{index}");
        let parent_id = (index > 0).then(|| format!("lineage-{}", index - 1));
        entries.push(lineage(
            &format!("shard-{index}"),
            &lineage_id,
            parent_id.as_deref(),
            index != storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES,
        ));
    }

    assert_eq!(
        SystemStreamLineageCatalog::try_new(scope(), entries),
        Err(SystemStreamContractError::BackendContract)
    );
}

#[test]
fn split_types_reject_malformed_identity_zero_sequence_and_duplicate_children() {
    assert_eq!(
        SystemStreamSplitOperationId::try_new(" "),
        Err(SystemStreamContractError::BackendContract)
    );
    assert_eq!(
        SystemStreamSplitChild::try_new(
            lineage("left", "lineage-left", Some("lineage-parent"), false),
            0,
        ),
        Err(SystemStreamContractError::BackendContract)
    );
    assert_eq!(
        SystemStreamSplitChild::try_new(
            lineage("left", "lineage-left", Some("lineage-parent"), false),
            u64::MAX,
        ),
        Err(SystemStreamContractError::BackendContract)
    );
    assert_eq!(
        SystemStreamSplitChild::try_new(lineage("left", "lineage-left", None, true), 1),
        Err(SystemStreamContractError::BackendContract)
    );
    assert_eq!(
        SystemStreamSplitChild::try_new(lineage("left", "lineage-left", None, false), 1),
        Err(SystemStreamContractError::BackendContract)
    );

    let left = children()[0].clone();
    assert_eq!(
        SystemStreamSplitRequest::try_new(
            scope(),
            parent(),
            [left.clone(), left],
            SystemStreamSplitOperationId::try_new("split-parent-1").expect("operation id"),
        ),
        Err(SystemStreamContractError::BackendContract)
    );
}

#[test]
fn given_split_receipt_when_replayed_then_only_exact_identity_is_accepted() {
    let request = request();
    let receipt = SystemStreamSplitReceipt::try_new(&request, 7).expect("split receipt");
    let result = &receipt.split;

    result
        .validate_retry(&request)
        .expect("exact retry identity");
    assert_eq!(result.disposition(), SystemStreamSplitDisposition::Applied);

    let changed_operation = SystemStreamSplitRequest::try_new(
        request.source_scope().clone(),
        request.parent().clone(),
        request.children().clone(),
        SystemStreamSplitOperationId::try_new("split-parent-2").expect("operation id"),
    )
    .expect("changed request");
    assert_eq!(
        result.validate_retry(&changed_operation),
        Err(SystemStreamContractError::BackendContract)
    );

    let changed_child = SystemStreamSplitRequest::try_new(
        request.source_scope().clone(),
        request.parent().clone(),
        [
            SystemStreamSplitChild::try_new(
                lineage("left", "lineage-left", Some("lineage-parent"), false),
                2,
            )
            .expect("changed left child"),
            request.children()[1].clone(),
        ],
        request.operation_id().clone(),
    )
    .expect("changed child request");
    assert_eq!(
        result.validate_retry(&changed_child),
        Err(SystemStreamContractError::BackendContract)
    );

    let replayed = receipt.replay(&request).expect("replayed split result");
    replayed
        .validate_retry(&request)
        .expect("exact replay identity");
    assert_eq!(
        replayed.disposition(),
        SystemStreamSplitDisposition::Replayed
    );

    assert_eq!(
        SystemStreamSplitResult::try_new(
            &request,
            request.parent().clone(),
            SystemStreamSplitDisposition::Applied,
        ),
        Err(SystemStreamContractError::BackendContract)
    );

    assert_eq!(
        SystemStreamSplitResult::try_new(
            &request,
            lineage("other", "lineage-other", None, true),
            SystemStreamSplitDisposition::Applied,
        ),
        Err(SystemStreamContractError::BackendContract)
    );
    assert!(SystemStreamSplitReceipt::try_new(&request, 0).is_err());
}

#[test]
fn given_trim_receipt_when_replayed_then_shared_page_contract_is_enforced() {
    let request = trim_request(10);
    let receipt = SystemStreamTrimPageReceipt::try_new(
        &request,
        4,
        7,
        Some(b"provider-native-cursor".to_vec()),
    )
    .expect("trim receipt");

    let replay = receipt.replay(&request).expect("trim replay");
    assert_eq!(replay.reclaimed_records(), 4);
    assert_eq!(replay.scanned_records(), 7);
    assert_eq!(
        replay
            .next_continuation()
            .expect("next continuation")
            .as_bytes(),
        b"provider-native-cursor"
    );
    assert!(receipt.replay(&trim_request(9)).is_err());

    let invalid = SystemStreamTrimPageReceipt {
        operation_id: request.operation_id().clone(),
        page_limit: request.page_limit(),
        input_sequence: 0,
        input_cursor: None,
        reclaimed_records: 8,
        scanned_records: 7,
        next_cursor: None,
    };
    assert!(invalid.replay(&request).is_err());
}

fn issued_checkpoint() -> SystemStreamIssuedCheckpoint {
    let checkpoint = storage_types::SystemStreamCheckpoint::try_new(
        scope(),
        vec![SystemStreamPosition::try_new("shard-a", "lineage-a", "0").expect("position")],
    )
    .expect("checkpoint");
    let token = SystemStreamCheckpointToken::encode(
        &checkpoint,
        storage_types::TimestampMillis::from_timestamp(0),
    )
    .expect("checkpoint token");
    SystemStreamIssuedCheckpoint::new(token, checkpoint).expect("issued checkpoint")
}

#[test]
fn system_stream_page_requires_logical_source_table_name_before_response() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: None,
        keys: HashMap::new().into(),
        sequence_number: "1".to_string(),
        item: None,
        system_position: Some(
            SystemStreamPosition::try_new("shard-a", "lineage-a", "1").expect("position"),
        ),
    };

    assert!(matches!(
        SystemStreamPage::new(scope(), vec![record], issued_checkpoint(), false),
        Err(SystemStreamContractError::MissingSourceTableName)
    ));
}

#[test]
fn system_stream_page_conversion_preserves_logical_source_table_name_on_wire() {
    let page = SystemStreamPage::new(
        scope(),
        vec![system_delete_record()],
        issued_checkpoint(),
        false,
    )
    .expect("page");
    let response = page.into_response().expect("response");
    let encoded = serde_json::to_value(response).expect("response JSON");

    assert_eq!(encoded["Records"][0]["SourceTableName"], "orders");
}

#[test]
fn given_system_stream_page_when_converted_then_issued_checkpoint_is_retained_for_all_page_shapes()
{
    let record = system_delete_record();
    let expected_token = issued_checkpoint().token().clone();

    for (terminal, records) in [
        (false, Vec::new()),
        (false, vec![record.clone()]),
        (true, Vec::new()),
        (true, vec![record.clone()]),
    ] {
        let page =
            SystemStreamPage::new(scope(), records, issued_checkpoint(), terminal).expect("page");
        let response = page.into_response().expect("response");

        assert_eq!(response.checkpoint(), Some(&expected_token));
        let encoded = serde_json::to_value(response).expect("response JSON");
        assert_eq!(encoded["Checkpoint"], json!(expected_token.as_str()));
    }
}

#[test]
fn given_overlapping_checkpoint_frontiers_when_protected_then_descendants_use_minimum_position() {
    let scope = SystemStreamScope::try_new("protection-test", 1).unwrap();
    let catalog = SystemStreamLineageCatalog::try_new(
        scope.clone(),
        vec![
            SystemStreamLineage::try_new("parent-shard", "parent", None, true).unwrap(),
            SystemStreamLineage::try_new("left-shard", "left", Some("parent".into()), false)
                .unwrap(),
            SystemStreamLineage::try_new("right-shard", "right", Some("parent".into()), false)
                .unwrap(),
        ],
    )
    .unwrap();
    let checkpoint = storage_types::SystemStreamCheckpoint::try_new(
        scope.clone(),
        vec![
            SystemStreamPosition::try_new("parent-shard", "parent", "9").unwrap(),
            SystemStreamPosition::try_new("right-shard", "right", "7").unwrap(),
        ],
    )
    .unwrap();
    let protected = catalog.protected_positions(&checkpoint).unwrap();
    assert_eq!(protected.len(), 3);
    for (lineage, offset) in [("parent", "9"), ("left", "0"), ("right", "0")] {
        assert_eq!(
            protected
                .iter()
                .find(|position| position.lineage_id().as_str() == lineage)
                .unwrap()
                .offset()
                .as_str(),
            offset
        );
    }
    let unknown = storage_types::SystemStreamCheckpoint::try_new(
        scope,
        vec![SystemStreamPosition::try_new("parent-shard", "unknown", "0").unwrap()],
    )
    .unwrap();
    assert_eq!(
        catalog.protected_positions(&unknown),
        Err(SystemStreamContractError::UnknownLineage)
    );
}

fn system_delete_record() -> StreamRecord {
    StreamRecord {
        cursor: None,
        source_table_name: Some(storage_types::TableName::new("orders")),
        keys: HashMap::new().into(),
        sequence_number: "1".to_string(),
        item: None,
        system_position: Some(SystemStreamPosition::try_new("shard-a", "lineage-a", "1").unwrap()),
    }
}

#[test]
fn given_split_branch_when_split_again_then_other_routes_remain_stable() {
    let before = SystemStreamLineageCatalog::try_new(
        scope(),
        vec![
            lineage("root", "root", None, true),
            lineage("left", "left", Some("root"), false),
            lineage("right", "right", Some("root"), false),
            lineage("other", "other", None, false),
        ],
    )
    .unwrap();
    let mut entries = vec![
        lineage("root", "root", None, true),
        lineage("left", "left", Some("root"), true),
        lineage("right", "right", Some("root"), false),
        lineage("other", "other", None, false),
        lineage("left-a", "left-a", Some("left"), false),
        lineage("left-b", "left-b", Some("left"), false),
    ];
    let after = SystemStreamLineageCatalog::try_new(scope(), entries.clone()).unwrap();
    entries.reverse();
    let reordered = SystemStreamLineageCatalog::try_new(scope(), entries).unwrap();
    let mut counts = [0; 3];
    let mut children_seen = [false; 2];
    for key in 0_u64..1024 {
        let key = key.to_be_bytes();
        let previous = before.writer_lineage(&key).unwrap().lineage_id().as_str();
        let next = after.writer_lineage(&key).unwrap().lineage_id().as_str();
        assert_eq!(
            next,
            reordered
                .writer_lineage(&key)
                .unwrap()
                .lineage_id()
                .as_str()
        );
        match previous {
            "left" => {
                counts[0] += 1;
                assert!(["left-a", "left-b"].contains(&next));
                children_seen[usize::from(next == "left-b")] = true;
            }
            "right" => {
                counts[1] += 1;
                assert_eq!(next, previous);
            }
            "other" => {
                counts[2] += 1;
                assert_eq!(next, previous);
            }
            _ => panic!("unexpected route"),
        }
    }
    assert!(counts.into_iter().all(|count| count > 0));
    assert!(children_seen.into_iter().all(|seen| seen));
}

#[test]
fn given_initial_root_count_when_catalog_is_built_then_identity_and_checkpoint_capacity_are_preserved()
 {
    for count in [1, 4, storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES] {
        let count = storage_types::InitialStreamLineages::try_from(count).unwrap();
        let catalog = SystemStreamLineageCatalog::initial_roots(scope(), count).unwrap();
        assert_eq!(catalog.len(), count.get());
        assert_eq!(
            catalog[0].shard_id().as_str(),
            crate::DYNAMO_SYSTEM_SHARD_ID
        );
        assert_eq!(
            catalog[0].lineage_id().as_str(),
            crate::DYNAMO_SYSTEM_LINEAGE_ID
        );
        assert!(
            catalog
                .iter()
                .all(|root| root.parent_lineage_id().is_none() && !root.sealed())
        );
        assert_eq!(
            catalog,
            SystemStreamLineageCatalog::initial_roots(scope(), count).unwrap()
        );
    }
}

#[test]
fn given_prepared_split_when_persisted_then_pending_plan_has_no_numeric_end() {
    let request = request();
    let receipt = SystemStreamSplitReceipt::try_new(&request, 7).expect("receipt");
    let result = receipt.split.clone();
    let known = crate::SystemStreamSplitPlan::KnownEnd(receipt);
    let pending = crate::SystemStreamSplitPlan::EndAtCommit(result.clone());
    assert_eq!(known.split(), pending.split());
    let bytes = storage_types::storage_serde::to_bytes(&pending).expect("pending codec");
    let decoded: crate::SystemStreamSplitPlan =
        storage_types::storage_serde::from_bytes(&bytes).expect("pending decode");
    assert!(matches!(
        &decoded,
        crate::SystemStreamSplitPlan::EndAtCommit(_)
    ));
    assert_eq!(decoded.into_result(), result);
    let bytes = storage_types::storage_serde::to_bytes(&known).expect("known codec");
    let decoded: crate::SystemStreamSplitPlan =
        storage_types::storage_serde::from_bytes(&bytes).expect("known decode");
    let crate::SystemStreamSplitPlan::KnownEnd(receipt) = decoded else {
        panic!("numeric plan changed kind");
    };
    assert_eq!(receipt.sealed_after_offset, 7);
}
