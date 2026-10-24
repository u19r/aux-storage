use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::json;
use storage_types::{
    AttributeValue, DurationSeconds, StreamItemId, StreamName, StreamRecord,
    SystemStreamCheckpointToken, SystemStreamContractError, SystemStreamPosition,
    SystemStreamScope, SystemStreamSourceTableName, TimestampMillis, UserStreamName,
};

use crate::{
    CursorName, CursorPage, CursorPosition, Stream, StreamCursor, StreamPage,
    StreamPartitioningMode, StreamProvider, StreamResult, SystemStreamIssuedCheckpoint,
    SystemStreamLineage, SystemStreamLineageCatalog, SystemStreamPage, SystemStreamPageRequest,
    SystemStreamSplitChild, SystemStreamSplitDisposition, SystemStreamSplitOperationId,
    SystemStreamSplitReceipt, SystemStreamSplitRequest, SystemStreamSplitResult,
    SystemStreamStoredRecord, SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest,
};

struct NoSystemStreamProvider;

#[async_trait]
impl StreamProvider for NoSystemStreamProvider {
    async fn initialize_stream(&self) -> StreamResult<()> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn create_stream(
        &self,
        _stream_name: UserStreamName,
        _ttl_seconds: Option<DurationSeconds>,
        _partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn delete_stream(&self, _stream_name: UserStreamName) -> StreamResult<()> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn get_stream(&self, _stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn append_item(
        &self,
        _stream_name: StreamName,
        _item_data: &[u8],
        _partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn read_forward(
        &self,
        _stream_name: StreamName,
        _exclusive_start_key: Option<StreamItemId>,
        _limit: u32,
    ) -> StreamResult<StreamPage> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn read_backward(
        &self,
        _stream_name: StreamName,
        _exclusive_start_key: Option<StreamItemId>,
        _limit: u32,
    ) -> StreamResult<StreamPage> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn create_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _position: CursorPosition,
    ) -> StreamResult<()> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn delete_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
    ) -> StreamResult<()> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn read_from_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _limit: u32,
    ) -> StreamResult<CursorPage> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn advance_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        unimplemented!("not needed for SystemStream default tests")
    }

    async fn get_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        unimplemented!("not needed for SystemStream default tests")
    }
}

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
fn stream_provider_system_stream_defaults_fail_closed_and_are_object_safe() {
    fn accepts_dyn_provider(_: &dyn StreamProvider) {}

    futures::executor::block_on(async {
        let provider = NoSystemStreamProvider;
        accepts_dyn_provider(&provider);

        assert!(
            provider
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope(), None, 1).expect("page request")
                )
                .await
                .is_err()
        );
        assert!(
            provider
                .system_stream_lineage_catalog(scope())
                .await
                .is_err()
        );
        assert!(provider.system_stream_scope().await.is_err());
        assert!(provider.split_system_stream(request()).await.is_err());
    });
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

#[test]
fn stored_system_record_requires_and_emits_logical_source_table_name() {
    let source_table_name =
        SystemStreamSourceTableName::try_new("orders").expect("logical source table");
    let stored = SystemStreamStoredRecord {
        source_table_name,
        keys: HashMap::new().into(),
        old_image: None,
        new_image: Some(
            storage_types::WireItem::try_from(&HashMap::from([(
                "id".to_string(),
                AttributeValue::S("1".to_string()),
            )]))
            .unwrap(),
        ),
        position: SystemStreamPosition::try_new("shard-a", "lineage-a", "1").expect("position"),
    };

    let encoded = serde_json::to_value(&stored).expect("stored record JSON");
    assert_eq!(encoded["source_table_name"], json!("orders"));
    let decoded = serde_json::from_value::<SystemStreamStoredRecord>(encoded.clone())
        .expect("stored record JSON should round-trip");
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(&stored).unwrap()
    );

    let mut changed = encoded;
    changed["source_table_name"] = json!("customers");
    let changed_record = serde_json::from_value::<SystemStreamStoredRecord>(changed)
        .expect("changed logical source table should remain syntactically valid");
    assert_ne!(
        serde_json::to_value(changed_record).unwrap(),
        serde_json::to_value(&stored).unwrap()
    );

    let mut physical = serde_json::to_value(&stored).expect("stored record JSON");
    physical["source_table_name"] = json!("arn:aws:dynamodb:table/orders");
    serde_json::from_value::<SystemStreamStoredRecord>(physical)
        .expect_err("physical source table identity must be rejected");

    let record = stored.into_stream_record().unwrap();
    assert_eq!(
        record.source_table_name.as_ref().map(ToString::to_string),
        Some("orders".to_string())
    );
    assert_eq!(
        record
            .system_position
            .as_ref()
            .expect("position")
            .offset()
            .as_str(),
        "1"
    );
}

fn issued_checkpoint() -> SystemStreamIssuedCheckpoint {
    let checkpoint = storage_types::SystemStreamCheckpoint::try_new(
        scope(),
        vec![SystemStreamPosition::try_new("shard-a", "lineage-a", "0").expect("position")],
    )
    .expect("checkpoint");
    let token = SystemStreamCheckpointToken::try_from(format!("v1.{}", "42".repeat(16)))
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
        old_image: None,
        new_image: None,
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
    let stored = SystemStreamStoredRecord {
        source_table_name: SystemStreamSourceTableName::try_new("orders").expect("source table"),
        keys: HashMap::new().into(),
        old_image: None,
        new_image: None,
        position: SystemStreamPosition::try_new("shard-a", "lineage-a", "1").expect("position"),
    };
    let page = SystemStreamPage::new(
        scope(),
        vec![stored.into_stream_record().unwrap()],
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
    let record = SystemStreamStoredRecord {
        source_table_name: SystemStreamSourceTableName::try_new("orders").expect("source table"),
        keys: HashMap::new().into(),
        old_image: None,
        new_image: None,
        position: SystemStreamPosition::try_new("shard-a", "lineage-a", "1").expect("position"),
    }
    .into_stream_record()
    .unwrap();
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
