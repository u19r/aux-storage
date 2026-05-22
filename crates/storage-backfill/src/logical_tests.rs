use storage_types::ItemStreamVersion;

use crate::*;

fn all_required_domains() -> Vec<LogicalBackfillDomain> {
    vec![
        LogicalBackfillDomain::TableMetadata,
        LogicalBackfillDomain::ItemRecords,
        LogicalBackfillDomain::Tombstones,
        LogicalBackfillDomain::DurableRevisions,
        LogicalBackfillDomain::StreamRecords,
        LogicalBackfillDomain::TtlRecords,
        LogicalBackfillDomain::GsiRecords,
        LogicalBackfillDomain::StorageControlPlane,
        LogicalBackfillDomain::BackgroundJobs,
        LogicalBackfillDomain::SyncControlPlane,
    ]
}

#[test]
fn caller_policies_select_distinct_activation_gates() {
    let sync = SyncLearnerCatchupPolicy;
    let multi_region = MultiRegionBootstrapPolicy;

    assert_eq!(sync.caller(), LogicalBackfillCaller::SyncLearnerCatchup);
    assert_eq!(
        sync.activation_gate(),
        LogicalBackfillActivationGate::RaftPromotionReadiness
    );
    assert_eq!(
        multi_region.caller(),
        LogicalBackfillCaller::MultiRegionBootstrap
    );
    assert_eq!(
        multi_region.activation_gate(),
        LogicalBackfillActivationGate::ReplicaActivationCursor
    );
    assert_eq!(
        sync.conflict_policy(),
        LogicalBackfillConflictPolicy::ItemStreamVersionOnly
    );
}

#[test]
fn manifest_records_caller_and_required_domains() {
    let manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest-1").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        all_required_domains(),
    );

    assert_eq!(manifest.caller, LogicalBackfillCaller::SyncLearnerCatchup);
    assert_eq!(
        manifest.activation_gate,
        LogicalBackfillActivationGate::RaftPromotionReadiness
    );
    assert_eq!(
        manifest.conflict_policy,
        LogicalBackfillConflictPolicy::ItemStreamVersionOnly
    );
    assert_eq!(
        manifest.tombstone_cleanup,
        LogicalBackfillTombstoneCleanup::AfterFinalCatchupDrain
    );
    assert!(
        manifest
            .domains
            .contains(&LogicalBackfillDomain::Tombstones)
    );
    assert!(
        manifest
            .domains
            .contains(&LogicalBackfillDomain::DurableRevisions)
    );
    assert!(
        manifest
            .domains
            .contains(&LogicalBackfillDomain::SyncControlPlane)
    );
}

#[test]
fn records_expose_item_stream_version_for_compare_and_set() {
    let present = LogicalBackfillRecord::PresentItem {
        table_name: "users".to_string(),
        key_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
        item_json: r#"{"pk":{"S":"u#1"},"name":{"S":"A"}}"#.to_string(),
        item_stream_version: ItemStreamVersion::new(7),
    };
    let tombstone = LogicalBackfillRecord::Tombstone(LogicalBackfillTombstone {
        table_name: "users".to_string(),
        key_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
        item_stream_version: ItemStreamVersion::new(8),
    });

    assert_eq!(
        present.item_stream_version(),
        Some(ItemStreamVersion::new(7))
    );
    assert_eq!(
        tombstone.item_stream_version(),
        Some(ItemStreamVersion::new(8))
    );
    assert_eq!(present.domain(), LogicalBackfillDomain::ItemRecords);
    assert_eq!(tombstone.domain(), LogicalBackfillDomain::Tombstones);
}

#[test]
fn identifiers_and_checksums_reject_empty_values() {
    assert_eq!(
        LogicalBackfillId::new("").unwrap_err(),
        LogicalBackfillError::EmptyId
    );
    assert_eq!(
        LogicalBackfillChunkId::new("").unwrap_err(),
        LogicalBackfillError::EmptyId
    );
    assert_eq!(
        LogicalBackfillChecksum::new("").unwrap_err(),
        LogicalBackfillError::EmptyChecksum
    );
}

#[test]
fn import_plan_applies_newer_present_items_and_tombstones() {
    let present = LogicalImportApplyCase::new(
        Some(ItemStreamVersion::new(7)),
        ItemStreamVersion::new(8),
        LogicalImportRecordKind::PresentItem,
    );
    let tombstone = LogicalImportApplyCase::new(
        Some(ItemStreamVersion::new(8)),
        ItemStreamVersion::new(9),
        LogicalImportRecordKind::Tombstone,
    );

    assert_eq!(
        plan_logical_import_apply(present),
        LogicalImportApplyDecision::ApplyPresentItem
    );
    assert_eq!(
        plan_logical_import_apply(tombstone),
        LogicalImportApplyDecision::ApplyTombstone
    );
}

#[test]
fn import_plan_rejects_stale_scan_images_after_newer_mutation() {
    let stale_scan = LogicalImportApplyCase::new(
        Some(ItemStreamVersion::new(9)),
        ItemStreamVersion::new(7),
        LogicalImportRecordKind::PresentItem,
    );

    assert_eq!(
        plan_logical_import_apply(stale_scan),
        LogicalImportApplyDecision::IgnoreStale
    );
}

#[test]
fn import_plan_tombstone_blocks_older_scan_image() {
    let older_scan_image = LogicalImportApplyCase::new(
        Some(ItemStreamVersion::new(10)),
        ItemStreamVersion::new(9),
        LogicalImportRecordKind::PresentItem,
    );

    assert_eq!(
        plan_logical_import_apply(older_scan_image),
        LogicalImportApplyDecision::IgnoreStale
    );
}

#[test]
fn import_plan_treats_same_version_replay_as_duplicate() {
    let replay = LogicalImportApplyCase::new(
        Some(ItemStreamVersion::new(9)),
        ItemStreamVersion::new(9),
        LogicalImportRecordKind::Tombstone,
    );

    assert_eq!(
        plan_logical_import_apply(replay),
        LogicalImportApplyDecision::IgnoreDuplicate
    );
}

#[test]
fn chunk_manifest_validation_rejects_undeclared_domain() {
    let manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest-1").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![LogicalBackfillDomain::ItemRecords],
    );
    let chunk = LogicalBackfillChunk {
        summary: LogicalBackfillChunkSummary {
            id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
            domain: LogicalBackfillDomain::Tombstones,
            record_count: 0,
            checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
        },
        records: Vec::new(),
    };

    assert_eq!(
        validate_logical_chunk_for_manifest(&manifest, &chunk).unwrap_err(),
        LogicalBackfillError::DomainNotInManifest {
            domain: LogicalBackfillDomain::Tombstones
        }
    );
}

#[test]
fn chunk_manifest_validation_rejects_record_count_mismatch() {
    let manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("manifest-1").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![LogicalBackfillDomain::ItemRecords],
    );
    let chunk = LogicalBackfillChunk {
        summary: LogicalBackfillChunkSummary {
            id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
            domain: LogicalBackfillDomain::ItemRecords,
            record_count: 2,
            checksum: LogicalBackfillChecksum::new("unchecked").unwrap(),
        },
        records: Vec::new(),
    };

    assert_eq!(
        validate_logical_chunk_for_manifest(&manifest, &chunk).unwrap_err(),
        LogicalBackfillError::RecordCountMismatch {
            expected: 2,
            actual: 0
        }
    );
}

#[test]
fn export_page_carries_domain_cursor_checksum_and_stream_versions() {
    let page = LogicalExportPage {
        domain: LogicalBackfillDomain::StreamRecords,
        records: vec![LogicalBackfillRecord::StreamRecord {
            stream_name: "table#users".to_string(),
            record_id: "cursor#1".to_string(),
            payload_json: r#"{"eventName":"INSERT"}"#.to_string(),
            item_stream_version: Some(ItemStreamVersion::new(11)),
        }],
        next_cursor: Some("cursor#2".to_string()),
        checksum: LogicalBackfillChecksum::new("sha256:abc").unwrap(),
    };

    assert_eq!(page.domain, LogicalBackfillDomain::StreamRecords);
    assert_eq!(page.next_cursor.as_deref(), Some("cursor#2"));
    assert_eq!(
        page.records[0].item_stream_version(),
        Some(ItemStreamVersion::new(11))
    );
}
