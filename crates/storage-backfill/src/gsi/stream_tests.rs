use std::num::NonZeroU64;

use storage_types::{SystemStreamPosition, SystemStreamScope};
use stream_provider::{
    SystemStreamHistory, SystemStreamHistoryBounds, SystemStreamHistoryReadPage,
    SystemStreamHistoryRecord, SystemStreamLineage, SystemStreamLineageCatalog,
};

use super::GsiStreamPageAssembly;

#[test]
fn given_sparse_lineages_when_first_is_empty_then_later_records_are_consumed() {
    let lineages = ["a", "b"].map(|id| SystemStreamLineage::try_new(id, id, None, false).unwrap());
    let bounds = lineages
        .iter()
        .map(|lineage| SystemStreamHistoryBounds {
            lineage_id: lineage.lineage_id().clone(),
            initial_sequence: NonZeroU64::new(1).unwrap(),
            next_sequence: NonZeroU64::new(101).unwrap(),
            sealed_after: None,
        })
        .collect();
    let position =
        |id: &str, offset| SystemStreamPosition::try_new(id, id, format!("{offset:020}")).unwrap();
    let history = SystemStreamHistory::try_new(
        SystemStreamLineageCatalog::try_new(
            SystemStreamScope::try_new("sparse", 1).unwrap(),
            lineages.to_vec(),
        )
        .unwrap(),
        bounds,
    )
    .unwrap()
    .with_sparse_numeric_ordering(vec![position("a", 100), position("b", 100)]);
    let plan = history
        .plan_page(
            &history.initial_checkpoint().unwrap(),
            &history.capture_high().unwrap(),
            2,
        )
        .unwrap();
    assert_eq!(plan.as_ref().len(), 2);
    let metadata = storage_types::ItemHistoryMetadata::try_new(
        storage_types::ItemHistoryAddress {
            history_id: 1_u32.into(),
            stream_name: storage_types::StreamName::from("test/stream-item/1"),
        },
        storage_types::ItemStreamVersion::new(1),
        None,
        storage_types::ItemHistoryPublication::Event {
            kind: storage_types::ItemHistoryKind::Image,
            source_scope: SystemStreamScope::try_new("sparse", 1).unwrap(),
            table_position: position("b", 10),
            created_at: storage_types::TimestampMillis::from_timestamp(0),
        },
    )
    .unwrap();
    let row = storage_types::SystemHistoryRecord::try_from(
        storage_types::ItemHistoryPointer::try_new(metadata, 100, None).unwrap(),
    )
    .unwrap();
    let pages = vec![
        SystemStreamHistoryReadPage {
            retired_through: None,
            records: vec![],
            observations: vec![],
            more: false,
        },
        SystemStreamHistoryReadPage {
            retired_through: None,
            records: vec![row],
            observations: vec![SystemStreamHistoryRecord {
                position: position("b", 10),
            }],
            more: false,
        },
    ];
    let mut assembly = GsiStreamPageAssembly::new(&plan, 2_u32.into());
    assert!(!assembly.push_pages(plan.as_ref(), pages).unwrap());
}
