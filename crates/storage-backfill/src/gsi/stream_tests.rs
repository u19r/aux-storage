use std::num::NonZeroU64;

use storage_types::{
    KeyAttributes, StreamItemId, SystemStreamPosition, SystemStreamScope,
    SystemStreamSourceTableName,
};
use stream_provider::{
    SystemStreamHistory, SystemStreamHistoryBounds, SystemStreamHistoryReadPage,
    SystemStreamHistoryRecord, SystemStreamLineage, SystemStreamLineageCatalog,
    SystemStreamStoredRecord,
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
    let row = SystemStreamStoredRecord {
        source_table_name: SystemStreamSourceTableName::try_new("test").unwrap(),
        keys: KeyAttributes::default(),
        old_image: None,
        new_image: None,
        position: position("b", 10),
    };
    let pages = vec![
        SystemStreamHistoryReadPage {
            retired_through: None,
            records: vec![],
            observations: vec![],
            pointers: vec![],
            more: false,
        },
        SystemStreamHistoryReadPage {
            retired_through: None,
            records: vec![row],
            observations: vec![SystemStreamHistoryRecord {
                position: position("b", 10),
                expected_pointer: StreamItemId::default(),
                actual_pointer: Some(StreamItemId::default()),
            }],
            pointers: vec![None],
            more: false,
        },
    ];
    let mut assembly = GsiStreamPageAssembly::new(&plan);
    assert!(!assembly.push_pages(plan.as_ref(), pages).unwrap());
}
