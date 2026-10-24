use async_trait::async_trait;
use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_types::{
    IndexName, IndexStatus, StorageResult, SystemStreamCheckpoint, SystemStreamPosition,
    SystemStreamScope, TableName, maintenance::TargetIncarnation,
};

use crate::{
    GsiIndexReadiness, GsiReadinessRecord, GsiReadinessSnapshot,
    maintenance::GsiLifecycleRepository,
};

fn target(index_name: &IndexName, incarnation: u8) -> GsiBuildTarget {
    GsiBuildTarget {
        table_name: TableName::new("orders"),
        index_name: index_name.clone(),
        incarnation: TargetIncarnation::from([incarnation; 16]),
    }
}

fn checkpoint() -> SystemStreamCheckpoint {
    SystemStreamCheckpoint::try_new(
        SystemStreamScope::try_new("readiness", 1).unwrap(),
        vec![SystemStreamPosition::try_new("root", "root", "00000000000000000000").unwrap()],
    )
    .unwrap()
}

#[test]
fn readiness_snapshot_owns_query_admission() {
    let index_name = IndexName::new("by-status");
    assert!(
        GsiReadinessSnapshot::from(GsiBuild::<String>::new_active(
            target(&index_name, 1),
            checkpoint()
        ))
        .require_queryable(&index_name)
        .is_ok()
    );
    assert!(
        GsiReadinessSnapshot::from(GsiBuild::<String>::new(
            target(&index_name, 1),
            checkpoint(),
        ))
        .require_queryable(&index_name)
        .is_err()
    );
    assert!(
        GsiReadinessSnapshot::try_from(Vec::<GsiBuild<String>>::new())
            .expect("empty readiness snapshot is valid")
            .require_queryable(&index_name)
            .is_err()
    );
}

#[test]
fn given_duplicate_gsi_incarnations_when_readiness_snapshot_is_created_then_it_is_rejected() {
    let first = IndexName::new("by-status");
    let second = IndexName::new("by-region");
    assert!(
        GsiReadinessSnapshot::try_from(vec![
            GsiBuild::<String>::new_active(target(&first, 1), checkpoint()),
            GsiBuild::<String>::new_active(target(&second, 1), checkpoint()),
        ])
        .is_err()
    );
}

struct ReadinessRepository;

#[async_trait]
impl GsiLifecycleRepository for ReadinessRepository {
    async fn read_gsi_readiness_records(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<GsiReadinessRecord>> {
        Ok(index_names
            .iter()
            .map(|index_name| GsiReadinessRecord {
                table_name: table_name.clone(),
                index_name: index_name.clone(),
                readiness: GsiIndexReadiness {
                    index_status: IndexStatus::Active,
                    backfilling: false,
                },
            })
            .collect())
    }
}

#[tokio::test]
async fn lifecycle_repository_uses_shared_readiness_selection() {
    let index_name = IndexName::new("by-status");
    let readiness = ReadinessRepository
        .read_gsi_index_readiness(&TableName::new("orders"), std::slice::from_ref(&index_name))
        .await
        .expect("shared readiness selection");

    assert_eq!(readiness.len(), 1);
    assert_eq!(readiness[0].0, index_name);
    assert!(matches!(readiness[0].1.index_status, IndexStatus::Active));
    assert!(!readiness[0].1.backfilling);
}
