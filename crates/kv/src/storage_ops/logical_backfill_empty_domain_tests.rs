use storage_backfill::{LogicalBackfillDomain, LogicalBackfillId, LogicalExportRequest};
use storage_provider::StorageProvider;

use crate::{SortedKvDbStorageProvider, kv_support_tests::create_test_store};

#[tokio::test]
async fn rocksdb_logical_empty_domains_export_without_unsupported_errors() {
    let provider = initialized_provider().await;

    for domain in [
        LogicalBackfillDomain::Tombstones,
        LogicalBackfillDomain::StorageControlPlane,
        LogicalBackfillDomain::BackgroundJobs,
        LogicalBackfillDomain::SyncControlPlane,
    ] {
        let page = provider
            .export_logical_backfill_page(export_request(domain))
            .await
            .expect("empty domain export should not be unsupported");
        assert_eq!(page.domain, domain);
        assert!(page.records.is_empty());
    }
}

async fn initialized_provider() -> SortedKvDbStorageProvider<crate::kv_support_tests::TestStore> {
    let provider = SortedKvDbStorageProvider::new(create_test_store());
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
}

fn export_request(domain: LogicalBackfillDomain) -> LogicalExportRequest {
    LogicalExportRequest {
        manifest_id: LogicalBackfillId::new("manifest").expect("manifest id"),
        domain,
        table_name: None,
        cursor: None,
        limit: 50,
    }
}
