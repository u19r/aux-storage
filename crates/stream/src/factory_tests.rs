use std::time::Instant;

use alloc_counter::AllocationGuard;
use storage_provider::{StorageBackend, StorageConfig};

use super::factory::create_stream_provider;

#[tokio::test]
#[ignore = "isolated ASTRA-931 allocation measurement"]
async fn given_postgres_factory_when_looping_then_report_allocations() {
    let guard = AllocationGuard::start(
        module_path!(),
        "stream_factory_allocations",
        file!(),
        line!(),
        Some("postgres-invalid-dsn"),
    );
    let started = Instant::now();
    for _ in 0..64 {
        let result = create_stream_provider(StorageConfig {
            backend_type: StorageBackend::Postgres,
            connection_string: Some("postgresql://[".to_string()),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        })
        .await;
        assert!(result.is_err());
    }
    let _ = alloc_counter::emit_report(&guard.finish());
    println!(
        "FACTORY_WORKLOAD label=postgres-invalid-dsn operations=64 elapsed_ns={}",
        started.elapsed().as_nanos()
    );
}
