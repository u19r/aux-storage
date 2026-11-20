//! Restore a verified archive only into an operator-isolated empty namespace.

use std::{
    collections::HashSet,
    fs,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::Path,
    time::{Duration, Instant},
};

use kv_backend::{
    RecoverySource,
    foundationdb::{FoundationDbBackend, FoundationDbOptions},
};
use sha2::{Digest, Sha256};
use storage::{DatabaseManager, DatabaseManagerRuntimeOptions};
use storage_types::{
    BillingMode, CreateGlobalSecondaryIndex, CreateTableRequest, KeyAttributes, PutItemRequest,
    StoredWireItem, TableName, TimeToLiveStatus,
};

use super::{ArchivePage, MAX_MANIFEST_BYTES, Manifest, Result, digest, recovery_error};

pub(super) async fn run(
    archive: &Path,
    expected_manifest: &str,
    config_path: &Path,
    destination_name: &str,
    isolation_evidence: &str,
    result: Option<&Path>,
    execute: bool,
) -> Result<()> {
    if isolation_evidence.trim().is_empty() {
        return Err(recovery_error(
            "dedicated inspection namespace evidence is required",
        ));
    }
    let manifest_path = archive.join("manifest.json");
    let manifest_metadata = fs::symlink_metadata(&manifest_path)
        .map_err(|_| recovery_error("archive manifest could not be inspected"))?;
    if !manifest_metadata.file_type().is_file()
        || manifest_metadata.len() > MAX_MANIFEST_BYTES as u64
    {
        return Err(recovery_error("archive manifest type or size is invalid"));
    }
    let bytes = fs::read(manifest_path)
        .map_err(|_| recovery_error("archive manifest could not be read"))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(recovery_error("archive manifest exceeds 4 MiB"));
    }
    if digest(&bytes) != expected_manifest {
        return Err(recovery_error("manifest checksum differs"));
    }
    let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
        recovery_error(format!(
            "archive manifest JSON decoding failed at line {} column {}",
            error.line(),
            error.column()
        ))
    })?;
    validate_archive(archive, &manifest, destination_name)?;
    let table_name = TableName::new(destination_name);
    let request = create_request(&manifest, table_name.clone())
        .map_err(|_| recovery_error("destination table schema validation failed"))?;
    let config = config::load(config_path).map_err(|_| {
        recovery_error("destination configuration could not be loaded or is invalid")
    })?;
    if config.root.features.storage_connections.is_some()
        || !config.root.features.regional_connections.is_empty()
    {
        return Err(recovery_error(
            "inspection requires a single unpaired FoundationDB connection",
        ));
    }
    let backends = &config.root.features.backends;
    if backends.sqlite.is_some()
        || backends.postgres.is_some()
        || backends.turso.is_some()
        || backends.rocksdb.is_some()
        || backends.remote.is_some()
    {
        return Err(recovery_error(
            "inspection requires only a FoundationDB backend",
        ));
    }
    let settings = backends
        .foundationdb
        .as_ref()
        .ok_or_else(|| recovery_error("FoundationDB backend is required"))?;
    if settings.cache_read_version_ms != 0 {
        return Err(recovery_error("recovery requires cache_read_version_ms=0"));
    }
    if settings
        .subspace_prefix
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err(recovery_error(
            "inspection requires an explicit dedicated subspace_prefix",
        ));
    }
    let probe = open_backend(settings).await?;
    if !probe
        .is_empty_subspace()
        .await
        .map_err(|_| recovery_error("inspection namespace emptiness check failed"))?
    {
        return Err(recovery_error("inspection namespace is not empty"));
    }
    if !execute {
        println!("archive and empty inspection namespace validated; no destination initialized");
        return Ok(());
    }
    let result = result.ok_or_else(|| recovery_error("--result is required with --execute"))?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(result)
        .map_err(|_| recovery_error("result directory could not be created"))?;
    let destination_config_sha256 = digest(
        &fs::read(config_path)
            .map_err(|_| recovery_error("destination configuration could not be read"))?,
    );
    let mut progress = serde_json::json!({
        "schema_version": 1,
        "manifest_sha256": expected_manifest,
        "destination_config_sha256": destination_config_sha256,
        "destination_table": destination_name,
        "isolation_evidence": isolation_evidence,
        "source_ttl": manifest.table.ttl,
        "phase": "bootstrap",
        "completed_pages": 0,
    });
    write_progress(result, &progress)?;
    drop(probe);
    let storage_config = storage::startup::storage_config_from_backends(backends)
        .map_err(|_| recovery_error("destination configuration is unsupported"))?;
    let manager = DatabaseManager::new_with_config_and_runtime_options(
        storage_config,
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(true)
            .build(),
    )
    .await
    .map_err(|_| recovery_error("inspection namespace bootstrap failed"))?;
    let read_backend = open_backend(settings).await?;
    let reader = RecoverySource::open(read_backend, table_name.clone())
        .await
        .map_err(|_| recovery_error("inspection namespace read-only open failed"))?;
    if uuid::Uuid::from_bytes(reader.database_id()).to_string() == manifest.source.database_id {
        return Err(recovery_error(
            "inspection namespace has the source database identity",
        ));
    }
    progress["destination_database_id"] = uuid::Uuid::from_bytes(reader.database_id())
        .to_string()
        .into();
    progress["phase"] = "creating_table".into();
    write_progress(result, &progress)?;
    manager
        .create_table(&request)
        .await
        .map_err(|_| recovery_error("destination table creation failed"))?;
    progress["phase"] = "copying".into();
    write_progress(result, &progress)?;
    for (page_number, page) in manifest.pages.iter().enumerate() {
        for (row_number, value) in read_page(archive, page, page_number)?
            .into_iter()
            .enumerate()
        {
            let (_, stored) = decode_row(&manifest, value)
                .map_err(|_| row_error(page_number, row_number, "decoding"))?;
            let mut put = PutItemRequest::new(table_name.clone(), stored.item);
            put.indexers = Some(stored.indexers);
            manager
                .put_item_request(put)
                .await
                .map_err(|_| row_error(page_number, row_number, "destination write"))?;
        }
        progress["completed_pages"] = (page_number as u64 + 1).into();
        write_progress(result, &progress)?;
    }
    progress["phase"] = "verifying".into();
    write_progress(result, &progress)?;
    verify_rows(archive, &manifest, &reader).await?;
    verify_schema(&manager, &request)
        .await
        .map_err(|_| recovery_error("destination schema verification failed"))?;
    verify_indexes(archive, &manifest, &manager, &request).await?;
    if !manager
        .describe_time_to_live(&table_name)
        .await
        .map_err(|_| recovery_error("destination TTL observation failed"))?
        .time_to_live_description
        .is_some_and(|value| value.time_to_live_status == TimeToLiveStatus::Disabled)
    {
        return Err(recovery_error("destination TTL is not disabled"));
    }
    manager
        .shutdown_background_services()
        .await
        .map_err(|_| recovery_error("destination background services could not be stopped"))?;
    progress["phase"] = "complete".into();
    write_progress(result, &progress)?;
    println!("inspection table restored; application routing and cutover were not changed");
    Ok(())
}

fn write_progress(directory: &Path, progress: &serde_json::Value) -> Result<()> {
    let temporary = directory.join("progress.pending");
    let current = directory.join("progress.json");
    let bytes = serde_json::to_vec_pretty(progress)
        .map_err(|_| recovery_error("result record encoding failed"))?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| recovery_error("result record could not be opened"))?;
    std::io::Write::write_all(&mut file, &bytes)
        .map_err(|_| recovery_error("result record could not be written"))?;
    file.sync_all()
        .map_err(|_| recovery_error("result record could not be synchronized"))?;
    fs::rename(temporary, current)
        .map_err(|_| recovery_error("result record could not be published"))?;
    fs::File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|_| recovery_error("result directory could not be synchronized"))?;
    Ok(())
}

async fn open_backend(settings: &config::FoundationdbBackendConfig) -> Result<FoundationDbBackend> {
    FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: settings.cluster_file.as_deref(),
        subspace_prefix: settings.subspace_prefix.as_deref().map(str::as_bytes),
        tenant_name: settings.tenant_name.as_deref().map(str::as_bytes),
        cache_read_version_ms: 0,
        reply_byte_limit: settings.reply_byte_limit,
        transaction_byte_limit: settings.transaction_byte_limit,
        report_conflicting_keys: false,
        attempt_deadlines: Default::default(),
    })
    .await
    .map_err(|_| recovery_error("destination FoundationDB connection failed"))
}

fn validate_archive(archive: &Path, manifest: &Manifest, destination: &str) -> Result<()> {
    if manifest.schema_version != 1
        || manifest.kind != "aux_storage_table_snapshot"
        || !manifest.complete
        || manifest.source.table_name == destination
        || destination.trim().is_empty()
        || manifest.source.snapshot_id.trim().is_empty()
        || manifest.source.stability.evidence_id.trim().is_empty()
        || !matches!(
            manifest.source.stability.kind.as_str(),
            "immutable_clone" | "quiesced_live"
        )
        || manifest.table.stored_table_info.table_name.as_ref() != manifest.source.table_name
        || manifest.table.key_schema != manifest.table.stored_table_info.key_schema
        || manifest.table.stored_table_info.table_status != storage_types::TableStatus::Active
        || !matches!(
            &manifest.table.ttl.time_to_live_status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Disabled
        )
        || (manifest.table.ttl.time_to_live_status == TimeToLiveStatus::Enabled
            && manifest
                .table
                .ttl
                .attribute_name
                .as_deref()
                .is_none_or(str::is_empty))
    {
        return Err(recovery_error(
            "archive metadata or destination is unsupported",
        ));
    }
    let mut previous_key: Option<Vec<u8>> = None;
    let mut count = 0u64;
    for (number, page) in manifest.pages.iter().enumerate() {
        if page.file != format!("pages/{number:08}.jsonl") {
            return Err(recovery_error("archive page path is invalid"));
        }
        for (row_number, value) in read_page(archive, page, number)?.into_iter().enumerate() {
            let (key, stored) = decode_row(manifest, value)
                .map_err(|_| row_error(number, row_number, "decoding"))?;
            storage_types::IndexerDeclaration::try_new(
                stored.indexers.clone(),
                manifest.table.stored_table_info.max_indexers,
            )
            .map_err(|_| row_error(number, row_number, "indexer validation"))?;
            stored
                .item
                .clone()
                .normalized_for_write()
                .map_err(|_| row_error(number, row_number, "item validation"))?;
            for index in manifest
                .table
                .stored_table_info
                .global_secondary_indexes
                .iter()
                .flatten()
                .filter(|index| !index.index_name.is_ttl_index())
            {
                projected_index_item(manifest, index, &stored)
                    .map_err(|_| row_error(number, row_number, "index projection validation"))?;
            }
            let ordered = storage_types::ItemKey::from_key_schema(
                manifest.table.stored_table_info.table_name.clone(),
                &manifest.table.key_schema,
                &key,
            )
            .and_then(|key| key.sorted_storage_suffix())
            .map_err(|_| row_error(number, row_number, "primary key validation"))?;
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &ordered)
            {
                return Err(recovery_error(
                    "archive keys are duplicated or out of native order",
                ));
            }
            previous_key = Some(ordered);
            count = count
                .checked_add(1)
                .ok_or_else(|| recovery_error("archive row count overflow"))?;
        }
    }
    if count != manifest.row_count {
        return Err(recovery_error("archive total count differs"));
    }
    Ok(())
}

fn read_page(
    archive: &Path,
    page: &ArchivePage,
    page_number: usize,
) -> Result<Vec<serde_json::Value>> {
    let path = archive.join(&page.file);
    let metadata =
        fs::symlink_metadata(&path).map_err(|_| page_error(page_number, "inspection"))?;
    if !metadata.file_type().is_file()
        || metadata.len() != page.bytes
        || page.bytes > 64 * 1024 * 1024
    {
        return Err(page_error(page_number, "type or size validation"));
    }
    let bytes = fs::read(path).map_err(|_| page_error(page_number, "read"))?;
    if digest(&bytes) != page.sha256 || (!bytes.is_empty() && bytes.last() != Some(&b'\n')) {
        return Err(page_error(
            page_number,
            "checksum or line ending validation",
        ));
    }
    let rows = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
        .map(|(row_number, line)| {
            serde_json::from_slice(line).map_err(|error| {
                recovery_error(format!(
                    "archive page {} row {} JSON decoding failed at column {}",
                    page_number + 1,
                    row_number + 1,
                    error.column()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if rows.len() as u64 != page.rows || rows.is_empty() {
        return Err(page_error(page_number, "row count validation"));
    }
    Ok(rows)
}

fn page_error(page: usize, stage: &str) -> Box<dyn std::error::Error + Send + Sync> {
    recovery_error(format!("archive page {} {stage} failed", page + 1))
}

fn row_error(page: usize, row: usize, stage: &str) -> Box<dyn std::error::Error + Send + Sync> {
    recovery_error(format!(
        "archive page {} row {} {stage} failed",
        page + 1,
        row + 1
    ))
}

fn decode_row(
    manifest: &Manifest,
    mut value: serde_json::Value,
) -> Result<(KeyAttributes, StoredWireItem)> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| recovery_error("archive row is not an object"))?;
    let keys: KeyAttributes = serde_json::from_value(
        object
            .remove("keys")
            .ok_or_else(|| recovery_error("archive row has no keys"))?,
    )?;
    let stored: StoredWireItem = serde_json::from_value(value)?;
    let derived = manifest
        .table
        .stored_table_info
        .table_key_from_wire(&stored.item)?;
    if serde_json::to_value(&keys)? != serde_json::to_value(derived)? {
        return Err(recovery_error("archive row keys differ from its item"));
    }
    Ok((keys, stored))
}

fn create_request(manifest: &Manifest, name: TableName) -> Result<CreateTableRequest> {
    let table = &manifest.table.stored_table_info;
    let mut request = CreateTableRequest::new(
        name,
        table.attribute_definitions.clone(),
        table.key_schema.clone(),
        BillingMode::PayPerRequest,
    );
    request.max_indexers = table.max_indexers;
    request.global_secondary_indexes = table.global_secondary_indexes.as_ref().map(|indexes| {
        indexes
            .iter()
            .filter(|index| !index.index_name.is_ttl_index())
            .map(|index| CreateGlobalSecondaryIndex {
                index_name: index.index_name.clone(),
                key_schema: index.key_schema.clone(),
                projection: index.projection.clone(),
                provisioned_throughput: None,
            })
            .collect()
    });
    if request
        .global_secondary_indexes
        .as_ref()
        .is_some_and(Vec::is_empty)
    {
        request.global_secondary_indexes = None;
    }
    request.stream_specification = table.stream_specification.clone();
    request.deletion_protection_enabled = Some(table.deletion_protection_enabled);
    request.aux_stream_duration_hours = Some(table.table_stream_duration);
    request.validate_for_storage()?;
    Ok(request)
}

async fn verify_rows(
    archive: &Path,
    manifest: &Manifest,
    source: &RecoverySource<FoundationDbBackend>,
) -> Result<()> {
    let mut cursor = None;
    let mut expected_page = 0usize;
    let mut expected_row = 0usize;
    let mut expected_rows = Vec::new().into_iter();
    loop {
        let page = source
            .page(cursor, 256)
            .await
            .map_err(|_| recovery_error("destination verification page read failed"))?;
        for row in page.rows {
            let expected = loop {
                if let Some(value) = expected_rows.next() {
                    break value;
                }
                let descriptor = manifest
                    .pages
                    .get(expected_page)
                    .ok_or_else(|| recovery_error("destination has extra rows"))?;
                expected_page += 1;
                expected_rows = read_page(archive, descriptor, expected_page - 1)?.into_iter();
                expected_row = 0;
            };
            let (keys, stored) = decode_row(manifest, expected)
                .map_err(|_| row_error(expected_page - 1, expected_row, "decoding"))?;
            expected_row += 1;
            if serde_json::to_value(keys)? != serde_json::to_value(row.keys)?
                || serde_json::to_value(stored)? != serde_json::to_value(row.item)?
            {
                return Err(recovery_error("destination row differs from archive"));
            }
        }
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    if expected_rows.next().is_some() || expected_page != manifest.pages.len() {
        return Err(recovery_error("destination is missing archive rows"));
    }
    Ok(())
}

async fn verify_schema(manager: &DatabaseManager, request: &CreateTableRequest) -> Result<()> {
    let mut observed = manager.get_table_info(&request.table_name).await?;
    let expected = request.stored_table_info_at(observed.created_at)?;
    observed.item_count = 0;
    observed.table_size_bytes = 0;
    if serde_json::to_value(&observed)? != serde_json::to_value(expected)? {
        return Err(recovery_error(
            "destination schema differs from the create request",
        ));
    }
    Ok(())
}

async fn verify_indexes(
    archive: &Path,
    manifest: &Manifest,
    manager: &DatabaseManager,
    request: &CreateTableRequest,
) -> Result<()> {
    let names = request
        .global_secondary_indexes
        .as_ref()
        .map(|indexes| {
            indexes
                .iter()
                .map(|index| index.index_name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let deadline = Instant::now() + Duration::from_secs(900);
    loop {
        let indexes = manager
            .get_gsi_index_readiness(&request.table_name, &names)
            .await
            .map_err(|_| recovery_error("destination index readiness observation failed"))?;
        if indexes.len() == names.len()
            && indexes.iter().all(|(_, status)| {
                matches!(&status.index_status, storage_types::IndexStatus::Active)
                    && !status.backfilling
            })
        {
            break;
        }
        if Instant::now() >= deadline {
            return Err(recovery_error("destination indexes did not become active"));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    for index in manifest
        .table
        .stored_table_info
        .global_secondary_indexes
        .iter()
        .flatten()
        .filter(|index| !index.index_name.is_ttl_index())
    {
        let mut expected = ItemDigest::default();
        for (page_number, page) in manifest.pages.iter().enumerate() {
            for (row_number, value) in read_page(archive, page, page_number)?
                .into_iter()
                .enumerate()
            {
                let (_, stored) = decode_row(manifest, value)
                    .map_err(|_| row_error(page_number, row_number, "decoding"))?;
                if let Some(item) = projected_index_item(manifest, index, &stored)
                    .map_err(|_| row_error(page_number, row_number, "index projection"))?
                {
                    let key = manifest
                        .table
                        .stored_table_info
                        .table_key_from_wire(&stored.item)
                        .map_err(|_| row_error(page_number, row_number, "primary key decoding"))?;
                    expected
                        .add(&key, &item)
                        .map_err(|_| row_error(page_number, row_number, "index comparison"))?;
                }
            }
        }
        loop {
            let mut actual = ItemDigest::default();
            let mut cursor = None;
            loop {
                let mut scan = storage_types::ScanRequest::new(request.table_name.clone());
                scan.index_name = Some(index.index_name.clone());
                scan.exclusive_start_key = cursor.map(storage_types::ExclusiveStartKey::Key);
                let page = manager
                    .scan(scan)
                    .await
                    .map_err(|_| recovery_error("destination index page read failed"))?;
                for item in page.items.unwrap_or_default() {
                    let key = manifest
                        .table
                        .stored_table_info
                        .table_key_from_wire(&item)?;
                    let value = serde_json::to_value(StoredWireItem {
                        item,
                        indexers: Vec::new(),
                    })?;
                    actual.add(
                        &key,
                        value
                            .get("item")
                            .ok_or_else(|| recovery_error("index scan has no item"))?,
                    )?;
                }
                cursor = page.last_evaluated_key;
                if cursor.is_none() {
                    break;
                }
            }
            if expected == actual {
                break;
            }
            if Instant::now() >= deadline {
                return Err(recovery_error(
                    "destination GSI content differs from archive",
                ));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    Ok(())
}

#[derive(Default, PartialEq, Eq)]
struct ItemDigest {
    count: u64,
    xor: [u8; 32],
}

impl ItemDigest {
    fn add(&mut self, key: &KeyAttributes, item: &serde_json::Value) -> Result<()> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| recovery_error("index count overflow"))?;
        let hash = Sha256::digest(serde_json::to_vec(&(serde_json::to_value(key)?, item))?);
        for (slot, byte) in self.xor.iter_mut().zip(hash) {
            *slot ^= byte;
        }
        Ok(())
    }
}

fn projected_index_item(
    manifest: &Manifest,
    index: &storage_types::GlobalSecondaryIndex,
    stored: &StoredWireItem,
) -> Result<Option<serde_json::Value>> {
    let table = &manifest.table.stored_table_info;
    let value = serde_json::to_value(stored)?;
    let mut item = value
        .get("item")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| recovery_error("archive item is not an object"))?
        .clone();
    for field in &index.key_schema {
        let Some(value) = item.get(&field.attribute_name) else {
            return Ok(None);
        };
        let definition = table
            .attribute_definitions
            .iter()
            .find(|entry| entry.attribute_name == field.attribute_name)
            .ok_or_else(|| recovery_error("index key type is missing"))?;
        let tag = match definition.attribute_type {
            storage_types::KeyAttributeType::S => "S",
            storage_types::KeyAttributeType::N => "N",
            storage_types::KeyAttributeType::B => "B",
        };
        if !value
            .as_object()
            .is_some_and(|attribute| attribute.len() == 1 && attribute.contains_key(tag))
        {
            return Err(recovery_error("archive has an unsupported index key type"));
        }
    }
    match index
        .projection
        .projection_type
        .as_ref()
        .ok_or_else(|| recovery_error("index projection type is missing"))?
    {
        storage_types::ProjectionType::All => {}
        storage_types::ProjectionType::KeysOnly | storage_types::ProjectionType::Include => {
            let names = table
                .key_schema
                .iter()
                .chain(index.key_schema.iter())
                .map(|key| key.attribute_name.as_str())
                .chain(
                    index
                        .projection
                        .non_key_attributes
                        .iter()
                        .flatten()
                        .map(String::as_str),
                )
                .collect::<HashSet<_>>();
            item.retain(|name, _| names.contains(name.as_str()));
        }
    }
    Ok(Some(serde_json::Value::Object(item)))
}
