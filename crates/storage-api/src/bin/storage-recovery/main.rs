//! Operator export of an externally stabilized FoundationDB table.

use std::{
    fs,
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use kv_backend::{
    RecoverySource,
    foundationdb::{FoundationDbBackend, FoundationDbOptions},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_types::{StoredTableInfo, TableName, TimeToLiveDescription};

mod restore;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
struct RecoveryError(String);

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RecoveryError {}

fn recovery_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(RecoveryError(message.into()))
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Observe identities from an externally stabilized source without writing
    /// it.
    InspectSource {
        #[arg(long)]
        source_config: PathBuf,
        #[arg(long)]
        source_table: String,
    },
    /// Export a table whose stability is attested by the operator.
    Export {
        #[arg(long)]
        source_config: PathBuf,
        #[arg(long)]
        source_table: String,
        #[arg(long)]
        source_receipt: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify an archive and restore it into a dedicated empty namespace.
    Restore {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        manifest_sha256: String,
        #[arg(long)]
        destination_config: PathBuf,
        #[arg(long)]
        destination_table: String,
        #[arg(long)]
        isolation_evidence: String,
        #[arg(long)]
        result: Option<PathBuf>,
        #[arg(long)]
        execute: bool,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceReceipt {
    database_id: String,
    snapshot_id: String,
    table_name: String,
    table_id: String,
    source_config_sha256: String,
    stability: Stability,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Stability {
    kind: String,
    evidence_id: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArchiveTable {
    key_schema: Vec<storage_types::KeySchemaElement>,
    stored_table_info: StoredTableInfo,
    ttl: TimeToLiveDescription,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArchivePage {
    file: String,
    rows: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    kind: String,
    source: ArchiveSource,
    table: ArchiveTable,
    pages: Vec<ArchivePage>,
    row_count: u64,
    complete: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArchiveSource {
    database_id: String,
    snapshot_id: String,
    table_name: String,
    table_id: String,
    source_config_sha256: String,
    stability: Stability,
    system_stream_scope: storage_types::SystemStreamScope,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            return std::process::ExitCode::SUCCESS;
        }
        Err(_) => {
            eprintln!("invalid recovery command arguments; use --help for usage");
            return std::process::ExitCode::FAILURE;
        }
    };
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            if let Some(safe) = error.downcast_ref::<RecoveryError>() {
                eprintln!("{safe}");
            } else {
                eprintln!("recovery operation failed");
            }
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::InspectSource {
            source_config,
            source_table,
        } => {
            let source = open_source(&source_config, &source_table).await?;
            let (table_id, _, _) = source
                .table()
                .await
                .map_err(|_| recovery_error("source table metadata observation failed"))?;
            let scope = source
                .system_stream_scope()
                .await
                .map_err(|_| recovery_error("source stream scope observation failed"))?;
            println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                "database_id": uuid::Uuid::from_bytes(source.database_id()).to_string(),
                "table_id": uuid::Uuid::from_bytes(*table_id.as_ref()).to_string(),
                "table_name": source_table,
                "source_config_sha256": digest(&fs::read(source_config).map_err(|_| recovery_error("source configuration could not be read"))?),
                "system_stream_scope": scope,
            })).map_err(|_| recovery_error("source observation encoding failed"))?);
            Ok(())
        }
        Command::Export {
            source_config,
            source_table,
            source_receipt,
            output,
        } => export(&source_config, &source_table, &source_receipt, &output).await,
        Command::Restore {
            archive,
            manifest_sha256,
            destination_config,
            destination_table,
            isolation_evidence,
            result,
            execute,
        } => {
            restore::run(
                &archive,
                &manifest_sha256,
                &destination_config,
                &destination_table,
                &isolation_evidence,
                result.as_deref(),
                execute,
            )
            .await
        }
    }
}

async fn export(
    config_path: &Path,
    table_name: &str,
    receipt_path: &Path,
    output: &Path,
) -> Result<()> {
    let receipt_bytes =
        fs::read(receipt_path).map_err(|_| recovery_error("source receipt could not be read"))?;
    let receipt: SourceReceipt = serde_json::from_slice(&receipt_bytes).map_err(|error| {
        recovery_error(format!(
            "source receipt JSON decoding failed at line {} column {}",
            error.line(),
            error.column()
        ))
    })?;
    if receipt.table_name != table_name
        || receipt.snapshot_id.trim().is_empty()
        || receipt.stability.evidence_id.trim().is_empty()
        || !matches!(
            receipt.stability.kind.as_str(),
            "immutable_clone" | "quiesced_live"
        )
        || receipt.source_config_sha256
            != digest(
                &fs::read(config_path)
                    .map_err(|_| recovery_error("source configuration could not be read"))?,
            )
    {
        return Err(recovery_error(
            "source receipt does not match the selected configuration and table",
        ));
    }
    let source = open_source(config_path, table_name).await?;
    let (table_id, table, ttl) = source
        .table()
        .await
        .map_err(|_| recovery_error("source table metadata observation failed"))?;
    let system_stream_scope = source
        .system_stream_scope()
        .await
        .map_err(|_| recovery_error("source stream scope observation failed"))?;
    if receipt.database_id != uuid::Uuid::from_bytes(source.database_id()).to_string()
        || receipt.table_id != uuid::Uuid::from_bytes(*table_id.as_ref()).to_string()
    {
        return Err(recovery_error(
            "source receipt does not match the observed database and table identities",
        ));
    }
    let mut manifest = Manifest {
        schema_version: 1,
        kind: "aux_storage_table_snapshot".to_owned(),
        source: ArchiveSource {
            database_id: receipt.database_id,
            snapshot_id: receipt.snapshot_id,
            table_name: receipt.table_name,
            table_id: receipt.table_id,
            source_config_sha256: receipt.source_config_sha256,
            stability: receipt.stability,
            system_stream_scope,
        },
        table: ArchiveTable {
            key_schema: table.key_schema.clone(),
            stored_table_info: table,
            ttl,
        },
        pages: Vec::new(),
        // Reserve the widest possible decimal count before collecting pages.
        row_count: u64::MAX,
        complete: true,
    };
    let mut manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|_| recovery_error("archive manifest encoding failed"))?
        .len();
    if manifest_bytes > MAX_MANIFEST_BYTES {
        return Err(recovery_error("archive manifest exceeds 4 MiB"));
    }
    fs::DirBuilder::new()
        .mode(0o700)
        .create(output)
        .map_err(|_| recovery_error("archive directory could not be created"))?;
    let pages_dir = output.join("pages");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&pages_dir)
        .map_err(|_| recovery_error("archive page directory could not be created"))?;
    let mut total = 0u64;
    let mut after = None;
    loop {
        let page = source.page(after, 256).await.map_err(|_| {
            recovery_error(format!(
                "source page {} read failed",
                manifest.pages.len() + 1
            ))
        })?;
        if !page.rows.is_empty() {
            let mut contents = Vec::new();
            for (row_number, row) in page.rows.into_iter().enumerate() {
                let row_error = || {
                    recovery_error(format!(
                        "source page {} row {} encoding failed",
                        manifest.pages.len() + 1,
                        row_number + 1
                    ))
                };
                let mut value = serde_json::to_value(row.item).map_err(|_| row_error())?;
                value.as_object_mut().ok_or_else(row_error)?.insert(
                    "keys".into(),
                    serde_json::to_value(row.keys).map_err(|_| row_error())?,
                );
                serde_json::to_writer(&mut contents, &value).map_err(|_| row_error())?;
                contents.push(b'\n');
                total = total
                    .checked_add(1)
                    .ok_or_else(|| recovery_error("row count overflow"))?;
            }
            if contents.len() > 64 * 1024 * 1024 {
                return Err(recovery_error("archive page exceeds 64 MiB"));
            }
            let name = format!("{:08}.jsonl", manifest.pages.len());
            let path = pages_dir.join(&name);
            let rows = contents.iter().filter(|byte| **byte == b'\n').count() as u64;
            let descriptor = ArchivePage {
                file: format!("pages/{name}"),
                rows,
                bytes: contents.len() as u64,
                sha256: digest(&contents),
            };
            // The empty-array manifest already accounts for `[]`; each page
            // adds its compact JSON plus at most one separating comma.
            manifest_bytes = manifest_bytes
                .checked_add(
                    serde_json::to_vec(&descriptor)
                        .map_err(|_| recovery_error("archive page description encoding failed"))?
                        .len(),
                )
                .and_then(|size| size.checked_add(usize::from(!manifest.pages.is_empty())))
                .ok_or_else(|| recovery_error("archive manifest size overflow"))?;
            if manifest_bytes > MAX_MANIFEST_BYTES {
                return Err(recovery_error("archive manifest exceeds 4 MiB"));
            }
            write_new(&path, &contents).map_err(|_| {
                recovery_error(format!(
                    "archive page {} could not be written",
                    manifest.pages.len() + 1
                ))
            })?;
            manifest.pages.push(descriptor);
        }
        after = page.continuation;
        if after.is_none() {
            break;
        }
    }
    manifest.row_count = total;
    let bytes = serde_json::to_vec(&manifest)
        .map_err(|_| recovery_error("archive manifest encoding failed"))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(recovery_error("archive manifest exceeds 4 MiB"));
    }
    fs::File::open(&pages_dir)
        .and_then(|file| file.sync_all())
        .map_err(|_| recovery_error("archive page directory synchronization failed"))?;
    let temporary = output.join("manifest.pending");
    write_new(&temporary, &bytes)
        .map_err(|_| recovery_error("archive manifest could not be written"))?;
    fs::rename(temporary, output.join("manifest.json"))
        .map_err(|_| recovery_error("archive manifest could not be published"))?;
    fs::File::open(output)
        .and_then(|file| file.sync_all())
        .map_err(|_| recovery_error("archive directory synchronization failed"))?;
    println!("manifest_sha256={}", digest(&bytes));
    Ok(())
}

async fn open_source(
    config_path: &Path,
    table_name: &str,
) -> Result<RecoverySource<FoundationDbBackend>> {
    let config = config::load(config_path)
        .map_err(|_| recovery_error("source configuration could not be loaded or is invalid"))?;
    if config.root.features.storage_connections.is_some()
        || !config.root.features.regional_connections.is_empty()
    {
        return Err(recovery_error(
            "recovery export requires a single unpaired FoundationDB connection",
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
            "recovery export requires only a FoundationDB backend",
        ));
    }
    let settings = backends
        .foundationdb
        .as_ref()
        .ok_or_else(|| recovery_error("FoundationDB backend is required"))?;
    if settings.cache_read_version_ms != 0 {
        return Err(recovery_error("recovery requires cache_read_version_ms=0"));
    }
    let backend = FoundationDbBackend::initialize(FoundationDbOptions {
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
    .map_err(|_| recovery_error("source FoundationDB connection failed"))?;
    RecoverySource::open(backend, TableName::new(table_name))
        .await
        .map_err(|_| recovery_error("source read-only namespace open failed"))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    storage_types::encode_hex(&Sha256::digest(bytes))
}
