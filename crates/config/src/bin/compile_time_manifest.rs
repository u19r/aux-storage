use std::{error::Error, path::PathBuf};

#[cfg(feature = "compile-time-manifest")]
use cargo_metadata::MetadataCommand;
use clap::Parser;
#[cfg(feature = "compile-time-manifest")]
use config::CompileTimeManifest;

#[derive(Parser, Debug)]
#[command(
    about = "Generate compile-time feature manifest for documentation workflows",
    version
)]
struct Cmd {
    /// Output path for the manifest TOML file.
    #[arg(long, default_value = "config/compile-time.toml")]
    output: PathBuf,
}

#[cfg(feature = "compile-time-manifest")]
fn main() -> Result<(), Box<dyn Error>> {
    let cmd = Cmd::parse();
    let workspace_manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("Cargo.toml");

    let metadata = MetadataCommand::new()
        .manifest_path(&workspace_manifest)
        .no_deps()
        .exec()?;

    let manifest = CompileTimeManifest::from_metadata(&metadata);
    manifest.write_to_path(cmd.output.as_path())?;

    println!(
        "Generated compile-time manifest with {} crates at {}",
        manifest.crates.len(),
        cmd.output.display()
    );

    Ok(())
}

#[cfg(not(feature = "compile-time-manifest"))]
fn main() -> Result<(), Box<dyn Error>> {
    Err("enable the `compile-time-manifest` feature to run this binary".into())
}
