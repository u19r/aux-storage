use std::path::PathBuf;

use cargo_metadata::MetadataCommand;
use clap::Parser;
use config::CompileTimeManifest;

#[derive(Parser, Debug)]
#[command(name = "config-compile-time-manifest")]
#[command(about = "Write workspace Cargo feature metadata to a TOML manifest")]
struct Args {
    /// Output path for the generated manifest TOML file.
    #[arg(long, default_value = "config/compile-time.toml")]
    output: PathBuf,

    /// Emit an empty manifest without calling cargo metadata.
    #[arg(long, default_value_t = false)]
    empty: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let manifest = if args.empty {
        CompileTimeManifest::empty()
    } else {
        let metadata = MetadataCommand::new().exec()?;
        CompileTimeManifest::from_metadata(&metadata)
    };
    manifest.write_to_path(args.output.as_path())?;
    Ok(())
}
