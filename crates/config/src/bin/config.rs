use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    about = "Config tooling: emit JSON schema or effective config",
    version
)]
struct Cmd {
    /// Write schema to the given path
    #[arg(long)]
    write_schema: Option<PathBuf>,
}

fn main() {
    let cmd = Cmd::parse();
    if let Some(path) = cmd.write_schema {
        if let Err(e) = config::Config::write_schema_to(&path) {
            eprintln!("error writing schema: {e}");
            std::process::exit(1);
        } else {
            println!("wrote schema to {}", path.display());
        }
        return;
    }

    eprintln!("no operation specified. Use --write-schema <path>");
    std::process::exit(2);
}
