#![cfg(feature = "remote")]

use std::{fs, path::PathBuf};

use clap::Parser;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod support_tests;

#[derive(Debug, Parser)]
struct FixtureArgs {
    #[arg(long)]
    origin_db: PathBuf,
    #[arg(long)]
    ready_file: PathBuf,
}

#[tokio::main]
async fn main() -> support_tests::FixtureResult<()> {
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init();
    let args = FixtureArgs::parse();
    let fixture = support_tests::start_fixture(&args.origin_db).await?;
    let endpoints = format!(
        "front_root={}\norigin_root={}\norigin_queue_endpoint={}\nremote_storage={}\n",
        fixture.front_root,
        fixture.origin.base_url,
        fixture.origin_queue_endpoint,
        fixture.remote.storage_url,
    );
    fs::write(&args.ready_file, endpoints)?;
    wait_for_shutdown().await;
    Ok(())
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
