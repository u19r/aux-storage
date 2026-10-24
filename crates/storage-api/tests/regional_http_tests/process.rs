use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result};
use serde_json::Value;
use storage::regional_test_support::{operator, provider::ProviderFixture, proxy::RegionalProxy};

use super::request::HttpRegion;

pub(super) struct HttpFixture {
    directory: tempfile::TempDir,
    pub(super) regions: [HttpRegion; 2],
    processes: [ApiProcess; 2],
    pub(super) proxy: RegionalProxy,
    pub(super) reverse_proxy: RegionalProxy,
    provider: ProviderFixture,
}

struct ApiProcess {
    child: Child,
    configuration: PathBuf,
    log: PathBuf,
}

impl HttpFixture {
    pub(super) async fn start() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let provider = ProviderFixture::start(directory.path()).await?;
        let prepared = async {
            let listeners = [
                TcpListener::bind("127.0.0.1:0")?,
                TcpListener::bind("127.0.0.1:0")?,
            ];
            let endpoints = [listeners[0].local_addr()?, listeners[1].local_addr()?];
            let proxy = RegionalProxy::start(endpoints[1], 0).await?;
            let reverse_proxy = RegionalProxy::start(endpoints[0], 0).await?;
            let source = operator::features(
                directory.path(),
                1,
                [endpoints[0], proxy.endpoint],
                65536,
                provider.backends[0].clone(),
            )?;
            let target = operator::features(
                directory.path(),
                2,
                [reverse_proxy.endpoint, endpoints[1]],
                65536,
                provider.backends[1].clone(),
            )?;
            for features in [&source, &target] {
                let path = &features.regional_connections["default"];
                let mut startup: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                // Keep recovery inside the existing public observation
                // deadline.
                startup["configuration"]["limits"]["frame_events"] = serde_json::json!(2);
                startup["configuration"]["limits"]["import_events"] = serde_json::json!(2);
                std::fs::write(path, serde_json::to_vec(&startup)?)?;
            }
            let http = [
                TcpListener::bind("127.0.0.1:0")?,
                TcpListener::bind("127.0.0.1:0")?,
            ];
            let addresses = [http[0].local_addr()?, http[1].local_addr()?];
            let configurations = [
                save_configuration(directory.path(), 1, addresses[0], source)?,
                save_configuration(directory.path(), 2, addresses[1], target)?,
            ];
            drop(listeners);
            drop(http);
            let mut processes = [
                ApiProcess::start(configurations[0].clone())?,
                ApiProcess::start(configurations[1].clone())?,
            ];
            let regions = [
                HttpRegion::new(addresses[0], 1)?,
                HttpRegion::new(addresses[1], 2)?,
            ];
            for (process, region) in processes.iter_mut().zip(&regions) {
                process.await_ready(region).await?;
            }
            Ok::<_, anyhow::Error>((regions, processes, proxy, reverse_proxy))
        }
        .await;
        let (regions, processes, proxy, reverse_proxy) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                provider.finish().await?;
                return Err(error);
            }
        };
        Ok(Self {
            directory,
            regions,
            processes,
            proxy,
            reverse_proxy,
            provider,
        })
    }

    pub(super) fn crash_region(&mut self, region: usize) -> Result<()> {
        self.processes[region].child.kill()?;
        self.processes[region].child.wait()?;
        Ok(())
    }

    pub(super) async fn crash_and_restart(
        &mut self,
        region: usize,
        require_delivery_lease: bool,
    ) -> Result<()> {
        self.crash_region(region)?;
        if require_delivery_lease {
            self.require_persisted_delivery_lease(region).await?;
        }
        self.processes[region].child = spawn(
            &self.processes[region].configuration,
            &self.processes[region].log,
        )?;
        self.processes[region]
            .await_ready(&self.regions[region])
            .await
    }

    pub(super) async fn require_persisted_delivery_lease(&self, region: usize) -> Result<()> {
        use scheduled_bg_jobs::{
            JobState, Timestamp,
            store::{RangeDirection, RangeRequest, RangeRequestInput},
        };
        let features = config::Features {
            backends: self.provider.backends[region].clone(),
            ..Default::default()
        };
        let manager = storage::startup::database_manager_from_features(
            &features,
            storage::DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await?;
        let store = manager
            .scheduled_job_store()
            .context("native job store after process death")?;
        let capabilities = store.capabilities();
        let now = Timestamp::from_millis(chrono::Utc::now().timestamp_millis());
        let mut continuation = None;
        let mut delivery_states = Vec::new();
        loop {
            let request = RangeRequest::try_from(RangeRequestInput {
                start: None,
                end: None,
                direction: RangeDirection::Forward,
                item_limit: capabilities.max_range_items(),
                byte_limit: capabilities.max_range_bytes(),
                continuation,
            })?;
            let page = store.read_range(request).await?;
            for entry in page.entries {
                if let Ok(record) =
                    scheduled_bg_jobs::test_support::codec::decode_job_record(&entry.value)
                    && record.kind.as_str() == "regional-delivery"
                {
                    delivery_states.push((
                        record.job_id,
                        record.namespace,
                        record.generation,
                        record.revision,
                        record.created_at,
                        record.scheduled_at,
                        record.attempts,
                        record.state,
                        now,
                    ));
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        manager.shutdown_background_services().await?;
        anyhow::ensure!(
            matches!(delivery_states.as_slice(), [(_, _, _, _, _, _, _, JobState::Leased(lease), _)] if lease.expires_at > now),
            "the fixture's single regional connection and unchanged journal generation require \
             exactly one unexpired persisted delivery lease; observed (job, namespace, \
             generation, revision, created, scheduled, attempts, state, now): {delivery_states:?}"
        );
        Ok(())
    }

    pub(super) async fn finish(mut self, mut result: Result<()>) -> Result<()> {
        for process in &mut self.processes {
            result = result.and(process.shutdown().await);
        }
        if result.is_err() {
            for (region, process) in self.processes.iter().enumerate() {
                let log = std::fs::read_to_string(&process.log).unwrap_or_default();
                let lines = log.lines().collect::<Vec<_>>();
                eprintln!(
                    "owned HTTP region {} diagnostic tail:\n{}",
                    region + 1,
                    lines[lines.len().saturating_sub(60)..].join("\n")
                );
            }
        }
        drop(self.processes);
        result = result.and(self.provider.finish().await);
        if result.is_err() {
            eprintln!(
                "owned HTTP regional fixture retained at: {}",
                self.directory.keep().display()
            );
        } else {
            drop(self.directory);
        }
        result
    }
}

impl ApiProcess {
    fn start(configuration: PathBuf) -> Result<Self> {
        let log = configuration.with_extension("log");
        Ok(Self {
            child: spawn(&configuration, &log)?,
            configuration,
            log,
        })
    }

    async fn await_ready(&mut self, region: &HttpRegion) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            if region.is_ready().await {
                return Ok(());
            }
            anyhow::ensure!(
                self.child.try_wait()?.is_none(),
                "HTTP region exited before readiness: {}",
                std::fs::read_to_string(&self.log)?
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!(
            "HTTP region did not publish readiness: {}",
            std::fs::read_to_string(&self.log)?
        )
    }

    async fn shutdown(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()?;
        anyhow::ensure!(status.success(), "could not signal the owned API process");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                anyhow::ensure!(
                    status.success(),
                    "owned API process did not stop cleanly: {}",
                    std::fs::read_to_string(&self.log)?
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!("owned API process exceeded its shutdown deadline")
    }
}

impl Drop for ApiProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(configuration: &Path, log: &Path) -> Result<Child> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)?;
    Ok(Command::new(env!("CARGO_BIN_EXE_storage-api"))
        .arg("--config")
        .arg(configuration)
        .env("APP_SHUTDOWN_GRACE_SECONDS", "0")
        .env("STORAGE_API_WORKER_THREADS", "4")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()?)
}

fn save_configuration(
    root: &Path,
    region: usize,
    endpoint: SocketAddr,
    features: config::Features,
) -> Result<PathBuf> {
    let mut config = config::RootConfig {
        features,
        ..Default::default()
    };
    config.features.metrics.enabled = false;
    config.features.tracing.log_level = Some("warn".into());
    config.http.bind_addr = endpoint.to_string();
    let path = root.join(format!("http-{region}.json"));
    std::fs::write(&path, serde_json::to_vec(&config)?)?;
    Ok(path)
}

pub(super) async fn require_invalid_startup_unpublished() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let provider = ProviderFixture::start(directory.path()).await?;
    let result = async {
        for (field, value) in [
            ("/budgets/attempt/call_guards", serde_json::json!(0)),
            ("/budgets/reserved_attempts", serde_json::json!(100)),
        ] {
            let regional = [
                TcpListener::bind("127.0.0.1:0")?,
                TcpListener::bind("127.0.0.1:0")?,
            ];
            let endpoint = regional[0].local_addr()?;
            let features = operator::features(
                directory.path(),
                1,
                [endpoint, regional[1].local_addr()?],
                65536,
                provider.backends[0].clone(),
            )?;
            let startup = &features.regional_connections["default"];
            let mut input: Value = serde_json::from_slice(&std::fs::read(startup)?)?;
            *input
                .pointer_mut(field)
                .context("declared invalid budget")? = value;
            std::fs::write(startup, serde_json::to_vec(&input)?)?;
            let http = TcpListener::bind("127.0.0.1:0")?;
            let address = http.local_addr()?;
            let configuration = save_configuration(directory.path(), 1, address, features)?;
            drop(regional);
            drop(http);
            let mut process = ApiProcess::start(configuration)?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                anyhow::ensure!(
                    tokio::net::TcpStream::connect(address).await.is_err(),
                    "invalid startup published HTTP intake"
                );
                anyhow::ensure!(
                    tokio::net::TcpStream::connect(endpoint).await.is_err(),
                    "invalid startup published regional intake"
                );
                if let Some(status) = process.child.try_wait()? {
                    anyhow::ensure!(!status.success(), "invalid startup exited successfully");
                    break;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "invalid startup did not exit within its bounded observation"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(())
    }
    .await;
    result.and(provider.finish().await)
}
