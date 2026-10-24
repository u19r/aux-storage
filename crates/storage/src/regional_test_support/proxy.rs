use std::{
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result};
use serde_json::{Value, json};

pub struct RegionalProxy {
    child: Child,
    client: reqwest::Client,
    api: String,
    log: tempfile::NamedTempFile,
    pub endpoint: SocketAddr,
}

impl RegionalProxy {
    pub async fn start(upstream: SocketAddr, latency: u64) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let log = tempfile::NamedTempFile::new()?;
        let child = Command::new("toxiproxy-server")
            .args([
                "-host",
                "127.0.0.1",
                "-port",
                &address.port().to_string(),
                "-seed",
                "70405",
            ])
            .env("LOG_LEVEL", "debug")
            .stdout(log.as_file().try_clone()?)
            .stderr(Stdio::null())
            .spawn()
            .context("start isolated Toxiproxy")?;
        let mut proxy = Self {
            child,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()?,
            api: format!("http://{address}"),
            log,
            endpoint: upstream,
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if proxy
                .client
                .get(format!("{}/version", proxy.api))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            anyhow::ensure!(
                proxy.child.try_wait()?.is_none(),
                "Toxiproxy exited before readiness"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let created = proxy.client.post(format!("{}/proxies", proxy.api)).json(&json!({"name": "regional", "listen": "127.0.0.1:0", "upstream": upstream.to_string(), "enabled": true}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        proxy.endpoint = created["listen"]
            .as_str()
            .context("proxy listen address")?
            .parse()?;
        if latency > 0 {
            proxy.latency(latency).await?;
        }
        Ok(proxy)
    }

    pub async fn toxic(
        &self,
        name: &str,
        kind: &str,
        direction: &str,
        attributes: Value,
    ) -> Result<()> {
        self.client.post(format!("{}/proxies/regional/toxics", self.api))
            .json(&json!({"name": name, "type": kind, "stream": direction, "toxicity": 1.0, "attributes": attributes}))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn latency(&self, milliseconds: u64) -> Result<()> {
        for direction in ["upstream", "downstream"] {
            self.toxic(
                &format!("latency-{direction}"),
                "latency",
                direction,
                json!({"latency": milliseconds, "jitter": 0}),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn remove(&self, name: &str) -> Result<()> {
        self.client
            .delete(format!("{}/proxies/regional/toxics/{name}", self.api))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub fn connections(&self) -> Result<usize> {
        Ok(std::fs::read_to_string(self.log.path())?
            .lines()
            .filter(|line| {
                line.contains("Setup connection") && line.contains("\"direction\":\"upstream\"")
            })
            .count())
    }

    pub async fn await_connection(&self, previous: usize) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            if self.connections()? > previous {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!("regional request did not open a connection through the held Toxiproxy link")
    }
}

impl Drop for RegionalProxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
