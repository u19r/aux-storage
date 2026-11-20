//! Test-only PostgreSQL process ownership; never reads the shared test DSN.
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

pub(super) struct OwnedServer {
    root: tempfile::TempDir,
    pub(super) bin: PathBuf,
    pub(super) child: Option<Child>,
}

impl OwnedServer {
    pub(super) async fn start() -> Self {
        let root = tempfile::Builder::new()
            .prefix("auxpg-")
            .tempdir_in("/tmp")
            .unwrap();
        let bin = PathBuf::from(std::env::var("TEST_POSTGRES_BIN").expect("TEST_POSTGRES_BIN"));
        assert!(bin.is_absolute());
        let mut server = Self {
            root,
            bin,
            child: None,
        };
        let output = Command::new(server.bin.join("initdb"))
            .args([
                "--auth=trust",
                "--no-locale",
                "--encoding=UTF8",
                "--username=postgres",
                "-D",
            ])
            .arg(server.data())
            .output()
            .unwrap();
        assert!(output.status.success(), "owned initdb failed");
        server.restart().await;
        server
    }

    fn data(&self) -> PathBuf {
        self.root.path().join("data")
    }

    pub(super) fn dsn(&self) -> String {
        format!(
            "host={} port=5432 user=postgres dbname=postgres",
            self.root.path().display()
        )
    }

    pub(super) fn config(&self) -> tokio_postgres::Config {
        let config: tokio_postgres::Config = self.dsn().parse().unwrap();
        assert_eq!(
            config.get_hosts(),
            &[tokio_postgres::config::Host::Unix(
                self.root.path().to_owned()
            )]
        );
        assert!(config.get_hostaddrs().is_empty());
        assert_eq!(config.get_ports(), &[5432]);
        config
    }

    pub(super) async fn connect(&self) -> OwnedConnection {
        let (client, connection) = self.config().connect(tokio_postgres::NoTls).await.unwrap();
        let warnings = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = warnings.clone();
        let task = tokio::spawn(async move {
            let mut connection = std::pin::pin!(connection);
            while let Some(message) =
                futures::future::poll_fn(|cx| connection.as_mut().poll_message(cx)).await
            {
                if message.is_err() {
                    break;
                }
                if let Ok(tokio_postgres::AsyncMessage::Notice(notice)) = message {
                    eprintln!(
                        "owned_notice code={} severity={:?}",
                        notice.code().code(),
                        notice.parsed_severity()
                    );
                    if notice.code().code() == "01000"
                        && notice.parsed_severity()
                            == Some(tokio_postgres::error::Severity::Warning)
                    {
                        observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        });
        OwnedConnection {
            client,
            task: Some(task),
            warnings,
        }
    }

    pub(super) async fn restart(&mut self) {
        assert!(self.child.is_none());
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join("server.log"))
            .unwrap();
        let child = Command::new(self.bin.join("postgres"))
            .arg("-D")
            .arg(self.data())
            .args([
                "-c",
                "listen_addresses=",
                "-c",
                "unix_socket_permissions=0700",
                "-k",
            ])
            .arg(self.root.path())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        eprintln!(
            "owned_server pid={} data={} socket={}",
            child.id(),
            self.data().display(),
            self.root.path().display()
        );
        self.child = Some(child);
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Ok((client, connection)) = self.config().connect(tokio_postgres::NoTls).await
                {
                    let task = tokio::spawn(connection);
                    drop(client);
                    task.await.unwrap().unwrap();
                    break;
                }
                assert!(
                    self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "owned postgres exited during startup"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("owned postgres readiness");
    }

    pub(super) fn finish(mut self) {
        self.stop();
        std::fs::remove_dir_all(self.root.path()).unwrap();
        assert!(!self.root.path().exists());
        eprintln!(
            "owned_directory_removed path={} verified=true",
            self.root.path().display()
        );
    }

    pub(super) fn stop(&mut self) {
        if self.child.is_none() {
            return;
        }
        // Retain ownership through every fallible shutdown step. If spawning
        // pg_ctl or waiting fails, Drop still owns the live child and can reap
        // it.
        let result = Command::new(self.bin.join("pg_ctl"))
            .arg("-D")
            .arg(self.data())
            .args(["stop", "-m", "fast", "-w", "-t", "15"])
            .output()
            .unwrap();
        let child = self
            .child
            .as_mut()
            .expect("owned child retained during shutdown");
        if !result.status.success() {
            let _ = child.kill();
        }
        let status = child.wait().unwrap();
        eprintln!(
            "owned_server_stopped pid={} joined=true status={status}",
            child.id()
        );
        self.child.take();
        assert!(result.status.success(), "owned graceful shutdown failed");
    }
}

impl Drop for OwnedServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let graceful = Command::new(self.bin.join("pg_ctl"))
                .arg("-D")
                .arg(self.data())
                .args(["stop", "-m", "fast", "-w", "-t", "5"])
                .output();
            if !graceful.is_ok_and(|result| result.status.success()) {
                let _ = Command::new(self.bin.join("pg_ctl"))
                    .arg("-D")
                    .arg(self.data())
                    .args(["stop", "-m", "immediate", "-w", "-t", "5"])
                    .output();
                let _ = child.kill();
            }
            let _ = child.wait();
            eprintln!("owned_failure_cleanup pid={} joined=true", child.id());
        }
        eprintln!("owned_server_removed path={}", self.root.path().display());
    }
}

pub(super) struct OwnedConnection {
    pub(super) client: tokio_postgres::Client,
    task: Option<tokio::task::JoinHandle<()>>,
    pub(super) warnings: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl OwnedConnection {
    pub(super) async fn close(mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        let result = task.await;
        assert!(result.is_ok() || result.unwrap_err().is_cancelled());
    }
}

impl Drop for OwnedConnection {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
