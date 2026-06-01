use std::{path::PathBuf, sync::Arc, time::Duration};

use config::{Config, runtime::MutableConfigManager};
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::sleep,
};

pub struct ConfigWatchGuard {
    _watcher: RecommendedWatcher,
    handle: JoinHandle<()>,
}

impl Drop for ConfigWatchGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "spawn takes ownership to satisfy watcher lifetime and async task requirements"
)]
pub fn spawn(
    path: PathBuf,
    initial: Arc<Config>,
    config_manager: Arc<dyn MutableConfigManager>,
) -> NotifyResult<ConfigWatchGuard> {
    let (tx, _rx) = watch::channel(initial);
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(16);
    let mut watcher = notify::recommended_watcher(move |res: NotifyResult<Event>| match res {
        Ok(event) => {
            if is_relevant(&event.kind) {
                let _ = event_tx.blocking_send(event);
            }
        }
        Err(err) => {
            tracing::warn!(target: "config", error = %err, "file watch error");
        }
    })?;

    watcher.watch(path.as_ref(), RecursiveMode::NonRecursive)?;

    let path_clone = path.clone();
    let sender = tx.clone();
    let handle = spawn_named("storage-api-config-watch", async move {
        while let Some(_event) = event_rx.recv().await {
            sleep(Duration::from_millis(200)).await;
            match config::load(path_clone.as_path()) {
                Ok(cfg) => {
                    config_manager.replace_config(cfg.clone());
                    if sender.send(cfg).is_err() {
                        tracing::debug!(target: "config", "no config watchers listening");
                    }
                }
                Err(err) => {
                    tracing::error!(target: "config", error = %err, "failed to reload config");
                }
            }
        }
    });

    Ok(ConfigWatchGuard {
        _watcher: watcher,
        handle,
    })
}

fn spawn_named<F>(task_name: &str, fut: F) -> JoinHandle<()>
where F: std::future::Future<Output = ()> + Send + 'static {
    #[cfg(tokio_unstable)]
    {
        match tokio::task::Builder::new().name(task_name).spawn(fut) {
            Ok(handle) => handle,
            Err(err) => {
                tracing::warn!(target: "config", error = %err, task_name, "failed to spawn named task");
                tokio::spawn(async {})
            }
        }
    }

    #[cfg(not(tokio_unstable))]
    {
        let _ = task_name;
        tokio::spawn(fut)
    }
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "notify callback provides references and the matcher remains clear with &EventKind"
)]
fn is_relevant(kind: &EventKind) -> bool {
    use notify::event::{AccessKind, AccessMode, CreateKind, ModifyKind, RemoveKind};

    matches!(
        kind,
        EventKind::Modify(ModifyKind::Data(_) | ModifyKind::Name(_) | ModifyKind::Metadata(_))
            | EventKind::Create(CreateKind::File | CreateKind::Any)
            | EventKind::Remove(RemoveKind::File | RemoveKind::Any)
            | EventKind::Access(AccessKind::Close(AccessMode::Write))
    )
}
