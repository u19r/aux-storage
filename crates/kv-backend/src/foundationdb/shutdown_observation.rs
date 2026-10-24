//! Retain measured contexts until the host has dropped all application owners.

use std::{
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use foundationdb::Database;
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult};
use uuid::Uuid;

static OBSERVATION: OnceLock<Mutex<StorageResult<Option<RetainedContexts<Database>>>>> =
    OnceLock::new();

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObservationControl {
    pub(super) directory: PathBuf,
    pub(super) nonce: Uuid,
}

pub(super) struct RetainedContexts<T> {
    pub(super) control: ObservationControl,
    pub(super) databases: Vec<Arc<T>>,
    pub(super) phase: ObservationPhase,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ObservationPhase {
    Open,
    Sampling,
    Finished,
}

#[derive(Serialize)]
struct ReadyObservation {
    pub(super) nonce: Uuid,
    pub(super) pid: u32,
    contexts: usize,
    ready_at_seconds: f64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleaseObservation {
    pub(super) nonce: Uuid,
    pub(super) pid: u32,
}

pub(super) fn register_database(database: &Arc<Database>) -> StorageResult<()> {
    let observation = OBSERVATION.get_or_init(|| Mutex::new(load_observation()));
    let mut state = observation
        .lock()
        .map_err(|_| StorageError::internal("FoundationDB observation lock poisoned"))?;
    let result = match state.as_mut() {
        Ok(Some(contexts)) => contexts.register_database(database),
        Ok(None) => Ok(()),
        Err(error) => Err(error.clone()),
    };
    if let Err(error) = &result {
        *state = Err(error.clone());
    }
    result
}

/// Called only after the host's application runtime has been destroyed.
/// Local contexts drop before the caller stops and joins the native network.
pub(super) fn complete_observation() -> StorageResult<()> {
    let Some(observation) = OBSERVATION.get() else {
        return Ok(());
    };
    let contexts = {
        let mut state = observation
            .lock()
            .map_err(|_| StorageError::internal("FoundationDB observation lock poisoned"))?;
        state
            .as_mut()
            .map_err(|error| error.clone())?
            .as_mut()
            .map(RetainedContexts::seal_contexts)
            .transpose()?
            .flatten()
    };
    let Some(contexts) = contexts else {
        return Ok(());
    };
    let result = contexts
        .drain_native_callbacks()
        .and_then(|()| contexts.complete_observation());
    let mut state = observation
        .lock()
        .map_err(|_| StorageError::internal("FoundationDB observation lock poisoned"))?;
    if let Err(error) = state.as_ref() {
        return Err(error.clone());
    }
    match &result {
        Ok(()) => {
            if let Ok(Some(contexts)) = state.as_mut() {
                contexts.phase = ObservationPhase::Finished;
            }
        }
        Err(error) => *state = Err(error.clone()),
    }
    result
}

fn load_observation() -> StorageResult<Option<RetainedContexts<Database>>> {
    let Some(value) = std::env::var_os("AUXFN_FDB_SHUTDOWN_OBSERVATION") else {
        return Ok(None);
    };
    let value = value.into_string().map_err(|_| {
        StorageError::internal("FoundationDB observation configuration is not UTF-8")
    })?;
    let control: ObservationControl = serde_json::from_str(&value)
        .map_err(|_| StorageError::internal("invalid FoundationDB observation configuration"))?;
    control.validate_directory()?;
    Ok(Some(RetainedContexts {
        control,
        databases: Vec::new(),
        phase: ObservationPhase::Open,
    }))
}

impl RetainedContexts<Database> {
    fn drain_native_callbacks(&self) -> StorageResult<()> {
        self.validate_owners()?;
        for database in &self.databases {
            wait_native_callback(database.perform_no_op())?;
        }
        self.validate_owners()
    }
}

fn wait_native_callback(
    future: impl Future<Output = foundationdb::FdbResult<()>>,
) -> StorageResult<()> {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result.map_err(super::error::map_operation_error);
        }
        if Instant::now() >= deadline {
            return Err(StorageError::internal(
                "FoundationDB observation callback timed out",
            ));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

impl<T> RetainedContexts<T> {
    fn seal_contexts(&mut self) -> StorageResult<Option<Self>> {
        match self.phase {
            ObservationPhase::Finished => return Ok(None),
            ObservationPhase::Sampling => {
                return Err(StorageError::internal(
                    "FoundationDB observation already sampling",
                ));
            }
            ObservationPhase::Open => self.phase = ObservationPhase::Sampling,
        }
        Ok(Some(Self {
            control: ObservationControl {
                directory: self.control.directory.clone(),
                nonce: self.control.nonce,
            },
            databases: std::mem::take(&mut self.databases),
            phase: ObservationPhase::Sampling,
        }))
    }

    pub(super) fn register_database(&mut self, database: &Arc<T>) -> StorageResult<()> {
        if self.phase != ObservationPhase::Open {
            return Err(StorageError::internal("FoundationDB observation is sealed"));
        }
        if self
            .databases
            .iter()
            .any(|entry| Arc::ptr_eq(entry, database))
        {
            return Ok(());
        }
        if self.databases.len() >= 128 {
            return Err(StorageError::internal(
                "FoundationDB observation context capacity exceeded",
            ));
        }
        self.databases.push(Arc::clone(database));
        Ok(())
    }

    pub(super) fn validate_owners(&self) -> StorageResult<()> {
        if self.phase != ObservationPhase::Sampling || self.databases.is_empty() {
            return Err(StorageError::internal(
                "FoundationDB observation has no sealed contexts",
            ));
        }
        if self
            .databases
            .iter()
            .any(|database| Arc::strong_count(database) != 1)
        {
            return Err(StorageError::internal(
                "FoundationDB application database owners remain alive",
            ));
        }
        Ok(())
    }

    fn complete_observation(self) -> StorageResult<()> {
        self.validate_owners()?;
        let ready = ReadyObservation {
            nonce: self.control.nonce,
            pid: std::process::id(),
            contexts: self.databases.len(),
            ready_at_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| {
                    StorageError::internal("FoundationDB observation clock precedes epoch")
                })?
                .as_secs_f64(),
        };
        self.control.write_ready(&ready)?;
        self.control
            .wait_release(Instant::now() + Duration::from_secs(60))?;
        self.validate_owners()
    }
}

impl ObservationControl {
    pub(super) fn validate_directory(&self) -> StorageResult<()> {
        let metadata = fs::symlink_metadata(&self.directory).map_err(map_io_error)?;
        if !self.directory.is_absolute() || !metadata.is_dir() || metadata.file_type().is_symlink()
        {
            return Err(StorageError::internal(
                "FoundationDB observation requires an absolute real directory",
            ));
        }
        if fs::read_dir(&self.directory)
            .map_err(map_io_error)?
            .next()
            .is_some()
        {
            return Err(StorageError::internal(
                "FoundationDB observation requires an empty directory",
            ));
        }
        Ok(())
    }

    fn write_ready(&self, ready: &ReadyObservation) -> StorageResult<()> {
        let pending = self.directory.join("ready.pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending)
            .map_err(map_io_error)?;
        serde_json::to_writer(&mut file, ready)
            .map_err(|_| StorageError::internal("cannot serialize FoundationDB observation"))?;
        file.write_all(b"\n").map_err(map_io_error)?;
        file.sync_all().map_err(map_io_error)?;
        let ready = self.directory.join("ready.json");
        if ready.exists() || self.directory.join("release.json").exists() {
            return Err(StorageError::internal(
                "FoundationDB observation markers already exist",
            ));
        }
        fs::rename(pending, ready).map_err(map_io_error)
    }

    pub(super) fn wait_release(&self, deadline: Instant) -> StorageResult<()> {
        let release = self.directory.join("release.json");
        loop {
            match fs::read(&release) {
                Ok(bytes) => return self.validate_release(&bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(map_io_error(error)),
            }
            if Instant::now() >= deadline {
                return Err(StorageError::internal(
                    "FoundationDB observation release timed out",
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    pub(super) fn validate_release(&self, bytes: &[u8]) -> StorageResult<()> {
        let release: ReleaseObservation = serde_json::from_slice(bytes)
            .map_err(|_| StorageError::internal("invalid FoundationDB observation release"))?;
        if release.nonce != self.nonce || release.pid != std::process::id() {
            return Err(StorageError::internal(
                "FoundationDB observation release identity differs",
            ));
        }
        Ok(())
    }
}

fn map_io_error(error: std::io::Error) -> StorageError {
    StorageError::internal(&error)
}
