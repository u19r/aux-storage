use std::{path::Path, sync::atomic::Ordering};

use rusqlite::OptionalExtension as _;
use storage_types::{AttributeValue, IndexStatus};

use super::{StatefulObservation, StatefulTestBackend, sql_statements};
use crate::provider::StorageBackend;

impl StatefulTestBackend {
    pub async fn observe(&self) -> Result<StatefulObservation, storage::StorageError> {
        // An unconfigured fixture observes as inactive: no peer activity has
        // staged anything yet.
        let Some(target) = self.target.lock().expect("stateful target lock").clone() else {
            return Ok(StatefulObservation {
                active: false,
                visible_items: Vec::new(),
                effect_count: 0,
                progress_revision: 0,
                generation: self.generation.load(Ordering::Relaxed),
            });
        };
        let database = self.database.as_ref().expect("fixture database is open");
        let gsi_target = self
            .gsi_target
            .lock()
            .expect("GSI fixture target lock")
            .clone();
        let active = if let Some(ref gsi_target) = gsi_target {
            database
                .get_gsi_index_readiness(
                    &gsi_target.table_name,
                    std::slice::from_ref(&gsi_target.index_name),
                )
                .await?
                .into_iter()
                .any(|(_, readiness)| {
                    matches!(readiness.index_status, IndexStatus::Active) && !readiness.backfilling
                })
        } else {
            database.table_exists(&target.table_name).await?
        };
        let progress_revision = match self.config.backend_type {
            StorageBackend::SQLite if gsi_target.is_none() => self
                .path
                .as_deref()
                .map(read_expiration_revision)
                .transpose()?
                .unwrap_or(0),
            _ => 0,
        };
        let generation = self.generation.load(Ordering::Relaxed);
        // GSI fixtures observe readiness here and verify membership separately
        // through Query. TTL effect observations require an enabled table
        // stream.
        if !active || gsi_target.is_some() {
            return Ok(StatefulObservation {
                active,
                visible_items: Vec::new(),
                effect_count: 0,
                progress_revision,
                generation,
            });
        }
        let item = database
            .get_item_decode::<_, storage_types::AttributeMap>(
                target.table_name.clone(),
                target.key,
            )
            .await?;
        let visible_items = item
            .and_then(|item| item.get("pk").cloned())
            .and_then(|value| match value {
                AttributeValue::S(value) => Some(value),
                _ => None,
            })
            .into_iter()
            .collect();
        let stream = database
            .get_stream_records_for_table_name(&target.table_name, None, Some(100))
            .await?;
        let effect_count = stream
            .records()
            .iter()
            .filter(|record| record.old_image.is_some() && record.new_image.is_none())
            .count() as u64;
        Ok(StatefulObservation {
            active,
            visible_items,
            effect_count,
            progress_revision,
            generation,
        })
    }
}

fn read_expiration_revision(path: &Path) -> Result<u64, storage::StorageError> {
    let connection = rusqlite::Connection::open(path)
        .map_err(|error| storage::StorageError::internal(&error))?;
    let revision = connection
        .query_row(
            sql_statements::expiration_progress_revision(),
            [storage_types::maintenance::MaintenanceKind::TtlSweep.storage_tag()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| storage::StorageError::internal(&error))?
        .unwrap_or(0);
    u64::try_from(revision).map_err(|error| storage::StorageError::internal(&error))
}
