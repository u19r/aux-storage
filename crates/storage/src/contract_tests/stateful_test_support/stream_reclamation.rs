use std::{collections::HashMap, sync::Arc};

use storage_types::{AttributeValue, StorageError, TimestampMillis};
use stream_provider::{
    SystemStreamLineage, SystemStreamPageRequest, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use super::{StatefulTestBackend, models::StreamReclamationFixtureTarget};
use crate::{AdmissionClass, database_manager::ROUTED_DEFAULT_CONNECTION_ID};

impl StatefulTestBackend {
    pub async fn establish_stream_reclamation_checkpoint(
        &self,
        name: &str,
    ) -> Result<(), StorageError> {
        let table_name = self.create_table_with_stream(name, true).await?;
        let database = self.database.as_ref().expect("fixture database is open");
        for version in 1..=4 {
            let item = HashMap::from([
                ("pk".to_owned(), AttributeValue::S("A".to_owned())),
                ("version".to_owned(), AttributeValue::N(version.to_string())),
            ]);
            database
                .put_item_encode(table_name.clone(), &item, None, None, None, None)
                .await?;
        }
        self.drive_gsi_until_idle().await?;
        let scope = database.get_system_stream_scope_for_control().await?;
        let page = database
            .get_system_stream_page_for_control(
                SystemStreamPageRequest::try_new(scope.clone(), None, 1)
                    .map_err(|error| StorageError::validation(error.to_string()))?,
            )
            .await?;
        assert_eq!(page.records().len(), 1);
        assert!(!page.terminal());
        let checkpoint = page.issued_checkpoint().token().clone();
        let catalog = database
            .run_stream_admitted(ROUTED_DEFAULT_CONNECTION_ID, AdmissionClass::PointRead, {
                let scope = scope.clone();
                move |stream| async move {
                    stream
                        .system_stream_lineage_catalog(scope)
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                }
            })
            .await?;
        let parent = catalog
            .into_iter()
            .find(|lineage| !lineage.sealed() && lineage.parent_lineage_id().is_none())
            .ok_or_else(|| StorageError::internal("active root SystemStream lineage is missing"))?;
        // This native cleanup seam accepts its cutoff directly. Admit all four
        // fixture rows.
        let cutoff_timestamp = TimestampMillis::now() + 60_000;
        *self
            .stream_reclamation_target
            .lock()
            .expect("stream reclamation target lock") = Some(StreamReclamationFixtureTarget {
            scope,
            parent,
            checkpoint,
            table_name,
            cutoff_timestamp,
        });
        Ok(())
    }

    pub async fn race_stream_trim_and_split(&self) -> Result<usize, StorageError> {
        let (scope, parent, cutoff_timestamp) = {
            let target = self
                .stream_reclamation_target
                .lock()
                .expect("stream reclamation target lock");
            let target = target
                .as_ref()
                .expect("stream reclamation target is configured");
            (
                target.scope.clone(),
                target.parent.clone(),
                target.cutoff_timestamp,
            )
        };
        let request = Self::stream_reclamation_split_request(scope.clone(), parent)?;
        let start = Arc::new(tokio::sync::Barrier::new(2));
        let split_start = Arc::clone(&start);
        let trim_start = start;
        let database = self.database.as_ref().expect("fixture database is open");
        let split = async {
            split_start.wait().await;
            Self::execute_stream_split(database, request).await
        };
        let trim = async {
            trim_start.wait().await;
            database
                .run_system_stream_trim_page(scope, cutoff_timestamp, 100, None)
                .await
        };
        let (split_result, trim_result) = tokio::join!(split, trim);
        split_result?;
        Ok(trim_result?.reclaimed_records())
    }

    pub async fn read_checkpoint_system_stream(&self) -> Result<usize, StorageError> {
        let (scope, checkpoint) = {
            let target = self
                .stream_reclamation_target
                .lock()
                .expect("stream reclamation target lock");
            let target = target
                .as_ref()
                .expect("stream reclamation target is configured");
            (target.scope.clone(), target.checkpoint.clone())
        };
        self.database
            .as_ref()
            .expect("fixture database is open")
            .get_system_stream_page_for_control(
                SystemStreamPageRequest::try_new(scope, Some(checkpoint), 100)
                    .map_err(|error| StorageError::validation(error.to_string()))?,
            )
            .await
            .map(|page| page.records().len())
    }

    pub async fn read_system_history_positions(
        &self,
    ) -> Result<Vec<storage_types::SystemStreamPosition>, StorageError> {
        let database = self.database.as_ref().expect("fixture database is open");
        let scope = database.get_system_stream_scope_for_control().await?;
        let page = database
            .get_system_stream_page_for_control(
                SystemStreamPageRequest::try_new(scope, None, 100)
                    .map_err(|error| StorageError::validation(error.to_string()))?,
            )
            .await?;
        assert!(
            page.terminal(),
            "fixture snapshot must include every source record"
        );
        Ok(page
            .records()
            .iter()
            .map(|record| record.system_position.clone().expect("system position"))
            .collect())
    }

    pub async fn read_reclamation_table_records(&self) -> Result<usize, StorageError> {
        let table = self
            .stream_reclamation_target
            .lock()
            .expect("stream reclamation target lock")
            .as_ref()
            .expect("stream reclamation target is configured")
            .table_name
            .clone();
        self.database
            .as_ref()
            .expect("fixture database is open")
            .get_stream_records_for_table_name(&table, None, Some(100))
            .await
            .map(|page| page.records().len())
    }

    fn stream_reclamation_split_request(
        scope: storage_types::SystemStreamScope,
        parent: SystemStreamLineage,
    ) -> Result<SystemStreamSplitRequest, StorageError> {
        let parent_id = parent.lineage_id().as_str().to_owned();
        let children = [
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    "reclamation-left-shard",
                    "reclamation-left-lineage",
                    Some(parent_id.clone()),
                    false,
                )
                .map_err(|error| StorageError::validation(error.to_string()))?,
                1,
            )
            .map_err(|error| StorageError::validation(error.to_string()))?,
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    "reclamation-right-shard",
                    "reclamation-right-lineage",
                    Some(parent_id),
                    false,
                )
                .map_err(|error| StorageError::validation(error.to_string()))?,
                1,
            )
            .map_err(|error| StorageError::validation(error.to_string()))?,
        ];
        SystemStreamSplitRequest::try_new(
            scope,
            parent,
            children,
            SystemStreamSplitOperationId::try_new("reclamation-checkpoint-race")
                .map_err(|error| StorageError::validation(error.to_string()))?,
        )
        .map_err(|error| StorageError::validation(error.to_string()))
    }

    async fn execute_stream_split(
        database: &crate::DatabaseManager,
        request: SystemStreamSplitRequest,
    ) -> Result<(), StorageError> {
        database
            .run_stream_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::Write,
                move |stream| async move {
                    stream
                        .split_system_stream(request)
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                },
            )
            .await
            .map(|_| ())
    }
}
