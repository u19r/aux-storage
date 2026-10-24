use std::{collections::HashMap, sync::Arc};

use storage_types::{AttributeValue, StorageError, StreamName, TimestampMillis};
use stream_provider::{
    BootstrapCursorName, CursorPosition, ProtectedCursorReadRequest, SystemStreamLineage,
    SystemStreamPageRequest, SystemStreamSplitChild, SystemStreamSplitOperationId,
    SystemStreamSplitRequest,
};

use super::{StatefulTestBackend, models::StreamReclamationFixtureTarget};
use crate::{AdmissionClass, database_manager::ROUTED_DEFAULT_CONNECTION_ID};

impl StatefulTestBackend {
    pub async fn establish_stream_reclamation_hold(
        &self,
        table_name: &str,
    ) -> Result<(), StorageError> {
        let table_name = self.create_table_with_stream(table_name, true).await?;
        for version in 1..=4 {
            let item = HashMap::from([
                ("pk".to_owned(), AttributeValue::S("A".to_owned())),
                ("version".to_owned(), AttributeValue::N(version.to_string())),
            ]);
            self.database
                .as_ref()
                .expect("fixture database is open")
                .put_item_encode(table_name.clone(), &item, None, None, None, None)
                .await?;
        }
        self.drive_gsi_until_idle().await?;

        let database = self.database.as_ref().expect("fixture database is open");
        let scope = database.get_system_stream_scope_for_control().await?;
        let page = database
            .get_system_stream_page_for_control(
                SystemStreamPageRequest::try_new(scope.clone(), None, 100)
                    .map_err(|error| StorageError::validation(error.to_string()))?,
            )
            .await?;
        if page.records().len() < 2 || !page.terminal() {
            return Err(StorageError::internal(
                "stream reclamation fixture did not establish a terminal protected frontier",
            ));
        }

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
        let cursor_name = BootstrapCursorName::try_new(
            table_name.as_ref(),
            "reclamation-contract",
            "generation-1",
        )
        .map_err(|error| StorageError::from(error.into_storage_enum()))?;
        let cursor = database
            .run_stream_admitted(ROUTED_DEFAULT_CONNECTION_ID, AdmissionClass::Write, {
                let cursor_name = cursor_name.clone();
                move |provider| async move {
                    provider
                        .create_protected_cursor(
                            StreamName::system_table_stream(),
                            cursor_name,
                            CursorPosition::Head,
                        )
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                }
            })
            .await?;
        database
            .run_stream_admitted(ROUTED_DEFAULT_CONNECTION_ID, AdmissionClass::Write, {
                let cursor_name = cursor_name.clone();
                move |stream| async move {
                    let replay = stream
                        .create_protected_cursor(
                            StreamName::system_table_stream(),
                            cursor_name.clone(),
                            CursorPosition::Tail,
                        )
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))?;
                    assert_eq!(
                        replay.position, cursor.position,
                        "hold replay must preserve its floor"
                    );
                    assert!(
                        stream
                            .create_cursor(
                                stream_provider::StreamRowKind::System(
                                    StreamName::system_table_stream()
                                ),
                                cursor_name.cursor_name().clone(),
                                CursorPosition::Head,
                            )
                            .await
                            .is_err(),
                        "ordinary cursors cannot acquire a protected identity"
                    );
                    let invalid = ProtectedCursorReadRequest::try_new(
                        StreamName::system_table_stream(),
                        cursor_name,
                        cursor.position,
                        storage_types::StreamItemId::from([255; 12]),
                        100,
                    )
                    .expect("valid request with stale authority");
                    assert!(
                        stream.read_from_protected_cursor(invalid).await.is_err(),
                        "a changed position cannot authorize a protected page"
                    );
                    Ok(())
                }
            })
            .await?;
        let cutoff_timestamp = TimestampMillis::now() + 60_000;
        *self
            .stream_reclamation_target
            .lock()
            .expect("stream reclamation target lock") = Some(StreamReclamationFixtureTarget {
            scope,
            parent,
            cursor_name,
            protected_boundary: cursor.position,
            cursor_position: cursor.position,
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

    pub async fn read_held_system_stream(&self) -> Result<usize, StorageError> {
        let (cursor_name, protected_boundary, cursor_position) = {
            let target = self
                .stream_reclamation_target
                .lock()
                .expect("stream reclamation target lock");
            let target = target
                .as_ref()
                .expect("stream reclamation target is configured");
            (
                target.cursor_name.clone(),
                target.protected_boundary,
                target.cursor_position,
            )
        };
        let request = ProtectedCursorReadRequest::try_new(
            StreamName::system_table_stream(),
            cursor_name,
            protected_boundary,
            cursor_position,
            100,
        )
        .map_err(|error| StorageError::from(error.into_storage_enum()))?;
        let page = self
            .database
            .as_ref()
            .expect("fixture database is open")
            .run_stream_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::PointRead,
                move |provider| async move {
                    provider
                        .read_from_protected_cursor(request)
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                },
            )
            .await?;
        Ok(page.items().len())
    }

    pub async fn release_stream_reclamation_hold(&self) -> Result<(), StorageError> {
        let cursor_name = self
            .stream_reclamation_target
            .lock()
            .expect("stream reclamation target lock")
            .as_ref()
            .expect("stream reclamation target is configured")
            .cursor_name
            .clone();
        let database = self.database.as_ref().expect("fixture database is open");
        let cursor_name_for_read = cursor_name.clone();
        database
            .run_stream_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::Write,
                move |provider| async move {
                    provider
                        .delete_protected_cursor(StreamName::system_table_stream(), cursor_name)
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                },
            )
            .await?;
        let remaining = database
            .run_stream_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::PointRead,
                move |provider| async move {
                    provider
                        .get_protected_cursor(
                            StreamName::system_table_stream(),
                            cursor_name_for_read,
                        )
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                },
            )
            .await?;
        if remaining.is_some() {
            return Err(StorageError::internal(
                "released stream reclamation hold remained visible",
            ));
        }
        Ok(())
    }

    pub async fn read_system_history_ids(
        &self,
    ) -> Result<Vec<storage_types::StreamItemId>, StorageError> {
        let page = self
            .database
            .as_ref()
            .expect("fixture database is open")
            .run_stream_admitted(
                ROUTED_DEFAULT_CONNECTION_ID,
                AdmissionClass::PointRead,
                |stream| async move {
                    stream
                        .read_forward(
                            stream_provider::StreamRowKind::System(
                                StreamName::system_table_stream(),
                            ),
                            None,
                            100,
                        )
                        .await
                        .map_err(|error| StorageError::from(error.into_storage_enum()))
                },
            )
            .await?;
        assert!(
            !page.has_more,
            "fixture snapshot must include every physical record"
        );
        Ok(page.items.into_iter().map(|item| item.id).collect())
    }

    pub async fn trim_released_system_stream(&self) -> Result<usize, StorageError> {
        // Active indexes are independent history consumers. They must
        // acknowledge the writes before releasing the last user cursor can
        // permit reclamation.
        let (scope, cutoff_timestamp) = {
            let target = self
                .stream_reclamation_target
                .lock()
                .expect("stream reclamation target lock");
            let target = target
                .as_ref()
                .expect("stream reclamation target is configured");
            (target.scope.clone(), target.cutoff_timestamp + 1)
        };
        self.database
            .as_ref()
            .expect("fixture database is open")
            .run_system_stream_trim_page(scope, cutoff_timestamp, 100, None)
            .await
            .map(|result| result.reclaimed_records())
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
            SystemStreamSplitOperationId::try_new("reclamation-hold-race")
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
