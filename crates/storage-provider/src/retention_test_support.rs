//! Native fixtures seed missing policy state; one contract checks worker
//! cleanup and reopen.
use storage_types::{StorageResult, TimestampMillis};

use crate::{
    StorageProvider, StreamDurationTrimConfig, StreamDurationTrimWorker, StreamRetentionRepository,
    StreamTrimDueMarker, StreamTrimPageLimit,
};

pub struct MissingPolicyMarker {
    marker: StreamTrimDueMarker,
    catalog: serde_json::Value,
}

impl MissingPolicyMarker {
    pub async fn reclaim(
        provider: &(impl StorageProvider + StreamRetentionRepository),
    ) -> StorageResult<Self> {
        let due = TimestampMillis::from_timestamp(i64::MAX);
        let markers = provider.list_due_stream_trim_markers(due, 2).await?;
        assert_eq!(
            markers.len(),
            1,
            "fixture must have exactly one orphaned marker"
        );
        let marker = markers.into_iter().next().expect("marker");
        assert!(
            provider
                .load_stream_trim_states(std::slice::from_ref(&marker.scope))
                .await?
                == vec![None]
        );
        let catalog =
            serde_json::to_value(provider.get_table_info(&marker.scope.table_name).await?)
                .expect("catalog JSON");
        let stats = StreamDurationTrimWorker::new(
            provider,
            StreamDurationTrimConfig {
                marker_page_size: StreamTrimPageLimit::try_from(1usize)?,
                stream_page_size: StreamTrimPageLimit::try_from(1usize)?,
            },
        )
        .run_due_page(due, TimestampMillis::now())
        .await?;
        assert_eq!(stats.due_markers_scanned, 1);
        assert_eq!(stats.missing_states, 1);
        assert_eq!(stats.state_writes, 0);
        assert_eq!(stats.rows_deleted, 0);
        let proof = Self { marker, catalog };
        proof.verify(provider).await?;
        Ok(proof)
    }

    pub async fn verify(
        &self,
        provider: &(impl StorageProvider + StreamRetentionRepository),
    ) -> StorageResult<()> {
        assert!(
            provider
                .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 2)
                .await?
                .is_empty(),
            "orphaned marker must remain removed after reopen"
        );
        assert!(
            provider
                .load_stream_trim_states(std::slice::from_ref(&self.marker.scope))
                .await?
                == vec![None],
            "cleanup must not recreate policy state"
        );
        assert_eq!(
            serde_json::to_value(
                provider
                    .get_table_info(&self.marker.scope.table_name)
                    .await?
            )
            .expect("catalog JSON"),
            self.catalog,
            "cleanup must preserve table metadata"
        );
        Ok(())
    }
}
