use storage_provider::{DynamoPort, TableReclaimAttempt, TableReclaimRepository, TableReclaimStep};

use crate::test_support as fixture;

pub(crate) async fn reclaim_deleted_table(
    source: &fixture::Store,
    reclaimer: &impl TableReclaimRepository,
    physical: storage_types::TableHistoryId,
) {
    for _ in 0..512 {
        source
            .with_dynamo_schema_attempt(move |attempt| {
                Box::pin(async move {
                    if let Some(target) = attempt.load_reclaim_target().await? {
                        assert_eq!(
                            target.history_id, physical,
                            "cleanup selected another incarnation"
                        );
                    }
                    Ok(())
                })
            })
            .await
            .unwrap_or_else(|error| panic!("reclaim target observation: {:#?}", error.as_ref()));
        if reclaimer
            .reclaim_table_page()
            .await
            .unwrap_or_else(|error| panic!("reclaim page: {:#?}", error.as_ref()))
            == TableReclaimStep::Complete
        {
            return;
        }
    }
    panic!("bounded regional and native cleanup did not finish");
}
