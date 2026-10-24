use storage_types::context::WrappedError as _;

use super::stateful_test_support::StatefulTestBackend;

const REINCARNATED_BODY: &str = "queue-cleanup-incarnation-b";

#[tokio::test]
async fn given_live_queue_reference_when_message_cleanup_advances_then_reincarnation_survives() {
    if !cfg!(feature = "rocksdb") {
        return;
    }
    assert_live_queue_reference_survives_cleanup(
        StatefulTestBackend::rocksdb()
            .await
            .expect("construct RocksDB stateful fixture"),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn given_live_queue_reference_on_foundationdb_when_message_cleanup_advances_then_reincarnation_survives()
 {
    let fixture = StatefulTestBackend::foundationdb()
        .await
        .expect("construct FoundationDB stateful fixture")
        .expect("FoundationDB stateful fixture requires a configured cluster file");
    assert_live_queue_reference_survives_cleanup(fixture).await;
}

async fn assert_live_queue_reference_survives_cleanup(fixture: StatefulTestBackend) {
    fixture
        .establish_deleted_queue_payload("queue-cleanup-race")
        .await
        .expect("establish deleted queue payload");
    fixture
        .begin_queue_message_cleanup()
        .await
        .expect("advance cleanup into the selected queue family");
    fixture
        .reincarnate_queue_payload(REINCARNATED_BODY)
        .await
        .expect("write replacement payload through the selected queue provider");
    fixture
        .finish_queue_message_cleanup()
        .await
        .unwrap_or_else(|error| panic!("finish queue message cleanup: {:?}", error.to_enum()));

    assert_eq!(
        fixture
            .receive_queue_payload()
            .await
            .expect("receive replacement queue payload")
            .as_deref(),
        Some(REINCARNATED_BODY),
    );
}
