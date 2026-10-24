#![cfg(all(feature = "regional", feature = "test-support"))]

use anyhow::{Context, Result};
use storage::{
    DatabaseManagerRuntimeOptions,
    regional_test_support::{
        RegionalFixture,
        archive::{archive_namespace, await_archive_handoff, await_archived_original},
        await_item, put,
    },
};

#[tokio::test]
async fn given_canonical_history_when_archive_fails_then_hot_evidence_survives_and_verified_handoff_rejects_missing_or_corrupt_objects_across_reopen()
-> Result<()> {
    let mut fixture = RegionalFixture::start_with_options(
        (65536, 128, None),
        DatabaseManagerRuntimeOptions::default(),
        true,
    )
    .await?;
    let result = async {
        fixture.create_global_table().await?;
        let root = fixture.directory.path().join("archive-1");
        let namespace = archive_namespace(&root)?;
        let held = fixture.directory.path().join("unavailable-archive");
        std::fs::rename(&namespace, &held)?;
        put(&fixture.managers[0], "archive-history", "first").await?;
        await_item(&fixture.managers[1], "archive-history", "first").await?;
        put(&fixture.managers[0], "archive-history", "second").await?;
        await_item(&fixture.managers[1], "archive-history", "second").await?;
        let originals = storage::regional_test_support::history::read_originals(
            &fixture.managers[0],
            1,
            1,
            "archive-history",
        )
        .await?;
        anyhow::ensure!(
            originals.len() == 2,
            "failed archive discarded winner or overwritten original"
        );
        anyhow::ensure!(
            storage::regional_test_support::history::read_originals(
                &fixture.managers[1],
                2,
                1,
                "archive-history"
            )
            .await?
                == originals,
            "archive failure changed delivered originals"
        );
        std::fs::rename(&held, &namespace)?;
        let object = await_archived_original(&namespace, originals[0].as_bytes()).await?;
        // Object publication alone is insufficient: a failed public read proves
        // hot fallback retired.
        let unavailable = object.with_extension("unavailable");
        await_archive_handoff(
            &fixture,
            (1, "archive-history", "second"),
            (&object, &unavailable),
        )
        .await?;
        require_originals(&fixture, &originals).await?;
        std::fs::write(&object, vec![b'!'; originals[0].len()])?;
        let corrupt = storage::regional_test_support::history::read_originals(
            &fixture.managers[0],
            1,
            1,
            "archive-history",
        )
        .await
        .err()
        .context("public history accepted corrupt archived canonical bytes")?;
        let corrupt = corrupt
            .downcast_ref::<storage_types::StorageError>()
            .context("corrupt archive failed outside its storage boundary")?;
        anyhow::ensure!(
            matches!(
                corrupt.as_ref(),
                storage_types::StorageEnum::InfrastructureResult(
                    storage_types::InfrastructureResultKind::Encoding {
                        context: storage_types::InfrastructureContext::PublicationHistory
                    }
                )
            ),
            "corrupt archive returned the wrong error: {corrupt:?}"
        );
        std::fs::write(&object, originals[0].as_bytes())?;
        require_originals(&fixture, &originals).await?;
        fixture.restart().await?;
        require_originals(&fixture, &originals).await?;
        put(&fixture.managers[1], "archive-history", "after-reopen").await?;
        await_item(&fixture.managers[0], "archive-history", "after-reopen").await?;
        require_originals(&fixture, &originals).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    result.and(fixture.stop().await)
}

async fn require_originals(fixture: &RegionalFixture, originals: &[String]) -> Result<()> {
    for (region, manager) in fixture.managers.iter().enumerate() {
        anyhow::ensure!(
            storage::regional_test_support::history::read_originals(
                manager,
                region + 1,
                1,
                "archive-history"
            )
            .await?
                == originals,
            "archive handoff or reopen changed exact originals"
        );
    }
    Ok(())
}
