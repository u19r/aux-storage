mod history;
mod process;
mod request;

use std::time::Duration;

use anyhow::{Context, Result};
use process::HttpFixture;
use serde_json::json;

const TABLE: &str = "regional_http_slice";

#[tokio::test]
async fn given_invalid_operator_budgets_when_api_process_starts_then_http_and_regional_intake_remain_unpublished()
-> Result<()> {
    process::require_invalid_startup_unpublished().await
}

#[tokio::test]
async fn given_independent_http_regions_when_wan_latency_ack_loss_and_source_death_occur_then_items_indexes_and_exact_originals_converge()
-> Result<()> {
    let mut fixture = HttpFixture::start().await?;
    let result = async {
        fixture.regions[0].request("CreateTable", json!({"TableName": "ordinary_during_enable", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]})).await?;
        fixture.proxy.toxic("bootstrap-wait", "timeout", "upstream", json!({"timeout": 0})).await?;
        let reservation_connections = fixture.proxy.connections()?;
        fixture.regions[0].enable().await?;
        fixture.proxy.await_connection(reservation_connections).await.context("held target reservation")?;
        fixture.regions[1].require_hidden().await?;
        let (status, conflict) = fixture.regions[0].response("UpdateTable", json!({"TableName": TABLE,
            "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]})).await?;
        anyhow::ensure!(status == reqwest::StatusCode::BAD_REQUEST && conflict["message"] == "table configuration control is in progress", "conflicting pending enable returned {status}: {conflict}");
        fixture.regions[0].put("captured", "before-restart").await?;
        fixture.regions[0].put("captured", "after-overwrite").await?;
        fixture.regions[0].put("deleted-before-capture", "must-not-resurrect").await?;
        fixture.regions[0].request("DeleteItem", json!({"TableName": TABLE, "Key": {"pk": {"S": "deleted-before-capture"}}})).await?;
        fixture.regions[0].request("PutItem", json!({"TableName": "ordinary_during_enable", "Item": request::item("ordinary", "while-bootstrap-waits")})).await?;
        fixture.crash_and_restart(0, false).await?;
        fixture.crash_and_restart(1, false).await?;
        fixture.regions[1].require_hidden().await?;
        fixture.regions[0].await_item("captured", "after-overwrite").await?;
        let ordinary = fixture.regions[0].request("GetItem", json!({"TableName": "ordinary_during_enable", "Key": {"pk": {"S": "ordinary"}}, "ConsistentRead": true})).await?;
        anyhow::ensure!(ordinary["Item"] == request::item("ordinary", "while-bootstrap-waits"), "ordinary table stopped or lost its accepted write during pending enable/restart");
        fixture.proxy.latency(150).await?;
        fixture.proxy.remove("bootstrap-wait").await?;
        let capture_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            fixture.regions[0].put("capture-window", "during-capture").await?;
            if !fixture.regions[0].originals(1, "capture-window").await?.is_empty() { break; }
            anyhow::ensure!(tokio::time::Instant::now() < capture_deadline, "source did not enter canonical capture before activation");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        fixture.proxy.toxic("capture-wait", "timeout", "upstream", json!({"timeout": 0})).await?;
        fixture.regions[1].require_hidden().await?;
        fixture.regions[0].put("capture-window", "after-capture-overwrite").await?;
        fixture.regions[0].put("capture-delete", "deleted-during-capture").await?;
        fixture.regions[0].request("DeleteItem", json!({"TableName": TABLE, "Key": {"pk": {"S": "capture-delete"}}})).await?;
        fixture.regions[0].request("PutItem", json!({"TableName": "ordinary_during_enable", "Item": request::item("ordinary", "while-capture-waits")})).await?;
        let captured = fixture.regions[0].originals(1, "capture-window").await?;
        let capture_deleted = fixture.regions[0].originals(1, "capture-delete").await?;
        anyhow::ensure!(captured.len() >= 2 && capture_deleted.len() == 2, "capture mutations did not retain exact authoring history");
        fixture.crash_and_restart(0, false).await?;
        fixture.crash_and_restart(1, false).await?;
        fixture.regions[1].require_hidden().await?;
        let ordinary = fixture.regions[0].request("GetItem", json!({"TableName": "ordinary_during_enable", "Key": {"pk": {"S": "ordinary"}}, "ConsistentRead": true})).await?;
        anyhow::ensure!(ordinary["Item"] == request::item("ordinary", "while-capture-waits"), "ordinary table lost its accepted write during held capture/restart");
        fixture.regions[0].request("PutItem", json!({"TableName": "ordinary_during_enable", "Item": request::item("ordinary", "after-capture-restart")})).await?;
        anyhow::ensure!(fixture.regions[0].originals(1, "capture-window").await? == captured
            && fixture.regions[0].originals(1, "capture-delete").await? == capture_deleted, "capture restart rewrote accepted originals");
        for (action, body) in [
            ("UpdateTable", json!({"TableName": TABLE, "BillingMode": "PAY_PER_REQUEST"})),
            ("UpdateTimeToLive", json!({"TableName": TABLE, "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}})),
        ] {
            let (status, conflict) = fixture.regions[0].response(action, body).await?;
            anyhow::ensure!(status == reqwest::StatusCode::BAD_REQUEST && conflict["message"] == "table configuration control is in progress", "conflicting capture control {action} returned {status}: {conflict}");
        }
        fixture.proxy.remove("capture-wait").await?;
        for direction in ["upstream", "downstream"] { fixture.proxy.remove(&format!("latency-{direction}")).await?; }
        fixture.regions[1].await_item("baseline", "before-enable").await?;
        fixture.regions[1].await_item("captured", "after-overwrite").await?;
        fixture.regions[1].await_item("capture-window", "after-capture-overwrite").await?;
        fixture.regions[1].await_originals(1, "capture-window", &captured).await?;
        fixture.regions[1].await_originals(1, "capture-delete", &capture_deleted).await?;
        let ordinary = fixture.regions[0].request("GetItem", json!({"TableName": "ordinary_during_enable", "Key": {"pk": {"S": "ordinary"}}, "ConsistentRead": true})).await?;
        anyhow::ensure!(ordinary["Item"] == request::item("ordinary", "after-capture-restart"), "activation changed an unrelated ordinary table");
        for key in ["captured", "deleted-before-capture"] {
            let originals = fixture.regions[0].originals(1, key).await?;
            anyhow::ensure!(originals.is_empty(), "pending reservation fabricated pre-capture originals for {key}");
            fixture.regions[1].await_originals(1, key, &originals).await?;
        }
        for region in &fixture.regions {
            let absent = region.request("GetItem", json!({"TableName": TABLE, "Key": {"pk": {"S": "deleted-before-capture"}}, "ConsistentRead": true})).await?;
            anyhow::ensure!(absent.get("Item").is_none() && region.query("must-not-resurrect").await?.is_empty(), "interrupted bootstrap resurrected a pre-capture deletion or its projection");
            anyhow::ensure!(region.query("before-restart").await?.is_empty(), "interrupted bootstrap retained an overwritten projection");
            let absent = region.request("GetItem", json!({"TableName": TABLE, "Key": {"pk": {"S": "capture-delete"}}, "ConsistentRead": true})).await?;
            anyhow::ensure!(absent.get("Item").is_none() && region.query("deleted-during-capture").await?.is_empty()
                && region.query("during-capture").await?.is_empty(), "capture recovery resurrected a deletion or stale projection");
        }
        for latency in [20, 75, 150] {
            fixture.proxy.latency(latency).await?;
            let value = format!("latency-{latency}");
            let started = std::time::Instant::now();
            fixture.regions[0].put("network", &value).await?;
            fixture.regions[1].await_item("network", &value).await?;
            anyhow::ensure!(started.elapsed() >= Duration::from_millis(latency), "HTTP mutation delivery bypassed Toxiproxy");
            for direction in ["upstream", "downstream"] { fixture.proxy.remove(&format!("latency-{direction}")).await?; }
        }
        for region in &fixture.regions { anyhow::ensure!(region.query("latency-20").await?.is_empty() && region.query("latency-75").await?.is_empty(), "overwrite retained an old GSI projection"); }
        fixture.proxy.toxic("lost-ack", "timeout", "downstream", json!({"timeout": 0})).await?;
        fixture.regions[0].put("network", "ack-lost").await?;
        fixture.regions[1].await_item("network", "ack-lost").await?;
        let accepted = fixture.regions[0].originals(1, "network").await?;
        anyhow::ensure!(accepted.len() == 4, "four HTTP mutations did not author exactly four originals");
        fixture.regions[1].await_originals(1, "network", &accepted).await?;
        fixture.crash_and_restart(0, false).await?;
        fixture.proxy.remove("lost-ack").await?;
        fixture.regions[0].await_item("network", "ack-lost").await?;
        anyhow::ensure!(fixture.regions[0].originals(1, "network").await? == accepted, "source restart changed accepted originals");
        // Dropping reply bytes alone still propagates the server's EOF.
        fixture.reverse_proxy.toxic("target-held-close", "slow_close", "downstream", json!({"delay": 10_000})).await?;
        fixture.reverse_proxy.toxic("target-lost-ack", "timeout", "downstream", json!({"timeout": 0})).await?;
        let reverse_connections = fixture.reverse_proxy.connections()?;
        fixture.regions[1].put("leased-target", "before-target-death").await?;
        let authored_target = fixture.regions[1].originals(2, "leased-target").await?;
        anyhow::ensure!(authored_target.len() == 1, "accepted target write did not author exactly one original");
        if let Err(error) = fixture.regions[0].await_imported_item(2, "leased-target", "before-target-death", &authored_target).await {
            fixture.crash_region(1)?;
            let delivery_lease = fixture.require_persisted_delivery_lease(1).await;
            anyhow::bail!("target original accepted while its delivery ACK is held: {error:#}; reverse connections before={reverse_connections}, after={:?}; authored originals={}; persisted delivery lease after crash={delivery_lease:?}", fixture.reverse_proxy.connections(), authored_target.len());
        }
        fixture.crash_and_restart(1, true).await?;
        fixture.reverse_proxy.remove("target-lost-ack").await?;
        fixture.reverse_proxy.remove("target-held-close").await?;
        fixture.regions[0].await_item("leased-target", "before-target-death").await?;
        let target_originals = fixture.regions[1].originals(2, "leased-target").await?;
        anyhow::ensure!(target_originals == authored_target, "target death rewrote its accepted original");
        fixture.regions[0].await_originals(2, "leased-target", &target_originals).await?;
        fixture.regions[1].put("network", "remote-after-restart").await?;
        fixture.regions[0].await_item("network", "remote-after-restart").await?;
        let remote = fixture.regions[1].originals(2, "network").await?;
        anyhow::ensure!(remote.len() == 1, "remote mutation did not author exactly one original");
        fixture.regions[0].await_originals(2, "network", &remote).await?;
        fixture.regions[1].await_originals(1, "network", &accepted).await?;
        fixture.proxy.toxic("delayed-loser", "timeout", "upstream", json!({"timeout": 0})).await?;
        fixture.regions[0].put("conflict", "older").await?;
        let older = fixture.regions[0].originals(1, "conflict").await?;
        anyhow::ensure!(older.len() == 1, "delayed source mutation did not retain its original");
        tokio::time::sleep(Duration::from_millis(25)).await;
        fixture.regions[1].put("conflict", "newer").await?;
        fixture.regions[0].await_item("conflict", "newer").await?;
        fixture.proxy.remove("delayed-loser").await?;
        fixture.regions[1].await_originals(1, "conflict", &older).await?;
        fixture.regions[1].await_item("conflict", "newer").await?;
        for region in &fixture.regions { anyhow::ensure!(region.query("older").await?.is_empty(), "losing original replaced a winning projection"); }
        let newer = fixture.regions[1].originals(2, "conflict").await?;
        anyhow::ensure!(newer.len() == 1, "winning mutation did not retain its original");
        fixture.regions[0].await_originals(2, "conflict", &newer).await?;
        let (status, response) = fixture.regions[0].response("GetCanonicalHistory", json!({"unknown": true})).await?;
        anyhow::ensure!(status == reqwest::StatusCode::BAD_REQUEST && response["__type"].as_str().is_some_and(|kind| kind.ends_with("ValidationException")), "malformed public history input returned {status}: {response}");
        let (status, response) = fixture.regions[0].response("GetCanonicalHistory", json!({"serving_region": "region-2", "serving_group": vec![2; 16], "table": TABLE, "after": null})).await?;
        anyhow::ensure!(status == reqwest::StatusCode::BAD_REQUEST && response["__type"].as_str().is_some_and(|kind| kind.ends_with("ValidationException")) && response["message"] == "History serving database does not match", "serving receiver mismatch returned {status}: {response}");
        Ok(())
    }.await;
    fixture.finish(result).await
}
