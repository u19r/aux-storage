use std::{num::NonZeroU32, sync::Arc, time::Duration};

use async_trait::async_trait;
use regional_replication::{
    RegionalConfig, RegionalRequestHandler,
    message::{
        LineageRef, PageAck, PairTable, PrepareTarget, RegionalRequest, RegionalResponse,
        ReplicationPage, ScanPage,
    },
};
use storage_provider::{
    LineageCursor, RegionalBindingOperations, RegionalPairingOperations, RegionalReceiveOperations,
    RegionalSendOperations, RegionalUnpairOperations, ScanTurn, SendLimits, SendTurn, SweepTurn,
};
use storage_types::{
    PairId, PairState, RegionId, RegionalBinding, StorageResult, SystemStreamPosition, TableName,
};

use super::{
    PairJobCheckpoint, PairJobPayload, RegionalPair,
    pair_job::{RegionalPairHandler, TurnEnd},
};

/// No operation is reached by `observe_lag`; every method is a test failure.
/// With `send` set, the binding is Active and the sender answers one empty page
/// (`caught_up` as given).
#[derive(Default)]
struct NoOperations {
    send: Option<bool>,
    local_error: bool,
}

#[async_trait]
impl RegionalBindingOperations for NoOperations {
    async fn replace_regional_binding(
        &self,
        _: &TableName,
        _: Option<PairId>,
        _: Option<RegionalBinding>,
    ) -> StorageResult<bool> {
        unreachable!()
    }
}

#[async_trait]
impl RegionalReceiveOperations for NoOperations {
    async fn receive_page(
        &self,
        _: regional_replication::message::ReplicationPage,
    ) -> StorageResult<RegionalResponse> {
        unreachable!()
    }
}

#[async_trait]
impl RegionalSendOperations for NoOperations {
    async fn next_send_page(
        &self,
        table: &TableName,
        pair: PairId,
        _: u32,
        _: SendLimits,
    ) -> StorageResult<SendTurn> {
        let Some(caught_up) = self.send else {
            unreachable!()
        };
        let position = SystemStreamPosition::try_new("shard", "lineage", "0").unwrap();
        Ok(SendTurn::Page {
            page: Box::new(ReplicationPage {
                pair,
                table: table.clone(),
                lineage: LineageRef {
                    shard: position.shard_id().clone(),
                    lineage: position.lineage_id().clone(),
                },
                parents: Vec::new(),
                after: None,
                through: None,
                sealed: false,
                entries: Vec::new(),
            }),
            next_lane: 0,
            lag_millis: 0,
            caught_up,
        })
    }

    async fn store_send_progress(
        &self,
        _: &TableName,
        _: PairId,
        _: LineageRef,
        _: LineageCursor,
        _: bool,
    ) -> StorageResult<()> {
        if self.send.is_some() {
            Ok(())
        } else {
            unreachable!()
        }
    }
}

#[async_trait]
impl RegionalPairingOperations for NoOperations {
    async fn begin_pairing(
        &self,
        _: &TableName,
        _: RegionId,
        _: RegionId,
        _: &str,
    ) -> StorageResult<PairId> {
        unreachable!()
    }

    async fn prepare_request(
        &self,
        _: &TableName,
        _: PairId,
    ) -> StorageResult<Option<PrepareTarget>> {
        unreachable!()
    }

    async fn abandon_pairing(&self, _: &TableName, _: PairId) -> StorageResult<bool> {
        unreachable!()
    }

    async fn prepare_target(
        &self,
        _: PrepareTarget,
        _: RegionId,
        _: RegionId,
        _: &str,
    ) -> StorageResult<RegionalResponse> {
        unreachable!()
    }

    async fn regional_binding(
        &self,
        _: &TableName,
        pair: PairId,
    ) -> StorageResult<Option<RegionalBinding>> {
        if self.local_error {
            return Err(storage_types::StorageError::internal(
                "local store unavailable",
            ));
        }
        if self.send.is_none() {
            unreachable!()
        }
        Ok(Some(RegionalBinding {
            pair,
            local: RegionId(std::num::NonZeroU16::new(1).unwrap()),
            peer: RegionId(std::num::NonZeroU16::new(2).unwrap()),
            state: PairState::Active,
            peer_name: "b".to_owned(),
        }))
    }

    async fn advance_pair_state(
        &self,
        _: &TableName,
        _: PairId,
        _: PairState,
        _: PairState,
    ) -> StorageResult<Option<PairState>> {
        unreachable!()
    }

    async fn next_scan_page(
        &self,
        _: &TableName,
        _: PairId,
        _: Option<String>,
        _: NonZeroU32,
    ) -> StorageResult<ScanTurn> {
        unreachable!()
    }

    async fn apply_scan_page(&self, _: ScanPage) -> StorageResult<RegionalResponse> {
        unreachable!()
    }

    async fn activate_target(&self, _: PairTable) -> StorageResult<RegionalResponse> {
        unreachable!()
    }

    async fn sweep_received_markers(
        &self,
        _: &TableName,
        _: PairId,
        _: Option<Vec<u8>>,
        _: NonZeroU32,
    ) -> StorageResult<SweepTurn> {
        unreachable!()
    }
}

#[async_trait]
impl RegionalUnpairOperations for NoOperations {
    async fn current_binding(&self, _: &TableName) -> StorageResult<Option<RegionalBinding>> {
        unreachable!()
    }

    async fn begin_leave(&self, _: &PairTable) -> StorageResult<Option<PairState>> {
        unreachable!()
    }

    async fn history_heads(
        &self,
        _: &TableName,
        _: PairId,
    ) -> StorageResult<Option<Vec<regional_replication::message::LineageHead>>> {
        unreachable!()
    }

    async fn finish_pair(
        &self,
        _: regional_replication::message::FinishPair,
    ) -> StorageResult<RegionalResponse> {
        unreachable!()
    }

    async fn force_unpair(&self, _: &TableName, _: Option<PairId>) -> StorageResult<bool> {
        unreachable!()
    }
}

fn handler() -> RegionalPairHandler {
    let regions = r#"[{"name":"a","rank":1,"address":"a.example:7000"},{"name":"b","rank":2,"address":"b.example:7000"}]"#;
    let json = format!(r#"{{"local_region":"a","listen":"127.0.0.1:7000","regions":{regions}}}"#);
    let mut config = RegionalConfig::from_json(&json).unwrap();
    config.lag_alert = Duration::from_millis(1_000);
    let config = Arc::new(config);
    RegionalPairHandler::new(&RegionalPair::new(
        "test",
        config,
        Arc::new(NoOperations::default()),
    ))
}

#[test]
fn given_lag_crossings_then_alert_flag_toggles_once() {
    let handler = handler();
    let job = PairJobPayload {
        table: TableName::new("orders"),
        pair: PairId::from_bytes([1; 16]),
    };
    let mut checkpoint = PairJobCheckpoint::default();
    let mut flags = Vec::new();
    // Below, above, still above, equal to the threshold (not above), above
    // again, then idle.
    for lag in [0, 5_000, 6_000, 1_000, 2_000, 0] {
        handler.observe_lag(&job, &mut checkpoint, lag);
        flags.push(checkpoint.lag_alerting);
    }
    assert_eq!(flags, [false, true, true, false, true, false]);
}

/// The peer acknowledges every page.
struct AckingPeer;

#[async_trait]
impl RegionalRequestHandler for AckingPeer {
    async fn handle(&self, _: RegionId, _: RegionalRequest) -> RegionalResponse {
        RegionalResponse::Ack(PageAck {
            through: None,
            sealed: false,
        })
    }
}

/// Handler of region `a` whose peer `b` is a loopback server that acknowledges
/// pages.
async fn handler_with_acking_peer(
    caught_up: bool,
) -> (RegionalPairHandler, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_port = listener.local_addr().unwrap().port();
    let json = |local: &str, own: u16, other: u16| {
        let (other_name, other_rank, rank) = if local == "a" {
            ("b", 2, 1)
        } else {
            ("a", 1, 2)
        };
        format!(
            r#"{{"local_region":"{local}","listen":"127.0.0.1:0","send_interval_ms":750,"regions":[
                {{"name":"{local}","rank":{rank},"address":"127.0.0.1:{own}"}},
                {{"name":"{other_name}","rank":{other_rank},"address":"127.0.0.1:{other}"}}]}}"#
        )
    };
    let config_a = Arc::new(RegionalConfig::from_json(&json("a", 1, peer_port)).unwrap());
    let config_b = Arc::new(RegionalConfig::from_json(&json("b", peer_port, 1)).unwrap());
    let server = regional_replication::spawn_server(listener, config_b, Arc::new(AckingPeer));
    let handler = RegionalPairHandler::new(&RegionalPair::new(
        "test",
        config_a,
        Arc::new(NoOperations {
            send: Some(caught_up),
            ..NoOperations::default()
        }),
    ));
    (handler, server)
}

#[tokio::test]
async fn given_empty_page_that_reached_the_head_when_acknowledged_then_next_turn_waits_send_interval()
 {
    let (handler, server) = handler_with_acking_peer(true).await;
    let job = PairJobPayload {
        table: TableName::new("orders"),
        pair: PairId::from_bytes([1; 16]),
    };
    let end = handler
        .send_turn(&job, &mut PairJobCheckpoint::default())
        .await
        .unwrap();
    server.abort();
    assert!(
        matches!(end, TurnEnd::Again(delay) if delay == handler.pair.config.send_interval),
        "idle waits the interval"
    );
}

#[tokio::test]
async fn given_empty_page_that_did_not_reach_the_head_when_acknowledged_then_next_turn_is_immediate()
 {
    let (handler, server) = handler_with_acking_peer(false).await;
    let job = PairJobPayload {
        table: TableName::new("orders"),
        pair: PairId::from_bytes([1; 16]),
    };
    let end = handler
        .send_turn(&job, &mut PairJobCheckpoint::default())
        .await
        .unwrap();
    server.abort();
    assert!(
        matches!(end, TurnEnd::Again(delay) if delay.is_zero()),
        "work remains: no delay"
    );
}

#[tokio::test]
async fn given_local_errors_when_turns_settle_then_local_backoff_grows_resets_and_peer_count_is_untouched()
 {
    let regions = r#"[{"name":"a","rank":1,"address":"a.example:7000"},{"name":"b","rank":2,"address":"b.example:7000"}]"#;
    let json = format!(r#"{{"local_region":"a","listen":"127.0.0.1:7000","regions":{regions}}}"#);
    let config = Arc::new(RegionalConfig::from_json(&json).unwrap());
    let handler = RegionalPairHandler::new(&RegionalPair::new(
        "test",
        config,
        Arc::new(NoOperations {
            local_error: true,
            ..NoOperations::default()
        }),
    ));
    let job = PairJobPayload {
        table: TableName::new("orders"),
        pair: PairId::from_bytes([1; 16]),
    };
    let mut checkpoint = PairJobCheckpoint {
        peer_failures: 3,
        ..PairJobCheckpoint::default()
    };
    let failures = || {
        handler
            .pair
            .local_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    for (expected, count) in [
        (Duration::from_millis(250), 1),
        (Duration::from_millis(500), 2),
        (Duration::from_millis(1000), 3),
    ] {
        let turn = handler.turn(&job, &mut checkpoint).await;
        assert!(turn.is_err());
        let end = handler.settle_turn(&job, turn);
        assert!(matches!(end, TurnEnd::Again(d) if d == expected));
        assert_eq!(failures(), count);
        assert_eq!(checkpoint.peer_failures, 3);
    }
    let end = handler.settle_turn(&job, Ok(TurnEnd::Again(Duration::ZERO)));
    assert!(matches!(end, TurnEnd::Again(d) if d.is_zero()));
    assert_eq!(failures(), 0);
    let turn = handler.turn(&job, &mut checkpoint).await;
    let end = handler.settle_turn(&job, turn);
    assert!(matches!(end, TurnEnd::Again(d) if d == Duration::from_millis(250)));
    assert_eq!(failures(), 1);
}
