use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use storage_types::{PairId, RegionId, TableName};
use tokio::net::TcpListener;

use crate::{
    client::RegionalPeerClient,
    config::RegionalConfig,
    message::{PairTable, RegionalRejection, RegionalRequest, RegionalResponse},
    server::{RegionalRequestHandler, spawn_server},
    transport::TransportError,
};

struct Counting(AtomicUsize);

#[async_trait]
impl RegionalRequestHandler for Counting {
    async fn handle(&self, _from: RegionId, _request: RegionalRequest) -> RegionalResponse {
        self.0.fetch_add(1, Ordering::SeqCst);
        RegionalResponse::Activated
    }
}

fn config(
    local: (&str, u16),
    peer: (&str, u16),
    peer_port: u16,
    timeout_ms: u64,
) -> Arc<RegionalConfig> {
    let json = format!(
        r#"{{"local_region":"{}","listen":"127.0.0.1:0","request_timeout_ms":{timeout_ms},"regions":[
            {{"name":"{}","rank":{},"address":"127.0.0.1:1"}},
            {{"name":"{}","rank":{},"address":"127.0.0.1:{peer_port}"}}]}}"#,
        local.0, local.0, local.1, peer.0, peer.1
    );
    Arc::new(RegionalConfig::from_json(&json).unwrap())
}

fn activate() -> RegionalRequest {
    RegionalRequest::Activate(PairTable {
        pair: PairId::from_bytes([1; 16]),
        table: TableName::new("t"),
    })
}

async fn bind() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

#[tokio::test]
async fn given_server_when_client_calls_then_handler_answers() {
    let (listener_b, port_b) = bind().await;
    let (_listener_a, port_a) = bind().await;
    let config_a = config(("a", 1), ("b", 2), port_b, 5_000);
    let config_b = config(("b", 2), ("a", 1), port_a, 5_000);
    let handler = Arc::new(Counting(AtomicUsize::new(0)));
    let server = spawn_server(listener_b, config_b, handler.clone());
    let response = RegionalPeerClient::new(config_a)
        .call(activate())
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::Activated),
        "{response:?}"
    );
    assert_eq!(handler.0.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn given_wrong_envelope_when_called_then_wrong_region_without_handler() {
    let (listener_b, port_b) = bind().await;
    let (_listener_a, port_a) = bind().await;
    let config_b = config(("b", 2), ("a", 1), port_a, 5_000);
    // A third region claims rank 3 and addresses B.
    let config_c = config(("c", 3), ("b", 2), port_b, 5_000);
    let handler = Arc::new(Counting(AtomicUsize::new(0)));
    let server = spawn_server(listener_b, config_b, handler.clone());
    let response = RegionalPeerClient::new(config_c)
        .call(activate())
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            RegionalResponse::Rejected(RegionalRejection::WrongRegion)
        ),
        "{response:?}"
    );
    assert_eq!(handler.0.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn given_silent_server_when_called_then_timeout() {
    let (listener, port) = bind().await;
    let accepter = tokio::spawn(async move {
        // Accept and hold the socket without answering.
        let held = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(held);
    });
    let client = RegionalPeerClient::new(config(("a", 1), ("b", 2), port, 200));
    let error = client.call(activate()).await.unwrap_err();
    assert!(matches!(error, TransportError::Timeout), "{error:?}");
    accepter.abort();
}
