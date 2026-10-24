use std::{
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use replication_provider::runtime_configuration::deployment::{
    DeploymentConfiguration, PeerConfiguration, QualifiedConfiguration, TimingConfiguration,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{
    codec,
    history_message::{HistoryCaptureAction, HistoryCaptureMessage},
    message::WireIdentity,
    seal_message::{SealAction, SealMessage},
};
use crate::{
    history_capture::HistoryCaptureRequest, lifecycle::SealRequest, test_support as fixture,
};

pub(super) struct Proxy {
    child: Child,
    api: String,
    client: reqwest::Client,
    pub endpoint: SocketAddr,
}

impl Proxy {
    pub async fn start(upstream: SocketAddr) -> Self {
        let api_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api_address = api_listener.local_addr().unwrap();
        drop(api_listener);
        let child = Command::new("toxiproxy-server")
            .args([
                "-host",
                "127.0.0.1",
                "-port",
                &api_address.port().to_string(),
                "-seed",
                "70405",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("install toxiproxy-server before running the regional network recipe");
        let mut proxy = Self {
            child,
            api: format!("http://{api_address}"),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            endpoint: upstream,
        };
        proxy.ready().await;
        let created = proxy.client.post(format!("{}/proxies", proxy.api)).json(&json!({"name": "regional", "listen": "127.0.0.1:0", "upstream": upstream.to_string(), "enabled": true})).send().await.unwrap().error_for_status().unwrap().json::<Value>().await.unwrap();
        proxy.endpoint = created["listen"].as_str().unwrap().parse().unwrap();
        assert_ne!(proxy.endpoint.port(), 0);
        proxy
    }

    async fn ready(&mut self) {
        for _ in 0..100 {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "Toxiproxy exited before readiness"
            );
            if self
                .client
                .get(format!("{}/version", self.api))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("Toxiproxy did not become ready");
    }

    pub async fn latency(&self, milliseconds: u64, jitter: u64) {
        for stream in ["upstream", "downstream"] {
            self.create_toxic(
                &format!("latency-{stream}"),
                "latency",
                stream,
                json!({"latency": milliseconds, "jitter": jitter}),
            )
            .await;
        }
    }

    pub async fn create_toxic(&self, name: &str, kind: &str, stream: &str, attributes: Value) {
        self.client.post(format!("{}/proxies/regional/toxics", self.api)).json(&json!({"name": name, "type": kind, "stream": stream, "toxicity": 1.0, "attributes": attributes})).send().await.unwrap().error_for_status().unwrap();
    }

    pub async fn remove_toxic(&self, name: &str) {
        self.client
            .delete(format!("{}/proxies/regional/toxics/{name}", self.api))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    pub async fn clear_latency(&self) {
        for stream in ["upstream", "downstream"] {
            self.remove_toxic(&format!("latency-{stream}")).await;
        }
    }

    pub async fn enabled(&self, enabled: bool) {
        self.client
            .post(format!("{}/proxies/regional", self.api))
            .json(&json!({"enabled": enabled}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(crate) fn configuration(
    index: u8,
    endpoint: SocketAddr,
    peer: SocketAddr,
) -> Arc<QualifiedConfiguration> {
    let limits = (*fixture::limits()).clone();
    let settings = DeploymentConfiguration {
        deployment: [8; 16].try_into().unwrap(),
        tenant: [7; 16].try_into().unwrap(),
        region: fixture::region(index),
        group: [index; 16].try_into().unwrap(),
        replica: [index; 16].try_into().unwrap(),
        journal_partitions: fixture::partitions(),
        journal_generation: 1.into(),
        endpoint,
        archive_root: None,
        time_trust: None,
        peers: vec![PeerConfiguration {
            replica: [3 - index; 16].try_into().unwrap(),
            region: fixture::region(3 - index),
            group: [3 - index; 16].try_into().unwrap(),
            generation: 1.into(),
            endpoint: peer,
        }],
        limits,
        backlog: fixture::backlog(),
        timing: TimingConfiguration {
            retry_min_ms: 5,
            retry_max_ms: 1000,
            clock_alert_ms: 1000.try_into().unwrap(),
            request_timeout_ms: 2000,
        },
    };
    Arc::new(settings.qualify(fixture::budgets()).unwrap())
}

pub(super) async fn reject_identity(
    endpoint: SocketAddr,
    import: &replication_types::regional_import::RegionalImport,
    from: super::message::WireIdentity,
    to: super::message::WireIdentity,
) {
    let frame = super::client::encode_page(import, (from, to), 1_048_576).unwrap();
    let socket = tokio::net::TcpStream::connect(endpoint).await.unwrap();
    let mut socket = super::connection::TransportConnection::new(
        socket,
        tokio::time::Instant::now() + Duration::from_secs(2),
    );
    super::framing::write_encoded(&mut socket, crate::constants::TRANSPORT_DATA_KIND, frame)
        .await
        .unwrap();
    let reply: Result<super::message::ImportReply, _> = super::framing::read(
        &mut socket,
        crate::constants::TRANSPORT_REPLY_KIND,
        1_048_576,
    )
    .await;
    assert!(
        reply.is_err(),
        "unconfigured database identity was accepted"
    );
}

pub(super) fn measure_encoding<T>(encode: impl FnOnce() -> T) -> (T, (u64, u64)) {
    let guard =
        alloc_counter::AllocationGuard::start(module_path!(), "encoding", file!(), line!(), None);
    let output = encode();
    let report = guard.finish();
    (output, (report.allocation_count, report.allocated_bytes))
}

#[derive(Serialize)]
struct OriginalRequestMessage<'request, R, A> {
    from: WireIdentity,
    to: WireIdentity,
    request: &'request R,
    action: A,
}

pub(crate) fn measure_history_request(request: &HistoryCaptureRequest) {
    let request = freeze_input(&format!("history-{}", request.region), request.clone());
    let request = &request;
    let original = OriginalRequestMessage {
        from: identity(1),
        to: identity(2),
        request,
        action: HistoryCaptureAction::Observe,
    };
    let bytes = serde_json::to_vec(&original).unwrap();
    let template: HistoryCaptureMessage = serde_json::from_slice(&bytes).unwrap();
    let oracle = codec::encode(&original, 1_048_576).unwrap();
    let expected = request.content().unwrap();
    for repetition in 0..5 {
        let (content, report) = measure_encoding(|| {
            request.validate().unwrap();
            request.content().unwrap()
        });
        assert_eq!(content, expected);
        print_measurement("history-preflight", &request.region, repetition, report);
        let recovered: Arc<HistoryCaptureRequest> = measure_request(
            ("history-message", &request.region, repetition),
            &oracle,
            (
                || HistoryCaptureMessage {
                    from: template.from.clone(),
                    to: template.to.clone(),
                    request: template.request.clone(),
                    action: HistoryCaptureAction::Observe,
                },
                |message: HistoryCaptureMessage| message.request,
            ),
        );
        assert_eq!(recovered.content().unwrap(), expected);
    }
    require_message_rejections::<HistoryCaptureMessage>(&bytes);
    let mut invalid: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    invalid["request"]["region"] = "region-99".into();
    let invalid: HistoryCaptureMessage = serde_json::from_value(invalid).unwrap();
    assert!(invalid.request.validate().is_err());
}

pub(crate) fn measure_seal_request(request: &SealRequest) {
    let request = freeze_input(&format!("seal-{}", request.region), request.clone());
    let request = &request;
    let original = OriginalRequestMessage {
        from: identity(1),
        to: identity(2),
        request,
        action: SealAction::Observe,
    };
    let bytes = serde_json::to_vec(&original).unwrap();
    let template: SealMessage = serde_json::from_slice(&bytes).unwrap();
    let oracle = codec::encode(&original, 1_048_576).unwrap();
    let expected = request.content().unwrap();
    for repetition in 0..5 {
        let recovered: Arc<SealRequest> = measure_request(
            ("seal-message", &request.region, repetition),
            &oracle,
            (
                || SealMessage {
                    from: template.from.clone(),
                    to: template.to.clone(),
                    request: template.request.clone(),
                    action: SealAction::Observe,
                },
                |message: SealMessage| message.request,
            ),
        );
        recovered.validate().unwrap();
        assert_eq!(recovered.content().unwrap(), expected);
    }
    require_message_rejections::<SealMessage>(&bytes);
}

fn measure_request<R, M: Serialize + DeserializeOwned, Request: Into<Arc<R>>>(
    scope: (&str, &replication_types::identity::replica::RegionId, usize),
    oracle: &codec::EncodedFrame,
    operation: (impl FnOnce() -> M, impl FnOnce(M) -> Request),
) -> Arc<R> {
    let (case, region, repetition) = scope;
    let (recovered, report) = measure_encoding(|| {
        let message = operation.0();
        let frame = codec::encode(&message, oracle.raw_bytes as usize).unwrap();
        assert_eq!(frame.raw_bytes, oracle.raw_bytes);
        assert_eq!(frame.packed, oracle.packed);
        drop(message);
        operation.1(codec::decode(frame.packed, frame.raw_bytes).unwrap()).into()
    });
    print_measurement(case, region, repetition, report);
    recovered
}

fn require_message_rejections<M: DeserializeOwned>(bytes: &[u8]) {
    let mut invalid: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    invalid["unexpected"] = true.into();
    assert!(serde_json::from_value::<M>(invalid).is_err());
    assert!(matches!(
        codec::encode(
            &serde_json::from_slice::<serde_json::Value>(bytes).unwrap(),
            bytes.len() - 1
        ),
        Err(super::TransportError::FrameTooLarge)
    ));
}

fn print_measurement(
    case: &str,
    region: &replication_types::identity::replica::RegionId,
    repetition: usize,
    report: (u64, u64),
) {
    eprintln!(
        "regional_lifecycle_allocation {}",
        serde_json::json!({"case": case, "region": region, "repetition": repetition, "allocations": report.0, "bytes": report.1})
    );
}

pub(crate) fn measure_import_reply(
    case: &str,
    receipt: &crate::import_receipt::CommittedImportPages,
) {
    let reply = super::message::ImportReply {
        from: identity(2),
        to: identity(1),
        result: Ok(super::message::ImportAcknowledgement {
            pages: receipt.pages().to_vec(),
            completed: receipt.completed().map(<[_]>::to_vec),
        }),
    };
    let reply = freeze_input(&format!("reply-{case}"), reply);
    let expected = reply.result.as_ref().unwrap();
    let oracle = codec::encode(&reply, 1_048_576).unwrap();
    for (index, page) in expected.pages.iter().enumerate() {
        if let Some(selection) = &page.selected_tables {
            require_selection_spelling((case, index), selection);
        }
        let mut forged = page.clone();
        forged.page.selection = Some(replication_types::protocol::CanonicalDigest::of(b"wrong"));
        assert!(forged.require_selection().is_err());
    }
    for repetition in 0..5 {
        let (decoded, report) = measure_encoding(|| {
            let frame = codec::encode(&reply, oracle.raw_bytes as usize).unwrap();
            assert_eq!(frame.raw_bytes, oracle.raw_bytes);
            assert_eq!(frame.packed, oracle.packed);
            codec::decode::<super::message::ImportReply>(frame.packed, frame.raw_bytes).unwrap()
        });
        let decoded = decoded.result.unwrap();
        assert_eq!(decoded.pages, expected.pages);
        assert_eq!(decoded.completed, expected.completed);
        for page in &decoded.pages {
            page.require_selection().unwrap();
        }
        eprintln!(
            "regional_serializer_allocation {}",
            serde_json::json!({
                "case": case, "repetition": repetition,
                "allocations": report.0, "bytes": report.1
            })
        );
    }
}

fn require_selection_spelling(
    source: (&str, usize),
    selection: &replication_types::event::interval::ImportTableSelection,
) {
    use replication_types::{event::interval::ImportTableSelection, protocol::CanonicalDigest};
    let original = Vec::from(selection.clone());
    for tables in [original[..1].to_vec(), original] {
        let selection = ImportTableSelection::try_from(tables.clone()).unwrap();
        let bytes = serde_json::to_vec(&selection).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&tables).unwrap());
        assert_eq!(selection.digest().unwrap(), CanonicalDigest::of(&bytes));
        let decoded: ImportTableSelection = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, selection);
        for repetition in 0..5 {
            let (encoded, report) = measure_encoding(|| serde_json::to_vec(&selection).unwrap());
            assert_eq!(encoded, bytes);
            eprintln!(
                "regional_serializer_allocation {}",
                serde_json::json!({
                    "case": "selection-value", "reply": source.0, "page": source.1, "width": tables.len(),
                    "repetition": repetition, "allocations": report.0, "bytes": report.1
                })
            );
        }
    }
    let first = Vec::from(selection.clone())[0];
    for invalid in [vec![], vec![first, first]] {
        let bytes = serde_json::to_vec(&invalid).unwrap();
        assert!(serde_json::from_slice::<ImportTableSelection>(&bytes).is_err());
    }
    let mut reversed = Vec::from(selection.clone());
    reversed.reverse();
    if reversed.len() > 1 {
        assert!(
            serde_json::from_slice::<ImportTableSelection>(&serde_json::to_vec(&reversed).unwrap())
                .is_err()
        );
    }
}

pub(super) fn require_region_spelling() {
    use replication_types::{identity::replica::RegionId, protocol::CanonicalDigest};
    for name in ["a".to_owned(), "a".repeat(RegionId::MAX_BYTES)] {
        let region = RegionId::try_from(name.clone()).unwrap();
        let bytes = serde_json::to_vec(&region).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&name).unwrap());
        assert_eq!(
            CanonicalDigest::of_json(&region).unwrap(),
            CanonicalDigest::of(&bytes)
        );
        assert_eq!(serde_json::from_slice::<RegionId>(&bytes).unwrap(), region);
        for repetition in 0..5 {
            let ((encoded, digest), report) = measure_encoding(|| {
                (
                    serde_json::to_vec(&region).unwrap(),
                    CanonicalDigest::of_json(&region).unwrap(),
                )
            });
            assert_eq!(encoded, bytes);
            assert_eq!(digest, CanonicalDigest::of(&bytes));
            eprintln!(
                "regional_serializer_allocation {}",
                serde_json::json!({
                    "case": "region-value", "width": name.len(), "repetition": repetition,
                    "allocations": report.0, "bytes": report.1
                })
            );
        }
    }
    for invalid in [
        "".to_owned(),
        "A".into(),
        "a_b".into(),
        "a".repeat(RegionId::MAX_BYTES + 1),
    ] {
        assert!(
            serde_json::from_slice::<RegionId>(&serde_json::to_vec(&invalid).unwrap()).is_err()
        );
    }
}

pub(super) fn identity(index: u8) -> WireIdentity {
    WireIdentity {
        deployment: [7; 16].try_into().unwrap(),
        replica: [index; 16].try_into().unwrap(),
        region: fixture::region(index),
        group: [index; 16].try_into().unwrap(),
        generation: 1.into(),
    }
}

pub(super) fn freeze_input<T: Serialize + DeserializeOwned>(name: &str, input: T) -> T {
    let Ok(root) = std::env::var("REGIONAL_ALLOCATION_FIXTURE_ROOT") else {
        return input;
    };
    let path = std::path::PathBuf::from(root).join(format!("{name}.json"));
    if path.exists() {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    } else {
        std::fs::write(path, serde_json::to_vec(&input).unwrap()).unwrap();
        input
    }
}

pub(super) fn freeze_import(
    import: &replication_types::regional_import::RegionalImport,
    width: u64,
) -> Arc<replication_types::regional_import::RegionalImport> {
    let frame = super::client::encode_page(import, (identity(1), identity(2)), 1_048_576).unwrap();
    let (packed, raw) = freeze_input(&format!("page-{width}"), (frame.packed, frame.raw_bytes));
    let page: super::message::IncomingPage = codec::decode(packed, raw).unwrap();
    Arc::new(
        page.into_import(&fixture::codec(), &fixture::limits())
            .unwrap(),
    )
}
