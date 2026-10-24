use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{net::TcpListener, sync::watch};

use super::{
    TcpTransport,
    test_support::{Proxy, configuration},
};
use crate::{
    import_receipt::{ImportReceiptObservation, reconcile_import_pages},
    test_support as fixture,
};

#[tokio::test]
#[ignore = "requires toxiproxy-server; run just regional-replication-network-test"]
async fn given_regional_latency_and_response_loss_when_original_pages_retry_then_native_receipts_converge()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination_path = directory.path().join("destination.sqlite");
    let destination = Arc::new(fixture::open(&destination_path).await);
    fixture::install_pair(&source, &destination).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination_endpoint = listener.local_addr().unwrap();
    let proxy = Proxy::start(destination_endpoint).await;
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = source_listener.local_addr().unwrap();
    let transport = TcpTransport::try_new(configure_chunked_transport(
        1,
        source_endpoint,
        proxy.endpoint,
    ))
    .unwrap();
    let client = transport.peer([2; 16].try_into().unwrap()).unwrap();
    let receiver = TcpTransport::try_new(configure_chunked_transport(
        2,
        destination_endpoint,
        source_endpoint,
    ))
    .unwrap()
    .receiver(
        Arc::clone(&destination),
        fixture::journal(2),
        fixture::codec(),
    )
    .unwrap();
    let (stop, stopping) = watch::channel(false);
    let serving = tokio::spawn(receiver.serve(listener, stopping));
    let mut after = 0;
    for latency in [20, 75, 150] {
        proxy.latency(latency, 0).await;
        fixture::put(&source, 1, &format!("latency-{latency}")).await;
        let page = fixture::frame(&source, 1, after, after + 1).await;
        let started = Instant::now();
        let accepted = client.deliver(Arc::clone(&page)).await.unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(latency),
            "delivery bypassed the delayed network"
        );
        let replay = client.deliver(Arc::clone(&page)).await.unwrap();
        assert_eq!(replay.pages(), accepted.pages());
        assert_eq!(
            u64::from(
                fixture::acknowledge(&source, Arc::clone(&page), accepted)
                    .await
                    .through()
            ),
            fixture::source_position(&source, 1, after + 1).await
        );
        assert_eq!(
            u64::from(fixture::acknowledge(&source, page, replay).await.through()),
            fixture::source_position(&source, 1, after + 1).await
        );
        fixture::assert_item(&destination, Some(&format!("latency-{latency}"))).await;
        proxy.clear_latency().await;
        after += 1;
    }
    // Exercise multiple physical chunks through the same native import/receipt
    // path.
    let mut random = 17u32;
    let random_value: String = (0..20000)
        .map(|_| {
            random = random.wrapping_mul(1664525).wrapping_add(1013904223);
            char::from(b' ' + ((random >> 24) % 95) as u8)
        })
        .collect();
    let value = format!("{}{random_value}", "\0".repeat(6000));
    fixture::put(&source, 1, &value).await;
    let page = fixture::frame(&source, 1, after, after + 1).await;
    let encoded = super::client::encode_page(
        &page,
        (
            client.resources.identity.clone(),
            client.destination_identity(),
        ),
        client.resources.semantic_limit,
    )
    .unwrap();
    assert!(encoded.raw_bytes as usize > client.resources.limit);
    assert!(encoded.packed.len() > client.resources.limit / 4);
    for latency in [20, 75, 150] {
        proxy.latency(latency, 0).await;
        let receipt = client.deliver(Arc::clone(&page)).await.unwrap();
        fixture::acknowledge(&source, Arc::clone(&page), receipt).await;
        fixture::assert_item(&destination, Some(&value)).await;
        proxy.clear_latency().await;
    }
    after += 1;
    proxy.latency(85, 65).await;
    fixture::put(&source, 1, "jitter").await;
    let jitter_page = fixture::frame(&source, 1, after, after + 1).await;
    let receipt = client.deliver(Arc::clone(&jitter_page)).await.unwrap();
    fixture::acknowledge(&source, jitter_page, receipt).await;
    fixture::assert_item(&destination, Some("jitter")).await;
    proxy.clear_latency().await;
    after += 1;
    proxy
        .create_toxic(
            "lost-response",
            "timeout",
            "downstream",
            serde_json::json!({"timeout": 0}),
        )
        .await;
    fixture::put(&source, 1, "lost-response").await;
    let pending = fixture::frame(&source, 1, after, after + 1).await;
    assert!(client.deliver(Arc::clone(&pending)).await.is_err());
    fixture::assert_item(&destination, Some("lost-response")).await;
    let native = reconcile_import_pages(
        destination.as_ref(),
        fixture::journal(2),
        (
            fixture::bounded_import(Arc::clone(&pending)),
            fixture::codec(),
        ),
    )
    .await
    .unwrap();
    let ImportReceiptObservation::Durable(native) = native else {
        panic!("response loss removed durable acceptance")
    };
    proxy.remove_toxic("lost-response").await;
    let receipt = client.deliver(Arc::clone(&pending)).await.unwrap();
    assert_eq!(receipt.pages(), native.pages());
    fixture::acknowledge(&source, pending, receipt).await;
    after += 1;
    proxy.enabled(false).await;
    fixture::put(&source, 1, "partitioned").await;
    let pending = fixture::frame(&source, 1, after, after + 1).await;
    assert!(client.deliver(Arc::clone(&pending)).await.is_err());
    fixture::assert_item(&source, Some("partitioned")).await;
    fixture::assert_item(&destination, Some("lost-response")).await;
    proxy.enabled(true).await;
    let scope = fixture::delivery_scope(&pending);
    let receipt = client.deliver(Arc::clone(&pending)).await.unwrap();
    let final_progress = fixture::acknowledge(&source, pending, receipt).await;
    assert_eq!(
        u64::from(final_progress.through()),
        fixture::source_position(&source, 1, after + 1).await
    );
    assert!(final_progress.pending().is_empty());
    drop(source);
    let source = fixture::reopen(&directory.path().join("source.sqlite")).await;
    let recovered =
        crate::delivery::load_delivery_progress(&source, fixture::journal(1), scope.clone())
            .await
            .unwrap();
    assert!(recovered == final_progress);
    proxy.latency(150, 0).await;
    let source = Arc::new(source);
    fixture::put(&source, 1, "scheduled-latency").await;
    let clock = Arc::new(bg_jobs_tokio::test_support::TestClock::new(
        scheduled_bg_jobs::Timestamp::from_millis(1_800_000_000_000),
        scheduled_bg_jobs::runtime::MonotonicDeadline::ZERO,
    ));
    let runtime = crate::delivery::test_support::runtime(
        Arc::clone(&source),
        configuration(1, source_endpoint, proxy.endpoint),
        (Arc::clone(&clock), 7),
    );
    runtime.ensure_seeds().await.unwrap();
    crate::delivery::test_support::drive_through(&[&runtime], &source, &scope, after + 2, &clock)
        .await;
    fixture::assert_item(&destination, Some("scheduled-latency")).await;
    let progress =
        crate::delivery::load_delivery_progress(source.as_ref(), fixture::journal(1), scope)
            .await
            .unwrap();
    assert_eq!(
        u64::from(progress.through()),
        fixture::source_position(&source, 1, after + 2).await
    );
    drop(runtime);
    proxy.clear_latency().await;
    stop.send(true).unwrap();
    serving.await.unwrap().unwrap();
    drop(destination);
    let reopened = fixture::reopen(&destination_path).await;
    fixture::assert_item(&reopened, Some("scheduled-latency")).await;
}

#[tokio::test]
async fn given_invalid_frames_when_followed_by_a_valid_page_then_no_invalid_effects_commit() {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = Arc::new(fixture::open(&directory.path().join("destination.sqlite")).await);
    fixture::install_pair(&source, &destination).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = source_listener.local_addr().unwrap();
    let transport = TcpTransport::try_new(configuration(1, source_endpoint, endpoint)).unwrap();
    let client = transport.peer([2; 16].try_into().unwrap()).unwrap();
    let receiver = TcpTransport::try_new(configuration(2, endpoint, source_endpoint))
        .unwrap()
        .receiver(
            Arc::clone(&destination),
            fixture::journal(2),
            fixture::codec(),
        )
        .unwrap();
    let (stop, stopping) = watch::channel(false);
    let serving = tokio::spawn(receiver.serve(listener, stopping));
    fixture::put(&source, 1, "original").await;
    let page = fixture::frame(&source, 1, 0, 1).await;
    reject_frame(
        endpoint,
        crate::constants::TRANSPORT_VERSION,
        1_048_577,
        vec![0],
    )
    .await;
    reject_frame(
        endpoint,
        crate::constants::TRANSPORT_VERSION + 1,
        1,
        vec![0],
    )
    .await;
    let packed = compress_bomb();
    reject_frame(endpoint, crate::constants::TRANSPORT_VERSION, 20, packed).await;
    fixture::assert_item(&destination, None).await;
    let tampered = String::from_utf8(page.events()[0].as_ref().bytes.as_ref().to_vec())
        .unwrap()
        .replace("original", "forged");
    let forged = super::message::OutgoingPage {
        from: transport.resources.identity.clone(),
        to: super::message::WireIdentity {
            deployment: transport.resources.identity.deployment,
            replica: [2; 16].try_into().unwrap(),
            region: fixture::region(2),
            group: [2; 16].try_into().unwrap(),
            generation: 1.into(),
        },
        manifest: page.manifest(),
        bindings: page.bindings(),
        fragment: page.fragment(),
        events: vec![&tampered],
    };
    for mismatch in 0..3 {
        let mut from = forged.from.clone();
        let mut to = forged.to.clone();
        match mismatch {
            0 => from.group = [9; 16].try_into().unwrap(),
            1 => from.generation = 2.into(),
            _ => to.generation = 2.into(),
        }
        super::test_support::reject_identity(endpoint, &page, from, to).await;
    }
    let encoded = super::codec::encode(&forged, 1_048_576).unwrap();
    let socket = tokio::net::TcpStream::connect(endpoint).await.unwrap();
    let mut socket = super::connection::TransportConnection::new(
        socket,
        tokio::time::Instant::now() + Duration::from_secs(2),
    );
    super::framing::write_encoded(&mut socket, crate::constants::TRANSPORT_DATA_KIND, encoded)
        .await
        .unwrap();
    let reply: super::message::ImportReply = super::framing::read(
        &mut socket,
        crate::constants::TRANSPORT_REPLY_KIND,
        1_048_576,
    )
    .await
    .unwrap();
    assert!(matches!(reply.result, Err(super::ImportNack::InvalidPage)));
    fixture::assert_item(&destination, None).await;
    assert!(matches!(
        reconcile_import_pages(
            destination.as_ref(),
            fixture::journal(2),
            (fixture::bounded_import(Arc::clone(&page)), fixture::codec())
        )
        .await
        .unwrap(),
        ImportReceiptObservation::Unresolved
    ));
    let accepted = client.deliver(Arc::clone(&page)).await.unwrap();
    assert_eq!(
        accepted.pages(),
        client.deliver(Arc::clone(&page)).await.unwrap().pages()
    );
    assert!(matches!(
        client.deliver(padded_import(Arc::clone(&page))).await,
        Err(super::TransportError::Nack(super::ImportNack::InvalidPage))
    ));
    assert_eq!(
        accepted.pages(),
        client.deliver(page).await.unwrap().pages()
    );
    fixture::assert_item(&destination, Some("original")).await;
    fixture::put(&source, 1, "spaced").await;
    let exact = padded_import(fixture::frame(&source, 1, 1, 2).await);
    let receipt = client.deliver(Arc::clone(&exact)).await.unwrap();
    assert_eq!(
        receipt.pages()[0].events[0].digest,
        exact.events()[0].as_ref().digest
    );
    assert_eq!(
        client.deliver(exact).await.unwrap().pages(),
        receipt.pages()
    );
    fixture::assert_item(&destination, Some("spaced")).await;
    stop.send(true).unwrap();
    serving.await.unwrap().unwrap();
}

async fn reject_frame(
    endpoint: std::net::SocketAddr,
    version: u16,
    declared: u32,
    packed: Vec<u8>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(endpoint).await.unwrap();
    let mut header = Vec::from(*b"AUXR");
    header.extend_from_slice(&version.to_be_bytes());
    header.extend_from_slice(&[1, 1]);
    header.extend_from_slice(&(packed.len() as u32).to_be_bytes());
    header.extend_from_slice(&declared.to_be_bytes());
    socket.write_all(&header).await.unwrap();
    let _ = socket.write_all(&packed).await;
    let mut response = [0; 1];
    let closed = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0) | Err(_)),
        "invalid frame received an ACK"
    );
}

fn compress_bomb() -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&vec![b'a'; 65_536]).unwrap();
    encoder.finish().unwrap()
}

fn padded_import(
    original: Arc<replication_types::regional_import::RegionalImport>,
) -> Arc<replication_types::regional_import::RegionalImport> {
    use replication_types::{
        event::interval::IntervalCodec, protocol::CanonicalDigest, regional_import::RegionalImport,
    };
    let codec = fixture::codec();
    let mut bytes = b" \n".to_vec();
    bytes.extend_from_slice(original.events()[0].as_ref().bytes.as_ref());
    bytes.extend_from_slice(b"\n ");
    let event = codec.decode_event(&bytes).unwrap();
    let mut manifest = original.manifest().clone();
    manifest.links[0].digest = event.as_ref().digest;
    manifest.digest = CanonicalDigest::of(codec.encode_interval(&manifest).unwrap().as_ref());
    Arc::new(
        RegionalImport::try_new(
            (original.bindings().to_vec(), manifest, vec![event]),
            codec.as_ref(),
        )
        .unwrap(),
    )
}

fn configure_chunked_transport(
    index: u8,
    endpoint: std::net::SocketAddr,
    peer: std::net::SocketAddr,
) -> Arc<replication_provider::runtime_configuration::deployment::QualifiedConfiguration> {
    let mut declaration = configuration(index, endpoint, peer)
        .as_ref()
        .as_ref()
        .clone();
    declaration.limits.frame_bytes = fixture::nz(65536);
    declaration.limits.import_bytes = fixture::nz(65536);
    Arc::new(declaration.qualify(fixture::budgets()).unwrap())
}
