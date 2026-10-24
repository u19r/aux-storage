use std::sync::Arc;

use replication_types::{
    event::interval::ImportFragment,
    protocol::{CanonicalBytes, CanonicalDigest},
    regional_import::RegionalImport,
};

use super::{
    chunk_message::{ByteChunk, OutgoingChunk},
    codec,
    message::{IncomingPage, OriginalEvents, OutgoingPage},
    test_support::{identity, measure_encoding},
};
use crate::{bootstrap::BaselineCopyPage, test_support as fixture};

#[tokio::test]
async fn given_native_originals_when_encoding_borrowed_pages_then_wire_bytes_and_bounds_agree() {
    super::test_support::require_region_spelling();
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    fixture::install_pair(&source, &destination).await;
    for index in 0..16 {
        fixture::put(&source, 1, &format!("{index}-葉\"\\\n{}", "x".repeat(1024))).await;
    }
    for width in [1, 7, 16] {
        let import = fixture::frame(&source, 1, 0, width).await;
        let import = super::test_support::freeze_import(&import, width);
        for repetition in 0..5 {
            let (old, before) = measure_encoding(|| {
                let events = import
                    .events()
                    .iter()
                    .map(|event| std::str::from_utf8(event.as_ref().bytes.as_ref()))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                codec::encode(
                    &OutgoingPage {
                        from: identity(1),
                        to: identity(2),
                        manifest: import.manifest(),
                        bindings: import.bindings(),
                        fragment: import.fragment(),
                        events,
                    },
                    1_048_576,
                )
                .unwrap()
            });
            let (new, after) = measure_encoding(|| {
                super::client::encode_page(&import, (identity(1), identity(2)), 1_048_576).unwrap()
            });
            assert_eq!(old.raw_bytes, new.raw_bytes);
            assert_eq!(old.packed, new.packed);
            assert!(after.0 < before.0 && after.1 < before.1);
            print_measurement("event-page", width as usize, repetition, before, after);
            measure_import_selection(&import, width as usize, repetition);
            let decoded: IncomingPage = codec::decode(new.packed, new.raw_bytes).unwrap();
            let recovered = decoded
                .into_import(&fixture::codec(), &fixture::limits())
                .unwrap();
            assert_eq!(recovered.manifest(), import.manifest());
            assert!(
                recovered
                    .events()
                    .iter()
                    .zip(import.events())
                    .all(|(a, b)| a.as_ref().bytes == b.as_ref().bytes)
            );
            assert!(
                super::client::encode_page(
                    &import,
                    (identity(1), identity(2)),
                    old.raw_bytes as usize
                )
                .is_ok()
            );
            assert!(matches!(
                super::client::encode_page(
                    &import,
                    (identity(1), identity(2)),
                    old.raw_bytes as usize - 1
                ),
                Err(super::TransportError::FrameTooLarge)
            ));
        }
    }
    let empty = codec::encode(&OriginalEvents(&[]), 64).unwrap();
    assert_eq!(
        codec::decode::<Vec<String>>(empty.packed, empty.raw_bytes).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn given_owned_payloads_when_borrowing_for_encoding_then_copies_disappear_without_wire_changes() {
    for seed in [17, 61, 89] {
        let bytes: Vec<u8> = (0..32768)
            .map(|index| ((index * seed + index / 7) % 256) as u8)
            .collect();
        for count in [1, 2, 4, 16] {
            for repetition in 0..5 {
                let (old, before) = measure_encoding(|| encode_chunks(&bytes, count, true));
                let (new, after) = measure_encoding(|| encode_chunks(&bytes, count, false));
                assert_eq!(before.0 - after.0, count as u64);
                assert_eq!(before.1 - after.1, bytes.len() as u64);
                for (index, (old, new)) in old.into_iter().zip(new).enumerate() {
                    assert_eq!(old.raw_bytes, new.raw_bytes);
                    assert_eq!(old.packed, new.packed);
                    let chunk: ByteChunk = codec::decode(new.packed, new.raw_bytes).unwrap();
                    assert_eq!(chunk.offset as usize, index * (bytes.len() / count));
                    assert_eq!(
                        chunk.bytes.as_ref(),
                        &bytes[index * (bytes.len() / count)..(index + 1) * (bytes.len() / count)]
                    );
                    assert_eq!(chunk.content, CanonicalDigest::of(&bytes));
                }
                print_measurement("chunk-encoding", count, repetition, before, after);
            }
        }
        for count in [0, 1, 16] {
            let page = Arc::new(BaselineCopyPage {
                sequence: seed as u64,
                items: (0..count)
                    .map(|_| {
                        CanonicalBytes::try_new(Arc::from(bytes.as_slice()), fixture::nz(65536))
                            .unwrap()
                    })
                    .collect(),
                complete: true,
            });
            for repetition in 0..5 {
                let (old, before) = measure_encoding(|| {
                    let owned = page.as_ref().clone();
                    codec::encode(&owned, 4_194_304).unwrap()
                });
                let (new, after) = measure_encoding(|| {
                    let shared = Arc::clone(&page);
                    codec::encode(&shared, 4_194_304).unwrap()
                });
                assert_eq!(old.raw_bytes, new.raw_bytes);
                assert_eq!(old.packed, new.packed);
                assert_eq!(before.0 - after.0, u64::from(count > 0));
                assert_eq!(
                    before.1 - after.1,
                    (count * std::mem::size_of::<CanonicalBytes>()) as u64
                );
                print_measurement("bootstrap-page", count, repetition, before, after);
            }
        }
    }
}

fn measure_import_selection(import: &RegionalImport, width: usize, repetition: usize) {
    let table = import.bindings()[0].source_table;
    assert!(import.select_table([255; 16].try_into().unwrap()).is_err());
    assert!(
        import
            .select_fragment((0, width as u32 + 1).try_into().unwrap())
            .is_err()
    );
    for fragment_width in (1..=width).filter(|value| *value == 1 || *value == width) {
        let input = (
            import.bindings().to_vec(),
            import.manifest().clone(),
            import.events().to_vec(),
        );
        let span = ImportFragment::try_from((0, fragment_width as u32)).unwrap();
        let ((fragment, frame), report) = measure_encoding(|| {
            let parent = RegionalImport::try_new(input, &*fixture::codec()).unwrap();
            let selected = parent.select_table(table).unwrap();
            drop(parent);
            let fragment = selected.select_fragment(span).unwrap();
            drop(selected);
            let frame =
                super::client::encode_page(&fragment, (identity(1), identity(2)), 1_048_576)
                    .unwrap();
            (fragment, frame)
        });
        let oracle = codec::encode(
            &OutgoingPage {
                from: identity(1),
                to: identity(2),
                manifest: import.manifest(),
                bindings: import.bindings(),
                fragment: Some(span),
                events: OriginalEvents(&import.events()[..fragment_width]),
            },
            1_048_576,
        )
        .unwrap();
        assert_eq!(frame.raw_bytes, oracle.raw_bytes);
        assert_eq!(frame.packed, oracle.packed);
        assert!(fragment.select_table(table).is_err());
        assert!(fragment.select_fragment(span).is_err());
        let decoded: IncomingPage = codec::decode(frame.packed, frame.raw_bytes).unwrap();
        let recovered = decoded
            .into_import(&fixture::codec(), &fixture::limits())
            .unwrap();
        assert_eq!(recovered.manifest(), import.manifest());
        assert_eq!(recovered.fragment(), Some(span));
        assert_eq!(recovered.events().len(), fragment_width);
        assert!(
            recovered
                .events()
                .iter()
                .zip(import.events())
                .all(|(a, b)| a.as_ref().bytes == b.as_ref().bytes)
        );
        eprintln!(
            "regional_import_allocation {}",
            serde_json::json!({"width": width, "fragment_width": fragment_width, "repetition": repetition, "allocations": report.0, "bytes": report.1})
        );
    }
}

fn encode_chunks(bytes: &[u8], count: usize, copy: bool) -> Vec<codec::EncodedFrame> {
    let content = CanonicalDigest::of(bytes);
    bytes
        .chunks(bytes.len() / count)
        .enumerate()
        .map(|(index, bytes)| {
            let owned = copy.then(|| bytes.to_vec());
            codec::encode(
                &OutgoingChunk {
                    content,
                    offset: (index * (32768 / count)) as u32,
                    bytes: owned.as_deref().unwrap_or(bytes),
                },
                262144,
            )
            .unwrap()
        })
        .collect()
}

fn print_measurement(
    case: &str,
    width: usize,
    repetition: usize,
    before: (u64, u64),
    after: (u64, u64),
) {
    eprintln!(
        "regional_allocation {}",
        serde_json::json!({"case": case, "width": width, "repetition": repetition, "before_allocations": before.0, "after_allocations": after.0, "before_bytes": before.1, "after_bytes": after.1})
    );
}
