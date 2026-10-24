use std::{num::NonZeroUsize, sync::Arc};

use replication_provider::{
    CANONICAL_EVENT_SEMANTIC_VERSION, canonical_event_codec::CanonicalJsonCodec,
};
use replication_types::{
    error::ReplicationProtocolError,
    event::{
        content::{
            AcceptedItemEvent, CanonicalEventCandidate, CanonicalEventCodec, CanonicalPayload,
            MutationCause, SourceImages, SourceImagesRef,
        },
        identity::{OriginEventId, VersionIdentity},
        order::{EffectRank, HybridLogicalClock, WinnerOrder},
    },
    protocol::{CanonicalBytes, CanonicalDigest, PayloadBounds},
};
use serde_json::{Value, json};
use storage_types::{AttributeValue, KeyAttributes, WireItem, WireItemKeyAttributes};

#[test]
fn given_sql_and_kv_images_when_encoding_then_equivalent_content_and_exact_originals_hold() {
    let codec = codec(8192);
    let sql = WireItem::dynamo_json(br#"{"pk":{"S":"p"},"nested":{"M":{"z":{"N":"1.00"},"a":{"L":[{"M":{"z":{"BOOL":true},"a":{"S":"first"}}}]}}}}"#.to_vec());
    let kv = WireItem::LocalSplit {
        primary_key: WireItemKeyAttributes::new("pk".into(), AttributeValue::S("p".into()), None, None),
        secondary_key: None,
        non_key_attributes_blob: Some(br#"{"nested":{"M":{"a":{"L":[{"M":{"a":{"S":"first"},"z":{"BOOL":true}}}]},"z":{"N":"1.00"}}}}"#.to_vec()),
    };
    let sql_bytes = encode_image(&codec, &sql);
    let kv_bytes = encode_image(&codec, &kv);
    assert_eq!(
        serde_json::from_slice::<Value>(sql_bytes.as_ref()).expect("SQL event"),
        serde_json::from_slice::<Value>(kv_bytes.as_ref()).expect("KV event")
    );
    assert_ne!(sql_bytes, kv_bytes);
    for bytes in [sql_bytes, kv_bytes] {
        let received = codec.decode_event(bytes.as_ref()).expect("received event");
        assert_eq!(received.as_ref().bytes, bytes);
        assert_eq!(
            received.as_ref().digest,
            CanonicalDigest::of(bytes.as_ref())
        );
    }
    let value: Value =
        serde_json::from_slice(encode_image(&codec, &kv).as_ref()).expect("event JSON");
    assert_eq!(
        value["payload"]["Item"]["images"]["new"]["pk"],
        json!({"S":"p"})
    );
    assert!(
        value["payload"]["Item"]["images"]["new"]
            .get("LocalSplit")
            .is_none()
    );
}

#[test]
fn given_equivalent_object_escaping_when_encoding_then_original_spelling_is_preserved() {
    let codec = codec(8192);
    let escaped =
        WireItem::dynamo_json(br#"{"pk":{"S":"\u0070"},"\u006e":{"S":"\u00e9\n"}}"#.to_vec());
    let plain = WireItem::dynamo_json(
        "{\"n\":{\"S\":\"é\\n\"},\"pk\":{\"S\":\"p\"}}"
            .as_bytes()
            .to_vec(),
    );
    let escaped_bytes = encode_image(&codec, &escaped);
    let plain_bytes = encode_image(&codec, &plain);
    assert_ne!(escaped_bytes, plain_bytes);
    assert_eq!(
        serde_json::from_slice::<Value>(escaped_bytes.as_ref()).expect("escaped event"),
        serde_json::from_slice::<Value>(plain_bytes.as_ref()).expect("plain event")
    );
    for (image, bytes) in [(&escaped, escaped_bytes), (&plain, plain_bytes)] {
        let WireItem::DynamoJson { data } = image else {
            unreachable!()
        };
        assert!(
            bytes
                .as_ref()
                .windows(data.len())
                .any(|slice| slice == data)
        );
        assert_eq!(
            codec
                .decode_event(bytes.as_ref())
                .expect("received")
                .as_ref()
                .bytes,
            bytes
        );
    }
}

#[test]
fn given_accepted_numbers_and_sets_when_encoding_then_original_spelling_and_order_retained() {
    let codec = codec(8192);
    let image = WireItem::dynamo_json(br#"{"pk":{"S":"p"},"n":{"N":"1.00"},"ns":{"NS":["2e0","1.00"]},"ss":{"SS":["z","a"]},"bs":{"BS":["eg==","YQ=="]}}"#.to_vec());
    let bytes = encode_image(&codec, &image);
    let value: Value = serde_json::from_slice(bytes.as_ref()).expect("event JSON");
    let new = &value["payload"]["Item"]["images"]["new"];
    assert_eq!(new["n"], json!({"N":"1.00"}));
    assert_eq!(new["ns"], json!({"NS":["2e0","1.00"]}));
    assert_eq!(new["ss"], json!({"SS":["z","a"]}));
    assert_eq!(new["bs"], json!({"BS":["eg==","YQ=="]}));
    let different = WireItem::dynamo_json(br#"{"pk":{"S":"p"},"n":{"N":"1"},"ns":{"NS":["1","2"]},"ss":{"SS":["a","z"]},"bs":{"BS":["YQ==","eg=="]}}"#.to_vec());
    assert_ne!(bytes, encode_image(&codec, &different));
}

#[test]
fn given_borrowed_images_when_moved_to_owned_payload_then_exact_event_bytes_validate() {
    let codec = codec(8192);
    let image = WireItem::dynamo_json(br#"{"pk":{"S":"p"}}"#.to_vec());
    let bytes = encode_image(&codec, &image);
    let payload = CanonicalPayload::Item(Box::new(item_event(
        &codec,
        SourceImages {
            old: None,
            new: Some(image),
            old_indexers: None,
            new_indexers: None,
        },
    )));
    let id = origin_id().into();
    assert_eq!(
        bytes,
        codec
            .encode_event(CANONICAL_EVENT_SEMANTIC_VERSION.into(), &id, &payload)
            .expect("owned event")
    );
    let persisted = serde_json::to_vec(&payload).expect("persisted payload");
    let restored: CanonicalPayload = serde_json::from_slice(&persisted).expect("logical images");
    assert_eq!(
        codec
            .encode_event(CANONICAL_EVENT_SEMANTIC_VERSION.into(), &id, &restored)
            .expect("restored event"),
        bytes
    );
    CanonicalEventCandidate::try_decode(
        bytes.clone(),
        CanonicalDigest::of(bytes.as_ref()),
        &codec,
        bounds(8192),
    )
    .expect("accepted event");
}

#[test]
fn given_numeric_key_spellings_when_encoding_then_storage_canonical_key_is_reused() {
    let codec = codec(8192);
    let mut left = KeyAttributes::new();
    left.insert("sk", AttributeValue::N("1.00E0".into()));
    left.insert("pk", AttributeValue::S("p".into()));
    let mut right = KeyAttributes::new();
    right.insert("pk", AttributeValue::S("p".into()));
    right.insert("sk", AttributeValue::N("1".into()));
    let key = codec.encode_key(&left).expect("canonical key");
    assert_eq!(key, codec.encode_key(&right).expect("same key"));
    assert_eq!(
        key.as_ref(),
        right
            .canonical_dynamo_json()
            .expect("storage owner")
            .as_bytes()
    );
}

#[test]
fn given_unsupported_semantics_when_encoding_then_reject_instead_of_reinterpreting() {
    let codec = codec(8192);
    let image = WireItem::dynamo_json(br#"{"pk":{"S":"p"}}"#.to_vec());
    let item = item_event(&codec, image_refs(&image));
    for semantics in [1, CANONICAL_EVENT_SEMANTIC_VERSION + 1] {
        assert!(matches!(
            codec.encode_item_event(semantics.into(), &origin_id().into(), &item),
            Err(ReplicationProtocolError::ProtocolIncompatible { .. })
        ));
        let original = encode_image(&codec, &image);
        let mut unsupported: Value = serde_json::from_slice(original.as_ref()).expect("event JSON");
        unsupported["semantics"] = json!(semantics);
        let unsupported = serde_json::to_vec(&unsupported).expect("unsupported event");
        assert!(matches!(
            codec.decode_event(&unsupported),
            Err(ReplicationProtocolError::ProtocolIncompatible { .. })
        ));
    }
}

#[test]
fn given_invalid_or_duplicate_image_fields_when_encoding_then_reject_encoding() {
    let codec = codec(8192);
    for raw in [
        "[]",
        "{",
        r#"{"pk":{"S":"p"},"pk":{"S":"p"}}"#,
        r#"{"pk":{"S":"p"},"nested":{"M":{"x":{"S":"a"},"x":{"S":"b"}}}}"#,
    ] {
        let image = WireItem::dynamo_json(raw.as_bytes().to_vec());
        let item = item_event(&codec, image_refs(&image));
        let result = codec.encode_item_event(
            CANONICAL_EVENT_SEMANTIC_VERSION.into(),
            &origin_id().into(),
            &item,
        );
        assert!(
            matches!(
                result,
                Err(ReplicationProtocolError::IdentityContentMismatch { .. })
            ),
            "fixture {raw}: {result:?}"
        );
    }
}

#[test]
fn given_oversized_source_or_event_when_encoding_then_enforce_input_and_output_bounds() {
    let limited = codec(128);
    let image = WireItem::dynamo_json(
        format!(
            "{{\"pk\":{{\"S\":\"p\"}},\"value\":{{\"S\":\"{}\"}}}}",
            "p".repeat(256)
        )
        .into_bytes(),
    );
    let item = item_event(&codec(8192), image_refs(&image));
    assert!(
        matches!(limited.encode_item_event(CANONICAL_EVENT_SEMANTIC_VERSION.into(), &origin_id().into(), &item),
        Err(ReplicationProtocolError::PayloadTooLarge { actual, limit: 128, .. }) if actual > 128)
    );
    let small = WireItem::dynamo_json(br#"{"pk":{"S":"p"}}"#.to_vec());
    let item = item_event(&codec(8192), image_refs(&small));
    assert!(matches!(
        limited.encode_item_event(
            CANONICAL_EVENT_SEMANTIC_VERSION.into(),
            &origin_id().into(),
            &item
        ),
        Err(ReplicationProtocolError::PayloadTooLarge { limit: 128, .. })
    ));
    let bytes = encode_image(&codec(8192), &small);
    assert_eq!(encode_image(&codec(bytes.as_ref().len()), &small), bytes);
}

#[test]
fn given_excessive_json_depth_when_encoding_or_decoding_then_reject_bounded_parser() {
    let codec = codec(8192);
    let raw = format!(
        "{{\"pk\":{{\"S\":\"p\"}},\"deep\":{}{}}}",
        "[".repeat(130),
        "]".repeat(130)
    );
    let image = WireItem::dynamo_json(raw.into_bytes());
    let item = item_event(&codec, image_refs(&image));
    assert!(matches!(
        codec.encode_item_event(
            CANONICAL_EVENT_SEMANTIC_VERSION.into(),
            &origin_id().into(),
            &item
        ),
        Err(ReplicationProtocolError::IdentityContentMismatch { .. })
    ));
    let raw = format!("{}0{}", "[".repeat(130), "]".repeat(130));
    assert!(matches!(
        codec.decode_event(raw.as_bytes()),
        Err(ReplicationProtocolError::InvalidEncoding { .. })
    ));
}

#[test]
fn given_received_original_when_spacing_changes_then_original_bytes_and_digest_are_preserved() {
    let codec = codec(8192);
    let image = WireItem::dynamo_json(br#"{"pk":{"S":"p"}}"#.to_vec());
    let original = encode_image(&codec, &image);
    let value: Value = serde_json::from_slice(original.as_ref()).expect("JSON");
    let spaced = serde_json::to_vec_pretty(&value).expect("spaced original");
    let accepted = codec.decode_event(&spaced).expect("received original");
    assert_eq!(accepted.as_ref().bytes.as_ref(), spaced);
    assert_eq!(accepted.as_ref().digest, CanonicalDigest::of(&spaced));
    assert_ne!(
        accepted.as_ref().digest,
        CanonicalDigest::of(original.as_ref())
    );
    assert!(matches!(
        CanonicalEventCandidate::try_decode(
            CanonicalBytes::try_new(Arc::from(spaced), bounds(8192).event_bytes).expect("bounded"),
            CanonicalDigest::of(original.as_ref()),
            &codec,
            bounds(8192)
        ),
        Err(ReplicationProtocolError::IdentityContentMismatch { .. })
    ));
}

#[test]
fn given_received_event_when_bindings_or_closed_fields_change_then_reject_before_acceptance() {
    let codec = codec(8192);
    let image = WireItem::dynamo_json(br#"{"pk":{"S":"p"}}"#.to_vec());
    let original = encode_image(&codec, &image);
    let value: Value = serde_json::from_slice(original.as_ref()).expect("event");
    let mut ordinal = value.clone();
    ordinal["id"]["Origin"]["ordinal"] = json!(1);
    let mut image_key = value.clone();
    image_key["payload"]["Item"]["images"]["new"]["pk"] = json!({"S":"other"});
    let mut lineage = value.clone();
    lineage["payload"]["Item"]["lineage"] = json!(vec![9; 16]);
    let mut unknown = value.clone();
    unknown["unexpected"] = json!(0);
    let mut old_origin = value;
    let origin = old_origin["id"]["Origin"]["origin"]
        .as_object_mut()
        .expect("origin");
    let group = origin.remove("database_group").expect("group");
    origin.insert("domain".into(), group);
    for invalid in [ordinal, image_key, lineage, unknown, old_origin] {
        assert!(
            codec
                .decode_event(&serde_json::to_vec(&invalid).expect("invalid"))
                .is_err()
        );
    }
    for raw in [b"".as_slice(), b"{", b"{}", b"null"] {
        assert!(codec.decode_event(raw).is_err());
    }
    assert!(matches!(
        self::codec(original.as_ref().len() - 1).decode_event(original.as_ref()),
        Err(ReplicationProtocolError::PayloadTooLarge { .. })
    ));
}

fn codec(limit: usize) -> CanonicalJsonCodec {
    CanonicalJsonCodec::new(bounds(limit))
}

fn bounds(limit: usize) -> PayloadBounds {
    let limit = NonZeroUsize::new(limit).expect("nonzero limit");
    PayloadBounds {
        event_bytes: limit,
        manifest_bytes: limit,
    }
}

fn encode_image(codec: &CanonicalJsonCodec, image: &WireItem) -> CanonicalBytes {
    codec
        .encode_item_event(
            CANONICAL_EVENT_SEMANTIC_VERSION.into(),
            &origin_id().into(),
            &item_event(codec, image_refs(image)),
        )
        .expect("canonical image event")
}

fn image_refs(image: &WireItem) -> SourceImagesRef<'_> {
    SourceImagesRef {
        old: None,
        new: Some(image),
        old_indexers: None,
        new_indexers: None,
    }
}

fn item_event<I>(codec: &CanonicalJsonCodec, images: I) -> AcceptedItemEvent<I> {
    let origin = origin_id();
    let mut key = KeyAttributes::new();
    key.insert("pk", AttributeValue::S("p".into()));
    AcceptedItemEvent {
        source_table: identity(1).try_into().expect("table"),
        lineage: origin.lineage,
        schema: 1.into(),
        canonical_key: codec.encode_key(&key).expect("key"),
        key,
        images,
        order: WinnerOrder::Mutation {
            clock: HybridLogicalClock {
                physical_ms: 9,
                logical: 0,
            },
            origin: origin.origin.clone(),
            position: origin.position,
            ordinal: origin.ordinal,
            rank: EffectRank::Mutation,
        },
        version: VersionIdentity::Mutation(origin),
        cause: MutationCause::Put,
        victim_order: None,
        parents: Vec::new(),
        predecessor: None,
        transaction: None,
        observations: Vec::new(),
    }
}

fn origin_id() -> OriginEventId {
    OriginEventId {
        lineage: identity(2).try_into().expect("lineage"),
        origin: replication_types::identity::origin::OriginId {
            region: "source".parse().expect("region"),
            database_group: identity(3).try_into().expect("group"),
            journal_partition: 0.into(),
            journal_generation: 5.into(),
        },
        position: 7.into(),
        ordinal: 0.into(),
    }
}

fn identity(first: u8) -> [u8; 16] {
    let mut identity = [0; 16];
    identity[0] = first;
    identity
}
