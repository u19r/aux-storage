use std::io::Cursor;

use serde_json::json;
use storage_types::{AttributeMap, AttributeValue};

use super::json_projection::RetainedJsonProjection;

#[test]
fn given_retained_numeric_images_when_projected_then_bounded_spelling_survives_and_corrupt_tokens_fail()
 {
    let boundary = "0".repeat(171);
    for (input, expected) in [
        ("1.00".to_owned(), "1.00".to_owned()),
        ("1700000000".to_owned(), "1700000000".to_owned()),
        ("-0.00".to_owned(), "-0.00".to_owned()),
        ("1e-130".to_owned(), "1e-130".to_owned()),
        (boundary.clone(), boundary.clone()),
        (format!("{boundary}0"), "0".to_owned()),
        (format!("{boundary}1"), "1".to_owned()),
        ("0".repeat(10_000), "0".to_owned()),
    ] {
        let image = serde_json::to_vec(&json!({
            "pk": {"S": "key"}, "number": {"N": input},
            "nested": {"M": {"values": {"NS": ["1.00", "2.00"]}}}
        }))
        .unwrap();
        let mut budget = 4096;
        let projected: AttributeMap = serde_json::from_reader(RetainedJsonProjection::for_item(
            Cursor::new(&image),
            &mut budget,
        ))
        .unwrap();
        assert_eq!(projected.get("number"), Some(&AttributeValue::N(expected)));
        assert_eq!(
            projected.get("nested"),
            Some(&AttributeValue::M(
                [(
                    "values".to_owned(),
                    AttributeValue::NS(vec!["1.00".into(), "2.00".into()])
                )]
                .into(),
            ))
        );
    }
    let escaped = format!(r#"{{"number":{{"N":"{}1"}}}}"#, r"\u0030".repeat(171));
    let mut budget = 4096;
    let projected: AttributeMap = serde_json::from_reader(RetainedJsonProjection::for_item(
        Cursor::new(escaped),
        &mut budget,
    ))
    .unwrap();
    assert_eq!(
        projected.get("number"),
        Some(&AttributeValue::N("1".into()))
    );

    for number in [
        format!("{boundary}x"),
        "1e126".into(),
        "1e-131".into(),
        "1e9223372036854775808".into(),
    ] {
        let image = serde_json::to_vec(&json!({"number": {"N": number}})).unwrap();
        let mut budget = 4096;
        assert!(
            serde_json::from_reader::<_, AttributeMap>(RetainedJsonProjection::for_item(
                Cursor::new(image),
                &mut budget
            ),)
            .is_err()
        );
    }
    for image in [
        br#"{"number":{"N":"1\u00xz"}}"#.as_slice(),
        br#"{"number":{"N":"123"#.as_slice(),
    ] {
        let mut budget = 4096;
        assert!(
            serde_json::from_reader::<_, AttributeMap>(RetainedJsonProjection::for_item(
                Cursor::new(image),
                &mut budget
            ),)
            .is_err()
        );
    }
}
