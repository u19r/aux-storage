use std::{borrow::Cow, fmt, hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, Error, IgnoredAny, MapAccess, Visitor},
};
use storage_types::{AttributeMap, AttributeValue, WireItem};

const FIELDS: [&str; 18] = [
    "pk", "sk", "ttl", "gsi1pk", "gsi1sk", "gsi2pk", "gsi2sk", "gsi3pk", "gsi3sk", "gsi4pk",
    "gsi4sk", "gsi5pk", "gsi5sk", "indexer1", "indexer2", "indexer3", "indexer4", "indexer5",
];

struct Case {
    name: &'static str,
    filler_count: usize,
    filler_bytes: usize,
    field_count: usize,
    present_field_count: usize,
    fields_last: bool,
    escape_selected: bool,
    iterations: usize,
}

const CASES: [Case; 22] = [
    Case {
        name: "all_selected_3",
        filler_count: 0,
        filler_bytes: 0,
        field_count: 3,
        present_field_count: 3,
        fields_last: false,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "all_selected_15",
        filler_count: 0,
        filler_bytes: 0,
        field_count: 15,
        present_field_count: 15,
        fields_last: false,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "all_selected_15_escaped",
        filler_count: 0,
        filler_bytes: 0,
        field_count: 15,
        present_field_count: 15,
        fields_last: false,
        escape_selected: true,
        iterations: 100_000,
    },
    Case {
        name: "tiny_early_3",
        filler_count: 2,
        filler_bytes: 16,
        field_count: 3,
        present_field_count: 3,
        fields_last: false,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "tiny_late_3",
        filler_count: 2,
        filler_bytes: 16,
        field_count: 3,
        present_field_count: 3,
        fields_last: true,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "tiny_early_15",
        filler_count: 2,
        filler_bytes: 16,
        field_count: 15,
        present_field_count: 15,
        fields_last: false,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "medium_wide_early_9",
        filler_count: 32,
        filler_bytes: 128,
        field_count: 9,
        present_field_count: 9,
        fields_last: false,
        escape_selected: false,
        iterations: 10_000,
    },
    Case {
        name: "medium_wide_late_9",
        filler_count: 32,
        filler_bytes: 128,
        field_count: 9,
        present_field_count: 9,
        fields_last: true,
        escape_selected: false,
        iterations: 10_000,
    },
    Case {
        name: "large_wide_early_9",
        filler_count: 128,
        filler_bytes: 512,
        field_count: 9,
        present_field_count: 9,
        fields_last: false,
        escape_selected: false,
        iterations: 2_000,
    },
    Case {
        name: "large_wide_late_15",
        filler_count: 128,
        filler_bytes: 512,
        field_count: 15,
        present_field_count: 15,
        fields_last: true,
        escape_selected: false,
        iterations: 2_000,
    },
    Case {
        name: "large_blob_early_9",
        filler_count: 1,
        filler_bytes: 65_536,
        field_count: 9,
        present_field_count: 9,
        fields_last: false,
        escape_selected: false,
        iterations: 2_000,
    },
    Case {
        name: "large_blob_late_9",
        filler_count: 1,
        filler_bytes: 65_536,
        field_count: 9,
        present_field_count: 9,
        fields_last: true,
        escape_selected: false,
        iterations: 2_000,
    },
    Case {
        name: "flat_all_selected_18",
        filler_count: 0,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "flat_extra_2_select_18",
        filler_count: 2,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 100_000,
    },
    Case {
        name: "flat_extra_16_select_18",
        filler_count: 16,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 20_000,
    },
    Case {
        name: "flat_extra_24_select_18",
        filler_count: 24,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 20_000,
    },
    Case {
        name: "flat_extra_32_select_18",
        filler_count: 32,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 20_000,
    },
    Case {
        name: "flat_extra_48_select_18",
        filler_count: 48,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 20_000,
    },
    Case {
        name: "flat_extra_64_select_18",
        filler_count: 64,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 10_000,
    },
    Case {
        name: "flat_extra_256_select_18",
        filler_count: 256,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 2_000,
    },
    Case {
        name: "flat_extra_512_select_18",
        filler_count: 512,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 18,
        fields_last: true,
        escape_selected: false,
        iterations: 2_000,
    },
    Case {
        name: "flat_sparse_18_present_3",
        filler_count: 2,
        filler_bytes: 6,
        field_count: 18,
        present_field_count: 3,
        fields_last: true,
        escape_selected: false,
        iterations: 100_000,
    },
];

fn build_wire_item(case: &Case) -> WireItem {
    let mut entries = Vec::with_capacity(case.filler_count + case.field_count);
    for (index, name) in FIELDS[..case.present_field_count].iter().enumerate() {
        let value = if *name == "ttl" {
            "{\"N\":\"4102444800\"}".to_owned()
        } else if case.escape_selected {
            let value = serde_json::to_string(&format!("key-{index:02}\n")).unwrap();
            format!("{{\"S\":{value}}}")
        } else {
            format!("{{\"S\":\"key-{index:02}\"}}")
        };
        entries.push(format!("\"{name}\":{value}"));
    }
    let keys = entries;
    let mut fillers = Vec::with_capacity(case.filler_count);
    let payload = "x".repeat(case.filler_bytes);
    for index in 0..case.filler_count {
        fillers.push(format!("\"filler{index:03}\":{{\"S\":\"{payload}\"}}"));
    }
    let entries = if case.fields_last {
        fillers.into_iter().chain(keys).collect::<Vec<_>>()
    } else {
        keys.into_iter().chain(fillers).collect::<Vec<_>>()
    };
    let capacity = entries.iter().map(String::len).sum::<usize>() + entries.len() + 2;
    let mut json = String::with_capacity(capacity);
    json.push('{');
    for (index, entry) in entries.iter().enumerate() {
        if index != 0 {
            json.push(',');
        }
        json.push_str(entry);
    }
    json.push('}');
    WireItem::dynamo_json(json.into_bytes())
}

fn extract_after_full_decode(wire: &WireItem, fields: &[&str]) -> AttributeMap {
    let mut decoded = AttributeMap::try_from(wire).unwrap();
    decoded.retain(|name, _| fields.contains(&name));
    decoded
}

struct BorrowedText<'a>(Cow<'a, str>);

impl<'de> Deserialize<'de> for BorrowedText<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = BorrowedText<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a scalar string")
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(BorrowedText(Cow::Borrowed(value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(BorrowedText(Cow::Owned(value.to_owned())))
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

struct BorrowedScalar<'a>(Cow<'a, str>);

impl<'de> Deserialize<'de> for BorrowedScalar<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct ScalarVisitor;
        impl<'de> Visitor<'de> for ScalarVisitor {
            type Value = BorrowedScalar<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("one DynamoDB S, N, or B scalar")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where M: MapAccess<'de> {
                let tag = map
                    .next_key::<BorrowedText<'de>>()?
                    .ok_or_else(|| M::Error::custom("missing scalar type"))?;
                if !matches!(tag.0.as_ref(), "S" | "N" | "B") {
                    return Err(M::Error::custom("unsupported scalar type"));
                }
                let value = map.next_value::<BorrowedText<'de>>()?;
                if map.next_key::<IgnoredAny>()?.is_some() {
                    return Err(M::Error::custom("multiple scalar types"));
                }
                Ok(BorrowedScalar(value.0))
            }
        }
        deserializer.deserialize_map(ScalarVisitor)
    }
}

struct ScalarSelection<'a>(&'a [&'a str]);

impl<'de> DeserializeSeed<'de> for ScalarSelection<'_> {
    type Value = Vec<Option<Cow<'de, str>>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ScalarSelection<'_> {
    type Value = Vec<Option<Cow<'de, str>>>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB item")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut values = vec![None; self.0.len()];
        while let Some(name) = map.next_key::<BorrowedText<'de>>()? {
            if let Some(index) = self.0.iter().position(|field| *field == name.0.as_ref()) {
                values[index] = Some(map.next_value::<BorrowedScalar<'de>>()?.0);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(values)
    }
}

fn extract_borrowed_scalars<'a>(wire: &'a WireItem, fields: &[&str]) -> Vec<Option<Cow<'a, str>>> {
    let WireItem::DynamoJson { data } = wire else {
        panic!("experiment requires raw DynamoDB JSON");
    };
    let mut deserializer = serde_json::Deserializer::from_slice(data);
    let selected = ScalarSelection(fields)
        .deserialize(&mut deserializer)
        .unwrap();
    deserializer.end().unwrap();
    selected
}

#[test]
fn given_varied_wire_items_when_extracting_selected_fields_then_match_full_decode() {
    for case in &CASES {
        let wire = build_wire_item(case);
        let fields = &FIELDS[..case.field_count];
        let expected = extract_after_full_decode(&wire, fields);
        let actual = wire.project_attributes(fields).unwrap();
        assert_eq!(actual, expected, "case {}", case.name);
        assert_eq!(actual.len(), case.present_field_count);
        assert_eq!(
            actual.get("ttl"),
            Some(&AttributeValue::N("4102444800".to_owned())),
        );
        let scalars = wire.scalar_attributes(fields).unwrap();
        let borrowed = extract_borrowed_scalars(&wire, fields);
        assert_eq!(borrowed, scalars);
        let borrowed_count = borrowed
            .iter()
            .flatten()
            .filter(|value| matches!(value, Cow::Borrowed(_)))
            .count();
        let expected_borrowed = if case.escape_selected {
            1
        } else {
            case.present_field_count
        };
        assert_eq!(borrowed_count, expected_borrowed);
        for (field, scalar) in fields.iter().zip(scalars) {
            let expected = actual.get(field).map(|value| match value {
                AttributeValue::S(value) | AttributeValue::N(value) => value.as_str(),
                value => panic!("unexpected fixture scalar: {value:?}"),
            });
            assert_eq!(scalar.as_deref(), expected);
        }
    }
}

#[test]
fn given_escaped_field_name_when_borrowing_then_find_selected_value() {
    let data = br#"{"\u0070k":{"S":"key"},"unused":{"S":"ignored"}}"#;
    let mut deserializer = serde_json::Deserializer::from_slice(data);
    let selected = ScalarSelection(&["pk"])
        .deserialize(&mut deserializer)
        .unwrap();
    deserializer.end().unwrap();
    assert_eq!(selected, vec![Some(Cow::Borrowed("key"))]);
}

#[test]
fn given_invalid_selected_scalars_when_borrowing_then_reject() {
    for data in [
        r#"{"pk":{}}"#,
        r#"{"pk":{"BOOL":true}}"#,
        r#"{"pk":{"S":"key","N":"1"}}"#,
        r#"{"pk":{"S":1}}"#,
    ] {
        let mut deserializer = serde_json::Deserializer::from_str(data);
        let error = ScalarSelection(&["pk"])
            .deserialize(&mut deserializer)
            .unwrap_err();
        assert!(error.is_data(), "{error}");
    }
    let mut deserializer = serde_json::Deserializer::from_str(r#"{"pk":{"S":"key"}}{}"#);
    ScalarSelection(&["pk"])
        .deserialize(&mut deserializer)
        .unwrap();
    assert!(deserializer.end().unwrap_err().is_syntax());
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_varied_wire_items_when_extracting_repeatedly_then_report_cost() {
    for case in &CASES {
        let wire = build_wire_item(case);
        let fields = &FIELDS[..case.field_count];
        let size = wire.payload_len();
        for mode in ["full_decode", "single_scan", "scalar_scan", "borrowed_scan"] {
            let label = format!("{}_{}", case.name, mode);
            let guard = AllocationGuard::start(
                module_path!(),
                "given_varied_wire_items_when_extracting_repeatedly_then_report_cost",
                file!(),
                line!(),
                Some(&label),
            );
            let started = Instant::now();
            let mut total = 0;
            for _ in 0..case.iterations {
                if matches!(mode, "scalar_scan" | "borrowed_scan") {
                    let selected = if mode == "scalar_scan" {
                        black_box(&wire).scalar_attributes(fields).unwrap()
                    } else {
                        extract_borrowed_scalars(black_box(&wire), fields)
                    };
                    total += black_box(selected).iter().flatten().count();
                    continue;
                }
                let selected = if mode == "full_decode" {
                    extract_after_full_decode(black_box(&wire), fields)
                } else {
                    black_box(&wire).project_attributes(fields).unwrap()
                };
                total += black_box(selected).len();
            }
            let elapsed = started.elapsed();
            let report = guard.finish();
            alloc_counter::emit_report(&report).unwrap();
            assert_eq!(total, case.iterations * case.present_field_count);
            println!(
                "write_extraction case={} mode={mode} item_bytes={size} fields={} iterations={} \
                 elapsed_ns={}",
                case.name,
                case.field_count,
                case.iterations,
                elapsed.as_nanos(),
            );
        }
    }
}
