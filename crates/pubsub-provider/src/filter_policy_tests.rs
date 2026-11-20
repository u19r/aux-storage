//! W3 test 13: the filter table of design section 3.6, with every semantic
//! row of section 14 (live AWS results) added.

use std::collections::HashMap;

use crate::{FilterPolicy, MessageAttribute, PubsubError};

fn attributes(entries: &[(&str, MessageAttribute)]) -> HashMap<String, MessageAttribute> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn text(value: &str) -> MessageAttribute {
    MessageAttribute::String(value.to_owned())
}

fn number(value: &str) -> MessageAttribute {
    MessageAttribute::Number(value.to_owned())
}

fn matches(policy: &str, entries: &[(&str, MessageAttribute)]) -> bool {
    FilterPolicy::parse(policy)
        .unwrap()
        .matches_attributes(&attributes(entries))
}

fn body_matches(policy: &str, body: &str) -> bool {
    FilterPolicy::parse(policy).unwrap().matches_body(body)
}

fn reason(policy: &str) -> String {
    match FilterPolicy::parse(policy) {
        Err(error @ PubsubError::Validation { .. }) => error.aws_query_message(),
        other => panic!("{policy}: {other:?}"),
    }
}

#[test]
fn given_attribute_policies_when_matched_then_rows_1_to_15_hold() {
    let colors = r#"{"c":["red","blue"]}"#;
    assert!(matches(colors, &[("c", text("blue"))]), "1");
    assert!(!matches(colors, &[("c", text("green"))]), "2");
    assert!(!matches(colors, &[]), "3");
    let range = r#"{"c":["red"],"s":[{"numeric":[">",0,"<=",150]}]}"#;
    assert!(
        matches(range, &[("c", text("red")), ("s", number("150"))]),
        "4"
    );
    assert!(
        matches(range, &[("c", text("red")), ("s", number("150.000001"))]),
        "5: extra digits are truncated (section 14: precision is accepted)"
    );
    assert!(
        !matches(r#"{"s":[{"numeric":["=",5]}]}"#, &[("s", text("5"))]),
        "6"
    );
    let anything_but = r#"{"c":[{"anything-but":["red"]}]}"#;
    assert!(matches(anything_but, &[("c", text("blue"))]), "7a");
    assert!(!matches(anything_but, &[("c", text("red"))]), "7b");
    assert!(
        !matches(anything_but, &[("x", text("y"))]),
        "7c: absent key"
    );
    assert!(
        !matches(
            r#"{"c":[{"anything-but":{"prefix":"r"}}]}"#,
            &[("c", text("rose"))]
        ),
        "8"
    );
    assert!(
        matches(r#"{"c":[{"prefix":"ord-"}]}"#, &[("c", text("ord-1"))]),
        "9"
    );
    assert!(
        !matches(r#"{"c":[{"suffix":".png"}]}"#, &[("c", text("a.PNG"))]),
        "10"
    );
    assert!(
        matches(
            r#"{"c":[{"equals-ignore-case":"Red"}]}"#,
            &[("c", text("rED"))]
        ),
        "11"
    );
    let absent = r#"{"c":[{"exists":false}]}"#;
    assert!(matches(absent, &[("x", text("y"))]), "12a");
    assert!(!matches(absent, &[("c", text("y"))]), "12b");
    assert!(
        matches(
            r#"{"c":["a"]}"#,
            &[(
                "c",
                MessageAttribute::StringArray(r#"["x","a"]"#.to_owned())
            )]
        ),
        "13"
    );
    assert!(
        !matches(
            r#"{"c":["a"]}"#,
            &[("c", MessageAttribute::Binary(b"a".to_vec()))]
        ),
        "14"
    );
    assert!(
        matches(r#"{"$or":[{"a":["1"]},{"b":["2"]}]}"#, &[("b", text("2"))]),
        "15"
    );
}

#[test]
fn given_body_policies_when_matched_then_rows_16_to_18_hold() {
    assert!(
        body_matches(r#"{"o":{"t":["x"]}}"#, r#"{"o":{"t":"x"}}"#),
        "16"
    );
    assert!(!body_matches(r#"{"o":{"t":["x"]}}"#, "not json"), "17");
    assert!(body_matches(r#"{"t":["x"]}"#, r#"{"t":["y","x"]}"#), "18");
}

#[test]
fn given_policy_limits_when_parsed_then_rows_19_to_24_follow_section_14() {
    assert_eq!(
        reason(r#"{"a":["1"],"b":["1"],"c":["1"],"d":["1"],"e":["1"],"f":["1"]}"#),
        "Invalid parameter: FilterPolicy: Filter policy can not have more than 5 keys",
        "19"
    );
    let values = (0..151).map(|value| value.to_string()).collect::<Vec<_>>();
    assert_eq!(
        reason(&format!(
            r#"{{"a":{}}}"#,
            serde_json::to_string(&values).unwrap()
        )),
        "Invalid parameter: FilterPolicy: Filter policy is too complex",
        "20: 151 combinations"
    );
    let at_limit = (0..150).map(|value| value.to_string()).collect::<Vec<_>>();
    assert!(
        FilterPolicy::parse(&format!(
            r#"{{"a":{}}}"#,
            serde_json::to_string(&at_limit).unwrap()
        ))
        .is_ok()
    );
    assert!(
        FilterPolicy::parse(r#"{"c":[{"cidr":"10.0.0.0/8"}]}"#).is_ok(),
        "21: cidr is in"
    );
    assert!(
        FilterPolicy::parse(r#"{"a":{"b":{"c":{"d":{"e":{"f":["x"]}}}}}}"#).is_ok(),
        "22: depth 6 is accepted"
    );
    assert!(
        FilterPolicy::parse(r#"{"c":[{"numeric":[">",1e10]}]}"#).is_ok(),
        "23: range accepted"
    );
    assert!(
        FilterPolicy::parse(r#"{"o":{"t":["x"]}}"#).is_ok(),
        "24: nested object accepted"
    );
    assert!(reason(r#"{"c":[{"bogus":"x"}]}"#).contains("Unrecognized match type bogus"));
    assert!(reason("[1]").contains("FilterPolicy"));
}

#[test]
fn given_section_14_semantics_when_matched_then_aws_results_hold() {
    assert!(
        !matches(r#"{"c":[{"exists":false}]}"#, &[]),
        "no attributes: exists false does not match"
    );
    assert!(
        !matches(
            r#"{"c":[{"exists":true}]}"#,
            &[("c", MessageAttribute::Binary(vec![1]))]
        ),
        "exists true does not match Binary"
    );
    assert!(
        !matches(r#"{"c":[{"numeric":[">",1]}]}"#, &[("c", text("5"))]),
        "numeric vs String"
    );
    assert!(matches(
        r#"{"c":[{"numeric":[">",1]}]}"#,
        &[("c", number("5"))]
    ));
    assert!(
        matches(r#"{"c":[5]}"#, &[("c", number("5.00"))]),
        "numeric literal"
    );
    assert!(
        !matches(r#"{"o":{"t":["x"]}}"#, &[("o", text("x"))]),
        "nested object never matches an attribute"
    );
    let array = MessageAttribute::StringArray(r#"["x","y"]"#.to_owned());
    assert!(
        matches(r#"{"c":["y"]}"#, &[("c", array)]),
        "one array element matches"
    );
}

#[test]
fn given_operators_from_section_14_when_matched_then_each_one_works() {
    let cidr = r#"{"ip":[{"cidr":"10.0.0.0/24"}]}"#;
    assert!(matches(cidr, &[("ip", text("10.0.0.7"))]));
    assert!(!matches(cidr, &[("ip", text("10.0.1.7"))]));
    assert!(matches(
        r#"{"ip":[{"cidr":"2001:db8::/32"}]}"#,
        &[("ip", text("2001:db8::1"))]
    ));
    let wildcard = r#"{"f":[{"wildcard":"img-*.png"}]}"#;
    assert!(matches(wildcard, &[("f", text("img-1.png"))]));
    assert!(!matches(wildcard, &[("f", text("img-1.jpg"))]));
    let suffix = r#"{"f":[{"anything-but":{"suffix":".png"}}]}"#;
    assert!(matches(suffix, &[("f", text("a.jpg"))]));
    assert!(!matches(suffix, &[("f", text("a.png"))]));
    assert!(
        body_matches(r#"{"v":[null]}"#, r#"{"v":null}"#),
        "null literal"
    );
    assert!(
        body_matches(r#"{"v":[true]}"#, r#"{"v":true}"#),
        "true literal"
    );
    assert!(
        !body_matches(r#"{"v":[false]}"#, r#"{"v":true}"#),
        "false literal"
    );
    assert!(
        !matches(r#"{"v":[true]}"#, &[("v", text("true"))]),
        "a literal does not match a String attribute"
    );
}

/// Review C1: numbers compare as exact decimals with five fraction digits;
/// a binary float product would make 0.29 and 0.28999 equal.
#[test]
fn given_decimal_numbers_when_compared_then_five_fraction_digits_are_exact() {
    let above = |bound: &str, value: &str| {
        matches(
            &format!(r#"{{"p":[{{"numeric":[">",{bound}]}}]}}"#),
            &[("p", number(value))],
        )
    };
    assert!(above("0.28999", "0.29"), "0.29 > 0.28999");
    assert!(!above("0.29", "0.29"));
    assert!(matches(r#"{"p":[0.57]}"#, &[("p", number("0.57"))]), "0.57");
    assert!(matches(r#"{"p":[4.35]}"#, &[("p", number("4.35"))]), "4.35");
    assert!(
        matches(
            r#"{"p":[999999999.99999]}"#,
            &[("p", number("999999999.99999"))]
        ),
        "999999999.99999"
    );
    assert!(above("999999999.99998", "999999999.99999"));
    assert!(body_matches(
        r#"{"p":[{"numeric":["=",0.57]}]}"#,
        r#"{"p":0.57}"#
    ));
    assert!(body_matches(
        r#"{"p":[{"numeric":[">",0.28999]}]}"#,
        r#"{"p":0.29}"#
    ));
    assert!(
        matches(r#"{"p":[1e2]}"#, &[("p", number("100"))]),
        "exponent form"
    );
    // Review D11: zero with any exponent is zero.
    assert!(matches(r#"{"p":[0]}"#, &[("p", number("0e100"))]), "0e100");
    assert!(
        matches(r#"{"p":[0]}"#, &[("p", number("0.0e20"))]),
        "0.0e20"
    );
    assert!(above("-1", "0e100"));
}
