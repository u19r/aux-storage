use syn::DeriveInput;

use crate::wire_attribute_helpers::{
    is_single_string_tuple_struct, parse_wire_attribute_type_attributes,
};

#[test]
fn given_wire_attribute_parse_with_path_when_parsing_then_returns_parser_path() {
    let input = syn::parse_str::<DeriveInput>(
        r#"
        #[wire_attribute(parse_with = crate::parse_name)]
        struct Name(String);
        "#,
    )
    .expect("derive input");

    let (parse_with, from_string) =
        parse_wire_attribute_type_attributes(&input).expect("attributes");

    assert_eq!(
        parse_with
            .expect("parser")
            .path
            .segments
            .last()
            .expect("last segment")
            .ident,
        "parse_name"
    );
    assert!(!from_string);
}

#[test]
fn given_wire_attribute_parse_with_string_when_parsing_then_accepts_string_path() {
    let input = syn::parse_str::<DeriveInput>(
        r#"
        #[wire_attribute(parse_with = "crate::parse_name", from_string)]
        struct Name(String);
        "#,
    )
    .expect("derive input");

    let (parse_with, from_string) =
        parse_wire_attribute_type_attributes(&input).expect("attributes");

    assert_eq!(
        parse_with
            .expect("parser")
            .path
            .segments
            .last()
            .expect("last segment")
            .ident,
        "parse_name"
    );
    assert!(from_string);
}

#[test]
fn given_unknown_wire_attribute_key_when_parsing_then_rejects_attribute() {
    let input = syn::parse_str::<DeriveInput>(
        r#"
        #[wire_attribute(other = crate::parse_name)]
        struct Name(String);
        "#,
    )
    .expect("derive input");

    let error = match parse_wire_attribute_type_attributes(&input) {
        Ok(_) => panic!("unknown key should fail"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("unknown wire_attribute key"));
}

#[test]
fn given_type_shape_when_checking_tuple_string_then_only_single_string_tuple_matches() {
    let single = syn::parse_str::<DeriveInput>("struct Name(String);").expect("single");
    let named = syn::parse_str::<DeriveInput>("struct Name { value: String }").expect("named");
    let two_fields =
        syn::parse_str::<DeriveInput>("struct Name(String, String);").expect("two fields");
    let non_string = syn::parse_str::<DeriveInput>("struct Name(u64);").expect("non string");

    assert!(is_single_string_tuple_struct(&single.data));
    assert!(!is_single_string_tuple_struct(&named.data));
    assert!(!is_single_string_tuple_struct(&two_fields.data));
    assert!(!is_single_string_tuple_struct(&non_string.data));
}
