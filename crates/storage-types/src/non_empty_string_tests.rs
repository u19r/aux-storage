use super::non_empty_string::NonEmptyString;

#[test]
fn given_value_when_try_new_then_ok() {
    let value = NonEmptyString::try_new("pk".to_owned()).expect("value constructs");
    assert_eq!(value.as_str(), "pk");
    assert_eq!(value.as_ref() as &str, "pk");
}

#[test]
fn given_empty_when_try_new_then_error() {
    NonEmptyString::try_new(String::new()).expect_err("empty rejects");
}
