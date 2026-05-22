use crate::dynamodb_limits::dynamodb_number_size;

#[test]
fn dynamodb_number_size_matches_decimal_and_exponent_boundaries() {
    let cases = [
        ("1.2300", 3),
        ("0.000001", 2),
        ("1E+37", 2),
        ("1E-37", 2),
        ("12300", 3),
        ("-1.2300", 4),
        ("-0.000001", 3),
        ("-1E+37", 3),
        ("+1.2300", 3),
        ("+1E+37", 2),
    ];

    for (value, expected_size) in cases {
        assert_eq!(
            dynamodb_number_size(value),
            expected_size,
            "unexpected DynamoDB item-size bytes for {value}"
        );
    }
}
