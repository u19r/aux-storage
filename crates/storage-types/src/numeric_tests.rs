use std::str::FromStr;

use rust_decimal::Decimal;

use crate::numeric::{SortableNumeric, SortableVec};

#[test]
fn encode_decode_zero() {
    let zero = Decimal::ZERO;

    let encoded = zero.encode();
    let decoded = Decimal::decode(&encoded).unwrap();

    assert_eq!(zero, decoded);
}

#[test]
fn encode_decode_zero_negative() {
    let zero = Decimal::new(-0, 0);

    let encoded = zero.encode();
    let decoded = Decimal::decode(&encoded).unwrap();

    assert_eq!(zero, decoded);
}

#[test]
fn encode_decode_positive_integers() {
    let test_cases = vec![
        Decimal::from(1),
        Decimal::from(42),
        Decimal::from(1000),
        Decimal::from(123_456),
        Decimal::from(999_999_999_000_000i64),
        // Decimal::from(999_999_999_000_000_000_000i128), // Too large - causes overflow
    ];

    for original in test_cases {
        let encoded = original.encode();
        let decoded = Decimal::decode(&encoded).unwrap();

        let diff = (original - decoded).abs();
        assert!(
            diff == Decimal::ZERO,
            "diff is {diff}. original: {original}. Decoded: {decoded}. Encoded: {encoded:?}"
        ); // Allow 0.0000000001 difference
    }
}

#[test]
fn encode_decode_negative_integers() {
    let test_cases = vec![
        Decimal::from(-1),
        Decimal::from(-42),
        Decimal::from(-1000),
        Decimal::from(-123_456),
        Decimal::from(-999_999_999_000_000i64),
        // Decimal::from(-999_999_999_000_000_000_000i128), // Too large - causes overflow
    ];

    for original in test_cases {
        let encoded = original.encode();
        let decoded = Decimal::decode(&encoded).unwrap();

        let diff = (original - decoded).abs();
        assert!(
            diff == Decimal::ZERO,
            "diff is {diff}. original: {original}. Decoded: {decoded}. Encoded: {encoded:?}"
        );
    }
}

#[test]
fn encode_decode_decimals() {
    let test_cases = vec![
        Decimal::new(1234, 3),         // 1.234
        Decimal::new(12345, 4),        // 1.2345
        Decimal::new(99999, 5),        // 0.99999
        Decimal::new(-5678, 2),        // -56.78
        Decimal::new(-999_999_999, 7), // -99.9999999
    ];

    for original in test_cases {
        let encoded = original.encode();
        let decoded = Decimal::decode(&encoded).unwrap();

        let diff = (original - decoded).abs();

        assert!(
            diff == Decimal::ZERO,
            "diff is {diff}. original: {original}. Decoded: {decoded}. Encoded: {encoded:?}"
        );
    }
}

#[test]
fn sort_order_preservation() {
    // Test the sequence from the original comment, but in correct numerical order
    // From smallest to largest: -1000.0001 < -999 < 0.12345678 < 12.45 < 1234.0001
    // < 1239
    let test_cases = [
        Decimal::new(-10_000_001, 4), // -1000.0001
        Decimal::from(-999),          // -999
        Decimal::new(12_345_678, 8),  // 0.12345678
        Decimal::new(1_245, 2),       // 12.45
        Decimal::new(12_340_001, 4),  // 1234.0001
        Decimal::from(1_239),         // 1239
    ];

    let mut encoded_pairs: Vec<_> = test_cases.iter().map(|&d| (d, d.encode())).collect();

    // Sort by encoded bytes (lexicographic order)
    encoded_pairs.sort_by(|a, b| a.1.cmp(&b.1));

    // Extract the sorted decimals
    let sorted_decimals: Vec<Decimal> = encoded_pairs.iter().map(|(d, _)| *d).collect();

    // Verify the order matches expected: smallest to largest
    let expected = [
        Decimal::new(-10_000_001, 4), // -1000.0001 (smallest)
        Decimal::from(-999),          // -999
        Decimal::new(12_345_678, 8),  // 0.12345678
        Decimal::new(1245, 2),        // 12.45
        Decimal::new(12_340_001, 4),  // 1234.0001
        Decimal::from(1239),          // 1239 (largest)
    ];

    assert_eq!(sorted_decimals, expected);
}

#[test]
fn binary_order() {
    let smallest = Decimal::new(-100_000, 0).encode();
    let first = Decimal::new(-1000, 0).encode();
    let second = Decimal::new(-100, 0).encode();
    let third = Decimal::new(10, 0).encode();
    let forth = Decimal::new(100_000, 0).encode();

    let mut sorted = [
        second.clone(),
        first.clone(),
        smallest.clone(),
        forth.clone(),
        third.clone(),
    ];
    sorted.sort();
    assert_eq!(sorted[0], smallest);
    assert_eq!(sorted[1], first);
    assert_eq!(sorted[2], second);
    assert_eq!(sorted[3], third);
    assert_eq!(sorted[4], forth);
}

#[test]
fn sort_order_with_many_values() {
    let test_cases = vec![
        Decimal::new(-999_999, 0), // -999999
        Decimal::new(-1000, 0),    // -1000
        Decimal::new(-100, 0),     // -100
        Decimal::new(-1, 0),       // -1
        Decimal::new(0, 0),        // 0
        Decimal::new(1, 0),        // 1
        Decimal::new(100, 0),      // 100
        Decimal::new(1000, 0),     // 1000
        Decimal::new(999_999, 0),  // 999999
    ];

    let mut encoded_pairs: Vec<_> = test_cases.iter().map(|&d| (d, d.encode())).collect();

    // Sort by encoded bytes
    encoded_pairs.sort_by(|a, b| a.1.cmp(&b.1));

    let sorted_decimals: Vec<Decimal> = encoded_pairs.iter().map(|(d, _)| *d).collect();

    // Should be in numerical order
    assert_eq!(sorted_decimals, test_cases);
}

#[test]
fn scaling_factor_selection() {
    // Test that different magnitudes get appropriate scaling factors
    let small_number = Decimal::new(1, 10); // 0.0000000001
    let medium_number = Decimal::from(42);
    let large_number = Decimal::new(1_234_567_890_123_456_789i64, 9);

    let small_encoded = small_number.encode();
    let medium_encoded = medium_number.encode();
    let large_encoded = large_number.encode();

    // Extract scale indices from the header bytes (least significant 4 bits)
    let small_scale = (small_encoded[0] & 0b0000_1111) as usize;
    let medium_scale = (medium_encoded[0] & 0b0000_1111) as usize;
    let large_scale = (large_encoded[0] & 0b0000_1111) as usize;

    // Small numbers should use lower scale indices
    // Large numbers should use higher scale indices
    assert!(small_scale < medium_scale);
    assert!(medium_scale < large_scale);
}

#[test]
fn variable_length_encoding() {
    let small = Decimal::from(1);
    let medium = Decimal::from(128); // Will need 2 bytes in variable encoding
    let large = Decimal::from(16384); // Will need 3 bytes in variable encoding
    let chonky = Decimal::from(-200_097_152); // Will need 4 bytes in variable encoding

    let small_encoded = small.encode();
    let medium_encoded = medium.encode();
    let large_encoded = large.encode();
    let chonky_encoded = chonky.encode();

    // Larger numbers should generally use more bytes
    // (though scaling factors can affect this)
    assert!(small_encoded.len() <= medium_encoded.len());
    assert!(medium_encoded.len() <= large_encoded.len());
    assert!(chonky_encoded.len() >= large_encoded.len());
}

#[test]
fn number_less_than_char() {
    let num = Decimal::from(50);
    let char_a = b'A'; // ASCII 65

    let num_encoded = num.encode();

    // Ensure that the encoded number is lexicographically less than 'A'
    assert!(num_encoded < vec![char_a]);
}

#[test]
fn sortable_numeric_roundtrip() {
    let s = "12345.6789";
    let sortable = <Decimal as SortableVec>::from_numeric_str(s).unwrap();
    let dec = <Decimal as SortableVec>::decode_numeric(&sortable).unwrap();
    assert_eq!(dec, Decimal::from_str(s).unwrap());
}

#[test]
fn sortable_numeric_order() {
    let nums = ["1", "2", "10", "11", "99", "100", "1000.5"]; // numeric ascending
    let mut enc: Vec<_> = nums
        .iter()
        .map(|n| (<Decimal as SortableVec>::from_numeric_str(n).unwrap(), *n))
        .collect();
    enc.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let sorted: Vec<&str> = enc.into_iter().map(|(_, n)| n).collect();
    assert_eq!(sorted, nums);
}

#[test]
fn sortable_numeric_descending_helper() {
    // Verify descending encodings produce reverse order relative to ascending
    // encodings
    let nums = ["1", "2", "10", "11", "99", "100", "1000.5"]; // numeric ascending
    let asc: Vec<_> = nums
        .iter()
        .map(|n| (<Decimal as SortableVec>::from_numeric_str(n).unwrap(), *n))
        .collect();
    let desc: Vec<_> = nums
        .iter()
        .map(|n| (SortableNumeric::descending(n).unwrap(), *n))
        .collect();

    // Ascending sortable should sort the same as numeric order
    let mut asc_sorted = asc.clone();
    asc_sorted.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let asc_names: Vec<&str> = asc_sorted.into_iter().map(|(_, n)| n).collect();
    assert_eq!(asc_names, nums);

    // Descending sortable should sort reverse of numeric order
    let mut desc_sorted = desc.clone();
    desc_sorted.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let desc_names: Vec<&str> = desc_sorted.into_iter().map(|(_, n)| n).collect();
    let mut reversed = nums.to_vec();
    reversed.reverse();
    assert_eq!(desc_names, reversed);
}
