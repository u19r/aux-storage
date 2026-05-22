//! Property-based tests for `SortableNumeric` ordering invariants.

use proptest::prelude::*;

use crate::numeric::SortableNumeric;

// Helper to generate decimal-like numeric strings (no exponent) with optional
// sign and decimals
fn numeric_string_strategy() -> impl Strategy<Value = String> {
    // Limit length to keep shrinking efficient and avoid extremely huge numbers
    // Format: optional '-' then 1..10 digits, optional fractional part with 1..6
    // digits
    let int_part = "[0-9]{1,10}"; // we will filter leading zeros except single zero below
    let frac_part = "[0-9]{1,6}";
    // Build a regex that captures integers or decimals: -?\d+(?:\.\d+)?
    // Use prop::string::string_regex
    prop::string::string_regex(&format!("-?(?:{int_part})(?:\\.{frac_part})?"))
        .unwrap()
        .prop_filter("normalize numbers", |s| {
            // Reject forms like '-' or '.' or '-.' (shouldn't happen) and enforce no
            // leading zeros unless zero itself
            if s.is_empty() || s == "-" {
                return false;
            }
            let parts: Vec<&str> = s.split('.').collect();
            let int = parts[0];
            // Use strip_prefix to satisfy clippy::manual_strip; safe because we only remove
            // ASCII '-'
            let int_digits = int.strip_prefix('-').unwrap_or(int);
            if int_digits.len() > 1 && int_digits.starts_with('0') {
                return false;
            }
            true
        })
}

proptest! {
    #[test]
    fn descending_preserves_total_order(a in numeric_string_strategy(), b in numeric_string_strategy()) {
        let enc_a = SortableNumeric::descending(&a).expect("encode a");
        let enc_b = SortableNumeric::descending(&b).expect("encode b");

        let dec_a: rust_decimal::Decimal = a.parse().unwrap();
        let dec_b: rust_decimal::Decimal = b.parse().unwrap();
        prop_assume!(dec_a != dec_b);

        let ord_original = dec_a.cmp(&dec_b);
        let ord_encoded = enc_a.as_str().cmp(enc_b.as_str());

        // Descending encoding means greater original number should produce lexicographically smaller string
        prop_assert_eq!(ord_original, ord_encoded.reverse());
    }

    #[test]
    fn descending_is_injective(a in numeric_string_strategy(), b in numeric_string_strategy()) {
        prop_assume!(a != b);
        let dec_a: rust_decimal::Decimal = a.parse().unwrap();
        let dec_b: rust_decimal::Decimal = b.parse().unwrap();
        prop_assume!(dec_a != dec_b);
        let enc_a = SortableNumeric::descending(&a).expect("encode a");
        let enc_b = SortableNumeric::descending(&b).expect("encode b");
        prop_assert_ne!(enc_a.as_str(), enc_b.as_str());
    }
}
