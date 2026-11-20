use storage_types::IndexName;

use super::key_layout::*;

#[test]
fn given_gsi_table_families_when_encoded_then_each_has_the_table_first_bytes() {
    let table = [b'T', 1, 2, 3, 4];
    assert_eq!(
        entry_key(0x0102_0304, 7, b"x").unwrap(),
        [&table[..], b"g", &[0, 7], b"x"].concat()
    );
    assert_eq!(
        gsi_build_key(0x0102_0304, 7).unwrap(),
        [&table[..], b"b", &[0, 7]].concat()
    );
    assert_eq!(
        gsi_name_lock_key(0x0102_0304, &IndexName::new("ix")).unwrap(),
        [&table[..], b"B", b"ix"].concat()
    );
    // The runnable index stays a global index of table ids.
    assert_eq!(gsi_runnable_key(0x0102_0304, 7), [b'f', 1, 2, 3, 4, 0, 7]);
}
