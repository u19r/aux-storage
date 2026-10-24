use super::{
    ConformanceTestBackend, parse_foundationdb_selector, require_selected_conformance_backends,
};

#[test]
#[should_panic(expected = "no storage-api conformance backend selected")]
fn empty_conformance_selection_is_rejected() {
    let _: Vec<ConformanceTestBackend> = require_selected_conformance_backends(Vec::new());
}

#[test]
fn foundationdb_selection_is_explicit() {
    assert_eq!(parse_foundationdb_selector(None), Ok(false));
    assert_eq!(parse_foundationdb_selector(Some("1")), Ok(true));
    assert!(parse_foundationdb_selector(Some("0")).is_err());
}
