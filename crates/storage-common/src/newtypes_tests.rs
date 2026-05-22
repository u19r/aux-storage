use crate::PageToken;
#[test]
fn conversions() {
    let t: PageToken = "abc".to_string().into();
    let s: String = t.into();
    assert_eq!(s, "abc");
}
