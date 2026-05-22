use crate::err_validation;

#[test]
fn validation_format() {
    let validation_error = err_validation("x", "bad");
    let msg = format!("{validation_error:?}");
    assert!(msg.contains("x:bad"));
}
