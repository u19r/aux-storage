use crate::GsiBackfillPhase;
#[test]
fn terminal() {
    assert!(GsiBackfillPhase::Done.is_terminal());
}
