#[derive(Clone, Copy)]
pub enum PointReadEvent {
    Attempt,
    Unprocessed(u64),
}

pub(super) type PointReadObserver = Option<fn(PointReadEvent)>;
