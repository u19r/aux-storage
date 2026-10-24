use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanProgress<Cut, Cursor> {
    pub low: Cut,
    pub after: Option<Cursor>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TailProgress<Cut> {
    pub cursor: Cut,
    pub high: Cut,
}

#[derive(PartialEq, Eq)]
pub enum ScanPageEnd<Cursor, Cut> {
    Continue(Cursor),
    Complete(Cut),
}

#[derive(PartialEq, Eq)]
pub enum TailPageEnd<Cut> {
    Continue(Cut),
    ReachedThrough,
}

pub enum ScanTransition<Cut, Cursor> {
    Scanning(ScanProgress<Cut, Cursor>),
    CatchingUp(TailProgress<Cut>),
}
