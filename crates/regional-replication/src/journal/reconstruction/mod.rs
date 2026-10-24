mod authority;
mod fold;
mod record;
mod scan;

pub(in crate::journal) use authority::{RetainedAuthorityRequest, WinnerAuthority};
pub(in crate::journal) use record::ExpectedWinner;
