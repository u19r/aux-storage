//! Internal HTTP error mapping helpers for aux-storage services.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod api_error;
mod constants;

pub use crate::{api_error::*, constants::*};
