//! Ordinary retained images and explicit record attachment share one physical
//! pool.

mod codec;
mod key_layout;
pub(crate) mod layout;
mod record;
mod store;

#[cfg(test)]
mod codec_tests;
#[cfg(test)]
mod key_layout_tests;
