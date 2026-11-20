//! The shared settings file has three views: sparse, effective, and client-redacted.
//! Serialize the complete read/modify/write transaction, including sidecar secrets.
#[cfg(feature = "native")]
mod failure;
#[cfg(feature = "native")]
mod mutations;
#[cfg(feature = "native")]
pub(crate) mod policy;
pub(crate) mod schema;
#[cfg(feature = "native")]
pub(crate) mod secrets;
#[cfg(feature = "native")]
mod service;
#[cfg(feature = "native")]
pub use service::*;

mod json_syntax;
#[allow(unused_imports)]
pub(crate) use json_syntax::parse;
