//! The shared settings file has three views: sparse, effective, and client-redacted.
//! Serialize the complete read/modify/write transaction, including sidecar secrets.
pub(crate) mod failure;
pub(crate) mod mutations;
pub(crate) mod policy;
pub(crate) mod schema;
pub(crate) mod secrets;
mod service;
pub use service::*;

mod json_syntax;
#[allow(unused_imports)]
pub(crate) use json_syntax::parse;
