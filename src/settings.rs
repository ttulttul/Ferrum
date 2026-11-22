//! The shared settings file has three views: sparse, effective, and client-redacted.
//! Serialize the complete read/modify/write transaction, including sidecar secrets.
#[cfg(feature = "native")]
pub(crate) mod failure;
// The patch semantics are shared with the control plane; its migrations are native.
#[cfg_attr(not(feature = "native"), allow(dead_code))]
pub(crate) mod mutations;
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
