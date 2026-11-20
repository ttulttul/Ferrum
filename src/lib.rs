#![recursion_limit = "512"]
// The workers build compiles a slice of modules written for the full server.
#![cfg_attr(
    not(feature = "native"),
    allow(dead_code, unused_imports, unused_variables)
)]
//! T3 Code environment server. The `native` feature (default) builds the full
//! server; `workers` builds the platform-neutral control plane for wasm32.

pub mod access_http;
#[cfg(feature = "native")]
pub mod asset_http;
pub mod auth_service;
#[cfg(feature = "native")]
pub mod config;
#[cfg(feature = "native")]
mod config_rpc;
mod contracts;
pub mod control;
pub mod cors;
mod dpop_proof;
pub mod environment;
#[cfg(feature = "native")]
mod environment_http;
#[cfg(feature = "native")]
pub mod guest_service;
#[cfg(feature = "native")]
pub mod host_rpc;
#[cfg(feature = "native")]
pub mod issues;
#[cfg(feature = "native")]
pub mod local_services;
#[cfg(feature = "native")]
pub mod mcp_oauth;
#[cfg(feature = "native")]
mod native_child_work;
#[cfg(feature = "native")]
mod npm_release;
#[cfg(feature = "native")]
mod orchestration;
#[cfg(feature = "native")]
mod projects;
mod rpc_protocol;
mod server_config;
mod settings;
mod wire_json;
#[cfg(feature = "native")]
pub use settings::policy::ManagedPolicyFileError;
mod thread_projection;
#[cfg(feature = "native")]
use orchestration::dispatch_orchestration_command;
#[cfg(feature = "native")]
pub mod provider_http;
#[cfg(feature = "native")]
pub mod provider_rpc;
#[cfg(feature = "native")]
pub mod pull_requests;
#[cfg(feature = "native")]
pub mod relay_client;
#[cfg(feature = "native")]
pub mod relay_http;
#[cfg(feature = "native")]
pub mod relay_service;
#[cfg(feature = "native")]
pub mod repository_identity;
#[cfg(feature = "native")]
mod rpc;
#[cfg(feature = "native")]
pub mod server_runtime;
#[cfg(feature = "native")]
mod static_http;
#[cfg(feature = "native")]
pub mod voice;
#[cfg(feature = "native")]
mod workspace;
#[cfg(feature = "native")]
pub mod worktree_setup;

pub use contracts::{http_known, rpc_known, HTTP_CONTRACTS, RPC_CONTRACTS};

pub const PROTOCOL_VERSION: u32 = 2;
pub mod event_log;
pub mod platform;
mod store;
pub use store::*;
pub(crate) use store::{now, CommandReceipt};
#[cfg(feature = "native")]
mod server;
pub use access_http::{auth_error, trace_id};
#[cfg(feature = "native")]
pub use server::*;
