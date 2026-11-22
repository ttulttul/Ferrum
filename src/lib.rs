#![recursion_limit = "512"]
// The workers build compiles a slice of modules written for the full server.
#![cfg_attr(
    not(feature = "native"),
    allow(dead_code, unused_imports, unused_variables)
)]
//! T3 Code environment server. The `native` feature (default) builds the full
//! server; `workers` builds the platform-neutral control plane for wasm32.

pub mod access_http;
pub mod asset_grant;
#[cfg(feature = "native")]
pub mod asset_http;
pub mod auth_service;
#[cfg(feature = "native")]
pub mod cloud_link;
mod codex_models;
#[cfg(feature = "native")]
pub mod config;
#[cfg(feature = "native")]
mod config_rpc;
mod contracts;
pub mod control;
pub mod control_state;
mod control_streams;
pub mod cors;
mod dispatch;
mod dpop_proof;
pub mod environment;
#[cfg(feature = "native")]
mod environment_http;
#[cfg(feature = "native")]
mod environment_theme;
#[cfg(feature = "native")]
pub mod guest_service;
#[cfg(feature = "native")]
pub mod host_rpc;
#[cfg(feature = "native")]
pub mod issues;
#[cfg(feature = "native")]
pub mod local_services;
#[cfg(feature = "native")]
pub mod login_shell;
#[cfg(feature = "native")]
pub mod machine_kind;
#[cfg(feature = "native")]
pub mod managed_endpoint;
pub mod mcp_oauth;
mod model_manifest;
#[cfg(feature = "native")]
mod native_child_work;
#[cfg(feature = "native")]
mod npm_release;
mod orchestration;
#[cfg(feature = "native")]
mod projects;
mod rpc_protocol;
pub mod runtime;
mod scratch;
mod server_config;
mod settings;
pub mod subscription;
mod wire_json;
#[cfg(feature = "native")]
pub use settings::policy::ManagedPolicyFileError;
mod thread_events;
mod thread_projection;
mod title_generation;
mod turn;
mod wire_items;
use orchestration::dispatch_orchestration_command;
#[cfg(feature = "native")]
pub mod provider_http;
#[cfg(feature = "native")]
pub mod provider_rpc;
#[cfg(feature = "native")]
mod provider_status;
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
mod scheduled_tasks;
#[cfg(feature = "native")]
pub mod server_runtime;
#[cfg(feature = "native")]
mod static_http;
#[cfg(feature = "native")]
mod storage_cleanup;
#[cfg(feature = "native")]
mod thread_search;
#[cfg(feature = "native")]
pub mod voice;
pub mod workspace;
pub mod workspace_protocol;
#[cfg(feature = "native")]
pub mod worktree_setup;

pub use contracts::{http_known, rpc_known, HTTP_CONTRACTS, RPC_CONTRACTS};

pub const PROTOCOL_VERSION: u32 = 2;
pub mod event_log;
pub mod persistence;
pub mod platform;
mod projection_shell;
mod provider_projection;
mod provider_state;
mod runtime_recovery;
pub mod shared_store;
mod shell_stream;
mod store;
pub use store::*;
pub(crate) use store::{now, CommandReceipt};
#[cfg(feature = "native")]
mod acp_install;
#[cfg(feature = "native")]
mod acp_registry;
#[cfg(feature = "native")]
mod peer_link_requests;
#[cfg(feature = "native")]
mod peer_links;
#[cfg(feature = "native")]
mod peer_mcp;
#[cfg(feature = "native")]
mod project_clone;
#[cfg(feature = "native")]
mod project_favicon;
#[cfg(feature = "native")]
mod server;
#[cfg(feature = "native")]
mod source_control_clone;
#[cfg(feature = "native")]
mod source_control_lookup;
#[cfg(feature = "native")]
mod startup_trace;
#[cfg(feature = "native")]
pub mod statev2;
pub use access_http::{auth_error, trace_id};
#[cfg(feature = "native")]
pub use server::*;
