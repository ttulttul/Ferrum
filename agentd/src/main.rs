//! `t3-agentd`: runs workspace operations (files, git, terminals, processes,
//! previews) for a T3 Code server that reaches it over a WebSocket.
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use t3_agentd::{Agentd, Config};
use t3code_server::workspace::LocalWorkspace;
use t3code_server::workspace_protocol::{POLL_INTERVAL_MS, TOKEN_ENV, TOKEN_FILE_ENV};

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// The address to listen on.
    #[arg(long, env = "T3_AGENTD_LISTEN", default_value = "127.0.0.1:7790")]
    listen: SocketAddr,
    /// The directory projects live under (default: the current directory).
    #[arg(long, env = "T3_AGENTD_ROOT")]
    root: Option<PathBuf>,
    /// Where agentd keeps its state (default: ~/.t3/agentd).
    #[arg(long, env = "T3_AGENTD_STATE_DIR")]
    state_dir: Option<PathBuf>,
    /// The most bytes of unacknowledged frames one stream may hold.
    #[arg(long, env = "T3_AGENTD_BUFFER_LIMIT", default_value_t = 64 * 1024 * 1024)]
    buffer_limit: u64,
}

/// The token from `T3_AGENTD_TOKEN`, or the file `T3_AGENTD_TOKEN_FILE` names.
fn token() -> anyhow::Result<String> {
    if let Ok(token) = std::env::var(TOKEN_ENV) {
        return Ok(token.trim().to_owned());
    }
    if let Ok(path) = std::env::var(TOKEN_FILE_ENV) {
        return Ok(std::fs::read_to_string(path)?.trim().to_owned());
    }
    anyhow::bail!("set {TOKEN_ENV} or {TOKEN_FILE_ENV} to the token servers must present")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let root = match args.root {
        Some(root) => root,
        None => std::env::current_dir()?,
    };
    let state_dir = match args.state_dir {
        Some(dir) => dir,
        None => std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.clone())
            .join(".t3/agentd"),
    };
    std::fs::create_dir_all(&state_dir)?;
    let workspace = Arc::new(LocalWorkspace::open(&state_dir)?);
    let agentd = Agentd::new(
        workspace,
        Config {
            token: token()?,
            root,
            buffer_dir: state_dir.join("streams"),
            buffer_limit: args.buffer_limit,
            poll_interval: Duration::from_millis(POLL_INTERVAL_MS),
        },
    )?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "t3-agentd listening");
    axum::serve(listener, agentd.router()).await?;
    Ok(())
}
