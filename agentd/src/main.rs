//! `t3-agentd`: runs workspace operations (files, git, terminals, processes,
//! previews) for a T3 Code server that reaches it over a WebSocket.
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use t3_agentd::{Agentd, Config, PrepareDirConfig, DEFAULT_PREPARE_DIR_TIMEOUT_SECS};
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
    /// Program to run before first using a directory under `--root` as a working directory.
    #[arg(long, env = "T3_AGENTD_PREPARE_DIR_COMMAND", value_name = "PROGRAM")]
    prepare_dir_command: Option<PathBuf>,
    /// Seconds to wait for `--prepare-dir-command` before timing out.
    #[arg(
        long,
        env = "T3_AGENTD_PREPARE_DIR_TIMEOUT_SECS",
        default_value_t = DEFAULT_PREPARE_DIR_TIMEOUT_SECS
    )]
    prepare_dir_timeout_secs: u64,
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
    // Worktrees are created under the state directory.
    let worktrees = t3code_server::config::state_base_dir(&state_dir).join("worktrees");
    let prepare_dir = args
        .prepare_dir_command
        .filter(|command| !command.as_os_str().is_empty())
        .map(|command| PrepareDirConfig {
            command,
            timeout: Duration::from_secs(args.prepare_dir_timeout_secs),
        });
    let agentd = Agentd::new(
        workspace,
        Config {
            token: token()?,
            root,
            extra_roots: vec![worktrees],
            buffer_dir: state_dir.join("streams"),
            buffer_limit: args.buffer_limit,
            poll_interval: Duration::from_millis(POLL_INTERVAL_MS),
            prepare_dir,
        },
    )?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "t3-agentd listening");
    axum::serve(listener, agentd.router()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prepare_dir_flags_with_default_timeout() {
        let parsed = Args::try_parse_from([
            "t3-agentd",
            "--prepare-dir-command",
            "/usr/local/bin/prepare-dir",
        ])
        .unwrap();
        assert_eq!(
            parsed.prepare_dir_command,
            Some(PathBuf::from("/usr/local/bin/prepare-dir"))
        );
        assert_eq!(parsed.prepare_dir_timeout_secs, 300);

        let custom = Args::try_parse_from([
            "t3-agentd",
            "--prepare-dir-command",
            "/usr/local/bin/prepare-dir",
            "--prepare-dir-timeout-secs",
            "45",
        ])
        .unwrap();
        assert_eq!(custom.prepare_dir_timeout_secs, 45);
    }
}
