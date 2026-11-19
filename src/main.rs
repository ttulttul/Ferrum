use clap::Parser;
use serde::Deserialize;
use std::{
    io::Read,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};
use t3code_server::auth_service::AuthService;
use t3code_server::config;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bootstrap {
    host: IpAddr,
    port: u16,
    t3_home: Option<PathBuf>,
    desktop_bootstrap_token: String,
}

fn read_bootstrap(fd: u32) -> anyhow::Result<Bootstrap> {
    let reader: Box<dyn Read> = match fd {
        0 => Box::new(std::io::stdin()),
        #[cfg(unix)]
        _ => Box::new(std::fs::File::open(format!("/dev/fd/{fd}"))?),
        #[cfg(not(unix))]
        _ => anyhow::bail!("bootstrap via inherited descriptors requires Unix"),
    };
    Ok(serde_json::from_reader(reader.take(65536))?)
}
use t3code_server::{default_provider_command, run_server, AppState};
#[derive(Parser, Debug)]
#[command(
    name = "t3code-server",
    about = "Headless T3 Code agent orchestration server"
)]
struct Args {
    #[arg(long)]
    listen: Option<SocketAddr>,
    #[arg(long, env = "T3CODE_PORT")]
    port: Option<u16>,
    #[arg(long, env = "T3CODE_HOST")]
    host: Option<IpAddr>,
    #[arg(long, env = "T3CODE_BOOTSTRAP_FD")]
    bootstrap_fd: Option<u32>,
    /// Serve the standalone relay contract on this listener.
    #[arg(long)]
    relay: bool,
    #[arg(long, env = "T3CODE_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,
    #[arg(long, env = "T3CODE_HOME")]
    base_dir: Option<std::path::PathBuf>,
    #[arg(long, env = "T3CODE_MODE")]
    mode: Option<String>,
    #[arg(long, env = "T3CODE_NO_BROWSER")]
    no_browser: Option<bool>,
    #[arg(long, env = "T3_PROVIDER_COMMAND", value_delimiter = ' ')]
    provider_command: Option<Vec<String>>,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let bootstrap = args.bootstrap_fd.map(read_bootstrap).transpose()?;
    let listen = args.listen.unwrap_or_else(|| {
        SocketAddr::new(
            args.host
                .or_else(|| bootstrap.as_ref().map(|b| b.host))
                .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            args.port
                .or_else(|| bootstrap.as_ref().map(|b| b.port))
                .unwrap_or(3773),
        )
    });
    // T3CODE_DATA_DIR is retained as a low-level embedding override. The
    // normal CLI and bootstrap paths use a base directory with userdata below.
    let dir = if let Some(data_dir) = args.data_dir {
        data_dir
    } else if let Some(base_dir) = args.base_dir {
        base_dir.join("userdata")
    } else if let Some(base_dir) = bootstrap.as_ref().and_then(|b| b.t3_home.clone()) {
        base_dir.join("userdata")
    } else {
        config::default_base_dir().join("userdata")
    };
    if args.relay {
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let relay = t3code_server::relay_service::RelayService::open(&dir, listener.local_addr()?)?;
        axum::serve(listener, t3code_server::relay_http::router(relay)).await?;
        return Ok(());
    }
    let auth = bootstrap
        .map(|b| AuthService::with_bootstrap(b.desktop_bootstrap_token))
        .unwrap_or_default();
    let state = AppState::with_auth(
        dir,
        args.provider_command
            .unwrap_or_else(default_provider_command),
        auth.with_port(listen.port()),
    )
    .await?;
    run_server(state, listen).await
}
