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
    tailscale_serve_enabled: Option<bool>,
    tailscale_serve_port: Option<u16>,
    shell_environment_prepared: Option<bool>,
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
use t3code_server::server_runtime::{
    ServerAlreadyRunningError, ServerOwnership, EXIT_CODE_STATE_DIR_OWNED,
};
use t3code_server::{default_provider_command, run_server_owned, AppState};
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
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    tailscale_serve: Option<bool>,
    #[arg(long)]
    tailscale_serve_port: Option<u16>,
    /// Built web client directory to serve at `/`.
    #[arg(long, env = "T3CODE_STATIC_DIR")]
    static_dir: Option<PathBuf>,
    #[arg(long, env = "T3_PROVIDER_COMMAND", value_delimiter = ' ')]
    provider_command: Option<Vec<String>>,
}
fn main() -> anyhow::Result<()> {
    // An ACP agent runs these for each session and tool call: they speak on
    // standard output, so they start before anything logs there.
    let mut argv = std::env::args().skip(1);
    match argv.next().as_deref() {
        Some(command @ ("acp-mcp-bridge" | "acp-mcp-call")) => {
            let rest: Vec<String> = argv.collect();
            std::process::exit(t3code_server::acp_mcp_bridge::run(command, &rest));
        }
        // `browser setup`, as the reference's `t3 browser setup`.
        Some("browser") => {
            let rest: Vec<String> = argv.collect();
            if rest.first().map(String::as_str) == Some("setup") {
                std::process::exit(t3code_server::browser_setup::run(&rest[1..]));
            }
            eprintln!("Usage: t3code-server browser setup [--base-dir <dir>]");
            std::process::exit(2);
        }
        _ => {}
    }
    // The reference logs at Info by default; RUST_LOG still overrides.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let args = Args::parse();
    let bootstrap = args.bootstrap_fd.map(read_bootstrap).transpose()?;
    if !args.relay {
        // As the reference's fixPath: before any thread starts, take the
        // login shell's PATH, which every provider launch and probe inherits.
        t3code_server::login_shell::fix_path(
            bootstrap
                .as_ref()
                .and_then(|b| b.shell_environment_prepared)
                .unwrap_or(false),
        );
    }
    // A worker polls a request's whole future chain on its stack, and a
    // debug build's async frames are large (answering a link card took
    // ~2 MiB, tokio's default), so the workers get more room.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(t3code_server::WORKER_STACK_BYTES)
        .enable_all()
        .build()?;
    match runtime.block_on(serve(args, bootstrap)) {
        Err(error) if error.is::<t3code_server::ManagedPolicyFileError>() => {
            eprintln!("{error}");
            std::process::exit(77)
        }
        Err(error) if error.is::<ServerAlreadyRunningError>() => {
            eprintln!("{error}");
            std::process::exit(EXIT_CODE_STATE_DIR_OWNED)
        }
        result => result,
    }
}

async fn serve(args: Args, bootstrap: Option<Bootstrap>) -> anyhow::Result<()> {
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
    // Before anything opens the database: one server per T3 home, shared
    // with the TypeScript server's lock on userdata/server-owner.sqlite.
    let ownership =
        ServerOwnership::acquire(config::ServerPaths::from_state_dir(&dir).runtime_state)?;
    let mut config = config::RuntimeConfig::load()?;
    if let Some(static_dir) = args.static_dir {
        config.static_dir = Some(static_dir);
    }
    config.mode = args.mode.unwrap_or_else(|| {
        if bootstrap.is_some() {
            "desktop"
        } else {
            "web"
        }
        .into()
    });
    config.requested_host = args
        .listen
        .map(|listen| listen.ip())
        .or(args.host)
        .or_else(|| bootstrap.as_ref().map(|b| b.host));
    config.host = Some(listen.ip());
    config.port = Some(listen.port());
    config.no_browser = args.no_browser.or(config.no_browser);
    config.tailscale_serve = args
        .tailscale_serve
        .or(config.tailscale_serve)
        .or_else(|| bootstrap.as_ref().and_then(|b| b.tailscale_serve_enabled));
    config.tailscale_serve_port = args
        .tailscale_serve_port
        .or(config.tailscale_serve_port)
        .or_else(|| bootstrap.as_ref().and_then(|b| b.tailscale_serve_port));
    let auth = bootstrap
        .map(|b| AuthService::with_bootstrap(b.desktop_bootstrap_token))
        .unwrap_or_default();
    let auth = auth
        .with_desktop(config.mode == "desktop")
        .with_port(listen.port());
    let state = AppState::with_config(
        dir,
        args.provider_command
            .unwrap_or_else(default_provider_command),
        auth,
        config,
    )
    .await?;
    state.enable_provider_status_checks();
    run_server_owned(state, listen, Some(ownership)).await
}
