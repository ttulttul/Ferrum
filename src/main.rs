use clap::Parser;
use t3code_server::{default_data_dir, default_provider_command, run_server, AppState};
#[derive(Parser, Debug)]
#[command(
    name = "t3code-server",
    about = "Headless T3 Code agent orchestration server"
)]
struct Args {
    #[arg(long, env = "T3CODE_LISTEN", default_value = "127.0.0.1:3773")]
    listen: std::net::SocketAddr,
    #[arg(long, env = "T3CODE_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,
    #[arg(long, env = "T3_PROVIDER_COMMAND", value_delimiter = ' ')]
    provider_command: Option<Vec<String>>,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let state = AppState::new(
        args.data_dir.unwrap_or_else(default_data_dir),
        args.provider_command
            .unwrap_or_else(default_provider_command),
    )
    .await?;
    run_server(state, args.listen).await
}
