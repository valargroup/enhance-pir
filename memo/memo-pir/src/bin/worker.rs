use clap::Parser;
use memo_pir::worker::{router, WorkerState};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "memo-pir-worker", about = "Ironwood memo PIR row-shard worker")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:8091")]
    listen: SocketAddr,
    #[arg(long, default_value = "./memo-worker-data")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let state = WorkerState::new(cli.data_dir)?;
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, "memo PIR worker ready");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
