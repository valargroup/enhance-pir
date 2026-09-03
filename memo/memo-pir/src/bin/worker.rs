use clap::Parser;
use memo_pir::worker::{router, WorkerState, DEFAULT_EVALUATION_SLOTS};
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
    /// Concurrent shard evaluations this worker admits.
    #[arg(long, default_value_t = DEFAULT_EVALUATION_SLOTS)]
    evaluation_slots: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let state = WorkerState::with_evaluation_slots(cli.data_dir, cli.evaluation_slots)?;
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, "memo PIR worker ready");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
