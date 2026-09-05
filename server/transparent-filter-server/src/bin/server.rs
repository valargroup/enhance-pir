//! Transparent activity filter service.
//!
//! Runs independently of the Enhance PIR coordinator. In the production
//! deployment it happens to sit on the same host, next to the same Zakura
//! archive node, but it shares no state with the coordinator and can be
//! stopped, restarted or moved without touching it.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;
use transparent_filter_server::ingest;
use transparent_filter_server::service::{router, Phase, ServiceState};
use transparent_filter_server::store::FilterStore;
use transparent_filter_server::zakura::ZakuraClient;

#[derive(Parser, Clone)]
#[command(
    name = "transparent-filter-server",
    about = "Zcash transparent activity filter service (zcash-transparent-basic-v1)"
)]
struct Cli {
    /// Loopback by default: this service has no public route.
    #[arg(long, default_value = "127.0.0.1:8090")]
    listen: SocketAddr,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    #[arg(long)]
    zakura_cookie: PathBuf,
    #[arg(long, default_value = "./transparent-filter-data")]
    data_dir: PathBuf,
    /// First height to publish a filter for. Defaults to Ironwood activation.
    #[arg(long, default_value_t = transparent_filter::START_HEIGHT)]
    start_height: u64,
    /// Transactions retained in the previous-output cache.
    #[arg(long, default_value_t = transparent_filter_server::prevout::DEFAULT_CACHE_TRANSACTIONS)]
    cache_transactions: usize,
    #[arg(long, default_value_t = 10)]
    poll_seconds: u64,
    /// Blocks between durable checkpoints during backfill.
    #[arg(long, default_value_t = 1_000)]
    commit_every: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();

    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    // Chain identity comes from the node, once, and is then pinned in the
    // store: a store built against one chain must never be served as another.
    let genesis_hash = zakura.genesis_hash().await?;
    if genesis_hash != transparent_filter::MAINNET_GENESIS_DISPLAY {
        tracing::warn!(
            %genesis_hash,
            expected = transparent_filter::MAINNET_GENESIS_DISPLAY,
            "node is not on Zcash mainnet; the store will be pinned to this chain"
        );
    }
    let store = FilterStore::open(
        &cli.data_dir,
        transparent_filter::PROFILE,
        &genesis_hash,
        cli.start_height,
    )?;
    tracing::info!(
        covered_through = ?store.covered_through(),
        filters_stored = store.filters_stored(),
        start_height = cli.start_height,
        "opened filter store"
    );

    let state = ServiceState::new(store);
    let ingest_state = state.clone();
    let ingest_cli = cli.clone();
    tokio::spawn(async move {
        loop {
            if let Err(error) = ingest::run(
                zakura.clone(),
                ingest_state.clone(),
                ingest_cli.cache_transactions,
                ingest_cli.poll_seconds,
                ingest_cli.commit_every,
            )
            .await
            {
                tracing::error!(%error, "filter ingestion stopped");
                ingest_state
                    .set_phase(Phase::Failed {
                        reason: "ingestion failed; retrying; inspect local logs".to_string(),
                    })
                    .await;
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, "transparent filter service started");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
