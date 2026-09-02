use clap::{Parser, ValueEnum};
use memo_pir::coordinator::{router, CoordinatorPhase, CoordinatorState, WorkerTarget};
use memo_pir::store::MemoStore;
use memo_pir::types::{
    Coverage, ACTIVATION_HEIGHT, CONFIRMATIONS, DEFAULT_LOOKBACK_BLOCKS, DEFAULT_MAX_ACTIVE_SHARDS,
};
use memo_pir::worker::WorkerState;
use memo_pir::zakura::ZakuraClient;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    DistributedFull,
    EmbeddedWindowed,
}

#[derive(Parser, Clone)]
#[command(
    name = "memo-pir-server",
    about = "Standalone Ironwood memo PIR coordinator"
)]
struct Cli {
    #[arg(long, value_enum, default_value_t = Mode::EmbeddedWindowed)]
    mode: Mode,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    #[arg(long)]
    zakura_cookie: PathBuf,
    #[arg(long, default_value = "./memo-data")]
    data_dir: PathBuf,
    #[arg(long = "worker-url")]
    worker_urls: Vec<String>,
    #[arg(long, default_value_t = DEFAULT_LOOKBACK_BLOCKS)]
    lookback_blocks: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_ACTIVE_SHARDS)]
    max_active_shards: u32,
    #[arg(long, default_value_t = 10)]
    poll_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let workers = match cli.mode {
        Mode::DistributedFull => {
            if cli.worker_urls.len() < 2 {
                return Err(
                    "distributed-full mode requires at least two --worker-url values".into(),
                );
            }
            cli.worker_urls
                .iter()
                .enumerate()
                .map(|(index, base_url)| WorkerTarget::Remote {
                    name: format!("worker-{}", index + 1),
                    base_url: base_url.trim_end_matches('/').to_string(),
                })
                .collect()
        }
        Mode::EmbeddedWindowed => vec![WorkerTarget::Embedded {
            name: "embedded".to_string(),
            state: WorkerState::new(cli.data_dir.join("embedded-worker"))?,
        }],
    };
    let state = CoordinatorState::new(workers)?;
    let ingest_state = state.clone();
    let ingest_cli = cli.clone();
    tokio::spawn(async move {
        loop {
            if let Err(error) = ingest(ingest_cli.clone(), ingest_state.clone()).await {
                tracing::error!(%error, "memo PIR ingestion stopped");
                ingest_state
                    .set_phase(CoordinatorPhase::Failed {
                        reason: "ingestion failed; retrying; inspect local logs".to_string(),
                    })
                    .await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, mode = ?cli.mode, "memo PIR coordinator started");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn ingest(
    cli: Cli,
    state: CoordinatorState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if cli.lookback_blocks == 0 || cli.max_active_shards == 0 {
        return Err("lookback and max-active-shards must be nonzero".into());
    }
    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    let tip = zakura.tip_height().await?;
    let finalized = tip.saturating_sub(CONFIRMATIONS);
    if finalized < ACTIVATION_HEIGHT {
        return Err("Zakura has not reached Ironwood activation plus confirmations".into());
    }
    let (initial_height, initial_base) = match cli.mode {
        Mode::DistributedFull => (ACTIVATION_HEIGHT, 0),
        Mode::EmbeddedWindowed => {
            zakura
                .window_start(finalized, cli.lookback_blocks, cli.max_active_shards)
                .await?
        }
    };
    let mut store = MemoStore::open(&cli.data_dir, initial_base)?;
    if let Some(last) = store.last_block() {
        let canonical = zakura.block(last.height).await?;
        if canonical.hash != last.hash {
            return Err(format!(
                "published finalized block {} changed from {} to {}",
                last.height, last.hash, canonical.hash
            )
            .into());
        }
    }

    loop {
        let tip = zakura.tip_height().await?;
        let target = tip.saturating_sub(CONFIRMATIONS);
        if let Some(last) = store.last_block() {
            if target < last.height {
                return Err(format!(
                    "finalized tip regressed below committed height {}",
                    last.height
                )
                .into());
            }
            let canonical = zakura.block(last.height).await?;
            if canonical.hash != last.hash {
                return Err(format!(
                    "committed finalized block {} changed from {} to {}",
                    last.height, last.hash, canonical.hash
                )
                .into());
            }
        }
        let mut next = store
            .last_block()
            .map_or(initial_height, |block| block.height + 1);
        if next <= target {
            state
                .set_phase(CoordinatorPhase::Syncing {
                    current_height: next.saturating_sub(1),
                    target_height: target,
                })
                .await;
        }
        while next <= target {
            let block = zakura.block(next).await?;
            let block_start = block
                .tree_size
                .checked_sub(block.records.len() as u64)
                .ok_or("block action count exceeds tree size")?;
            if store.tree_size() < block_start || store.tree_size() > block.tree_size {
                return Err(format!(
                    "tree continuity mismatch at height {next}: store {}, block {}..{}",
                    store.tree_size(),
                    block_start,
                    block.tree_size
                )
                .into());
            }
            let skip = (store.tree_size() - block_start) as usize;
            store.append_block(block.height, block.hash, &block.records[skip..])?;
            if store.tree_size() != block.tree_size {
                return Err(format!(
                    "Ironwood tree size mismatch at height {next}: ingested {}, Zakura {}",
                    store.tree_size(),
                    block.tree_size
                )
                .into());
            }
            next += 1;
            if next.is_multiple_of(256) {
                state
                    .set_phase(CoordinatorPhase::Syncing {
                        current_height: next - 1,
                        target_height: target,
                    })
                    .await;
            }
        }

        if let Some(anchor) = store.last_block().filter(|block| block.height == target) {
            let coverage = match cli.mode {
                Mode::DistributedFull => Coverage::Full {
                    covered_position_start: 0,
                },
                Mode::EmbeddedWindowed => {
                    let (computed_height, computed_position) = zakura
                        .window_start(target, cli.lookback_blocks, cli.max_active_shards)
                        .await?;
                    let covered_position_start = computed_position.max(store.base_position());
                    let effective_start_height = if covered_position_start == computed_position {
                        computed_height
                    } else {
                        store
                            .effective_height_for_position(covered_position_start)
                            .unwrap_or(initial_height)
                    };
                    Coverage::Windowed {
                        requested_lookback_blocks: cli.lookback_blocks,
                        max_active_shards: cli.max_active_shards,
                        covered_position_start,
                        effective_start_height,
                    }
                }
            };
            let already_published = state
                .metadata()
                .is_some_and(|metadata| metadata.anchor_height == anchor.height);
            if !already_published {
                state
                    .publish_from_store(&store, coverage, anchor.height, anchor.hash.clone())
                    .await?;
                tracing::info!(
                    anchor_height = anchor.height,
                    tree_size = store.tree_size(),
                    "published memo PIR generation"
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(cli.poll_seconds)).await;
    }
}
