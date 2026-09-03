use clap::{Parser, ValueEnum};
use memo_pir::coordinator::{router, CoordinatorPhase, CoordinatorState, WorkerTarget};
use memo_pir::store::RecordJournal;
use memo_pir::types::{DatabaseId, ACTION_LAYOUT, ACTIVATION_HEIGHT, CONFIRMATIONS};
use memo_pir::worker::WorkerState;
use memo_pir::zakura::ZakuraClient;
use serde::Deserialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    /// Production: at least two remote workers, full pool from activation.
    #[value(alias = "distributed-full")]
    Distributed,
    /// Development only: one in-process worker, full pool from activation.
    Embedded,
}

#[derive(Parser, Clone)]
#[command(
    name = "memo-pir-server",
    about = "Standalone Ironwood memo PIR coordinator"
)]
struct Cli {
    #[arg(long, value_enum, default_value_t = Mode::Distributed)]
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
    /// Ordered worker inventory. Existing entries must remain prefix-stable so
    /// that adding capacity never changes sealed shard ownership.
    #[arg(long, conflicts_with = "worker_urls")]
    worker_config: Option<PathBuf>,
    #[arg(long, default_value_t = 10)]
    poll_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfigFile {
    workers: Vec<WorkerConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    name: String,
    url: String,
}

fn remote_workers(cli: &Cli) -> Result<Vec<WorkerTarget>, Box<dyn std::error::Error>> {
    let configured = if let Some(path) = &cli.worker_config {
        let bytes = std::fs::read(path)?;
        parse_worker_config(&bytes)?
    } else {
        cli.worker_urls
            .iter()
            .enumerate()
            .map(|(index, url)| WorkerConfig {
                name: format!("worker-{}", index + 1),
                url: url.clone(),
            })
            .collect()
    };

    if configured.len() < 2 {
        return Err("distributed mode requires at least two workers".into());
    }

    let mut names = HashSet::new();
    let mut urls = HashSet::new();
    configured
        .into_iter()
        .map(|worker| {
            if worker.name.is_empty()
                || !worker
                    .name
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
            {
                return Err(format!("invalid worker name: {:?}", worker.name));
            }
            let parsed = reqwest::Url::parse(&worker.url)
                .map_err(|error| format!("invalid URL for {}: {error}", worker.name))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || parsed.username() != ""
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(format!("invalid URL for {}", worker.name));
            }
            let url = worker.url.trim_end_matches('/').to_string();
            if !names.insert(worker.name.clone()) {
                return Err(format!("duplicate worker name: {}", worker.name));
            }
            if !urls.insert(url.clone()) {
                return Err(format!("duplicate worker URL: {url}"));
            }
            Ok(WorkerTarget::Remote {
                name: worker.name,
                base_url: url,
            })
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(Into::into)
}

fn parse_worker_config(bytes: &[u8]) -> Result<Vec<WorkerConfig>, serde_json::Error> {
    serde_json::from_slice::<WorkerConfigFile>(bytes).map(|config| config.workers)
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
        Mode::Distributed => remote_workers(&cli)?,
        Mode::Embedded => {
            tracing::warn!("embedded mode runs one in-process worker; not for production");
            vec![WorkerTarget::Embedded {
                name: "embedded".to_string(),
                state: WorkerState::new(cli.data_dir.join("embedded-worker"))?,
            }]
        }
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
    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    let tip = zakura.tip_height().await?;
    let finalized = tip.saturating_sub(CONFIRMATIONS);
    if finalized < ACTIVATION_HEIGHT {
        return Err("Zakura has not reached Ironwood activation plus confirmations".into());
    }
    let initial_height = ACTIVATION_HEIGHT;
    let mut store = RecordJournal::open(&cli.data_dir, DatabaseId::Action, ACTION_LAYOUT)?;
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
            let already_published = state
                .metadata()
                .is_some_and(|metadata| metadata.anchor_height == anchor.height);
            if !already_published {
                state
                    .publish_from_store(&store, anchor.height, anchor.hash.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(worker_config: Option<PathBuf>, worker_urls: Vec<String>) -> Cli {
        Cli {
            mode: Mode::Distributed,
            listen: "127.0.0.1:8080".parse().unwrap(),
            zakura_rpc_url: "http://127.0.0.1:8232".to_string(),
            zakura_cookie: PathBuf::from("cookie"),
            data_dir: PathBuf::from("data"),
            worker_urls,
            worker_config,
            poll_seconds: 10,
        }
    }

    #[test]
    fn parses_explicit_worker_names_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("workers.json");
        std::fs::write(
            &path,
            br#"{"workers":[{"name":"worker-a","url":"http://10.0.0.2:8091"},{"name":"worker-b","url":"http://10.0.0.3:8091/"}]}"#,
        )
        .unwrap();

        let workers = remote_workers(&cli(Some(path), vec![])).unwrap();
        assert_eq!(workers[0].name(), "worker-a");
        assert_eq!(workers[1].name(), "worker-b");
    }

    #[test]
    fn rejects_duplicate_names_and_urls() {
        let duplicate_name = br#"{"workers":[{"name":"same","url":"http://10.0.0.2:8091"},{"name":"same","url":"http://10.0.0.3:8091"}]}"#;
        let duplicate_url = br#"{"workers":[{"name":"a","url":"http://10.0.0.2:8091"},{"name":"b","url":"http://10.0.0.2:8091/"}]}"#;

        for config in [duplicate_name.as_slice(), duplicate_url.as_slice()] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("workers.json");
            std::fs::write(&path, config).unwrap();
            assert!(remote_workers(&cli(Some(path), vec![])).is_err());
        }
    }
}
