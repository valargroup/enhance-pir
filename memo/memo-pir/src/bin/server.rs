use clap::{Parser, ValueEnum};
use memo_pir::coordinator::{
    router, Anchor, CoordinatorPhase, CoordinatorState, TableJournal, TableSetup, WorkerTarget,
    FRONTIER_UPDATES_RETAINED,
};
use memo_pir::ingest::Journals;
use memo_pir::nullifier::NullifierTables;
use memo_pir::types::{DatabaseId, ACTIVATION_HEIGHT, COLD_CHECKPOINT_INTERVAL, CONFIRMATIONS};
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
    /// Production: one or more remote workers, full pool from activation.
    /// A single worker owns every shard; see `worker_index_for_shard`.
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
    /// Which PIR tables to build and serve, by wire name (`action`,
    /// `witness`, `witness-roots`, `nf-cold`, `nf-warm`). `action` is
    /// mandatory. Every table's journal is always written, so widening the
    /// served set later needs no re-ingest. The production scope serves
    /// `action` only; the other tables exist for the DAG-sync pass, which the
    /// wallet stands down when they are absent from the manifest.
    #[arg(long, value_delimiter = ',', default_value = "action")]
    tables: Vec<DatabaseId>,
}

/// Validates and de-duplicates the served table set, preserving the canonical
/// `DatabaseId::ALL` order. ACTION must always be served: it carries the tree
/// size that anchors every generation.
fn served_tables(requested: &[DatabaseId]) -> Result<Vec<DatabaseId>, String> {
    if !requested.contains(&DatabaseId::Action) {
        return Err("the `action` table must be served".to_string());
    }
    Ok(DatabaseId::ALL
        .into_iter()
        .filter(|table| requested.contains(table))
        .collect())
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

    if configured.is_empty() {
        return Err("distributed mode requires at least one worker".into());
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
    let tables = served_tables(&cli.tables)?;
    tracing::info!(tables = ?tables, "serving PIR tables");
    let state = CoordinatorState::new(
        tables
            .iter()
            .map(|&table| TableSetup {
                table,
                pool: workers.clone(),
            })
            .collect(),
    )?;
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
    let mut journals = Journals::open(&cli.data_dir)?;
    if let Some((height, hash)) = journals.highest_committed() {
        let canonical = zakura.block(height).await?;
        if canonical.hash != hash {
            return Err(format!(
                "published finalized block {height} changed from {hash} to {}",
                canonical.hash
            )
            .into());
        }
    }

    let mut nullifier_tables: Option<NullifierTables> = None;
    loop {
        let tip = zakura.tip_height().await?;
        let target = tip.saturating_sub(CONFIRMATIONS);
        if let Some((height, hash)) = journals.highest_committed() {
            if target < height {
                return Err(
                    format!("finalized tip regressed below committed height {height}").into(),
                );
            }
            let canonical = zakura.block(height).await?;
            if canonical.hash != hash {
                return Err(format!(
                    "committed finalized block {height} changed from {hash} to {}",
                    canonical.hash
                )
                .into());
            }
        }
        let mut next = journals
            .committed_height()
            .map_or(initial_height, |height| height + 1);
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
            journals.append_block(&block)?;
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

        if journals.all_at(target) {
            let already_published = state
                .manifest()
                .is_some_and(|manifest| manifest.anchor_height == target);
            if !already_published {
                let hash = journals
                    .action
                    .last_block()
                    .map(|block| block.hash.clone())
                    .unwrap_or_default();
                let action = TableJournal::new(DatabaseId::Action, &journals.action)?;
                let witness = TableJournal::new(DatabaseId::Witness, &journals.witness)?;
                let witness_roots =
                    TableJournal::new(DatabaseId::WitnessRoots, &journals.witness_roots)?;
                let checkpoint = target - target % COLD_CHECKPOINT_INTERVAL;
                let serves_nullifiers = state
                    .tables()
                    .any(|table| matches!(table, DatabaseId::NfCold | DatabaseId::NfWarm));
                if serves_nullifiers {
                    if nullifier_tables
                        .as_ref()
                        .is_none_or(|tables| tables.checkpoint != checkpoint)
                    {
                        nullifier_tables =
                            Some(NullifierTables::build(&journals.nullifiers, checkpoint)?);
                    } else {
                        // The cold table is unchanged; only the warm side moves.
                        let fresh = NullifierTables::build(&journals.nullifiers, checkpoint)?;
                        nullifier_tables = Some(fresh);
                    }
                }
                let serves_witness = state.tables().any(|table| table == DatabaseId::Witness);
                let (witness_cap, frontier) = if serves_witness {
                    (
                        Some(journals.witness_cap(target)?),
                        journals.frontier_updates(
                            target.saturating_sub(FRONTIER_UPDATES_RETAINED as u64 - 1),
                            target,
                        )?,
                    )
                } else {
                    (None, Vec::new())
                };
                // `publish` ignores sources for tables that are not served.
                let mut sources: Vec<&dyn memo_pir::coordinator::TableSource> =
                    vec![&action, &witness, &witness_roots];
                if let Some(tables) = nullifier_tables.as_ref().filter(|_| serves_nullifiers) {
                    sources.push(&tables.cold);
                    sources.push(&tables.warm);
                }
                state
                    .publish(
                        &sources,
                        Anchor {
                            height: target,
                            hash,
                            cold_checkpoint_height: checkpoint,
                            witness_cap,
                            frontier,
                        },
                    )
                    .await?;
                tracing::info!(
                    anchor_height = target,
                    tree_size = journals.action.tree_size(),
                    "published PIR generation"
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
            tables: vec![DatabaseId::Action],
        }
    }

    #[test]
    fn default_table_set_is_action_only() {
        let cli = Cli::parse_from(["memo-pir-server", "--zakura-cookie", "cookie"]);
        assert_eq!(
            served_tables(&cli.tables).unwrap(),
            vec![DatabaseId::Action]
        );
    }

    #[test]
    fn table_list_parses_wire_names_in_canonical_order() {
        let cli = Cli::parse_from([
            "memo-pir-server",
            "--zakura-cookie",
            "cookie",
            "--tables",
            "nf-warm,action,witness,action",
        ]);
        assert_eq!(
            served_tables(&cli.tables).unwrap(),
            vec![DatabaseId::Action, DatabaseId::Witness, DatabaseId::NfWarm]
        );
    }

    #[test]
    fn table_list_requires_action() {
        assert!(served_tables(&[DatabaseId::Witness]).is_err());
        assert!(Cli::try_parse_from([
            "memo-pir-server",
            "--zakura-cookie",
            "cookie",
            "--tables",
            "memo",
        ])
        .is_err());
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
