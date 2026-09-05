use clap::{Parser, ValueEnum};
use enhance_pir::types::ACTIVATION_HEIGHT;
use enhance_pir_server::coordinator::{
    router, Anchor, CoordinatorPhase, CoordinatorState, TableJournal, TableSetup, WorkerGroup,
    WorkerTarget,
};
use enhance_pir_server::ingest::EnhanceJournal;
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::DatabaseId;
use enhance_pir_server::worker::WorkerState;
use enhance_pir_server::zakura::ZakuraClient;
use serde::Deserialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Distributed,
    Embedded,
}

#[derive(Parser, Clone)]
#[command(
    name = "enhance-pir-server",
    about = "Ironwood Enhance PIR coordinator"
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
    #[arg(long, default_value = "./enhance-data")]
    data_dir: PathBuf,
    #[arg(long = "worker-url")]
    worker_urls: Vec<String>,
    #[arg(long, conflicts_with = "worker_urls")]
    worker_config: Option<PathBuf>,
    #[arg(long, default_value_t = 10)]
    poll_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfigFile {
    groups: Vec<WorkerGroupConfig>,
    /// Accepted and ignored. The transparent-spend tables are not served, but a
    /// config file written for the previous release must not stop the
    /// coordinator starting.
    #[serde(default)]
    transparent_spend_groups: Vec<WorkerGroupConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerGroupConfig {
    name: String,
    replicas: Vec<WorkerConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    name: String,
    url: String,
}

fn remote_worker_setups(cli: &Cli) -> Result<Vec<TableSetup>, Box<dyn std::error::Error>> {
    let enhance = if let Some(path) = &cli.worker_config {
        let config = parse_worker_config(&std::fs::read(path)?)?;
        if !config.transparent_spend_groups.is_empty() {
            tracing::warn!(
                "worker config declares transparent_spend_groups; those tables are \
                 no longer served and the entry is ignored"
            );
        }
        config.groups
    } else {
        let groups: Vec<_> = cli
            .worker_urls
            .iter()
            .enumerate()
            .map(|(index, url)| WorkerGroupConfig {
                name: format!("shard-group-{}", index + 1),
                replicas: vec![WorkerConfig {
                    name: format!("worker-{}", index + 1),
                    url: url.clone(),
                }],
            })
            .collect();
        groups
    };
    if enhance.is_empty() {
        return Err("distributed mode requires at least one worker group".into());
    }
    Ok(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: validate_worker_groups(enhance)?,
    }])
}

fn validate_worker_groups(
    configured: Vec<WorkerGroupConfig>,
) -> Result<Vec<WorkerGroup>, Box<dyn std::error::Error>> {
    let mut group_names = HashSet::new();
    let mut names = HashSet::new();
    let mut urls = HashSet::new();
    configured
        .into_iter()
        .map(|group| {
            validate_name("worker group", &group.name)?;
            if !group_names.insert(group.name.clone()) {
                return Err(format!("duplicate worker group name: {}", group.name));
            }
            if group.replicas.is_empty() {
                return Err(format!("worker group {} has no replicas", group.name));
            }
            let replicas = group
                .replicas
                .into_iter()
                .map(|worker| {
                    validate_name("worker", &worker.name)?;
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
                .collect::<Result<Vec<_>, String>>()?;
            Ok(WorkerGroup {
                name: group.name,
                replicas,
            })
        })
        .collect::<Result<Vec<WorkerGroup>, String>>()
        .map_err(Into::into)
}

fn validate_name(kind: &str, name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err(format!("invalid {kind} name: {name:?}"));
    }
    Ok(())
}

fn parse_worker_config(bytes: &[u8]) -> Result<WorkerConfigFile, serde_json::Error> {
    serde_json::from_slice(bytes)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let setups = match cli.mode {
        Mode::Distributed => remote_worker_setups(&cli)?,
        Mode::Embedded => {
            tracing::warn!("embedded mode runs one in-process worker; not for production");
            let groups = vec![WorkerGroup {
                name: "embedded-group".to_string(),
                replicas: vec![WorkerTarget::Embedded {
                    name: "embedded".to_string(),
                    state: WorkerState::new(cli.data_dir.join("embedded-worker"))?,
                }],
            }];
            DatabaseId::ALL
                .into_iter()
                .map(|table| TableSetup {
                    table,
                    groups: groups.clone(),
                })
                .collect()
        }
    };
    let state = CoordinatorState::new(setups)?;
    let ingest_state = state.clone();
    let ingest_cli = cli.clone();
    tokio::spawn(async move {
        loop {
            if let Err(error) = ingest(ingest_cli.clone(), ingest_state.clone()).await {
                tracing::error!(%error, "Enhance PIR ingestion stopped");
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
    tracing::info!(listen = %cli.listen, mode = ?cli.mode, "Enhance PIR coordinator started");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn ingest(
    cli: Cli,
    state: CoordinatorState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    let mut enhance = EnhanceJournal::open(&cli.data_dir)?;
    loop {
        let target = zakura.tip_height().await?;
        reconcile(&zakura, &mut enhance.records, target).await?;
        if target < ACTIVATION_HEIGHT {
            return Err("Zakura has not reached Ironwood activation".into());
        }
        let mut next = enhance
            .committed_height()
            .map_or(ACTIVATION_HEIGHT, |height| height + 1);
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
            enhance.append_block(&block)?;
            next += 1;
        }
        let current_hash = enhance
            .highest_committed()
            .filter(|(height, _)| *height == target)
            .map(|(_, hash)| hash);
        let already_published = state.manifest().is_some_and(|manifest| {
            manifest.anchor_height == target
                && current_hash.as_deref() == Some(manifest.anchor_block_hash.as_str())
        });
        if enhance.committed_height() == Some(target) && !already_published {
            // Do not publish a height that ceased to be the best-chain tip
            // while its mutable tail shards were being assembled.
            if zakura.tip_height().await? != target {
                continue;
            }
            let hash = current_hash.expect("committed target has a block hash");
            if zakura.block_hash(target).await? != hash {
                continue;
            }
            let enhance_table = TableJournal::new(DatabaseId::Enhance, &enhance.records)?;
            state
                .publish(
                    &[&enhance_table],
                    Anchor {
                        height: target,
                        hash,
                    },
                )
                .await?;
            tracing::info!(
                tip_height = target,
                positions = enhance.records.tree_size(),
                "published tip-bound Enhance PIR generation"
            );
        }
        tokio::time::sleep(Duration::from_secs(cli.poll_seconds)).await;
    }
}

async fn reconcile(
    zakura: &ZakuraClient,
    journal: &mut RecordJournal,
    tip: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let Some(last) = journal.last_block().cloned() else {
            return Ok(());
        };
        if last.height <= tip && zakura.block_hash(last.height).await? == last.hash {
            return Ok(());
        }
        let previous = journal
            .blocks()
            .iter()
            .rev()
            .nth(1)
            .map(|block| block.height);
        tracing::warn!(
            height = last.height,
            "rewinding PIR journal after best-chain change"
        );
        journal.rewind_to_height(previous)?;
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
    fn a_config_without_a_spend_pool_is_accepted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("workers.json");
        std::fs::write(
            &path,
            br#"{"groups":[{"name":"group-a","replicas":[{"name":"worker-a","url":"http://10.0.0.2:8091"},{"name":"worker-b","url":"http://10.0.0.3:8091/"}]}]}"#,
        )
        .unwrap();
        let setups = remote_worker_setups(&cli(Some(path), vec![])).unwrap();
        assert_eq!(setups.len(), 1);
        assert_eq!(setups[0].table, DatabaseId::Enhance);
    }

    #[test]
    fn a_config_still_declaring_a_spend_pool_is_accepted_and_ignored() {
        // A worker config written for the previous release must not stop the
        // coordinator starting; the entry is ignored, not rejected.
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("workers.json");
        std::fs::write(
            &path,
            br#"{"groups":[{"name":"enhance","replicas":[{"name":"enhance-a","url":"http://10.0.0.2:8091"}]}],"transparent_spend_groups":[{"name":"spend","replicas":[{"name":"transparent-spend-worker-01","url":"http://10.0.0.4:8091"}]}]}"#,
        )
        .unwrap();
        let setups = remote_worker_setups(&cli(Some(path), vec![])).unwrap();
        assert_eq!(setups.len(), 1);
        assert_eq!(setups[0].table, DatabaseId::Enhance);
        assert_eq!(setups[0].groups[0].replicas[0].name(), "enhance-a");
    }

    #[test]
    fn rejects_removed_tables_option() {
        assert!(Cli::try_parse_from([
            "enhance-pir-server",
            "--zakura-cookie",
            "cookie",
            "--tables",
            "enhance"
        ])
        .is_err());
    }
}
