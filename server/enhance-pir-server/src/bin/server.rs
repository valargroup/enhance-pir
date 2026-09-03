use clap::{Parser, ValueEnum};
use enhance_pir::types::{ACTIVATION_HEIGHT, CONFIRMATIONS};
use enhance_pir_server::coordinator::{
    router, CoordinatorPhase, CoordinatorState, TableSetup, WorkerGroup, WorkerTarget,
};
use enhance_pir_server::ingest::EnhanceJournal;
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerGroupConfig {
    name: String,
    replicas: Vec<WorkerConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    name: String,
    url: String,
}

fn remote_worker_groups(cli: &Cli) -> Result<Vec<WorkerGroup>, Box<dyn std::error::Error>> {
    let configured = if let Some(path) = &cli.worker_config {
        parse_worker_config(&std::fs::read(path)?)?
    } else {
        cli.worker_urls
            .iter()
            .enumerate()
            .map(|(index, url)| WorkerGroupConfig {
                name: format!("shard-group-{}", index + 1),
                replicas: vec![WorkerConfig {
                    name: format!("worker-{}", index + 1),
                    url: url.clone(),
                }],
            })
            .collect()
    };
    if configured.is_empty() {
        return Err("distributed mode requires at least one worker group".into());
    }
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

fn parse_worker_config(bytes: &[u8]) -> Result<Vec<WorkerGroupConfig>, serde_json::Error> {
    serde_json::from_slice::<WorkerConfigFile>(bytes).map(|config| config.groups)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let groups = match cli.mode {
        Mode::Distributed => remote_worker_groups(&cli)?,
        Mode::Embedded => {
            tracing::warn!("embedded mode runs one in-process worker; not for production");
            vec![WorkerGroup {
                name: "embedded-group".to_string(),
                replicas: vec![WorkerTarget::Embedded {
                    name: "embedded".to_string(),
                    state: WorkerState::new(cli.data_dir.join("embedded-worker"))?,
                }],
            }]
        }
    };
    let state = CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups,
    }])?;
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
    let mut journal = EnhanceJournal::open(&cli.data_dir)?;
    if let Some((height, hash)) = journal.highest_committed() {
        let canonical = zakura.block(height).await?;
        if canonical.hash != hash {
            return Err(format!("finalized block {height} changed from {hash}").into());
        }
    }
    loop {
        let target = zakura.tip_height().await?.saturating_sub(CONFIRMATIONS);
        if target < ACTIVATION_HEIGHT {
            return Err("Zakura has not reached Ironwood activation plus confirmations".into());
        }
        let mut next = journal
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
            journal.append_block(&zakura.block(next).await?)?;
            next += 1;
        }
        let already_published = state
            .manifest()
            .is_some_and(|manifest| manifest.anchor_height == target);
        if journal.committed_height() == Some(target) && !already_published {
            let hash = journal
                .records
                .last_block()
                .map(|block| block.hash.clone())
                .unwrap_or_default();
            state
                .publish_from_store(&journal.records, target, hash)
                .await?;
            tracing::info!(
                anchor_height = target,
                positions = journal.records.tree_size(),
                "published Enhance PIR generation"
            );
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
    fn parses_explicit_worker_groups_and_replicas_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("workers.json");
        std::fs::write(
            &path,
            br#"{"groups":[{"name":"group-a","replicas":[{"name":"worker-a","url":"http://10.0.0.2:8091"},{"name":"worker-b","url":"http://10.0.0.3:8091/"}]}]}"#,
        )
        .unwrap();
        let groups = remote_worker_groups(&cli(Some(path), vec![])).unwrap();
        assert_eq!(groups[0].name, "group-a");
        assert_eq!(groups[0].replicas[0].name(), "worker-a");
        assert_eq!(groups[0].replicas[1].name(), "worker-b");
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
