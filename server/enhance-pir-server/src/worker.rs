use crate::ipir::{global_parameters, shard_parameters, ShardRuntime};
use crate::store::RecordJournal;
use crate::types::{setup_seed_bytes, DatabaseId};
use crate::wire::{
    decode_evaluate_request, encode_crs_blocks, encode_evaluate_response, EvaluateRequest,
};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post, put};
use axum::Router;
use ipir_sp::server::add_intermediate_assign_mod;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

/// Generations a worker keeps answerable. The coordinator serves the same
/// number, so a session built against a recent snapshot survives publishes.
/// A generation is published per block; a wallet pass (parameter fetch,
/// anchor gate, one fixed query envelope) must fit inside the retained window
/// even when blocks arrive in a burst, so eight (about ten minutes at the
/// 75-second target) rather than the two that a single straddled publish
/// would need.
pub const RETAINED_GENERATIONS: usize = 8;

/// Default concurrent shard evaluations per worker process.
pub const DEFAULT_EVALUATION_SLOTS: usize = 2;

/// A worker hosts shards of every table. Runtimes are keyed by table, shard,
/// and row digest: re-preparing the frontier shard for a new generation must
/// not replace the runtime the previous generation still answers with.
#[derive(Clone)]
pub struct WorkerState {
    rlwe: Arc<BTreeMap<DatabaseId, inspiring::RlweParams>>,
    shards: Arc<RwLock<HashMap<ShardKey, Arc<ShardRuntime>>>>,
    active: Arc<RwLock<BTreeMap<u64, ActiveGeneration>>>,
    artifact_dir: Arc<PathBuf>,
    evaluation_slots: Arc<Semaphore>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ShardKey {
    table: DatabaseId,
    shard_id: u64,
    rows_sha256: String,
}

#[derive(Clone, Debug)]
struct ActiveGeneration {
    generation: u64,
    /// Per table, the complete sorted assignment with each shard's digest.
    tables: BTreeMap<DatabaseId, Vec<ActivateShard>>,
}

impl ActiveGeneration {
    fn shard_ids(&self, table: DatabaseId) -> Option<Vec<u64>> {
        self.tables
            .get(&table)
            .map(|shards| shards.iter().map(|shard| shard.shard_id).collect())
    }
}

#[derive(Debug, Deserialize)]
struct PrepareQuery {
    query_row_start: usize,
    logical_rows: u64,
    rows_sha256: String,
}

/// Activates one generation across every table the worker serves. Sent once
/// per publish; tables the worker holds no shards of are simply absent.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateRequest {
    pub generation: u64,
    pub tables: BTreeMap<DatabaseId, Vec<ActivateShard>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivateShard {
    pub shard_id: u64,
    pub rows_sha256: String,
}

#[derive(Debug, Serialize)]
struct PrepareResponse {
    status: &'static str,
    table: DatabaseId,
    shard_id: u64,
    rows_sha256: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    generation: Option<u64>,
    /// Active shard count per table in the newest generation.
    active_shards: BTreeMap<DatabaseId, usize>,
    cached_shards: usize,
    total_memory_bytes: u64,
    available_memory_bytes: u64,
    process_rss_bytes: u64,
}

/// Host memory as (total, available, this process's RSS), in bytes. Cheap
/// enough to call on every health probe; the coordinator relays it to the
/// dashboard so operators can see worker headroom without shell access.
pub fn host_memory() -> (u64, u64, u64) {
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    let rss = sysinfo::get_current_pid()
        .ok()
        .and_then(|pid| {
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            system.process(pid).map(|process| process.memory())
        })
        .unwrap_or(0);
    (system.total_memory(), system.available_memory(), rss)
}

impl WorkerState {
    pub fn new(artifact_dir: PathBuf) -> Result<Self, inspiring::InspiringError> {
        Self::with_evaluation_slots(artifact_dir, DEFAULT_EVALUATION_SLOTS)
    }

    pub fn with_evaluation_slots(
        artifact_dir: PathBuf,
        evaluation_slots: usize,
    ) -> Result<Self, inspiring::InspiringError> {
        let mut rlwe = BTreeMap::new();
        for table in DatabaseId::ALL {
            let (params, _) = shard_parameters(&table.layout())?;
            rlwe.insert(table, params);
        }
        Ok(Self {
            rlwe: Arc::new(rlwe),
            shards: Arc::new(RwLock::new(HashMap::new())),
            active: Arc::new(RwLock::new(BTreeMap::new())),
            artifact_dir: Arc::new(artifact_dir),
            evaluation_slots: Arc::new(Semaphore::new(evaluation_slots.max(1))),
        })
    }

    fn rlwe(&self, table: DatabaseId) -> &inspiring::RlweParams {
        self.rlwe.get(&table).expect("every table has parameters")
    }

    pub async fn prepare_local(
        &self,
        table: DatabaseId,
        shard_id: u64,
        query_row_start: usize,
        logical_rows: u64,
        rows_sha256: String,
        rows: Vec<u8>,
    ) -> Result<bool, String> {
        if RecordJournal::rows_digest(&rows) != rows_sha256 {
            return Err("row digest mismatch".to_string());
        }
        let key = ShardKey {
            table,
            shard_id,
            rows_sha256: rows_sha256.clone(),
        };
        if let Some(existing) = self.shards.read().await.get(&key) {
            if existing.query_row_start == query_row_start {
                return Ok(false);
            }
        }
        let layout = table.layout();
        let rlwe = self.rlwe(table).clone();
        let (global_rlwe, global_params) =
            global_parameters(logical_rows, &layout).map_err(|e| e.to_string())?;
        if global_rlwe.d != rlwe.d || global_rlwe.q != rlwe.q {
            return Err("global and worker RLWE parameters differ".to_string());
        }
        let client = ipir_sp::IPIRClient::new(&global_rlwe, &global_params);
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed_bytes());
        let artifact_dir = self.artifact_dir.clone();
        let (runtime, built) = tokio::task::spawn_blocking(move || {
            ShardRuntime::load_or_build(
                &artifact_dir,
                table,
                &layout,
                shard_id,
                query_row_start,
                rows_sha256,
                &rows,
                &rlwe,
                &setup,
            )
        })
        .await
        .map_err(|_| "shard build task failed".to_string())?
        .map_err(|e| e.to_string())?;
        self.shards.write().await.insert(key, Arc::new(runtime));
        Ok(built)
    }

    pub async fn ensure_local(
        &self,
        table: DatabaseId,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
    ) -> Result<(), String> {
        let key = ShardKey {
            table,
            shard_id,
            rows_sha256: rows_sha256.clone(),
        };
        if let Some(existing) = self.shards.read().await.get(&key) {
            if existing.query_row_start == query_row_start {
                return Ok(());
            }
        }
        let layout = table.layout();
        let rlwe = self.rlwe(table).clone();
        let artifact_dir = self.artifact_dir.clone();
        let runtime = tokio::task::spawn_blocking(move || {
            ShardRuntime::load_cached(
                &artifact_dir,
                table,
                &layout,
                shard_id,
                query_row_start,
                &rows_sha256,
                &rlwe,
            )
        })
        .await
        .map_err(|_| "shard load task failed".to_string())??;
        self.shards.write().await.insert(key, Arc::new(runtime));
        Ok(())
    }

    /// Activates a generation and evicts every runtime no retained generation
    /// references, so memory holds exactly the shards that can still be asked.
    pub async fn activate_local(&self, request: ActivateRequest) -> Result<(), String> {
        if request.tables.values().all(Vec::is_empty) {
            return Err("an active generation needs at least one shard".to_string());
        }
        let mut tables = BTreeMap::new();
        {
            let cached = self.shards.read().await;
            for (table, shards) in request.tables {
                let mut shards = shards;
                shards.sort_by_key(|shard| shard.shard_id);
                if shards
                    .windows(2)
                    .any(|pair| pair[0].shard_id == pair[1].shard_id)
                {
                    return Err(format!("duplicate active shard in {table}"));
                }
                for shard in &shards {
                    let key = ShardKey {
                        table,
                        shard_id: shard.shard_id,
                        rows_sha256: shard.rows_sha256.clone(),
                    };
                    if !cached.contains_key(&key) {
                        return Err(format!(
                            "{table} shard {} with digest {} is not prepared",
                            shard.shard_id, shard.rows_sha256
                        ));
                    }
                }
                if !shards.is_empty() {
                    tables.insert(table, shards);
                }
            }
        }
        let mut active = self.active.write().await;
        active.insert(
            request.generation,
            ActiveGeneration {
                generation: request.generation,
                tables,
            },
        );
        while active.len() > RETAINED_GENERATIONS {
            let oldest = *active.keys().next().expect("nonempty generation map");
            active.remove(&oldest);
        }
        let referenced: HashSet<ShardKey> = active
            .values()
            .flat_map(|generation| {
                generation.tables.iter().flat_map(|(table, shards)| {
                    shards.iter().map(|shard| ShardKey {
                        table: *table,
                        shard_id: shard.shard_id,
                        rows_sha256: shard.rows_sha256.clone(),
                    })
                })
            })
            .collect();
        drop(active);
        self.shards
            .write()
            .await
            .retain(|key, _| referenced.contains(key));
        Ok(())
    }

    pub async fn evaluate_local(
        &self,
        table: DatabaseId,
        request: EvaluateRequest,
    ) -> Result<Vec<u64>, String> {
        let _permit = self
            .evaluation_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| "worker is at its evaluation limit".to_string())?;
        let active = self
            .active
            .read()
            .await
            .get(&request.generation)
            .cloned()
            .ok_or_else(|| "generation mismatch".to_string())?;
        let assignment = active
            .tables
            .get(&table)
            .ok_or_else(|| format!("generation has no {table} shards on this worker"))?;
        let mut requested_ids: Vec<_> = request.shards.iter().map(|shard| shard.shard_id).collect();
        requested_ids.sort_unstable();
        if Some(requested_ids) != active.shard_ids(table) {
            return Err("request does not cover the complete active shard assignment".to_string());
        }
        let rlwe = self.rlwe(table);
        let (_, params) = shard_parameters(&table.layout()).map_err(|e| e.to_string())?;
        let cached = self.shards.read().await;
        let mut combined = vec![0u64; params.db_cols];
        for shard_query in request.shards {
            let digest = &assignment
                .iter()
                .find(|shard| shard.shard_id == shard_query.shard_id)
                .expect("assignment covers requested ids")
                .rows_sha256;
            let key = ShardKey {
                table,
                shard_id: shard_query.shard_id,
                rows_sha256: digest.clone(),
            };
            let runtime = cached
                .get(&key)
                .ok_or_else(|| "active shard disappeared".to_string())?;
            let partial = runtime
                .evaluate(rlwe, &shard_query.coefficients)
                .map_err(|e| e.to_string())?;
            add_intermediate_assign_mod(&mut combined, &partial, rlwe.q)
                .map_err(|e| e.to_string())?;
        }
        Ok(combined)
    }

    /// The CRS hint of the most recently prepared runtime for the shard.
    pub async fn crs_local(&self, table: DatabaseId, shard_id: u64) -> Result<Vec<u8>, String> {
        let cached = self.shards.read().await;
        let runtime = cached
            .iter()
            .filter(|(key, _)| key.table == table && key.shard_id == shard_id)
            .map(|(_, runtime)| runtime)
            .max_by_key(|runtime| runtime.prepared_at)
            .ok_or_else(|| "shard is not prepared".to_string())?;
        Ok(encode_crs_blocks(&runtime.crs_blocks))
    }

    /// Runtimes currently held, for tests and health.
    pub async fn cached_shard_count(&self) -> usize {
        self.shards.read().await.len()
    }
}

fn max_shard_bytes() -> usize {
    DatabaseId::ALL
        .iter()
        .map(|table| table.layout().shard_bytes())
        .max()
        .expect("at least one table")
}

pub fn router(state: WorkerState) -> Router {
    Router::new()
        .route("/internal/health", get(health))
        .route("/internal/:table/shards/:shard_id", put(prepare))
        .route("/internal/:table/shards/:shard_id/load", post(load))
        .route("/internal/:table/shards/:shard_id/hint", get(hint))
        .route("/internal/:table/evaluate", post(evaluate))
        .route("/internal/activate", post(activate))
        .layer(axum::extract::DefaultBodyLimit::max(
            max_shard_bytes() + 1024 * 1024,
        ))
        .with_state(state)
}

fn parse_table(name: &str) -> Result<DatabaseId, StatusCode> {
    name.parse().map_err(|_| StatusCode::NOT_FOUND)
}

async fn health(State(state): State<WorkerState>) -> Json<HealthResponse> {
    let active = state.active.read().await;
    let cached_shards = state.shards.read().await.len();
    let newest = active.last_key_value().map(|(_, generation)| generation);
    let (total_memory_bytes, available_memory_bytes, process_rss_bytes) = host_memory();
    Json(HealthResponse {
        status: "ok",
        generation: newest.map(|generation| generation.generation),
        active_shards: newest.map_or_else(BTreeMap::new, |generation| {
            generation
                .tables
                .iter()
                .map(|(table, shards)| (*table, shards.len()))
                .collect()
        }),
        cached_shards,
        total_memory_bytes,
        available_memory_bytes,
        process_rss_bytes,
    })
}

async fn prepare(
    State(state): State<WorkerState>,
    Path((table, shard_id)): Path<(String, u64)>,
    Query(query): Query<PrepareQuery>,
    body: Bytes,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    match state
        .prepare_local(
            table,
            shard_id,
            query.query_row_start,
            query.logical_rows,
            query.rows_sha256.clone(),
            body.to_vec(),
        )
        .await
    {
        Ok(built) => Json(PrepareResponse {
            status: if built { "built" } else { "cached" },
            table,
            shard_id,
            rows_sha256: query.rows_sha256,
        })
        .into_response(),
        Err(error) => {
            tracing::warn!(%error, %table, shard_id, "shard preparation rejected");
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

async fn load(
    State(state): State<WorkerState>,
    Path((table, shard_id)): Path<(String, u64)>,
    Query(query): Query<PrepareQuery>,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    match state
        .ensure_local(table, shard_id, query.query_row_start, query.rows_sha256)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            tracing::warn!(%error, %table, shard_id, "cached shard load rejected");
            StatusCode::CONFLICT.into_response()
        }
    }
}

async fn activate(
    State(state): State<WorkerState>,
    Json(request): Json<ActivateRequest>,
) -> Response {
    match state.activate_local(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            tracing::warn!(%error, "generation activation rejected");
            StatusCode::CONFLICT.into_response()
        }
    }
}

async fn hint(
    State(state): State<WorkerState>,
    Path((table, shard_id)): Path<(String, u64)>,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    match state.crs_local(table, shard_id).await {
        Ok(bytes) => bytes.into_response(),
        Err(error) => {
            tracing::warn!(%error, %table, shard_id, "hint request rejected");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

async fn evaluate(
    State(state): State<WorkerState>,
    Path(table): Path<String>,
    body: Bytes,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    let request = match decode_evaluate_request(&body) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(%error, "worker query rejected");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let generation = request.generation;
    match state.evaluate_local(table, request).await {
        Ok(coefficients) => encode_evaluate_response(generation, &coefficients).into_response(),
        Err(error) => {
            tracing::warn!(%error, %table, "worker evaluation failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_memory_reports_a_real_total() {
        let (total, available, _rss) = super::host_memory();
        assert!(total > 0);
        assert!(available <= total);
    }

    use super::*;
    use crate::types::SHARD_ROWS;
    use crate::wire::ShardQuery;

    #[tokio::test]
    async fn evaluation_requires_an_active_generation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = WorkerState::new(dir.path().to_path_buf()).expect("worker");
        let error = state
            .evaluate_local(
                DatabaseId::Enhance,
                EvaluateRequest {
                    generation: 7,
                    shards: vec![ShardQuery {
                        shard_id: 0,
                        coefficients: vec![0; SHARD_ROWS],
                    }],
                },
            )
            .await
            .expect_err("nothing is active");
        assert!(error.contains("generation mismatch"), "{error}");
    }

    #[tokio::test]
    async fn activation_requires_prepared_shards() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = WorkerState::new(dir.path().to_path_buf()).expect("worker");
        assert!(state
            .activate_local(ActivateRequest {
                generation: 1,
                tables: BTreeMap::new(),
            })
            .await
            .is_err());
        let mut tables = BTreeMap::new();
        tables.insert(
            DatabaseId::Enhance,
            vec![ActivateShard {
                shard_id: 3,
                rows_sha256: "00".repeat(32),
            }],
        );
        let error = state
            .activate_local(ActivateRequest {
                generation: 1,
                tables,
            })
            .await
            .expect_err("shard 3 was never prepared");
        assert!(error.contains("enhance shard 3"), "{error}");
    }

    #[test]
    fn every_table_has_shard_parameters_and_the_body_limit_covers_them() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = WorkerState::new(dir.path().to_path_buf()).expect("worker");
        for table in DatabaseId::ALL {
            assert_eq!(state.rlwe(table).d, 2_048, "{table}");
            assert!(table.layout().shard_bytes() <= max_shard_bytes());
        }
    }
}
