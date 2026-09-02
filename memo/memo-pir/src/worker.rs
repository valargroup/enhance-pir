use crate::ipir::{global_parameters, ShardRuntime};
use crate::store::MemoStore;
use crate::types::{ITEM_SIZE_BITS, ROW_BYTES, SHARD_ROWS};
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
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

#[derive(Clone)]
pub struct WorkerState {
    rlwe: Arc<inspiring::RlweParams>,
    shards: Arc<RwLock<HashMap<u64, Arc<ShardRuntime>>>>,
    active: Arc<RwLock<BTreeMap<u64, ActiveGeneration>>>,
    artifact_dir: Arc<PathBuf>,
    evaluation_slots: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
struct ActiveGeneration {
    generation: u64,
    shard_ids: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct PrepareQuery {
    query_row_start: usize,
    logical_rows: u64,
    rows_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateRequest {
    pub generation: u64,
    pub shards: Vec<ActivateShard>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateShard {
    pub shard_id: u64,
    pub rows_sha256: String,
}

#[derive(Debug, Serialize)]
struct PrepareResponse {
    status: &'static str,
    shard_id: u64,
    rows_sha256: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    generation: Option<u64>,
    active_shards: usize,
    cached_shards: usize,
}

impl WorkerState {
    pub fn new(artifact_dir: PathBuf) -> Result<Self, inspiring::InspiringError> {
        let (rlwe, _) = global_parameters(SHARD_ROWS as u64)?;
        Ok(Self {
            rlwe: Arc::new(rlwe),
            shards: Arc::new(RwLock::new(HashMap::new())),
            active: Arc::new(RwLock::new(BTreeMap::new())),
            artifact_dir: Arc::new(artifact_dir),
            evaluation_slots: Arc::new(Semaphore::new(2)),
        })
    }

    pub async fn prepare_local(
        &self,
        shard_id: u64,
        query_row_start: usize,
        logical_rows: u64,
        rows_sha256: String,
        rows: Vec<u8>,
    ) -> Result<bool, String> {
        if MemoStore::rows_digest(&rows) != rows_sha256 {
            return Err("row digest mismatch".to_string());
        }
        if let Some(existing) = self.shards.read().await.get(&shard_id) {
            if existing.rows_sha256 == rows_sha256 && existing.query_row_start == query_row_start {
                return Ok(false);
            }
        }
        let (global_rlwe, global_params) =
            global_parameters(logical_rows).map_err(|e| e.to_string())?;
        if global_rlwe.d != self.rlwe.d || global_rlwe.q != self.rlwe.q {
            return Err("global and worker RLWE parameters differ".to_string());
        }
        let client = ipir_sp::IPIRClient::new(&global_rlwe, &global_params);
        let setup = client.generate_public_query_setup_simplepir_from_seed(
            crate::coordinator::memo_setup_seed_bytes(),
        );
        let rlwe = self.rlwe.clone();
        let artifact_dir = self.artifact_dir.clone();
        let runtime = tokio::task::spawn_blocking(move || {
            ShardRuntime::load_or_build(
                &artifact_dir,
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
        let (runtime, built) = runtime;
        self.shards
            .write()
            .await
            .insert(shard_id, Arc::new(runtime));
        Ok(built)
    }

    pub async fn ensure_local(
        &self,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
    ) -> Result<(), String> {
        if let Some(existing) = self.shards.read().await.get(&shard_id) {
            if existing.rows_sha256 == rows_sha256 && existing.query_row_start == query_row_start {
                return Ok(());
            }
        }
        let rlwe = self.rlwe.clone();
        let artifact_dir = self.artifact_dir.clone();
        let runtime = tokio::task::spawn_blocking(move || {
            ShardRuntime::load_cached(
                &artifact_dir,
                shard_id,
                query_row_start,
                &rows_sha256,
                &rlwe,
            )
        })
        .await
        .map_err(|_| "shard load task failed".to_string())??;
        self.shards
            .write()
            .await
            .insert(shard_id, Arc::new(runtime));
        Ok(())
    }

    pub async fn activate_local(&self, request: ActivateRequest) -> Result<(), String> {
        if request.shards.is_empty() {
            return Err("an active generation needs at least one shard".to_string());
        }
        let cached = self.shards.read().await;
        for shard in &request.shards {
            let runtime = cached
                .get(&shard.shard_id)
                .ok_or_else(|| format!("shard {} is not prepared", shard.shard_id))?;
            if runtime.rows_sha256 != shard.rows_sha256 {
                return Err(format!("shard {} digest mismatch", shard.shard_id));
            }
        }
        let mut shard_ids: Vec<_> = request
            .shards
            .into_iter()
            .map(|shard| shard.shard_id)
            .collect();
        shard_ids.sort_unstable();
        if shard_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("duplicate active shard".to_string());
        }
        drop(cached);
        let mut active = self.active.write().await;
        active.insert(
            request.generation,
            ActiveGeneration {
                generation: request.generation,
                shard_ids,
            },
        );
        while active.len() > 2 {
            let oldest = *active.keys().next().expect("nonempty generation map");
            active.remove(&oldest);
        }
        Ok(())
    }

    pub async fn evaluate_local(&self, request: EvaluateRequest) -> Result<Vec<u64>, String> {
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
        let mut requested_ids: Vec<_> = request.shards.iter().map(|shard| shard.shard_id).collect();
        requested_ids.sort_unstable();
        if requested_ids != active.shard_ids {
            return Err("request does not cover the complete active shard assignment".to_string());
        }
        let cached = self.shards.read().await;
        let mut combined =
            vec![0u64; (ITEM_SIZE_BITS as usize).div_ceil(self.rlwe.d * 14) * self.rlwe.d];
        for shard_query in request.shards {
            let runtime = cached
                .get(&shard_query.shard_id)
                .ok_or_else(|| "active shard disappeared".to_string())?;
            let partial = runtime
                .evaluate(&self.rlwe, &shard_query.coefficients)
                .map_err(|e| e.to_string())?;
            add_intermediate_assign_mod(&mut combined, &partial, self.rlwe.q)
                .map_err(|e| e.to_string())?;
        }
        Ok(combined)
    }

    pub async fn crs_local(&self, shard_id: u64) -> Result<Vec<u8>, String> {
        let cached = self.shards.read().await;
        let runtime = cached
            .get(&shard_id)
            .ok_or_else(|| "shard is not prepared".to_string())?;
        Ok(encode_crs_blocks(&runtime.crs_blocks))
    }
}

pub fn router(state: WorkerState) -> Router {
    Router::new()
        .route("/internal/health", get(health))
        .route("/internal/shards/:shard_id", put(prepare))
        .route("/internal/shards/:shard_id/load", post(load))
        .route("/internal/shards/:shard_id/hint", get(hint))
        .route("/internal/activate", post(activate))
        .route("/internal/evaluate", post(evaluate))
        .layer(axum::extract::DefaultBodyLimit::max(
            SHARD_ROWS * ROW_BYTES + 1024 * 1024,
        ))
        .with_state(state)
}

async fn health(State(state): State<WorkerState>) -> Json<HealthResponse> {
    let active = state.active.read().await;
    let cached_shards = state.shards.read().await.len();
    Json(HealthResponse {
        status: "ok",
        generation: active
            .last_key_value()
            .map(|(_, generation)| generation.generation),
        active_shards: active
            .last_key_value()
            .map_or(0, |(_, generation)| generation.shard_ids.len()),
        cached_shards,
    })
}

async fn prepare(
    State(state): State<WorkerState>,
    Path(shard_id): Path<u64>,
    Query(query): Query<PrepareQuery>,
    body: Bytes,
) -> Response {
    match state
        .prepare_local(
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
            shard_id,
            rows_sha256: query.rows_sha256,
        })
        .into_response(),
        Err(error) => {
            tracing::warn!(%error, shard_id, "shard preparation rejected");
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

async fn load(
    State(state): State<WorkerState>,
    Path(shard_id): Path<u64>,
    Query(query): Query<PrepareQuery>,
) -> Response {
    match state
        .ensure_local(shard_id, query.query_row_start, query.rows_sha256)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            tracing::warn!(%error, shard_id, "cached shard load rejected");
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

async fn hint(State(state): State<WorkerState>, Path(shard_id): Path<u64>) -> Response {
    match state.crs_local(shard_id).await {
        Ok(bytes) => bytes.into_response(),
        Err(error) => {
            tracing::warn!(%error, shard_id, "hint request rejected");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

async fn evaluate(State(state): State<WorkerState>, body: Bytes) -> Response {
    let request = match decode_evaluate_request(&body) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(%error, "worker query rejected");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let generation = request.generation;
    match state.evaluate_local(request).await {
        Ok(coefficients) => encode_evaluate_response(generation, &coefficients).into_response(),
        Err(error) => {
            tracing::warn!(%error, "worker evaluation failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
