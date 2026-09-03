use crate::ipir::global_parameters;
use crate::metrics;
use crate::store::MemoStore;
use crate::types::{
    logical_rows_for, worker_index_for_shard, Coverage, MemoSnapshotMetadata, ShardDescriptor,
    ACTION_LAYOUT, NETWORK, POOL, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES, SCHEMA_VERSION,
    SHARD_POSITIONS, SHARD_ROWS,
};
use crate::wire::{
    decode_crs_blocks, decode_evaluate_response, encode_evaluate_request, EvaluateRequest,
    ShardQuery,
};
use crate::worker::{ActivateRequest, ActivateShard, WorkerState};
use arc_swap::ArcSwapOption;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, build_pack_preprocessed_blocks,
    deserialize_first_dim_query, pack_intermediate_blocks, published_c1_rows, CrsBlock,
};
use ipir_sp::YpirSchemeParams;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};

/// First eight bytes, little-endian, of
/// `SHA-256("zcash/ironwood-memo-pir/setup-seed/v1")`.
pub const MEMO_SETUP_SEED: u64 = 0xaf1a_e284_ec07_131a;

pub fn memo_setup_seed_bytes() -> [u8; 32] {
    let mut seed = [0; 32];
    seed[..8].copy_from_slice(&MEMO_SETUP_SEED.to_le_bytes());
    seed
}

#[derive(Clone)]
pub enum WorkerTarget {
    Remote { name: String, base_url: String },
    Embedded { name: String, state: WorkerState },
}

impl WorkerTarget {
    pub fn name(&self) -> &str {
        match self {
            Self::Remote { name, .. } | Self::Embedded { name, .. } => name,
        }
    }
}

pub struct LiveSnapshot {
    pub metadata: MemoSnapshotMetadata,
    pub ypir: YpirSchemeParams,
    pub preprocessed: Vec<QueryPackPreprocessed<'static>>,
    pub top_key_images: TopKeyImages<'static>,
    pub public_params: Vec<u8>,
    pub public_params_epoch: [u8; 8],
}

#[derive(Clone)]
pub struct CoordinatorState {
    rlwe: &'static RlweParams,
    workers: Arc<Vec<WorkerTarget>>,
    http: reqwest::Client,
    live: Arc<ArcSwapOption<LiveSnapshot>>,
    phase: Arc<RwLock<CoordinatorPhase>>,
    hint_cache: Arc<RwLock<HashMap<String, Arc<Vec<CrsBlock>>>>>,
    query_slots: Arc<Semaphore>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum CoordinatorPhase {
    Syncing {
        current_height: u64,
        target_height: u64,
    },
    Building {
        anchor_height: u64,
    },
    Serving,
    Failed {
        reason: String,
    },
}

#[derive(Serialize)]
struct HealthResponse {
    phase: CoordinatorPhase,
    anchor_height: Option<u64>,
    ironwood_tree_size: Option<u64>,
    workers: usize,
}

impl CoordinatorState {
    pub fn new(workers: Vec<WorkerTarget>) -> Result<Self, String> {
        if workers.is_empty() {
            return Err("at least one PIR worker is required".to_string());
        }
        let mut names: Vec<_> = workers.iter().map(|worker| worker.name()).collect();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("worker names must be unique".to_string());
        }
        let (rlwe, _) =
            global_parameters(SHARD_ROWS as u64, &ACTION_LAYOUT).map_err(|e| e.to_string())?;
        let rlwe = Box::leak(Box::new(rlwe));
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            rlwe,
            workers: Arc::new(workers),
            http,
            live: Arc::new(ArcSwapOption::empty()),
            phase: Arc::new(RwLock::new(CoordinatorPhase::Syncing {
                current_height: 0,
                target_height: 0,
            })),
            hint_cache: Arc::new(RwLock::new(HashMap::new())),
            query_slots: Arc::new(Semaphore::new(2)),
        })
    }

    pub async fn set_phase(&self, phase: CoordinatorPhase) {
        *self.phase.write().await = phase;
    }

    /// Point-in-time gauges for `/metrics`. Reads only aggregate state.
    pub async fn observe(&self) -> metrics::Observation {
        let phase = self.phase.read().await.clone();
        let live = self.live.load();
        let metadata = live.as_ref().map(|snapshot| &snapshot.metadata);
        metrics::Observation {
            phase: Some(phase),
            anchor_height: metadata.map_or(0, |m| m.anchor_height),
            generation: metadata.map_or(0, |m| m.generation),
            ironwood_tree_size: metadata.map_or(0, |m| m.ironwood_tree_size),
            used_rows: metadata.map_or(0, |m| m.used_rows),
            shards: metadata.map_or(0, |m| m.shards.len() as u64),
            workers: self.workers.len() as u64,
            query_slots_available: self.query_slots.available_permits() as u64,
        }
    }

    pub fn metadata(&self) -> Option<MemoSnapshotMetadata> {
        self.live
            .load_full()
            .map(|snapshot| snapshot.metadata.clone())
    }

    /// Scheme parameters of the live snapshot, as served by `/memo/params`.
    pub fn params(&self) -> Option<YpirSchemeParams> {
        self.live.load_full().map(|snapshot| snapshot.ypir.clone())
    }

    /// Published packing material of the live snapshot, as served by
    /// `/memo/public-params`.
    pub fn public_params(&self) -> Option<Vec<u8>> {
        self.live
            .load_full()
            .map(|snapshot| snapshot.public_params.clone())
    }

    pub async fn publish_from_store(
        &self,
        store: &MemoStore,
        coverage: Coverage,
        anchor_height: u64,
        anchor_hash: String,
    ) -> Result<(), String> {
        if store.tree_size() <= store.base_position() {
            return Err("cannot publish an empty memo database".to_string());
        }
        self.set_phase(CoordinatorPhase::Building { anchor_height })
            .await;
        let global_used_rows = store.tree_size().div_ceil(RECORDS_PER_ROW as u64);
        let logical_rows = logical_rows_for(global_used_rows);
        let (global_rlwe, ypir) =
            global_parameters(logical_rows, &ACTION_LAYOUT).map_err(|e| e.to_string())?;
        if global_rlwe.d != self.rlwe.d || global_rlwe.q != self.rlwe.q {
            return Err("global RLWE parameters changed unexpectedly".to_string());
        }

        let generation = anchor_height;
        let first_covered_shard = coverage.covered_position_start() / SHARD_POSITIONS as u64;
        let shard_ids: Vec<_> = store
            .shard_ids()
            .filter(|shard_id| *shard_id >= first_covered_shard)
            .collect();
        let mut descriptors = Vec::with_capacity(shard_ids.len());
        let mut assignments: BTreeMap<String, Vec<ActivateShard>> = BTreeMap::new();
        let mut combined_crs: Option<Vec<CrsBlock>> = None;

        for shard_id in shard_ids {
            let worker_index =
                worker_index_for_shard(shard_id, self.workers.len()).ok_or_else(|| {
                    format!(
                        "shard {shard_id} exceeds the capacity of {} workers",
                        self.workers.len()
                    )
                })?;
            let worker = self.workers.get(worker_index).ok_or_else(|| {
                format!(
                    "shard {shard_id} needs worker {}, but only {} workers are configured",
                    worker_index + 1,
                    self.workers.len()
                )
            })?;
            let rows = store.read_shard_rows(shard_id).map_err(|e| e.to_string())?;
            let digest = MemoStore::rows_digest(&rows);
            let query_row_start = shard_id as usize * SHARD_ROWS;
            let cache_key = format!("{}:{shard_id}:{query_row_start}:{digest}", worker.name());
            let cached_hint = {
                let cache = self.hint_cache.read().await;
                cache.get(&cache_key).cloned()
            };
            let hint = if let Some(hint) = cached_hint {
                self.ensure_worker(worker, shard_id, query_row_start, digest.clone())
                    .await?;
                hint
            } else {
                self.prepare_worker(
                    worker,
                    shard_id,
                    query_row_start,
                    logical_rows,
                    digest.clone(),
                    rows,
                )
                .await?;
                let hint = self.fetch_hint(worker, shard_id).await?;
                let hint = Arc::new(
                    decode_crs_blocks(&hint, ypir.db_cols / self.rlwe.d, self.rlwe.d)
                        .map_err(|e| e.to_string())?,
                );
                let mut cache = self.hint_cache.write().await;
                let shard_prefix = format!("{}:{shard_id}:", worker.name());
                cache.retain(|key, _| !key.starts_with(&shard_prefix));
                cache.insert(cache_key, hint.clone());
                hint
            };
            if let Some(accumulator) = &mut combined_crs {
                add_crs_blocks_assign_mod(accumulator, &hint, self.rlwe)
                    .map_err(|e| e.to_string())?;
            } else {
                combined_crs = Some((*hint).clone());
            }
            assignments
                .entry(worker.name().to_string())
                .or_default()
                .push(ActivateShard {
                    shard_id,
                    rows_sha256: digest.clone(),
                });
            descriptors.push(ShardDescriptor {
                shard_id,
                global_row_start: shard_id * SHARD_ROWS as u64,
                populated_positions: store.populated_positions_in_shard(shard_id),
                rows_sha256: digest,
                sealed: store.populated_positions_in_shard(shard_id) == SHARD_POSITIONS as u64,
                worker: worker.name().to_string(),
            });
        }

        for worker in self.workers.iter() {
            if let Some(shards) = assignments.remove(worker.name()) {
                self.activate_worker(worker, ActivateRequest { generation, shards })
                    .await?;
            }
        }

        let combined_crs = combined_crs.ok_or_else(|| "no CRS contributions".to_string())?;
        let preprocessed =
            build_pack_preprocessed_blocks(self.rlwe, &combined_crs).map_err(|e| e.to_string())?;
        let top_key_images = TopKeyImages::build(self.rlwe);
        let public_params = published_c1_rows(&preprocessed, self.rlwe.q);
        let public_digest = Sha256::digest(&public_params);
        let mut epoch = [0; 8];
        epoch.copy_from_slice(&public_digest[..8]);
        let parameter_id = format!(
            "ipir-sp-e875404-d{}-p{}-rows{}-cols{}",
            self.rlwe.d, ypir.p, ypir.db_rows, ypir.db_cols
        );
        let metadata = MemoSnapshotMetadata {
            schema_version: SCHEMA_VERSION,
            network: NETWORK.to_string(),
            pool: POOL.to_string(),
            anchor_height,
            anchor_block_hash: anchor_hash,
            ironwood_tree_size: store.tree_size(),
            coverage,
            record_bytes: RECORD_BYTES as u32,
            records_per_row: RECORDS_PER_ROW as u32,
            row_bytes: ROW_BYTES as u32,
            shard_rows: SHARD_ROWS as u32,
            used_rows: global_used_rows,
            logical_rows,
            first_global_row: store.base_position() / RECORDS_PER_ROW as u64,
            generation,
            parameter_id,
            setup_seed: MEMO_SETUP_SEED,
            public_params_epoch: hex::encode(epoch),
            public_params_sha256: hex::encode(public_digest),
            shards: descriptors,
        };
        self.live.store(Some(Arc::new(LiveSnapshot {
            metadata,
            ypir,
            preprocessed,
            top_key_images,
            public_params,
            public_params_epoch: epoch,
        })));
        self.set_phase(CoordinatorPhase::Serving).await;
        Ok(())
    }

    async fn prepare_worker(
        &self,
        worker: &WorkerTarget,
        shard_id: u64,
        query_row_start: usize,
        logical_rows: u64,
        rows_sha256: String,
        rows: Vec<u8>,
    ) -> Result<(), String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state
                .prepare_local(shard_id, query_row_start, logical_rows, rows_sha256, rows)
                .await
                .map(|_| ()),
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .put(format!("{base_url}/internal/shards/{shard_id}"))
                    .query(&[
                        ("query_row_start", query_row_start.to_string()),
                        ("logical_rows", logical_rows.to_string()),
                        ("rows_sha256", rows_sha256),
                    ])
                    .body(rows)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!(
                        "worker shard preparation returned {}",
                        response.status()
                    ));
                }
                Ok(())
            }
        }
    }

    async fn ensure_worker(
        &self,
        worker: &WorkerTarget,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
    ) -> Result<(), String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => {
                state
                    .ensure_local(shard_id, query_row_start, rows_sha256)
                    .await
            }
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .post(format!("{base_url}/internal/shards/{shard_id}/load"))
                    .query(&[
                        ("query_row_start", query_row_start.to_string()),
                        ("logical_rows", "0".to_string()),
                        ("rows_sha256", rows_sha256),
                    ])
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!(
                        "worker cached shard load returned {}",
                        response.status()
                    ));
                }
                Ok(())
            }
        }
    }

    async fn fetch_hint(&self, worker: &WorkerTarget, shard_id: u64) -> Result<Vec<u8>, String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state.crs_local(shard_id).await,
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .get(format!("{base_url}/internal/shards/{shard_id}/hint"))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!(
                        "worker hint request returned {}",
                        response.status()
                    ));
                }
                read_worker_body(response, 80 * 1024 * 1024).await
            }
        }
    }

    async fn activate_worker(
        &self,
        worker: &WorkerTarget,
        request: ActivateRequest,
    ) -> Result<(), String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state.activate_local(request).await,
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .post(format!("{base_url}/internal/activate"))
                    .json(&request)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!("worker activation returned {}", response.status()));
                }
                Ok(())
            }
        }
    }

    async fn evaluate_worker(
        &self,
        worker: &WorkerTarget,
        request: EvaluateRequest,
    ) -> Result<Vec<u64>, String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state.evaluate_local(request).await,
            WorkerTarget::Remote { base_url, .. } => {
                let generation = request.generation;
                let response = self
                    .http
                    .post(format!("{base_url}/internal/evaluate"))
                    .body(encode_evaluate_request(&request))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!("worker evaluation returned {}", response.status()));
                }
                let bytes = read_worker_body(response, 1024 * 1024).await?;
                let (response_generation, coefficients) =
                    decode_evaluate_response(&bytes).map_err(|e| e.to_string())?;
                if response_generation != generation {
                    return Err("worker response generation mismatch".to_string());
                }
                Ok(coefficients)
            }
        }
    }

    /// Answers one opaque client query against the live snapshot. Exposed so
    /// in-process tests can drive the coordinator without HTTP; the `/memo/query`
    /// handler is a thin wrapper that adds admission control and metrics.
    pub async fn answer_query(&self, body: &[u8]) -> Result<Vec<u8>, String> {
        let live = self
            .live
            .load_full()
            .ok_or_else(|| "no live snapshot".to_string())?;
        let generation_bytes: [u8; 8] = body
            .get(..8)
            .ok_or_else(|| "query is truncated".to_string())?
            .try_into()
            .expect("eight-byte generation");
        let generation = u64::from_le_bytes(generation_bytes);
        if generation != live.metadata.generation {
            return Err("query generation mismatch".to_string());
        }
        let packing_len = serialized_packing_keys_len(self.rlwe);
        let switched_len = (live.ypir.db_rows * live.ypir.query_bits).div_ceil(8);
        if body.len() != 8 + packing_len + switched_len {
            return Err("query has the wrong fixed length".to_string());
        }
        let packing_keys = deserialize_packing_keys(self.rlwe, &body[8..8 + packing_len])
            .map_err(|e| e.to_string())?;
        let global_query =
            deserialize_first_dim_query(self.rlwe, &live.ypir, &body[8 + packing_len..])
                .map_err(|e| e.to_string())?;

        let mut by_worker: BTreeMap<String, Vec<ShardQuery>> = BTreeMap::new();
        for shard in &live.metadata.shards {
            let start = shard.global_row_start as usize;
            let coefficients = global_query
                .get(start..start + SHARD_ROWS)
                .ok_or_else(|| "query does not cover a published shard".to_string())?
                .to_vec();
            by_worker
                .entry(shard.worker.clone())
                .or_default()
                .push(ShardQuery {
                    shard_id: shard.shard_id,
                    coefficients,
                });
        }

        let mut tasks = tokio::task::JoinSet::new();
        for worker in self.workers.iter() {
            let Some(shards) = by_worker.remove(worker.name()) else {
                continue;
            };
            let worker = worker.clone();
            let state = self.clone();
            tasks.spawn(async move {
                state
                    .evaluate_worker(&worker, EvaluateRequest { generation, shards })
                    .await
            });
        }
        if !by_worker.is_empty() {
            return Err("snapshot references an unknown worker".to_string());
        }
        let mut combined = vec![0u64; live.ypir.db_cols];
        let mut partial_count = 0usize;
        while let Some(result) = tasks.join_next().await {
            let partial = result.map_err(|_| "worker evaluation task failed".to_string())??;
            add_intermediate_assign_mod(&mut combined, &partial, self.rlwe.q)
                .map_err(|e| e.to_string())?;
            partial_count += 1;
        }
        if partial_count == 0 {
            return Err("no worker evaluated the query".to_string());
        }

        let packed = pack_intermediate_blocks(
            &combined,
            &packing_keys,
            &live.top_key_images,
            &live.preprocessed,
        )
        .map_err(|e| e.to_string())?;
        let c2 =
            ipir_sp::modulus_switch::serialize_rlwe_response_bodies(&packed, live.ypir.q_prime_1);
        let mut response = Vec::with_capacity(16 + c2.len());
        response.extend_from_slice(&generation.to_le_bytes());
        response.extend_from_slice(&live.public_params_epoch);
        response.extend_from_slice(&c2);
        Ok(response)
    }
}

async fn read_worker_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("worker response exceeds limit".to_string());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err("worker response exceeds limit".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub fn router(state: CoordinatorState) -> Router {
    Router::new()
        .route("/memo/health", get(health))
        .route("/memo/metadata", get(metadata))
        .route("/memo/params", get(params))
        .route("/memo/public-params", get(public_params))
        .route("/memo/query", post(query))
        .route("/metrics", get(handle_metrics))
        .route("/ready", get(ready))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(axum::middleware::from_fn(metrics::track_request))
        .with_state(state)
}

/// `GET /metrics`: Prometheus text exposition for the local `pir-apm` sidecar.
/// Refreshes the snapshot gauges from live coordinator state on every scrape.
/// Caddy blocks this path publicly; it is loopback-only by policy.
async fn handle_metrics(State(state): State<CoordinatorState>) -> Response {
    metrics::record_observation(&state.observe().await);
    let (status, content_type, body) = metrics::encode();
    (
        status,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        body,
    )
        .into_response()
}

/// `GET /ready`: 200 only while the coordinator is serving a live snapshot.
/// Deploy tooling and the sidecar use this as the readiness gate; `/memo/health`
/// stays the richer JSON view.
async fn ready(State(state): State<CoordinatorState>) -> Response {
    let phase = state.phase.read().await.clone();
    if matches!(phase, CoordinatorPhase::Serving) && state.live.load().is_some() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

async fn health(State(state): State<CoordinatorState>) -> Response {
    let phase = state.phase.read().await.clone();
    let live = state.live.load();
    let status = if matches!(phase, CoordinatorPhase::Serving) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(HealthResponse {
            phase,
            anchor_height: live
                .as_ref()
                .map(|snapshot| snapshot.metadata.anchor_height),
            ironwood_tree_size: live
                .as_ref()
                .map(|snapshot| snapshot.metadata.ironwood_tree_size),
            workers: state.workers.len(),
        }),
    )
        .into_response()
}

async fn metadata(State(state): State<CoordinatorState>) -> Response {
    match state.live.load().as_ref() {
        Some(snapshot) => Json(snapshot.metadata.clone()).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn params(State(state): State<CoordinatorState>) -> Response {
    match state.live.load().as_ref() {
        Some(snapshot) => Json(snapshot.ypir.clone()).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn public_params(State(state): State<CoordinatorState>) -> Response {
    match state.live.load().as_ref() {
        Some(snapshot) => snapshot.public_params.clone().into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn query(State(state): State<CoordinatorState>, body: Bytes) -> Response {
    let Ok(_permit) = state.query_slots.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // The body is fully received by now, so this measures server work only.
    let _processing = metrics::start_processing("query");
    match state.answer_query(&body).await {
        Ok(response) => response.into_response(),
        Err(error) => {
            tracing::warn!(%error, "memo PIR query failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
