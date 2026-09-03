use crate::ipir::{global_parameters, shard_parameters};
use crate::metrics;
use crate::store::RecordJournal;
use crate::types::{
    worker_index_for_shard, DatabaseId, DatabaseLayout, GenerationManifest, MemoSnapshotMetadata,
    ShardDescriptor, TableManifest, DEFAULT_ENVELOPE, MANIFEST_SCHEMA_VERSION, NETWORK, POOL,
    PROTOCOL_REVISION,
};
use crate::wire::{
    decode_crs_blocks, decode_evaluate_response, encode_evaluate_request, EvaluateRequest,
    ShardQuery,
};
use crate::witness::{FrontierUpdate, WitnessCap};
use crate::worker::{ActivateRequest, ActivateShard, WorkerState, RETAINED_GENERATIONS};
use arc_swap::ArcSwap;
use axum::body::Bytes;
use axum::extract::{Path, State};
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

pub use crate::types::MEMO_SETUP_SEED;

/// The ACTION table's expanded setup seed.
pub fn memo_setup_seed_bytes() -> [u8; 32] {
    crate::types::setup_seed_bytes(MEMO_SETUP_SEED)
}

/// Default concurrent queries admitted per table pool.
pub const DEFAULT_QUERY_SLOTS: usize = 2;

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

/// One table the coordinator serves and the ordered worker pool that owns its
/// shards. Pools may share physical hosts; ownership is per pool.
#[derive(Clone)]
pub struct TableSetup {
    pub table: DatabaseId,
    pub pool: Vec<WorkerTarget>,
}

/// What the coordinator needs from a table's rows to publish it. Journals
/// implement it directly; built tables (the nullifier buckets) implement it
/// from memory.
pub trait TableSource: Sync {
    fn table(&self) -> DatabaseId;
    fn layout(&self) -> DatabaseLayout;
    /// Populated positions (records).
    fn positions(&self) -> u64;
    fn shard_ids(&self) -> std::ops::RangeInclusive<u64>;
    /// The full padded shard, deterministic so its digest is stable.
    fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, String>;
    fn populated_positions_in_shard(&self, shard_id: u64) -> u64;
}

/// A journal viewed as the table it backs.
pub struct TableJournal<'a> {
    table: DatabaseId,
    journal: &'a RecordJournal,
}

impl<'a> TableJournal<'a> {
    pub fn new(table: DatabaseId, journal: &'a RecordJournal) -> Result<Self, String> {
        if journal.table() != Some(table) {
            return Err(format!(
                "journal {} does not back the {table} table",
                journal.name()
            ));
        }
        Ok(Self { table, journal })
    }
}

impl TableSource for TableJournal<'_> {
    fn table(&self) -> DatabaseId {
        self.table
    }

    fn layout(&self) -> DatabaseLayout {
        *self.journal.layout()
    }

    fn positions(&self) -> u64 {
        self.journal.tree_size()
    }

    fn shard_ids(&self) -> std::ops::RangeInclusive<u64> {
        self.journal.shard_ids()
    }

    fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, String> {
        self.journal
            .read_shard_rows(shard_id)
            .map_err(|e| e.to_string())
    }

    fn populated_positions_in_shard(&self, shard_id: u64) -> u64 {
        self.journal.populated_positions_in_shard(shard_id)
    }
}

/// The chain state one generation is anchored to, plus the public witness
/// material published beside the tables.
#[derive(Clone, Debug, Default)]
pub struct Anchor {
    pub height: u64,
    /// Hex, display byte order.
    pub hash: String,
    pub cold_checkpoint_height: u64,
    /// The public tree summary, once the witness tables are served.
    pub witness_cap: Option<WitnessCap>,
    /// Recent per-block frontier updates, oldest first.
    pub frontier: Vec<FrontierUpdate>,
}

/// How many frontier updates a generation carries (about a day of blocks).
pub const FRONTIER_UPDATES_RETAINED: usize = 2_000;

struct TableState {
    setup: TableSetup,
    rlwe: &'static RlweParams,
    hint_cache: RwLock<HashMap<String, Arc<Vec<CrsBlock>>>>,
    query_slots: Arc<Semaphore>,
}

/// One table as published in one generation: what a query is answered with.
pub struct TableSnapshot {
    pub manifest: TableManifest,
    pub ypir: YpirSchemeParams,
    pub preprocessed: Vec<QueryPackPreprocessed<'static>>,
    pub top_key_images: TopKeyImages<'static>,
    pub public_params: Vec<u8>,
    pub public_params_epoch: [u8; 8],
}

/// Every table at one anchor. Immutable once published.
pub struct GenerationSnapshot {
    pub manifest: GenerationManifest,
    pub tables: BTreeMap<DatabaseId, Arc<TableSnapshot>>,
    /// Public tree summary, present once the witness tables are served.
    pub witness_cap: Option<WitnessCap>,
    /// Recent per-block frontier updates, oldest first.
    pub frontier: Vec<FrontierUpdate>,
}

/// Retained generations, newest first.
type Generations = Vec<Arc<GenerationSnapshot>>;

#[derive(Clone)]
pub struct CoordinatorState {
    tables: Arc<BTreeMap<DatabaseId, TableState>>,
    http: reqwest::Client,
    live: Arc<ArcSwap<Generations>>,
    phase: Arc<RwLock<CoordinatorPhase>>,
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
struct LegacyHealthResponse {
    phase: CoordinatorPhase,
    anchor_height: Option<u64>,
    ironwood_tree_size: Option<u64>,
    workers: usize,
}

#[derive(Serialize)]
struct TableHealth {
    shards: usize,
    workers: usize,
}

#[derive(Serialize)]
struct HealthResponse {
    phase: CoordinatorPhase,
    generation: Option<u64>,
    retained_generations: usize,
    anchor_height: Option<u64>,
    ironwood_tree_size: Option<u64>,
    tables: BTreeMap<DatabaseId, TableHealth>,
}

impl CoordinatorState {
    pub fn new(setups: Vec<TableSetup>) -> Result<Self, String> {
        Self::with_query_slots(setups, DEFAULT_QUERY_SLOTS)
    }

    pub fn with_query_slots(setups: Vec<TableSetup>, query_slots: usize) -> Result<Self, String> {
        if setups.is_empty() {
            return Err("at least one PIR table is required".to_string());
        }
        let mut tables = BTreeMap::new();
        for setup in setups {
            if setup.pool.is_empty() {
                return Err(format!("table {} needs at least one worker", setup.table));
            }
            let mut names: Vec<_> = setup.pool.iter().map(|worker| worker.name()).collect();
            names.sort_unstable();
            if names.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(format!(
                    "worker names in the {} pool must be unique",
                    setup.table
                ));
            }
            if tables.contains_key(&setup.table) {
                return Err(format!("table {} is configured twice", setup.table));
            }
            let (rlwe, _) = shard_parameters(&setup.table.layout()).map_err(|e| e.to_string())?;
            tables.insert(
                setup.table,
                TableState {
                    setup,
                    rlwe: Box::leak(Box::new(rlwe)),
                    hint_cache: RwLock::new(HashMap::new()),
                    query_slots: Arc::new(Semaphore::new(query_slots.max(1))),
                },
            );
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            tables: Arc::new(tables),
            http,
            live: Arc::new(ArcSwap::from_pointee(Vec::new())),
            phase: Arc::new(RwLock::new(CoordinatorPhase::Syncing {
                current_height: 0,
                target_height: 0,
            })),
        })
    }

    pub async fn set_phase(&self, phase: CoordinatorPhase) {
        *self.phase.write().await = phase;
    }

    pub fn tables(&self) -> impl Iterator<Item = DatabaseId> + '_ {
        self.tables.keys().copied()
    }

    fn table(&self, table: DatabaseId) -> Result<&TableState, String> {
        self.tables
            .get(&table)
            .ok_or_else(|| format!("table {table} is not served"))
    }

    /// The newest retained generation.
    pub fn newest(&self) -> Option<Arc<GenerationSnapshot>> {
        self.live.load().first().cloned()
    }

    /// The retained generation with this id, if still answerable.
    pub fn generation(&self, generation: u64) -> Option<Arc<GenerationSnapshot>> {
        self.live
            .load()
            .iter()
            .find(|snapshot| snapshot.manifest.generation == generation)
            .cloned()
    }

    pub fn manifest(&self) -> Option<GenerationManifest> {
        self.newest().map(|snapshot| snapshot.manifest.clone())
    }

    /// Legacy ACTION-only view of the newest generation (`/memo/metadata`).
    pub fn metadata(&self) -> Option<MemoSnapshotMetadata> {
        self.newest()
            .and_then(|snapshot| MemoSnapshotMetadata::from_manifest(&snapshot.manifest))
    }

    /// The requested retained generation, or the newest when none is named.
    /// Clients pin every fetch of a session to the manifest's generation so a
    /// publish between two fetches cannot hand them mismatched material.
    fn pinned(&self, generation: Option<u64>) -> Option<Arc<GenerationSnapshot>> {
        match generation {
            Some(generation) => self.generation(generation),
            None => self.newest(),
        }
    }

    /// Scheme parameters of a table in the newest generation.
    pub fn params(&self, table: DatabaseId) -> Option<YpirSchemeParams> {
        self.params_at(table, None)
    }

    /// Scheme parameters of a table in a retained generation.
    pub fn params_at(
        &self,
        table: DatabaseId,
        generation: Option<u64>,
    ) -> Option<YpirSchemeParams> {
        self.pinned(generation)?
            .tables
            .get(&table)
            .map(|snapshot| snapshot.ypir.clone())
    }

    /// Published packing material of a table in the newest generation.
    pub fn public_params(&self, table: DatabaseId) -> Option<Vec<u8>> {
        self.public_params_at(table, None)
    }

    /// Published packing material of a table in a retained generation.
    pub fn public_params_at(&self, table: DatabaseId, generation: Option<u64>) -> Option<Vec<u8>> {
        self.pinned(generation)?
            .tables
            .get(&table)
            .map(|snapshot| snapshot.public_params.clone())
    }

    /// The witness cap of a retained generation (newest when unnamed).
    pub fn witness_cap_at(&self, generation: Option<u64>) -> Option<WitnessCap> {
        self.pinned(generation)?.witness_cap.clone()
    }

    /// Every distinct worker across all pools, in first-seen pool order.
    fn workers(&self) -> Vec<WorkerTarget> {
        let mut seen = std::collections::HashSet::new();
        self.tables
            .values()
            .flat_map(|table| table.setup.pool.iter())
            .filter(|worker| seen.insert(worker.name().to_string()))
            .cloned()
            .collect()
    }

    /// Point-in-time gauges for `/metrics`. Reads only aggregate state.
    /// Probes each remote worker's health with a short timeout so a dead
    /// worker shows up on the dashboard without slowing the scrape down.
    pub async fn observe(&self) -> metrics::Observation {
        let phase = self.phase.read().await.clone();
        let retained = self.live.load();
        let newest = retained.first();
        let manifest = newest.map(|snapshot| &snapshot.manifest);

        let tables = DatabaseId::ALL
            .into_iter()
            .map(|table| {
                let state = self.tables.get(&table);
                let published = manifest.and_then(|m| m.tables.get(&table));
                metrics::TableObservation {
                    table,
                    registered: state.is_some(),
                    pool_workers: state.map_or(0, |t| t.setup.pool.len() as u64),
                    query_slots_available: state
                        .map_or(0, |t| t.query_slots.available_permits() as u64),
                    positions: published.map_or(0, |t| t.positions),
                    used_rows: published.map_or(0, |t| t.used_rows),
                    logical_rows: published.map_or(0, |t| t.logical_rows),
                    shards: published.map_or(0, |t| t.shards.len() as u64),
                    sealed_shards: published
                        .map_or(0, |t| t.shards.iter().filter(|s| s.sealed).count() as u64),
                }
            })
            .collect();

        let mut probes = tokio::task::JoinSet::new();
        for (index, worker) in self.workers().into_iter().enumerate() {
            let name = worker.name().to_string();
            let shares: Vec<(DatabaseId, u64, u64, u64)> = self
                .tables
                .iter()
                .filter_map(|(table, state)| {
                    let pool_index = state
                        .setup
                        .pool
                        .iter()
                        .position(|candidate| candidate.name() == name)?;
                    let assigned: Vec<&ShardDescriptor> = manifest
                        .and_then(|m| m.tables.get(table))
                        .map(|t| t.shards.iter().filter(|s| s.worker == name).collect())
                        .unwrap_or_default();
                    Some((
                        *table,
                        pool_index as u64,
                        assigned.len() as u64,
                        assigned.iter().map(|s| s.populated_positions).sum(),
                    ))
                })
                .collect();
            let probe = match &worker {
                WorkerTarget::Embedded { .. } => None,
                WorkerTarget::Remote { base_url, .. } => {
                    Some((self.http.clone(), base_url.clone()))
                }
            };
            probes.spawn(async move {
                let probe = match probe {
                    None => {
                        let (total, available, rss) = crate::worker::host_memory();
                        WorkerProbe {
                            up: true,
                            total_memory_bytes: total,
                            available_memory_bytes: available,
                            process_rss_bytes: rss,
                            ..Default::default()
                        }
                    }
                    Some((client, base_url)) => probe_worker_health(&client, &base_url).await,
                };
                let observation = metrics::WorkerObservation {
                    name,
                    index: index as u64,
                    up: probe.up,
                    generation: probe.generation,
                    total_memory_bytes: probe.total_memory_bytes,
                    available_memory_bytes: probe.available_memory_bytes,
                    process_rss_bytes: probe.process_rss_bytes,
                    tables: shares
                        .into_iter()
                        .map(
                            |(table, pool_index, assigned_shards, populated_positions)| {
                                metrics::WorkerTableObservation {
                                    table,
                                    index: pool_index,
                                    assigned_shards,
                                    populated_positions,
                                    active_shards: probe
                                        .active_shards
                                        .get(table.as_str())
                                        .copied()
                                        .unwrap_or(0),
                                }
                            },
                        )
                        .collect(),
                };
                (index, observation)
            });
        }
        let mut worker_details: Vec<(usize, metrics::WorkerObservation)> = Vec::new();
        while let Some(result) = probes.join_next().await {
            if let Ok(entry) = result {
                worker_details.push(entry);
            }
        }
        worker_details.sort_by_key(|(index, _)| *index);
        let worker_details = worker_details
            .into_iter()
            .map(|(_, observation)| observation)
            .collect();
        metrics::Observation {
            phase: Some(phase),
            anchor_height: manifest.map_or(0, |m| m.anchor_height),
            generation: manifest.map_or(0, |m| m.generation),
            ironwood_tree_size: manifest.map_or(0, |m| m.ironwood_tree_size),
            retained_generations: retained.len() as u64,
            tables,
            worker_details,
        }
    }

    /// Publishes one generation from the ACTION journal alone.
    pub async fn publish_from_store(
        &self,
        store: &RecordJournal,
        anchor_height: u64,
        anchor_hash: String,
    ) -> Result<(), String> {
        let action = TableJournal::new(DatabaseId::Action, store)?;
        self.publish(
            &[&action],
            Anchor {
                height: anchor_height,
                hash: anchor_hash,
                ..Anchor::default()
            },
        )
        .await
    }

    /// Builds every table's snapshot from its source, activates every worker
    /// once with all of its tables, then swaps in the new generation while
    /// keeping the previous one answerable. Sources for tables this
    /// coordinator does not serve are skipped.
    pub async fn publish(
        &self,
        sources: &[&dyn TableSource],
        anchor: Anchor,
    ) -> Result<(), String> {
        let sources: Vec<&dyn TableSource> = sources
            .iter()
            .copied()
            .filter(|source| self.tables.contains_key(&source.table()))
            .collect();
        if sources.is_empty() {
            return Err("nothing to publish".to_string());
        }
        let anchor_height = anchor.height;
        self.set_phase(CoordinatorPhase::Building { anchor_height })
            .await;
        let generation = anchor_height;
        let ironwood_tree_size = sources
            .iter()
            .find(|source| source.table() == DatabaseId::Action)
            .map(|source| source.positions())
            .or_else(|| {
                self.newest()
                    .map(|snapshot| snapshot.manifest.ironwood_tree_size)
            })
            .unwrap_or(0);

        let mut assignments: BTreeMap<
            String,
            (WorkerTarget, BTreeMap<DatabaseId, Vec<ActivateShard>>),
        > = BTreeMap::new();
        let mut snapshots = BTreeMap::new();
        let mut manifests = BTreeMap::new();
        for source in sources {
            let table = source.table();
            let snapshot = self.build_table(source, &mut assignments).await?;
            manifests.insert(table, snapshot.manifest.clone());
            snapshots.insert(table, Arc::new(snapshot));
        }

        for (_, (worker, tables)) in assignments {
            self.activate_worker(&worker, ActivateRequest { generation, tables })
                .await?;
        }

        let manifest = GenerationManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.to_string(),
            network: NETWORK.to_string(),
            pool: POOL.to_string(),
            anchor_height,
            anchor_block_hash: anchor.hash,
            ironwood_tree_size,
            generation,
            anchor_tree_root: anchor
                .witness_cap
                .as_ref()
                .map(|cap| cap.tree_root.clone())
                .unwrap_or_default(),
            cold_checkpoint_height: anchor.cold_checkpoint_height,
            envelope: DEFAULT_ENVELOPE,
            tables: manifests,
        };
        let mut frontier = anchor.frontier;
        if frontier.len() > FRONTIER_UPDATES_RETAINED {
            frontier.drain(..frontier.len() - FRONTIER_UPDATES_RETAINED);
        }
        let snapshot = Arc::new(GenerationSnapshot {
            manifest,
            tables: snapshots,
            witness_cap: anchor.witness_cap,
            frontier,
        });
        let mut retained: Generations = Vec::with_capacity(RETAINED_GENERATIONS);
        retained.push(snapshot);
        for previous in self.live.load().iter() {
            if retained.len() >= RETAINED_GENERATIONS {
                break;
            }
            if previous.manifest.generation != generation {
                retained.push(previous.clone());
            }
        }
        self.live.store(Arc::new(retained));
        self.set_phase(CoordinatorPhase::Serving).await;
        Ok(())
    }

    /// Prepares every shard of one table on its pool and sums the CRS hints.
    /// Records each worker's assignment for the single activation call.
    async fn build_table(
        &self,
        journal: &dyn TableSource,
        assignments: &mut BTreeMap<
            String,
            (WorkerTarget, BTreeMap<DatabaseId, Vec<ActivateShard>>),
        >,
    ) -> Result<TableSnapshot, String> {
        let table = journal.table();
        let state = self.table(table)?;
        let layout = journal.layout();
        if layout != table.layout() {
            return Err(format!(
                "source layout for {table} does not match the protocol"
            ));
        }
        if journal.positions() == 0 {
            return Err(format!("cannot publish an empty {table} table"));
        }
        let used_rows = layout.used_rows_for(journal.positions());
        let logical_rows = layout.logical_rows_for(used_rows);
        let (global_rlwe, ypir) =
            global_parameters(logical_rows, &layout).map_err(|e| e.to_string())?;
        if global_rlwe.d != state.rlwe.d || global_rlwe.q != state.rlwe.q {
            return Err(format!(
                "{table}: global RLWE parameters changed unexpectedly"
            ));
        }
        let rlwe = state.rlwe;
        let pool = &state.setup.pool;

        let mut descriptors = Vec::new();
        let mut combined_crs: Option<Vec<CrsBlock>> = None;
        for shard_id in journal.shard_ids() {
            let worker_index = worker_index_for_shard(shard_id, pool).ok_or_else(|| {
                format!(
                    "{table} shard {shard_id} exceeds the capacity of {} workers",
                    pool.len()
                )
            })?;
            let worker = &pool[worker_index];
            let rows = journal.read_shard_rows(shard_id)?;
            let digest = RecordJournal::rows_digest(&rows);
            let query_row_start = shard_id as usize * layout.shard_rows;
            let cache_key = format!("{}:{shard_id}:{query_row_start}:{digest}", worker.name());
            let cached_hint = state.hint_cache.read().await.get(&cache_key).cloned();
            let hint = if let Some(hint) = cached_hint {
                self.ensure_worker(worker, table, shard_id, query_row_start, digest.clone())
                    .await?;
                hint
            } else {
                self.prepare_worker(
                    worker,
                    table,
                    shard_id,
                    query_row_start,
                    logical_rows,
                    digest.clone(),
                    rows,
                )
                .await?;
                let hint = self.fetch_hint(worker, table, shard_id).await?;
                let hint = Arc::new(
                    decode_crs_blocks(&hint, ypir.db_cols / rlwe.d, rlwe.d)
                        .map_err(|e| e.to_string())?,
                );
                let mut cache = state.hint_cache.write().await;
                let shard_prefix = format!("{}:{shard_id}:", worker.name());
                cache.retain(|key, _| !key.starts_with(&shard_prefix));
                cache.insert(cache_key, hint.clone());
                hint
            };
            if let Some(accumulator) = &mut combined_crs {
                add_crs_blocks_assign_mod(accumulator, &hint, rlwe).map_err(|e| e.to_string())?;
            } else {
                combined_crs = Some((*hint).clone());
            }
            assignments
                .entry(worker.name().to_string())
                .or_insert_with(|| (worker.clone(), BTreeMap::new()))
                .1
                .entry(table)
                .or_default()
                .push(ActivateShard {
                    shard_id,
                    rows_sha256: digest.clone(),
                });
            let populated = journal.populated_positions_in_shard(shard_id);
            descriptors.push(ShardDescriptor {
                shard_id,
                global_row_start: shard_id * layout.shard_rows as u64,
                populated_positions: populated,
                rows_sha256: digest,
                sealed: populated == layout.shard_positions() as u64,
                worker: worker.name().to_string(),
            });
        }

        let combined_crs = combined_crs.ok_or_else(|| "no CRS contributions".to_string())?;
        let preprocessed =
            build_pack_preprocessed_blocks(rlwe, &combined_crs).map_err(|e| e.to_string())?;
        let top_key_images = TopKeyImages::build(rlwe);
        let public_params = published_c1_rows(&preprocessed, rlwe.q);
        let public_digest = Sha256::digest(&public_params);
        let mut epoch = [0; 8];
        epoch.copy_from_slice(&public_digest[..8]);
        let manifest = TableManifest {
            record_bytes: layout.record_bytes as u32,
            records_per_row: layout.records_per_row as u32,
            row_bytes: layout.row_bytes() as u32,
            shard_rows: layout.shard_rows as u32,
            positions: journal.positions(),
            used_rows,
            logical_rows,
            parameter_id: format!(
                "{PROTOCOL_REVISION}-{table}-d{}-p{}-rows{}-cols{}",
                rlwe.d, ypir.p, ypir.db_rows, ypir.db_cols
            ),
            setup_seed: table.setup_seed(),
            public_params_epoch: hex::encode(epoch),
            public_params_sha256: hex::encode(public_digest),
            shards: descriptors,
        };
        Ok(TableSnapshot {
            manifest,
            ypir,
            preprocessed,
            top_key_images,
            public_params,
            public_params_epoch: epoch,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_worker(
        &self,
        worker: &WorkerTarget,
        table: DatabaseId,
        shard_id: u64,
        query_row_start: usize,
        logical_rows: u64,
        rows_sha256: String,
        rows: Vec<u8>,
    ) -> Result<(), String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state
                .prepare_local(
                    table,
                    shard_id,
                    query_row_start,
                    logical_rows,
                    rows_sha256,
                    rows,
                )
                .await
                .map(|_| ()),
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .put(format!("{base_url}/internal/{table}/shards/{shard_id}"))
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
        table: DatabaseId,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
    ) -> Result<(), String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => {
                state
                    .ensure_local(table, shard_id, query_row_start, rows_sha256)
                    .await
            }
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .post(format!(
                        "{base_url}/internal/{table}/shards/{shard_id}/load"
                    ))
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

    async fn fetch_hint(
        &self,
        worker: &WorkerTarget,
        table: DatabaseId,
        shard_id: u64,
    ) -> Result<Vec<u8>, String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state.crs_local(table, shard_id).await,
            WorkerTarget::Remote { base_url, .. } => {
                let response = self
                    .http
                    .get(format!(
                        "{base_url}/internal/{table}/shards/{shard_id}/hint"
                    ))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !response.status().is_success() {
                    return Err(format!(
                        "worker hint request returned {}",
                        response.status()
                    ));
                }
                read_worker_body(response, worker_hint_limit()).await
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
        table: DatabaseId,
        request: EvaluateRequest,
    ) -> Result<Vec<u64>, String> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state.evaluate_local(table, request).await,
            WorkerTarget::Remote { base_url, .. } => {
                let generation = request.generation;
                let response = self
                    .http
                    .post(format!("{base_url}/internal/{table}/evaluate"))
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

    /// Answers one opaque client query for `table` against whichever retained
    /// generation the body names. Exposed so in-process tests can drive the
    /// coordinator without HTTP; the query handlers add admission control and
    /// metrics on top.
    pub async fn answer_query(&self, table: DatabaseId, body: &[u8]) -> Result<Vec<u8>, String> {
        let state = self.table(table)?;
        let rlwe = state.rlwe;
        let generation_bytes: [u8; 8] = body
            .get(..8)
            .ok_or_else(|| "query is truncated".to_string())?
            .try_into()
            .expect("eight-byte generation");
        let generation = u64::from_le_bytes(generation_bytes);
        let snapshot = self
            .generation(generation)
            .ok_or_else(|| "query generation is not retained".to_string())?;
        let live = snapshot
            .tables
            .get(&table)
            .ok_or_else(|| format!("generation has no {table} table"))?;
        let packing_len = serialized_packing_keys_len(rlwe);
        let switched_len = (live.ypir.db_rows * live.ypir.query_bits).div_ceil(8);
        if body.len() != 8 + packing_len + switched_len {
            return Err("query has the wrong fixed length".to_string());
        }
        let packing_keys =
            deserialize_packing_keys(rlwe, &body[8..8 + packing_len]).map_err(|e| e.to_string())?;
        let global_query = deserialize_first_dim_query(rlwe, &live.ypir, &body[8 + packing_len..])
            .map_err(|e| e.to_string())?;

        let shard_rows = table.layout().shard_rows;
        let mut by_worker: BTreeMap<String, Vec<ShardQuery>> = BTreeMap::new();
        for shard in &live.manifest.shards {
            let start = shard.global_row_start as usize;
            let coefficients = global_query
                .get(start..start + shard_rows)
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
        for worker in state.setup.pool.iter() {
            let Some(shards) = by_worker.remove(worker.name()) else {
                continue;
            };
            let worker = worker.clone();
            let coordinator = self.clone();
            tasks.spawn(async move {
                coordinator
                    .evaluate_worker(&worker, table, EvaluateRequest { generation, shards })
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
            add_intermediate_assign_mod(&mut combined, &partial, rlwe.q)
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

/// Largest hint (CRS block) response accepted from a worker. A shard's hint
/// grows with its row bytes: measured on the POC, the ACTION shard (54 MB of
/// rows) yields a 67 MB hint and the WITNESS shard (67 MB) a 101 MB hint, so
/// the bound is a multiple of the largest table's shard rather than a literal
/// that silently stops covering a new table.
fn worker_hint_limit() -> usize {
    DatabaseId::ALL
        .iter()
        .map(|table| table.layout().shard_bytes())
        .max()
        .expect("at least one table")
        * 4
}

/// Largest query body accepted on the query routes. A query grows with the
/// logical row count; this bound covers every capacity the POC can reach
/// before the request-size work in the deployment plan lands.
const QUERY_BODY_LIMIT: usize = 64 * 1024 * 1024;

pub fn router(state: CoordinatorState) -> Router {
    let queries = Router::new()
        .route("/v1/:table/query", post(query))
        .route("/memo/query", post(legacy_query))
        .layer(axum::extract::DefaultBodyLimit::max(QUERY_BODY_LIMIT));
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/generation", get(generation_manifest))
        .route("/v1/witness/cap", get(witness_cap))
        .route("/v1/witness/frontier", get(witness_frontier))
        .route("/v1/:table/params", get(params))
        .route("/v1/:table/public-params", get(public_params))
        .route("/memo/health", get(legacy_health))
        .route("/memo/metadata", get(legacy_metadata))
        .route("/memo/params", get(legacy_params))
        .route("/memo/public-params", get(legacy_public_params))
        .merge(queries)
        .route("/metrics", get(handle_metrics))
        .route("/ready", get(ready))
        .layer(axum::middleware::from_fn(metrics::track_request))
        .with_state(state)
}

fn parse_table(name: &str) -> Result<DatabaseId, StatusCode> {
    name.parse().map_err(|_| StatusCode::NOT_FOUND)
}

/// What one `/internal/health` probe yielded. All zeros when the worker is down.
#[derive(Clone, Debug, Default)]
struct WorkerProbe {
    up: bool,
    generation: u64,
    /// Active shard count per table wire name, from the newest generation.
    active_shards: BTreeMap<String, u64>,
    total_memory_bytes: u64,
    available_memory_bytes: u64,
    process_rss_bytes: u64,
}

/// Ask a worker for `/internal/health`. Any error, non-2xx, or malformed
/// body counts as down.
async fn probe_worker_health(client: &reqwest::Client, base_url: &str) -> WorkerProbe {
    let response = client
        .get(format!("{base_url}/internal/health"))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    let Ok(response) = response else {
        return WorkerProbe::default();
    };
    if !response.status().is_success() {
        return WorkerProbe::default();
    }
    let Ok(body) = response.json::<serde_json::Value>().await else {
        return WorkerProbe::default();
    };
    let field = |name: &str| body.get(name).and_then(|v| v.as_u64()).unwrap_or(0);
    // `active_shards` is a per-table object keyed by wire name on current
    // workers; an older bare number is attributed to the ACTION table.
    let active_shards = match body.get("active_shards") {
        Some(serde_json::Value::Object(per_table)) => per_table
            .iter()
            .filter_map(|(table, v)| v.as_u64().map(|count| (table.clone(), count)))
            .collect(),
        Some(value) => value
            .as_u64()
            .map(|count| BTreeMap::from([(DatabaseId::Action.as_str().to_string(), count)]))
            .unwrap_or_default(),
        None => BTreeMap::new(),
    };
    WorkerProbe {
        up: body.get("status").and_then(|v| v.as_str()) == Some("ok"),
        generation: field("generation"),
        active_shards,
        total_memory_bytes: field("total_memory_bytes"),
        available_memory_bytes: field("available_memory_bytes"),
        process_rss_bytes: field("process_rss_bytes"),
    }
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

/// `GET /ready`: 200 while queries can be answered from a live generation.
///
/// The previous generation keeps serving during a `building` rebuild, so
/// readiness follows the live snapshot rather than the `serving` phase; only
/// a failed ingest, or having nothing published yet, reports 503.
async fn ready(State(state): State<CoordinatorState>) -> Response {
    let phase = state.phase.read().await.clone();
    if is_ready(&phase, state.newest().is_some()) {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

fn is_ready(phase: &CoordinatorPhase, has_live_snapshot: bool) -> bool {
    has_live_snapshot
        && matches!(
            phase,
            CoordinatorPhase::Serving | CoordinatorPhase::Building { .. }
        )
}

async fn health(State(state): State<CoordinatorState>) -> Response {
    let phase = state.phase.read().await.clone();
    let retained = state.live.load();
    let newest = retained.first();
    let status = if matches!(phase, CoordinatorPhase::Serving) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let tables = state
        .tables
        .iter()
        .map(|(table, table_state)| {
            (
                *table,
                TableHealth {
                    shards: newest
                        .and_then(|snapshot| snapshot.manifest.tables.get(table))
                        .map_or(0, |manifest| manifest.shards.len()),
                    workers: table_state.setup.pool.len(),
                },
            )
        })
        .collect();
    (
        status,
        Json(HealthResponse {
            phase,
            generation: newest.map(|snapshot| snapshot.manifest.generation),
            retained_generations: retained.len(),
            anchor_height: newest.map(|snapshot| snapshot.manifest.anchor_height),
            ironwood_tree_size: newest.map(|snapshot| snapshot.manifest.ironwood_tree_size),
            tables,
        }),
    )
        .into_response()
}

async fn legacy_health(State(state): State<CoordinatorState>) -> Response {
    let phase = state.phase.read().await.clone();
    let newest = state.newest();
    let status = if matches!(phase, CoordinatorPhase::Serving) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(LegacyHealthResponse {
            phase,
            anchor_height: newest
                .as_ref()
                .map(|snapshot| snapshot.manifest.anchor_height),
            ironwood_tree_size: newest
                .as_ref()
                .map(|snapshot| snapshot.manifest.ironwood_tree_size),
            workers: state
                .tables
                .get(&DatabaseId::Action)
                .map_or(0, |table| table.setup.pool.len()),
        }),
    )
        .into_response()
}

async fn generation_manifest(State(state): State<CoordinatorState>) -> Response {
    match state.manifest() {
        Some(manifest) => Json(manifest).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// Optional `?generation=` on the per-generation public routes.
#[derive(serde::Deserialize)]
struct GenerationQuery {
    generation: Option<u64>,
}

async fn witness_cap(
    State(state): State<CoordinatorState>,
    axum::extract::Query(pin): axum::extract::Query<GenerationQuery>,
) -> Response {
    match state.witness_cap_at(pin.generation) {
        Some(cap) => Json(cap).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

#[derive(serde::Deserialize)]
struct FrontierRange {
    from: u64,
    to: u64,
}

/// Frontier updates for heights `from..=to`, bounded to what the newest
/// generation carries. Public data, the same for every client.
async fn witness_frontier(
    State(state): State<CoordinatorState>,
    axum::extract::Query(range): axum::extract::Query<FrontierRange>,
) -> Response {
    let Some(snapshot) = state.newest() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if range.to < range.from || range.to - range.from > FRONTIER_UPDATES_RETAINED as u64 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let updates: Vec<&FrontierUpdate> = snapshot
        .frontier
        .iter()
        .filter(|update| update.height >= range.from && update.height <= range.to)
        .collect();
    Json(updates).into_response()
}

async fn legacy_metadata(State(state): State<CoordinatorState>) -> Response {
    match state.metadata() {
        Some(metadata) => Json(metadata).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn params(
    State(state): State<CoordinatorState>,
    Path(table): Path<String>,
    axum::extract::Query(pin): axum::extract::Query<GenerationQuery>,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    match state.params_at(table, pin.generation) {
        Some(ypir) => Json(ypir).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn legacy_params(State(state): State<CoordinatorState>) -> Response {
    match state.params(DatabaseId::Action) {
        Some(ypir) => Json(ypir).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn public_params(
    State(state): State<CoordinatorState>,
    Path(table): Path<String>,
    axum::extract::Query(pin): axum::extract::Query<GenerationQuery>,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    match state.public_params_at(table, pin.generation) {
        Some(bytes) => bytes.into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn legacy_public_params(State(state): State<CoordinatorState>) -> Response {
    match state.public_params(DatabaseId::Action) {
        Some(bytes) => bytes.into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn query(
    State(state): State<CoordinatorState>,
    Path(table): Path<String>,
    body: Bytes,
) -> Response {
    let table = match parse_table(&table) {
        Ok(table) => table,
        Err(status) => return status.into_response(),
    };
    answer(&state, table, &body).await
}

async fn legacy_query(State(state): State<CoordinatorState>, body: Bytes) -> Response {
    answer(&state, DatabaseId::Action, &body).await
}

async fn answer(state: &CoordinatorState, table: DatabaseId, body: &[u8]) -> Response {
    let Ok(table_state) = state.table(table) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(_permit) = table_state.query_slots.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // The body is fully received by now, so this measures server work only.
    let _processing = metrics::start_processing(metrics::query_endpoint(table));
    match state.answer_query(table, body).await {
        Ok(response) => response.into_response(),
        Err(error) => {
            tracing::warn!(%error, %table, "PIR query failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    /// Every table's hint (at most about 1.5x its shard bytes on the POC) must
    /// fit the read limit with room to spare.
    #[test]
    fn worker_hint_limit_covers_every_table() {
        for table in super::DatabaseId::ALL {
            let shard_bytes = table.layout().shard_bytes();
            assert!(
                super::worker_hint_limit() >= shard_bytes * 2,
                "{table}: hint limit too small for a {shard_bytes}-byte shard"
            );
        }
    }

    use super::{is_ready, CoordinatorPhase};

    #[test]
    fn readiness_follows_the_live_snapshot() {
        let building = CoordinatorPhase::Building { anchor_height: 1 };
        let syncing = CoordinatorPhase::Syncing {
            current_height: 0,
            target_height: 1,
        };
        let failed = CoordinatorPhase::Failed { reason: "x".into() };
        assert!(is_ready(&CoordinatorPhase::Serving, true));
        assert!(is_ready(&building, true));
        assert!(!is_ready(&CoordinatorPhase::Serving, false));
        assert!(!is_ready(&building, false));
        assert!(!is_ready(&syncing, true));
        assert!(!is_ready(&failed, true));
    }
}
