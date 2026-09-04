use crate::ipir::{global_parameters, shard_parameters};
use crate::metrics;
use crate::store::RecordJournal;
use crate::types::{
    group_index_for_shard, DatabaseId, DatabaseLayout, GenerationManifest, ShardDescriptor,
    TableManifest, PROTOCOL_REVISION,
};
use crate::wire::{
    decode_crs_blocks, decode_evaluate_response, encode_evaluate_request, EvaluateRequest,
    ShardQuery,
};
use crate::worker::{ActivateRequest, ActivateShard, WorkerState, RETAINED_GENERATIONS};
use arc_swap::ArcSwap;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use enhance_pir::EnhanceSession;
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, build_pack_preprocessed_blocks,
    deserialize_first_dim_query, pack_intermediate_blocks, published_c1_rows, CrsBlock,
};
use ipir_sp::YpirSchemeParams;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};

pub use crate::types::ENHANCE_SETUP_SEED;

/// The ENHANCE table's expanded setup seed.
pub fn enhance_setup_seed_bytes() -> [u8; 32] {
    crate::types::setup_seed_bytes()
}

/// Default concurrent queries admitted per table pool.
pub const DEFAULT_QUERY_SLOTS: usize = 2;

const HEALTH_PHASE_GRACE_PERIOD: Duration = Duration::from_secs(30);

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

/// Two or more interchangeable workers holding the same complete shard range.
/// The name is stable placement identity; replica names identify physical
/// processes and may be replaced without moving shards.
#[derive(Clone)]
pub struct WorkerGroup {
    pub name: String,
    pub replicas: Vec<WorkerTarget>,
}

/// One table the coordinator serves and the ordered worker groups that own its
/// shards. Groups may share physical hosts; ownership is per table.
#[derive(Clone)]
pub struct TableSetup {
    pub table: DatabaseId,
    pub groups: Vec<WorkerGroup>,
}

/// What the coordinator needs from a table's rows to publish it. Journals
/// implement it directly; sealed tables implement it from memory.
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

/// The chain state one generation is anchored to.
#[derive(Clone, Debug, Default)]
pub struct Anchor {
    pub height: u64,
    /// Hex, display byte order.
    pub hash: String,
}

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
    ready_groups: BTreeMap<String, Arc<ReadyWorkerGroup>>,
}

struct ReadyWorkerGroup {
    replicas: Vec<WorkerTarget>,
    next: AtomicUsize,
}

#[derive(Debug)]
struct WorkerEvaluationError {
    message: String,
    retryable: bool,
}

impl std::fmt::Display for WorkerEvaluationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl ReadyWorkerGroup {
    fn replicas_for_request(&self) -> Vec<WorkerTarget> {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
        (0..self.replicas.len())
            .map(|offset| self.replicas[(start + offset) % self.replicas.len()].clone())
            .collect()
    }
}

/// Every table at one anchor. Immutable once published.
pub struct GenerationSnapshot {
    pub manifest: GenerationManifest,
    pub tables: BTreeMap<DatabaseId, Arc<TableSnapshot>>,
}

/// Retained generations, newest first.
type Generations = Vec<Arc<GenerationSnapshot>>;

#[derive(Clone)]
pub struct CoordinatorState {
    tables: Arc<BTreeMap<DatabaseId, TableState>>,
    http: reqwest::Client,
    live: Arc<ArcSwap<Generations>>,
    status: Arc<RwLock<CoordinatorStatus>>,
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

struct CoordinatorStatus {
    phase: CoordinatorPhase,
    non_serving_since: Option<Instant>,
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
            if setup.groups.is_empty() {
                return Err(format!(
                    "table {} needs at least one worker group",
                    setup.table
                ));
            }
            let mut group_names: Vec<_> = setup
                .groups
                .iter()
                .map(|group| group.name.as_str())
                .collect();
            group_names.sort_unstable();
            if group_names.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(format!(
                    "worker group names in the {} pool must be unique",
                    setup.table
                ));
            }
            let mut replica_names = HashSet::new();
            for group in &setup.groups {
                if group.name.is_empty() {
                    return Err(format!(
                        "worker group name in {} must not be empty",
                        setup.table
                    ));
                }
                if group.replicas.is_empty() {
                    return Err(format!(
                        "worker group {} in {} needs at least one replica",
                        group.name, setup.table
                    ));
                }
                for replica in &group.replicas {
                    if !replica_names.insert(replica.name().to_string()) {
                        return Err(format!(
                            "worker replica names in the {} pool must be unique",
                            setup.table
                        ));
                    }
                }
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
            status: Arc::new(RwLock::new(CoordinatorStatus {
                phase: CoordinatorPhase::Syncing {
                    current_height: 0,
                    target_height: 0,
                },
                non_serving_since: Some(Instant::now()),
            })),
        })
    }

    pub async fn set_phase(&self, phase: CoordinatorPhase) {
        let mut status = self.status.write().await;
        if matches!(phase, CoordinatorPhase::Serving) {
            status.non_serving_since = None;
        } else if status.non_serving_since.is_none() {
            status.non_serving_since = Some(Instant::now());
        }
        status.phase = phase;
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

    /// All public material for the newest generation, captured from one
    /// retained snapshot so a publication cannot mix setup epochs.
    pub fn session(&self) -> Option<EnhanceSession> {
        let snapshot = self.newest()?;
        let table = snapshot.tables.get(&DatabaseId::Enhance)?;
        Some(EnhanceSession {
            generation: snapshot.manifest.public()?,
            params: table.ypir.clone(),
            public_params_base64: BASE64_STANDARD.encode(&table.public_params),
        })
    }

    /// Every distinct replica across all pools, in first-seen group order.
    fn workers(&self) -> Vec<WorkerTarget> {
        let mut seen = HashSet::new();
        self.tables
            .values()
            .flat_map(|table| table.setup.groups.iter())
            .flat_map(|group| group.replicas.iter())
            .filter(|worker| seen.insert(worker.name().to_string()))
            .cloned()
            .collect()
    }

    /// Point-in-time gauges for `/metrics`. Reads only aggregate state.
    /// Probes each remote worker's health with a short timeout so a dead
    /// worker shows up on the dashboard without slowing the scrape down.
    pub async fn observe(&self) -> metrics::Observation {
        let phase = self.status.read().await.phase.clone();
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
                    // Retain the existing metric field name, but report
                    // logical groups so capacity is not doubled by replicas.
                    pool_workers: state.map_or(0, |t| t.setup.groups.len() as u64),
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
                    let (pool_index, group) =
                        state.setup.groups.iter().enumerate().find(|(_, group)| {
                            group
                                .replicas
                                .iter()
                                .any(|candidate| candidate.name() == name)
                        })?;
                    let assigned: Vec<&ShardDescriptor> = manifest
                        .and_then(|m| m.tables.get(table))
                        .map(|t| t.shards.iter().filter(|s| s.worker == group.name).collect())
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
        let worker_groups = self
            .tables
            .iter()
            .flat_map(|(table, state)| {
                state.setup.groups.iter().map(move |group| {
                    let ready_replicas = newest
                        .and_then(|snapshot| snapshot.tables.get(table))
                        .and_then(|snapshot| snapshot.ready_groups.get(&group.name))
                        .map_or(0, |ready| ready.replicas.len() as u64);
                    metrics::WorkerGroupObservation {
                        table: *table,
                        name: group.name.clone(),
                        configured_replicas: group.replicas.len() as u64,
                        ready_replicas,
                    }
                })
            })
            .collect();
        metrics::Observation {
            phase: Some(phase),
            anchor_height: manifest.map_or(0, |m| m.anchor_height),
            generation: manifest.map_or(0, |m| m.generation),
            ironwood_tree_size: manifest.map_or(0, |m| m.ironwood_tree_size),
            retained_generations: retained.len() as u64,
            tables,
            worker_details,
            worker_groups,
        }
    }

    /// Publishes one generation from the ENHANCE journal alone.
    pub async fn publish_from_store(
        &self,
        store: &RecordJournal,
        anchor_height: u64,
        anchor_hash: String,
    ) -> Result<(), String> {
        let enhance = TableJournal::new(DatabaseId::Enhance, store)?;
        self.publish(
            &[&enhance],
            Anchor {
                height: anchor_height,
                hash: anchor_hash,
            },
        )
        .await
    }

    /// Builds the Enhance snapshot, activates at least one replica in every
    /// shard group, then swaps in the new generation while keeping previous
    /// generations answerable. Sources this coordinator does not serve are
    /// skipped.
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
            .find(|source| source.table() == DatabaseId::Enhance)
            .map(|source| source.positions())
            .or_else(|| {
                self.newest()
                    .map(|snapshot| snapshot.manifest.ironwood_tree_size)
            })
            .unwrap_or(0);

        let mut snapshots = BTreeMap::new();
        let mut manifests = BTreeMap::new();
        for source in sources {
            let table = source.table();
            let snapshot = self.build_table(source, generation).await?;
            manifests.insert(table, snapshot.manifest.clone());
            snapshots.insert(table, Arc::new(snapshot));
        }

        let manifest = GenerationManifest {
            anchor_height,
            anchor_block_hash: anchor.hash,
            ironwood_tree_size,
            generation,
            tables: manifests,
        };
        let snapshot = Arc::new(GenerationSnapshot {
            manifest,
            tables: snapshots,
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

    /// Prepares every shard on all available replicas, sums one CRS hint per
    /// shard, and activates every replica that completed the group's full
    /// assignment. One ready replica per group is the publication quorum.
    async fn build_table(
        &self,
        journal: &dyn TableSource,
        generation: u64,
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
        let groups = &state.setup.groups;

        let mut candidates: BTreeMap<String, BTreeMap<String, (WorkerTarget, Vec<ActivateShard>)>> =
            groups
                .iter()
                .map(|group| {
                    (
                        group.name.clone(),
                        group
                            .replicas
                            .iter()
                            .cloned()
                            .map(|replica| (replica.name().to_string(), (replica, Vec::new())))
                            .collect(),
                    )
                })
                .collect();

        let mut descriptors = Vec::new();
        let mut combined_crs: Option<Vec<CrsBlock>> = None;
        for shard_id in journal.shard_ids() {
            let group_index = group_index_for_shard(shard_id, groups.len()).ok_or_else(|| {
                format!(
                    "{table} shard {shard_id} exceeds the capacity of {} worker groups",
                    groups.len()
                )
            })?;
            let group = &groups[group_index];
            let rows = journal.read_shard_rows(shard_id)?;
            let digest = RecordJournal::rows_digest(&rows);
            let query_row_start = shard_id as usize * layout.shard_rows;

            let replicas: Vec<WorkerTarget> = candidates[&group.name]
                .values()
                .map(|(replica, _)| replica.clone())
                .collect();
            if replicas.is_empty() {
                return Err(format!(
                    "worker group {} has no replica with a complete {table} assignment",
                    group.name
                ));
            }
            let mut tasks = tokio::task::JoinSet::new();
            for replica in replicas {
                let coordinator = self.clone();
                let rows = rows.clone();
                let digest = digest.clone();
                tasks.spawn(async move {
                    let name = replica.name().to_string();
                    let result = coordinator
                        .prepare_replica_shard(
                            &replica,
                            table,
                            shard_id,
                            query_row_start,
                            logical_rows,
                            digest,
                            rows,
                            ypir.db_cols / rlwe.d,
                            rlwe.d,
                        )
                        .await;
                    (name, result)
                });
            }

            let mut successful = Vec::new();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok((name, Ok(hint))) => successful.push((name, hint)),
                    Ok((name, Err(error))) => {
                        tracing::warn!(%error, replica = %name, group = %group.name, shard_id,
                            "replica shard preparation failed");
                    }
                    Err(error) => {
                        tracing::warn!(%error, group = %group.name, shard_id,
                            "replica shard preparation task failed");
                    }
                }
            }
            let Some((_, canonical_hint)) = successful.first() else {
                return Err(format!(
                    "worker group {} has no ready replica for {table} shard {shard_id}",
                    group.name
                ));
            };
            let canonical_hint = canonical_hint.clone();
            let accepted: HashSet<String> = successful
                .into_iter()
                .filter_map(|(name, hint)| {
                    if *hint == *canonical_hint {
                        Some(name)
                    } else {
                        tracing::warn!(replica = %name, group = %group.name, shard_id,
                            "replica CRS hint differs from its peer");
                        None
                    }
                })
                .collect();
            let group_candidates = candidates
                .get_mut(&group.name)
                .expect("configured worker group");
            group_candidates.retain(|name, _| accepted.contains(name));
            for (_, assignment) in group_candidates.values_mut() {
                assignment.push(ActivateShard {
                    shard_id,
                    rows_sha256: digest.clone(),
                });
            }
            if group_candidates.is_empty() {
                return Err(format!(
                    "worker group {} has no replica with a complete matching {table} assignment",
                    group.name
                ));
            }

            let hint = canonical_hint;
            if let Some(accumulator) = &mut combined_crs {
                add_crs_blocks_assign_mod(accumulator, &hint, rlwe).map_err(|e| e.to_string())?;
            } else {
                combined_crs = Some((*hint).clone());
            }
            let populated = journal.populated_positions_in_shard(shard_id);
            descriptors.push(ShardDescriptor {
                shard_id,
                global_row_start: shard_id * layout.shard_rows as u64,
                populated_positions: populated,
                rows_sha256: digest,
                sealed: populated == layout.shard_positions() as u64,
                // Kept for wire compatibility; this is now the stable logical
                // group identity rather than a physical replica name.
                worker: group.name.clone(),
            });
        }

        let used_groups: HashSet<&str> = descriptors
            .iter()
            .map(|shard| shard.worker.as_str())
            .collect();
        let mut ready_groups = BTreeMap::new();
        for (group_name, replicas) in candidates {
            if !used_groups.contains(group_name.as_str()) {
                continue;
            }
            let mut ready_replicas = Vec::new();
            for (_, (replica, shards)) in replicas {
                let mut tables = BTreeMap::new();
                tables.insert(table, shards);
                match self
                    .activate_worker(&replica, ActivateRequest { generation, tables })
                    .await
                {
                    Ok(()) => ready_replicas.push(replica),
                    Err(error) => {
                        tracing::warn!(%error, replica = %replica.name(), group = %group_name,
                        generation, "replica activation failed")
                    }
                }
            }
            if ready_replicas.is_empty() {
                return Err(format!(
                    "worker group {group_name} did not activate any replica for generation {generation}"
                ));
            }
            ready_groups.insert(
                group_name,
                Arc::new(ReadyWorkerGroup {
                    replicas: ready_replicas,
                    next: AtomicUsize::new(0),
                }),
            );
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
            ready_groups,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_replica_shard(
        &self,
        replica: &WorkerTarget,
        table: DatabaseId,
        shard_id: u64,
        query_row_start: usize,
        logical_rows: u64,
        rows_sha256: String,
        rows: Vec<u8>,
        expected_blocks: usize,
        degree: usize,
    ) -> Result<Arc<Vec<CrsBlock>>, String> {
        let state = self.table(table)?;
        let cache_key = format!(
            "{}:{shard_id}:{query_row_start}:{rows_sha256}",
            replica.name()
        );
        if let Some(hint) = state.hint_cache.read().await.get(&cache_key).cloned() {
            if self
                .ensure_worker(
                    replica,
                    table,
                    shard_id,
                    query_row_start,
                    rows_sha256.clone(),
                )
                .await
                .is_ok()
            {
                return Ok(hint);
            }
        }

        self.prepare_worker(
            replica,
            table,
            shard_id,
            query_row_start,
            logical_rows,
            rows_sha256,
            rows,
        )
        .await?;
        let encoded = self.fetch_hint(replica, table, shard_id).await?;
        let hint = Arc::new(
            decode_crs_blocks(&encoded, expected_blocks, degree).map_err(|e| e.to_string())?,
        );
        let mut cache = state.hint_cache.write().await;
        let shard_prefix = format!("{}:{shard_id}:", replica.name());
        cache.retain(|key, _| !key.starts_with(&shard_prefix));
        cache.insert(cache_key, hint.clone());
        Ok(hint)
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
    ) -> Result<Vec<u64>, WorkerEvaluationError> {
        match worker {
            WorkerTarget::Embedded { state, .. } => state
                .evaluate_local(table, request)
                .await
                .map_err(|message| WorkerEvaluationError {
                    retryable: message.contains("evaluation limit")
                        || message.contains("generation mismatch")
                        || message.contains("active shard disappeared"),
                    message,
                }),
            WorkerTarget::Remote { base_url, .. } => {
                let generation = request.generation;
                let response = self
                    .http
                    .post(format!("{base_url}/internal/{table}/evaluate"))
                    .body(encode_evaluate_request(&request))
                    .send()
                    .await
                    .map_err(|error| WorkerEvaluationError {
                        message: error.to_string(),
                        retryable: true,
                    })?;
                if !response.status().is_success() {
                    let status = response.status();
                    return Err(WorkerEvaluationError {
                        message: format!("worker evaluation returned {status}"),
                        retryable: status == StatusCode::TOO_MANY_REQUESTS
                            || status.is_server_error(),
                    });
                }
                let bytes = read_worker_body(response, 1024 * 1024)
                    .await
                    .map_err(|message| WorkerEvaluationError {
                        message,
                        retryable: false,
                    })?;
                let (response_generation, coefficients) = decode_evaluate_response(&bytes)
                    .map_err(|error| WorkerEvaluationError {
                        message: error.to_string(),
                        retryable: false,
                    })?;
                if response_generation != generation {
                    return Err(WorkerEvaluationError {
                        message: "worker response generation mismatch".to_string(),
                        retryable: true,
                    });
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
        let mut by_group: BTreeMap<String, Vec<ShardQuery>> = BTreeMap::new();
        for shard in &live.manifest.shards {
            let start = shard.global_row_start as usize;
            let coefficients = global_query
                .get(start..start + shard_rows)
                .ok_or_else(|| "query does not cover a published shard".to_string())?
                .to_vec();
            by_group
                .entry(shard.worker.clone())
                .or_default()
                .push(ShardQuery {
                    shard_id: shard.shard_id,
                    coefficients,
                });
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (group_name, group) in &live.ready_groups {
            let Some(shards) = by_group.remove(group_name) else {
                continue;
            };
            let replicas = group.replicas_for_request();
            let group_name = group_name.clone();
            let coordinator = self.clone();
            tasks.spawn(async move {
                let mut last_error = None;
                for (attempt, replica) in replicas.into_iter().enumerate() {
                    if attempt > 0 {
                        metrics::record_replica_request(&group_name, replica.name(), "retry");
                    }
                    metrics::record_replica_request(&group_name, replica.name(), "selected");
                    let timer = metrics::start_worker_replica_request(&group_name, replica.name());
                    match coordinator
                        .evaluate_worker(
                            &replica,
                            table,
                            EvaluateRequest {
                                generation,
                                shards: shards.clone(),
                            },
                        )
                        .await
                    {
                        Ok(partial) => {
                            timer.succeeded();
                            return Ok(partial);
                        }
                        Err(error) => {
                            timer.failed();
                            tracing::warn!(%error, replica = %replica.name(), generation,
                                "worker replica evaluation failed; trying peer");
                            if !error.retryable {
                                return Err(error.message);
                            }
                            last_error = Some(error.message);
                        }
                    }
                }
                Err(last_error.unwrap_or_else(|| "worker group has no ready replica".to_string()))
            });
        }
        if !by_group.is_empty() {
            return Err("snapshot references an unavailable worker group".to_string());
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
/// grows with its row bytes: measured on the POC, the ENHANCE shard (54 MB of
/// rows) yields a conservative per-shard memory hint, so
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

/// Longest a query waits for a free per-table slot before it is refused.
const QUERY_QUEUE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Largest query body accepted on the query routes. A query grows with the
/// logical row count; this bound covers every capacity the POC can reach
/// before the request-size work in the deployment plan lands.
const QUERY_BODY_LIMIT: usize = 64 * 1024 * 1024;

pub fn router(state: CoordinatorState) -> Router {
    let queries = Router::new()
        .route("/v1/enhance/query", post(query))
        .layer(axum::extract::DefaultBodyLimit::max(QUERY_BODY_LIMIT));
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/enhance/init", get(enhance_session))
        .merge(queries)
        .route("/metrics", get(handle_metrics))
        .route("/ready", get(ready))
        .layer(axum::middleware::from_fn(metrics::track_request))
        .with_state(state)
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
    // workers; an older bare number is attributed to the ENHANCE table.
    let active_shards = match body.get("active_shards") {
        Some(serde_json::Value::Object(per_table)) => per_table
            .iter()
            .filter_map(|(table, v)| v.as_u64().map(|count| (table.clone(), count)))
            .collect(),
        Some(value) => value
            .as_u64()
            .map(|count| BTreeMap::from([(DatabaseId::Enhance.as_str().to_string(), count)]))
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
    let phase = state.status.read().await.phase.clone();
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

fn health_status(phase: &CoordinatorPhase, non_serving_for: Duration) -> StatusCode {
    match phase {
        CoordinatorPhase::Serving => StatusCode::OK,
        CoordinatorPhase::Failed { .. } => StatusCode::SERVICE_UNAVAILABLE,
        CoordinatorPhase::Syncing { .. } | CoordinatorPhase::Building { .. } => {
            if non_serving_for > HEALTH_PHASE_GRACE_PERIOD {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            }
        }
    }
}

async fn health(State(state): State<CoordinatorState>) -> Response {
    let (phase, non_serving_for) = {
        let status = state.status.read().await;
        (
            status.phase.clone(),
            status
                .non_serving_since
                .map_or(Duration::ZERO, |since| since.elapsed()),
        )
    };
    let retained = state.live.load();
    let newest = retained.first();
    let status = health_status(&phase, non_serving_for);
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
                    workers: table_state
                        .setup
                        .groups
                        .iter()
                        .map(|group| group.replicas.len())
                        .sum(),
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

async fn enhance_session(State(state): State<CoordinatorState>) -> Response {
    match state.session() {
        Some(session) => Json(session).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn query(State(state): State<CoordinatorState>, body: Bytes) -> Response {
    answer(&state, DatabaseId::Enhance, &body).await
}

async fn answer(state: &CoordinatorState, table: DatabaseId, body: &[u8]) -> Response {
    // Axum has extracted the complete body before entering this function. Start
    // the post-body scope before validation or admission queueing so it covers
    // every server-side step that remains before the response is ready.
    let _processing = metrics::start_processing(metrics::query_endpoint(table));
    let Ok(table_state) = state.table(table) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // A wallet pass sends its fixed envelope as a burst; queue it for a
    // bounded wait rather than refusing everything beyond the slot count.
    let Ok(Ok(_permit)) = tokio::time::timeout(
        QUERY_QUEUE_WAIT,
        table_state.query_slots.clone().acquire_owned(),
    )
    .await
    else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
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

    use super::{health_status, is_ready, CoordinatorPhase, HEALTH_PHASE_GRACE_PERIOD};
    use axum::http::StatusCode;
    use std::time::Duration;

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

    #[test]
    fn health_graces_short_syncs_and_builds() {
        let syncing = CoordinatorPhase::Syncing {
            current_height: 0,
            target_height: 1,
        };
        let building = CoordinatorPhase::Building { anchor_height: 1 };
        let failed = CoordinatorPhase::Failed { reason: "x".into() };

        assert_eq!(
            health_status(&CoordinatorPhase::Serving, Duration::ZERO),
            StatusCode::OK
        );
        assert_eq!(health_status(&syncing, Duration::ZERO), StatusCode::OK);
        assert_eq!(
            health_status(&building, HEALTH_PHASE_GRACE_PERIOD),
            StatusCode::OK
        );
        assert_eq!(
            health_status(
                &syncing,
                HEALTH_PHASE_GRACE_PERIOD + Duration::from_millis(1)
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            health_status(&failed, Duration::ZERO),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
