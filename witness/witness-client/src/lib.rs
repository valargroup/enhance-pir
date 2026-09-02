#[cfg(feature = "ipir")]
use ipir_sp::modulus_switch::recover_published_c1;
#[cfg(feature = "ipir")]
use ipir_sp::serialize::serialize_packing_keys;
#[cfg(feature = "ipir")]
use ipir_sp::{params_for_simplepir, IPIRClient, IPIRSeed, YpirSchemeParams};
use pir_types::YpirScenario;
#[cfg(feature = "ipir")]
use pir_types::{public_params_epoch, split_epoch, IPIR_SETUP_SEED, PIR_EPOCH_BYTES};
use thiserror::Error;
use witness_types::*;
#[cfg(not(feature = "ipir"))]
use ypir::client::YPIRClient;
#[cfg(not(feature = "ipir"))]
use ypir::params::params_for_scenario_simplepir;
#[cfg(not(feature = "ipir"))]
use ypir::serialize::ToBytes;

pub mod reconstruct;

#[derive(Error, Debug)]
pub enum WitnessClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server unavailable (503)")]
    ServerUnavailable,
    #[error("invalid params from server: {0}")]
    InvalidParams(String),
    #[error("query failed: {0}")]
    QueryFailed(String),
    #[error("server public parameters kept changing under us; the snapshot is rotating faster than a query round trip")]
    PublicParamsUnstable,
    #[error("server response is not a tagged PIR response ({0} bytes)")]
    MalformedResponse(usize),
    #[error("position {0} is outside the server's PIR window (shards {1}..{2})")]
    PositionOutsideWindow(u64, u32, u32),
    #[error("witness verification failed for position {0}: computed root does not match anchor")]
    VerificationFailed(u64),
    #[error(
        "server serves the {actual} pool, but this client requires {expected}; \
         point it at an Ironwood witness-server"
    )]
    PoolMismatch {
        expected: &'static str,
        actual: String,
    },
}

pub type Result<T> = std::result::Result<T, WitnessClientError>;

pub struct WitnessClient {
    http: reqwest::Client,
    base_url: String,
    #[allow(dead_code)]
    scenario: YpirScenario,
    broadcast: BroadcastData,
    #[cfg(feature = "ipir")]
    ipir_client: IpirClientState,
    #[cfg(not(feature = "ipir"))]
    ypir_client: YPIRClient,
}

#[cfg(feature = "ipir")]
struct IpirClientState {
    client: IPIRClient,
    ypir: YpirSchemeParams,
    offline_query_polys: Vec<Vec<u64>>,
    /// Snapshot-constant `c1` rows, refreshed when the server rotates.
    ///
    /// Behind a lock rather than in `&mut self` so that `get_witness` keeps its
    /// `&self` signature; the guard is never held across an await.
    published: std::sync::RwLock<PublishedParams>,
}

/// The server's `c1` rows and the epoch identifying them.
#[cfg(feature = "ipir")]
struct PublishedParams {
    c1: Vec<Vec<u64>>,
    epoch: [u8; PIR_EPOCH_BYTES],
}

/// The subset of the server's `/metadata` response this client needs.
///
/// Deliberately not the server's own `WitnessMetadata` type — the client crate
/// does not depend on `witness-server`, and only the pool identity is load
/// bearing here.
#[derive(Debug, serde::Deserialize)]
struct ServerMetadata {
    /// Absent on pre-Ironwood servers, which served Orchard.
    #[serde(default = "legacy_pool")]
    pool: String,
}

fn legacy_pool() -> String {
    "orchard".to_string()
}

/// Fetch and decode the server's snapshot-constant `c1` rows.
///
/// The blocks count is `db_cols / d` — one published row per RLWE output block
/// — and `recover_published_c1` asserts on any other length, so a server that
/// does not serve `/public-params` fails loudly here rather than silently
/// decoding to noise later.
#[cfg(feature = "ipir")]
async fn fetch_public_params(
    http: &reqwest::Client,
    base_url: &str,
    rlwe: &inspiring::RlweParams,
    ypir: &YpirSchemeParams,
) -> Result<PublishedParams> {
    let resp = http.get(format!("{base_url}/public-params")).send().await?;

    if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Err(WitnessClientError::ServerUnavailable);
    }

    let bytes = resp.error_for_status()?.bytes().await?;
    let blocks = ypir.db_cols / rlwe.d;
    let expected = blocks * ipir_sp::modulus_switch::published_c1_len(rlwe.d, rlwe.q);
    if bytes.len() != expected {
        return Err(WitnessClientError::InvalidParams(format!(
            "/public-params returned {} bytes, expected {expected} for {blocks} output blocks",
            bytes.len(),
        )));
    }

    Ok(PublishedParams {
        epoch: public_params_epoch(&bytes),
        c1: recover_published_c1(&bytes, rlwe.d, blocks, rlwe.q),
    })
}

impl WitnessClient {
    /// Connect to a witness-server, fetch params and broadcast data, initialize
    /// the PIR client. The broadcast download is ~104 KB and cached for the
    /// lifetime of this client.
    pub async fn connect(url: &str) -> Result<Self> {
        let t0 = std::time::Instant::now();
        let base_url = url.trim_end_matches('/').to_string();
        let http = reqwest::Client::new();

        let scenario: YpirScenario = http
            .get(format!("{base_url}/params"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        tracing::info!(elapsed_ms = t0.elapsed().as_millis(), "fetched /params");

        // Check the pool before downloading the ~104 KB broadcast: an Orchard
        // server's Merkle paths are for a different tree entirely, and a
        // witness that verifies against the wrong anchor is worse than none.
        let metadata_resp = http.get(format!("{base_url}/metadata")).send().await?;
        if metadata_resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }
        let metadata: ServerMetadata = metadata_resp.error_for_status()?.json().await?;
        if metadata.pool != pir_types::POOL {
            return Err(WitnessClientError::PoolMismatch {
                expected: pir_types::POOL,
                actual: metadata.pool,
            });
        }

        let t1 = std::time::Instant::now();
        let broadcast_resp = http.get(format!("{base_url}/broadcast")).send().await?;
        if broadcast_resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }
        let broadcast: BroadcastData = broadcast_resp.error_for_status()?.json().await?;
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            broadcast_bytes = serde_json::to_vec(&broadcast).map(|v| v.len()).unwrap_or(0),
            "fetched /broadcast",
        );

        let t2 = std::time::Instant::now();
        #[cfg(feature = "ipir")]
        let ipir_client = {
            let (rlwe, ypir) = params_for_simplepir(scenario.num_items, scenario.item_size_bits)
                .map_err(|e| WitnessClientError::InvalidParams(e.to_string()))?;
            let client = IPIRClient::new(&rlwe, &ypir);
            let offline_query_polys =
                client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);
            // Responses carry only `c2`; `c1` is constant for the snapshot and
            // is fetched once here instead of riding along with every answer.
            let published = fetch_public_params(&http, &base_url, &rlwe, &ypir).await?;
            IpirClientState {
                client,
                ypir,
                offline_query_polys,
                published: std::sync::RwLock::new(published),
            }
        };
        #[cfg(not(feature = "ipir"))]
        let ypir_client = {
            let params = params_for_scenario_simplepir(scenario.num_items, scenario.item_size_bits);
            YPIRClient::new(&params)
        };
        tracing::info!(
            elapsed_ms = t2.elapsed().as_millis(),
            num_items = scenario.num_items,
            item_size_bits = scenario.item_size_bits,
            "PIR client initialized",
        );

        tracing::info!(
            base_url,
            total_connect_ms = t0.elapsed().as_millis(),
            anchor_height = broadcast.anchor_height,
            window_start = broadcast.window_start_shard,
            window_count = broadcast.window_shard_count,
            cap_shards = broadcast.cap.shard_roots.len(),
            "connected to witness-server",
        );

        Ok(Self {
            http,
            base_url,
            scenario,
            broadcast,
            #[cfg(feature = "ipir")]
            ipir_client,
            #[cfg(not(feature = "ipir"))]
            ypir_client,
        })
    }

    /// Fetch a note commitment witness for the given tree position.
    ///
    /// Issues a single PIR query to retrieve the
    /// subshard row containing the note's leaf. Combines the PIR response with
    /// the cached broadcast data to reconstruct the full 32-level authentication
    /// path. Self-verifies the witness before returning.
    pub async fn get_witness(&self, position: u64) -> Result<PirWitness> {
        let t0 = std::time::Instant::now();
        let (shard_idx, subshard_idx, leaf_idx) = decompose_position(position);
        let window_end = self.broadcast.window_start_shard + self.broadcast.window_shard_count;

        if shard_idx < self.broadcast.window_start_shard || shard_idx >= window_end {
            return Err(WitnessClientError::PositionOutsideWindow(
                position,
                self.broadcast.window_start_shard,
                window_end,
            ));
        }

        let row_idx =
            physical_row_index(shard_idx, subshard_idx, self.broadcast.window_start_shard);

        let t1 = std::time::Instant::now();
        #[cfg(feature = "ipir")]
        let decoded_row = self.ipir_round_trip(row_idx).await?;
        #[cfg(not(feature = "ipir"))]
        let decoded_row = {
            let (query, seed) = self.ypir_client.generate_query_simplepir(row_idx);
            let response_bytes = self.post_query(query.to_bytes()).await?;
            self.ypir_client
                .decode_response_simplepir(seed, &response_bytes)
        };
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            decoded_elements = decoded_row.len(),
            row_idx,
            position,
            "PIR round trip complete",
        );

        let t4 = std::time::Instant::now();
        let witness = reconstruct::reconstruct_witness(
            position,
            shard_idx,
            subshard_idx,
            leaf_idx,
            &decoded_row,
            &self.broadcast,
        )?;
        tracing::info!(
            elapsed_ms = t4.elapsed().as_millis(),
            total_ms = t0.elapsed().as_millis(),
            position,
            "witness reconstructed",
        );

        Ok(witness)
    }

    #[cfg(feature = "ipir")]
    fn generate_ipir_query(&self, row_idx: usize) -> Result<(Vec<u8>, IPIRSeed)> {
        let (query, packing_keys, seed) = self
            .ipir_client
            .client
            .generate_fresh_query_simplepir(&self.ipir_client.offline_query_polys, row_idx);
        let mut query_bytes =
            serialize_packing_keys(self.ipir_client.client.rlwe_params(), &packing_keys)
                .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?;
        // The query is transmitted at the derived width, not at full `q`: the
        // precision above it is noise the server's accumulator cannot use.
        query_bytes.extend(query.to_switched_bytes(
            self.ipir_client.client.rlwe_params().q,
            self.ipir_client.ypir.query_bits,
        ));
        Ok((query_bytes, seed))
    }

    /// POST a query body and return the raw response bytes.
    async fn post_query(&self, query_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let resp = self
            .http
            .post(format!("{}/query", self.base_url))
            .body(query_bytes)
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }

        Ok(resp
            .error_for_status()
            .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?
            .bytes()
            .await?
            .to_vec())
    }

    /// Query one row, refreshing `c1` if the server rotated its snapshot.
    ///
    /// A response only decodes against the `c1` rows of the snapshot that
    /// produced it. Decoding against stale rows yields noise rather than an
    /// error — here it would surface as a witness that fails to verify against
    /// the anchor — so a mismatch is resolved by refetching and asking again.
    #[cfg(feature = "ipir")]
    async fn ipir_round_trip(&self, row_idx: usize) -> Result<Vec<u8>> {
        // Two passes: one to discover a rotation, one to answer after it. A
        // second mismatch means the server is rotating faster than a round
        // trip, which no amount of retrying fixes.
        for attempt in 0..2 {
            let (query_bytes, seed) = self.generate_ipir_query(row_idx)?;
            let response_bytes = self.post_query(query_bytes).await?;
            let (epoch, body) = split_epoch(&response_bytes)
                .ok_or(WitnessClientError::MalformedResponse(response_bytes.len()))?;

            {
                let published = self
                    .ipir_client
                    .published
                    .read()
                    .expect("published params lock poisoned");
                if published.epoch == epoch {
                    return Ok(self.ipir_client.client.decode_response_simplepir(
                        seed,
                        &published.c1,
                        body,
                    ));
                }
            }

            if attempt == 0 {
                tracing::info!("server public parameters rotated; refreshing and retrying");
                self.refresh_public_params().await?;
            }
        }

        Err(WitnessClientError::PublicParamsUnstable)
    }

    /// Re-fetch the server's `c1` rows after a snapshot rotation.
    #[cfg(feature = "ipir")]
    async fn refresh_public_params(&self) -> Result<()> {
        let refreshed = fetch_public_params(
            &self.http,
            &self.base_url,
            self.ipir_client.client.rlwe_params(),
            &self.ipir_client.ypir,
        )
        .await?;
        *self
            .ipir_client
            .published
            .write()
            .expect("published params lock poisoned") = refreshed;
        Ok(())
    }

    /// Re-fetch broadcast data from the server (new anchor, updated tree).
    pub async fn refresh_broadcast(&mut self) -> Result<()> {
        let resp = self
            .http
            .get(format!("{}/broadcast", self.base_url))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }

        self.broadcast = resp.error_for_status()?.json().await?;
        Ok(())
    }

    pub fn anchor_height(&self) -> u64 {
        self.broadcast.anchor_height
    }

    pub fn broadcast(&self) -> &BroadcastData {
        &self.broadcast
    }
}

/// Blocking wrapper for use from synchronous FFI contexts.
pub struct WitnessClientBlocking {
    rt: tokio::runtime::Runtime,
    client: WitnessClient,
}

impl WitnessClientBlocking {
    pub fn connect(url: &str) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?;
        let client = rt.block_on(WitnessClient::connect(url))?;
        Ok(Self { rt, client })
    }

    /// Fetch witnesses for a batch of positions.
    /// Returns a `Vec<PirWitness>` parallel to the input positions.
    /// Calls `progress` after each query with fraction complete (0.0..=1.0).
    pub fn get_witnesses(
        &self,
        positions: &[u64],
        progress: impl Fn(f64),
    ) -> Result<Vec<PirWitness>> {
        let total = positions.len();
        let mut results = Vec::with_capacity(total);
        for (i, &pos) in positions.iter().enumerate() {
            let witness = self.rt.block_on(self.client.get_witness(pos))?;
            results.push(witness);
            progress((i + 1) as f64 / total as f64);
        }
        Ok(results)
    }

    pub fn anchor_height(&self) -> u64 {
        self.client.anchor_height()
    }

    pub fn broadcast(&self) -> &BroadcastData {
        self.client.broadcast()
    }
}
