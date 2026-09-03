#[cfg(feature = "ipir")]
use ipir_sp::modulus_switch::recover_published_c1;
#[cfg(feature = "ipir")]
use ipir_sp::serialize::serialize_packing_keys;
#[cfg(feature = "ipir")]
use ipir_sp::{params_for_simplepir, IPIRClient, IPIRSeed, YpirSchemeParams};
use spend_types::{
    hash_to_bucket, SpendMetadata, SpendabilityMetadata, YpirScenario, BUCKET_BYTES, ENTRY_BYTES,
};
#[cfg(feature = "ipir")]
use spend_types::{public_params_epoch, split_epoch, IPIR_SETUP_SEED, PIR_EPOCH_BYTES};
use thiserror::Error;
#[cfg(not(feature = "ipir"))]
use ypir::client::YPIRClient;
#[cfg(not(feature = "ipir"))]
use ypir::params::params_for_scenario_simplepir;
#[cfg(not(feature = "ipir"))]
use ypir::serialize::ToBytes;

#[derive(Error, Debug)]
pub enum SpendClientError {
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
    #[error(
        "server serves the {actual} pool, but this client requires {expected}; \
         point it at an Ironwood spend-server"
    )]
    PoolMismatch {
        expected: &'static str,
        actual: String,
    },
}

pub type Result<T> = std::result::Result<T, SpendClientError>;

pub struct SpendClient {
    http: reqwest::Client,
    base_url: String,
    scenario: YpirScenario,
    metadata: SpendabilityMetadata,
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
    /// Behind a lock rather than in `&mut self` so that `is_spent` keeps its
    /// `&self` signature; the guard is never held across an await.
    published: std::sync::RwLock<PublishedParams>,
}

/// The server's `c1` rows and the epoch identifying them.
#[cfg(feature = "ipir")]
struct PublishedParams {
    c1: Vec<Vec<u64>>,
    epoch: [u8; PIR_EPOCH_BYTES],
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
        return Err(SpendClientError::ServerUnavailable);
    }

    let bytes = resp.error_for_status()?.bytes().await?;
    let blocks = ypir.db_cols / rlwe.d;
    let expected = blocks * ipir_sp::modulus_switch::published_c1_len(rlwe.d, rlwe.q);
    if bytes.len() != expected {
        return Err(SpendClientError::InvalidParams(format!(
            "/public-params returned {} bytes, expected {expected} for {blocks} output blocks",
            bytes.len(),
        )));
    }

    Ok(PublishedParams {
        epoch: public_params_epoch(&bytes),
        c1: recover_published_c1(&bytes, rlwe.d, blocks, rlwe.q),
    })
}

impl SpendClient {
    /// Connect to a spend-server, fetch params and metadata, initialize the PIR client.
    pub async fn connect(url: &str) -> Result<Self> {
        let base_url = url.trim_end_matches('/').to_string();
        let http = reqwest::Client::new();

        let scenario: YpirScenario = http
            .get(format!("{base_url}/params"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let metadata_resp = http.get(format!("{base_url}/metadata")).send().await?;

        if metadata_resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SpendClientError::ServerUnavailable);
        }

        let metadata: SpendabilityMetadata = metadata_resp.error_for_status()?.json().await?;

        // Refuse an Orchard-era server outright: its nullifiers answer a
        // different question, and a false "not spent" is the dangerous
        // direction to be wrong in.
        if !metadata.is_expected_pool() {
            return Err(SpendClientError::PoolMismatch {
                expected: spend_types::POOL,
                actual: metadata.pool.clone(),
            });
        }

        if scenario.item_size_bits < 2048 * 14 {
            return Err(SpendClientError::InvalidParams(format!(
                "item_size_bits {} below SimplePIR minimum 28672",
                scenario.item_size_bits,
            )));
        }

        #[cfg(feature = "ipir")]
        let ipir_client = {
            let (rlwe, ypir) = params_for_simplepir(scenario.num_items, scenario.item_size_bits)
                .map_err(|e| SpendClientError::InvalidParams(e.to_string()))?;
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
            base_url,
            earliest = metadata.earliest_height,
            latest = metadata.latest_height,
            nullifiers = metadata.num_nullifiers,
            pool = %metadata.pool,
            "connected to spend-server",
        );

        Ok(Self {
            http,
            base_url,
            scenario,
            metadata,
            #[cfg(feature = "ipir")]
            ipir_client,
            #[cfg(not(feature = "ipir"))]
            ypir_client,
        })
    }

    /// Check if a nullifier has been spent, returning spend metadata on match.
    pub async fn is_spent(&self, nf: &[u8; 32]) -> Result<Option<SpendMetadata>> {
        let bucket_idx = hash_to_bucket(nf) as usize;

        #[cfg(feature = "ipir")]
        let decoded = self.ipir_round_trip(bucket_idx).await?;
        #[cfg(not(feature = "ipir"))]
        let decoded = {
            let (query, seed) = self.ypir_client.generate_query_simplepir(bucket_idx);
            let response_bytes = self.post_query(query.to_bytes()).await?;
            self.ypir_client
                .decode_response_simplepir(seed, &response_bytes)
        };

        Ok(scan_bucket_for_nf(&decoded, nf))
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
            return Err(SpendClientError::ServerUnavailable);
        }

        Ok(resp
            .error_for_status()
            .map_err(|e| SpendClientError::QueryFailed(e.to_string()))?
            .bytes()
            .await?
            .to_vec())
    }

    /// Query one row, refreshing `c1` if the server rotated its snapshot.
    ///
    /// A response only decodes against the `c1` rows of the snapshot that
    /// produced it. Decoding against stale rows yields noise rather than an
    /// error, and a garbage bucket scan reads as "not spent" — so a mismatch is
    /// resolved by refetching and asking again, never by decoding anyway.
    #[cfg(feature = "ipir")]
    async fn ipir_round_trip(&self, row_idx: usize) -> Result<Vec<u8>> {
        // Two passes: one to discover a rotation, one to answer after it. A
        // second mismatch means the server is rotating faster than a round
        // trip, which no amount of retrying fixes.
        for attempt in 0..2 {
            let (query_bytes, seed) = self.generate_ipir_query(row_idx)?;
            let response_bytes = self.post_query(query_bytes).await?;
            let (epoch, body) = split_epoch(&response_bytes)
                .ok_or(SpendClientError::MalformedResponse(response_bytes.len()))?;

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

        Err(SpendClientError::PublicParamsUnstable)
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

    #[cfg(feature = "ipir")]
    fn generate_ipir_query(&self, bucket_idx: usize) -> Result<(Vec<u8>, IPIRSeed)> {
        let (query, packing_keys, seed) = self
            .ipir_client
            .client
            .generate_fresh_query_simplepir(&self.ipir_client.offline_query_polys, bucket_idx);
        let mut query_bytes =
            serialize_packing_keys(self.ipir_client.client.rlwe_params(), &packing_keys)
                .map_err(|e| SpendClientError::QueryFailed(e.to_string()))?;
        // The query is transmitted at the derived width, not at full `q`: the
        // precision above it is noise the server's accumulator cannot use.
        query_bytes.extend(query.to_switched_bytes(
            self.ipir_client.client.rlwe_params().q,
            self.ipir_client.ypir.query_bits,
        ));
        Ok((query_bytes, seed))
    }

    /// Re-fetch metadata from the server to get updated heights.
    pub async fn refresh_metadata(&mut self) -> Result<()> {
        let resp = self
            .http
            .get(format!("{}/metadata", self.base_url))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SpendClientError::ServerUnavailable);
        }

        let metadata: SpendabilityMetadata = resp.error_for_status()?.json().await?;
        if !metadata.is_expected_pool() {
            return Err(SpendClientError::PoolMismatch {
                expected: spend_types::POOL,
                actual: metadata.pool,
            });
        }
        self.metadata = metadata;
        Ok(())
    }

    pub fn earliest_height(&self) -> u64 {
        self.metadata.earliest_height
    }

    pub fn latest_height(&self) -> u64 {
        self.metadata.latest_height
    }

    pub fn metadata(&self) -> &SpendabilityMetadata {
        &self.metadata
    }

    pub fn scenario(&self) -> &YpirScenario {
        &self.scenario
    }
}

/// Blocking wrapper around `SpendClient` for use from synchronous FFI contexts.
/// Owns a single-threaded tokio runtime internally.
pub struct SpendClientBlocking {
    rt: tokio::runtime::Runtime,
    client: SpendClient,
}

impl SpendClientBlocking {
    pub fn connect(url: &str) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SpendClientError::QueryFailed(e.to_string()))?;
        let client = rt.block_on(SpendClient::connect(url))?;
        Ok(Self { rt, client })
    }

    /// Check a batch of nullifiers against the PIR database.
    /// Parallel to the input: `Some(meta)` = spent, `None` = not spent.
    /// Calls `progress` after each query with fraction complete (0.0..=1.0).
    pub fn check_nullifiers(
        &self,
        nullifiers: &[[u8; 32]],
        progress: impl Fn(f64),
    ) -> Result<Vec<Option<SpendMetadata>>> {
        let total = nullifiers.len();
        let mut results = Vec::with_capacity(total);
        for (i, nf) in nullifiers.iter().enumerate() {
            let meta = self.rt.block_on(self.client.is_spent(nf))?;
            results.push(meta);
            progress((i + 1) as f64 / total as f64);
        }
        Ok(results)
    }

    pub fn metadata(&self) -> &SpendabilityMetadata {
        self.client.metadata()
    }

    pub fn earliest_height(&self) -> u64 {
        self.client.earliest_height()
    }

    pub fn latest_height(&self) -> u64 {
        self.client.latest_height()
    }
}

/// Scan the decoded bucket bytes for a nullifier match, returning spend metadata.
pub fn scan_bucket_for_nf(decoded_row: &[u8], nf: &[u8; 32]) -> Option<SpendMetadata> {
    let bucket_data = if decoded_row.len() >= BUCKET_BYTES {
        &decoded_row[..BUCKET_BYTES]
    } else {
        decoded_row
    };

    bucket_data
        .chunks_exact(ENTRY_BYTES)
        .find(|entry| entry[..32] == nf[..])
        .map(|entry| SpendMetadata::from_entry_tail(entry[32..41].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spend_types::NullifierEntry;

    fn make_nf(seed: u32) -> [u8; 32] {
        let mut nf = [0u8; 32];
        nf[0..4].copy_from_slice(&seed.to_le_bytes());
        for (i, byte) in nf.iter_mut().enumerate().skip(4) {
            *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
        }
        nf
    }

    fn make_entry(nf: [u8; 32], height: u32, pos: u32, count: u8) -> NullifierEntry {
        NullifierEntry {
            nullifier: nf,
            spend_height: height,
            first_output_position: pos,
            action_count: count,
        }
    }

    fn place_entry(bucket: &mut [u8], slot: usize, entry: &NullifierEntry) {
        let offset = slot * ENTRY_BYTES;
        bucket[offset..offset + ENTRY_BYTES].copy_from_slice(&entry.to_bytes());
    }

    #[test]
    fn test_bucket_scan_found() {
        let nf = make_nf(42);
        let entry = make_entry(nf, 100, 5000, 3);
        let mut bucket = vec![0u8; BUCKET_BYTES];
        place_entry(&mut bucket, 3, &entry);

        let meta = scan_bucket_for_nf(&bucket, &nf).unwrap();
        assert_eq!(meta.spend_height, 100);
        assert_eq!(meta.first_output_position, 5000);
        assert_eq!(meta.action_count, 3);
    }

    #[test]
    fn test_bucket_scan_not_found() {
        let nf = make_nf(42);
        let absent = make_nf(99);
        let entry = make_entry(nf, 100, 5000, 3);
        let mut bucket = vec![0u8; BUCKET_BYTES];
        place_entry(&mut bucket, 3, &entry);

        assert!(scan_bucket_for_nf(&bucket, &absent).is_none());
    }

    #[test]
    fn test_bucket_scan_empty() {
        let nf = make_nf(42);
        let bucket = vec![0u8; BUCKET_BYTES];
        assert!(scan_bucket_for_nf(&bucket, &nf).is_none());
    }

    #[test]
    fn test_bucket_scan_last_slot() {
        let nf = make_nf(42);
        let entry = make_entry(nf, 200, 8000, 1);
        let mut bucket = vec![0u8; BUCKET_BYTES];
        let last_slot = (BUCKET_BYTES / ENTRY_BYTES) - 1;
        place_entry(&mut bucket, last_slot, &entry);

        let meta = scan_bucket_for_nf(&bucket, &nf).unwrap();
        assert_eq!(meta.spend_height, 200);
    }

    #[test]
    fn test_bucket_scan_oversized_row() {
        let nf = make_nf(42);
        let entry = make_entry(nf, 300, 9000, 5);
        let mut row = vec![0u8; BUCKET_BYTES + 1024];
        place_entry(&mut row, 5, &entry);

        assert!(scan_bucket_for_nf(&row, &nf).is_some());
    }
}
