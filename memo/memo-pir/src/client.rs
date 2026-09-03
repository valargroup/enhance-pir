use crate::coordinator::{memo_setup_seed_bytes, MEMO_SETUP_SEED};
use crate::types::{
    ActionRecord, MemoSnapshotMetadata, ITEM_SIZE_BITS, NETWORK, POOL, RECORDS_PER_ROW,
    RECORD_BYTES, ROW_BYTES, SCHEMA_VERSION, SHARD_ROWS,
};
use ipir_sp::modulus_switch::{published_c1_len, recover_published_c1, response_body_len};
use ipir_sp::serialize::serialize_packing_keys;
use ipir_sp::{IPIRClient, YpirSchemeParams};
use rand::{rngs::OsRng, Rng};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("server metadata is incompatible: {0}")]
    Metadata(String),
    #[error("position {0} is outside advertised coverage")]
    OutsideCoverage(u64),
    #[error("PIR error: {0}")]
    Pir(String),
    #[error("malformed PIR response: {0}")]
    Response(String),
}

/// Everything a client needs to build and decode queries against one published
/// snapshot, independent of transport. `MemoPirClient` wraps it over HTTP; tests
/// drive it straight into `CoordinatorState::answer_query`.
pub struct QuerySession {
    metadata: MemoSnapshotMetadata,
    ypir: YpirSchemeParams,
    client: IPIRClient,
    setup: Vec<Vec<u64>>,
    published_c1: Vec<Vec<u64>>,
    epoch: [u8; 8],
}

/// A query body bound to the randomness needed to decode its response. Single
/// use: `decode` consumes it.
pub struct PreparedQuery {
    row: usize,
    body: Vec<u8>,
    seed: ipir_sp::IPIRSeed,
}

impl PreparedQuery {
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn row(&self) -> usize {
        self.row
    }
}

impl QuerySession {
    /// Validates the advertised snapshot against this protocol's pinned geometry,
    /// seed, parameter generator, and public-parameter digest, exactly as
    /// `MemoPirClient::connect` does after fetching the three documents.
    pub fn new(
        metadata: MemoSnapshotMetadata,
        ypir: YpirSchemeParams,
        public_params: &[u8],
    ) -> Result<Self, ClientError> {
        if metadata.schema_version != SCHEMA_VERSION
            || metadata.network != NETWORK
            || metadata.pool != POOL
        {
            return Err(ClientError::Metadata("wrong network or pool".to_string()));
        }
        if metadata.setup_seed != MEMO_SETUP_SEED {
            return Err(ClientError::Metadata(
                "snapshot setup seed does not match the memo-PIR protocol".to_string(),
            ));
        }
        if metadata.record_bytes as usize != RECORD_BYTES
            || metadata.records_per_row as usize != RECORDS_PER_ROW
            || metadata.row_bytes as usize != ROW_BYTES
            || metadata.shard_rows as usize != SHARD_ROWS
            || metadata.logical_rows < metadata.used_rows
            || !metadata.logical_rows.is_power_of_two()
            || metadata.logical_rows < SHARD_ROWS as u64
            || metadata.used_rows != metadata.ironwood_tree_size.div_ceil(RECORDS_PER_ROW as u64)
        {
            return Err(ClientError::Metadata(
                "invalid database geometry".to_string(),
            ));
        }
        let (rlwe, expected) = ipir_sp::params_for_simplepir(metadata.logical_rows, ITEM_SIZE_BITS)
            .map_err(|e| ClientError::Pir(e.to_string()))?;
        if ypir != expected {
            return Err(ClientError::Metadata(
                "server parameters do not match the pinned generator".to_string(),
            ));
        }
        let digest = Sha256::digest(public_params);
        if hex::encode(digest) != metadata.public_params_sha256 {
            return Err(ClientError::Metadata(
                "public parameter digest mismatch".to_string(),
            ));
        }
        let mut epoch = [0; 8];
        epoch.copy_from_slice(&digest[..8]);
        if hex::encode(epoch) != metadata.public_params_epoch {
            return Err(ClientError::Metadata(
                "public parameter epoch mismatch".to_string(),
            ));
        }
        let blocks = ypir.db_cols / rlwe.d;
        let expected_len = blocks * published_c1_len(rlwe.d, rlwe.q);
        if public_params.len() != expected_len {
            return Err(ClientError::Metadata(format!(
                "public parameters have {} bytes, expected {expected_len}",
                public_params.len()
            )));
        }
        let published_c1 = recover_published_c1(public_params, rlwe.d, blocks, rlwe.q);
        let client = IPIRClient::new(&rlwe, &ypir);
        let setup = client.generate_public_query_setup_simplepir_from_seed(memo_setup_seed_bytes());
        Ok(Self {
            metadata,
            ypir,
            client,
            setup,
            published_c1,
            epoch,
        })
    }

    pub fn metadata(&self) -> &MemoSnapshotMetadata {
        &self.metadata
    }

    pub fn params(&self) -> &YpirSchemeParams {
        &self.ypir
    }

    /// The seeded public setup polynomials, shared with every server shard.
    pub fn setup(&self) -> &[Vec<u64>] {
        &self.setup
    }

    pub fn published_c1(&self) -> &[Vec<u64>] {
        &self.published_c1
    }

    /// Builds the query for the row holding `position`, returning the slot too.
    pub fn prepare_position(&self, position: u64) -> Result<(PreparedQuery, usize), ClientError> {
        let (row, slot) = self
            .metadata
            .local_row_for_position(position)
            .ok_or(ClientError::OutsideCoverage(position))?;
        Ok((self.prepare_row(row)?, slot))
    }

    /// A cover query at a uniformly random logical row.
    pub fn prepare_dummy(&self) -> Result<PreparedQuery, ClientError> {
        self.prepare_row(OsRng.gen_range(0..self.ypir.db_rows))
    }

    pub fn prepare_row(&self, row: usize) -> Result<PreparedQuery, ClientError> {
        if row >= self.ypir.db_rows {
            return Err(ClientError::OutsideCoverage(row as u64));
        }
        let (query, packing_keys, seed) =
            self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut body = self.metadata.generation.to_le_bytes().to_vec();
        body.extend(
            serialize_packing_keys(self.client.rlwe_params(), &packing_keys)
                .map_err(|e| ClientError::Pir(e.to_string()))?,
        );
        body.extend(query.to_switched_bytes(self.client.rlwe_params().q, self.ypir.query_bits));
        Ok(PreparedQuery { row, body, seed })
    }

    /// Validates the response framing and returns exactly one decoded row.
    pub fn decode(&self, query: PreparedQuery, response: &[u8]) -> Result<Vec<u8>, ClientError> {
        let generation = response
            .get(..8)
            .ok_or_else(|| ClientError::Response("missing generation".to_string()))?;
        if generation != self.metadata.generation.to_le_bytes() {
            return Err(ClientError::Response("generation mismatch".to_string()));
        }
        let epoch = response
            .get(8..16)
            .ok_or_else(|| ClientError::Response("missing public parameter epoch".to_string()))?;
        if epoch != self.epoch {
            return Err(ClientError::Response(
                "public parameter epoch mismatch".to_string(),
            ));
        }
        let expected_body_len = (self.ypir.db_cols / self.client.rlwe_params().d)
            * response_body_len(self.client.rlwe_params().d, self.ypir.q_prime_1);
        if response.len() != 16 + expected_body_len {
            return Err(ClientError::Response(format!(
                "response has {} bytes, expected {}",
                response.len(),
                16 + expected_body_len
            )));
        }
        let decoded =
            self.client
                .decode_response_simplepir(query.seed, &self.published_c1, &response[16..]);
        if decoded.len() < ROW_BYTES {
            return Err(ClientError::Response(format!(
                "decoded row has {} bytes, expected at least {ROW_BYTES}",
                decoded.len()
            )));
        }
        Ok(decoded[..ROW_BYTES].to_vec())
    }
}

/// Extracts the record at `slot` from a decoded row.
pub fn record_in_row(row: &[u8], slot: usize) -> ActionRecord {
    let start = slot * RECORD_BYTES;
    let bytes: [u8; RECORD_BYTES] = row[start..start + RECORD_BYTES]
        .try_into()
        .expect("validated memo row bounds");
    ActionRecord(bytes)
}

pub struct MemoPirClient {
    http: reqwest::Client,
    base_url: String,
    session: QuerySession,
}

impl MemoPirClient {
    pub async fn connect(base_url: &str) -> Result<Self, ClientError> {
        let base_url = base_url.trim_end_matches('/').to_string();
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        let metadata: MemoSnapshotMetadata = serde_json::from_slice(
            &read_limited(
                http.get(format!("{base_url}/memo/metadata")).send().await?,
                1024 * 1024,
            )
            .await?,
        )?;
        let ypir: YpirSchemeParams = serde_json::from_slice(
            &read_limited(
                http.get(format!("{base_url}/memo/params")).send().await?,
                64 * 1024,
            )
            .await?,
        )?;
        let public_params = read_limited(
            http.get(format!("{base_url}/memo/public-params"))
                .send()
                .await?,
            16 * 1024 * 1024,
        )
        .await?;
        let session = QuerySession::new(metadata, ypir, &public_params)?;
        Ok(Self {
            http,
            base_url,
            session,
        })
    }

    pub fn metadata(&self) -> &MemoSnapshotMetadata {
        self.session.metadata()
    }

    pub async fn query_position(&self, position: u64) -> Result<ActionRecord, ClientError> {
        let (query, slot) = self.session.prepare_position(position)?;
        let row = self.send(query).await?;
        Ok(record_in_row(&row, slot))
    }

    pub async fn query_dummy(&self) -> Result<(), ClientError> {
        self.send(self.session.prepare_dummy()?).await.map(|_| ())
    }

    async fn send(&self, query: PreparedQuery) -> Result<Vec<u8>, ClientError> {
        let response = self
            .http
            .post(format!("{}/memo/query", self.base_url))
            .body(query.body().to_vec())
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(ClientError::Response(format!(
                "server returned {}",
                response.status()
            )));
        }
        let response = read_limited(response, 16 * 1024 * 1024).await?;
        self.session.decode(query, &response)
    }
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, ClientError> {
    let mut response = response.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ClientError::Response("HTTP body exceeds limit".to_string()));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ClientError::Response("HTTP body exceeds limit".to_string()));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
