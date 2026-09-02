use crate::types::{ITEM_SIZE_BITS, ROW_BYTES, SHARD_ROWS};
use crate::wire::{decode_crs_blocks, encode_crs_blocks};
use inspiring::{InspiringError, RlweParams};
use ipir_sp::server::{CrsBlock, IPIRServer};
use ipir_sp::YpirSchemeParams;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

// Version 2 binds cached shard preprocessing to the domain-separated memo setup seed.
const ARTIFACT_VERSION: u16 = 2;

#[derive(Serialize, Deserialize)]
struct ArtifactMetadata {
    version: u16,
    rlwe_degree: usize,
    rlwe_modulus: u64,
    db_rows: usize,
    db_cols: usize,
    plaintext_modulus: u64,
    shard_id: u64,
    query_row_start: usize,
    rows_sha256: String,
    database_sha256: String,
    crs_sha256: String,
}

pub struct ShardRuntime {
    pub shard_id: u64,
    pub query_row_start: usize,
    pub rows_sha256: String,
    pub server: IPIRServer<u16>,
    pub crs_blocks: Vec<CrsBlock>,
}

impl ShardRuntime {
    pub fn load_cached(
        artifact_root: &Path,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
    ) -> Result<Self, String> {
        Self::load(
            &artifact_root.join(format!("shard-{shard_id:08}")),
            shard_id,
            query_row_start,
            rows_sha256,
            rlwe,
        )
    }

    pub fn load_or_build(
        artifact_root: &Path,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
    ) -> Result<(Self, bool), String> {
        let directory = artifact_root.join(format!("shard-{shard_id:08}"));
        if let Ok(runtime) = Self::load(&directory, shard_id, query_row_start, &rows_sha256, rlwe) {
            return Ok((runtime, false));
        }
        let runtime = Self::build(
            shard_id,
            query_row_start,
            rows_sha256,
            rows,
            rlwe,
            global_setup,
        )
        .map_err(|error| error.to_string())?;
        runtime
            .persist(&directory, rlwe)
            .map_err(|error| error.to_string())?;
        Ok((runtime, true))
    }

    pub fn build(
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
    ) -> Result<Self, InspiringError> {
        if rows.len() != SHARD_ROWS * ROW_BYTES {
            return Err(InspiringError::PreprocessMismatch(format!(
                "memo shard must be {} bytes, got {}",
                SHARD_ROWS * ROW_BYTES,
                rows.len()
            )));
        }
        if !query_row_start.is_multiple_of(rlwe.d) {
            return Err(InspiringError::PreprocessMismatch(
                "shard query row is not polynomial aligned".to_string(),
            ));
        }

        let (_, local_params) = ipir_sp::params_for_simplepir(SHARD_ROWS as u64, ITEM_SIZE_BITS)?;
        let coefficients = RowPlaintextIter::new(
            rows,
            ROW_BYTES,
            local_params.db_rows,
            local_params.db_cols,
            local_params.p.trailing_zeros() as usize,
        );
        let server = IPIRServer::<u16>::new_auto_kernel(local_params, coefficients, false, true);
        let first_poly = query_row_start / rlwe.d;
        let poly_count = SHARD_ROWS / rlwe.d;
        let setup = global_setup
            .get(first_poly..first_poly + poly_count)
            .ok_or_else(|| {
                InspiringError::PreprocessMismatch("global setup does not cover shard".to_string())
            })?;
        let crs_blocks = server
            .perform_offline_precomputation_simplepir(rlwe, setup)
            .crs_blocks;

        Ok(Self {
            shard_id,
            query_row_start,
            rows_sha256,
            server,
            crs_blocks,
        })
    }

    pub fn evaluate(&self, rlwe: &RlweParams, query: &[u64]) -> Result<Vec<u64>, InspiringError> {
        if query.len() != SHARD_ROWS {
            return Err(InspiringError::LweShape(format!(
                "shard query must contain {SHARD_ROWS} coefficients, got {}",
                query.len()
            )));
        }
        if query.iter().any(|coefficient| *coefficient >= rlwe.q) {
            return Err(InspiringError::PreprocessMismatch(
                "query coefficient is not reduced modulo q".to_string(),
            ));
        }
        Ok(self.server.multiply_query(rlwe, query))
    }

    fn load(
        directory: &Path,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
    ) -> Result<Self, String> {
        let metadata: ArtifactMetadata = serde_json::from_slice(
            &fs::read(directory.join("metadata.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if metadata.version != ARTIFACT_VERSION
            || metadata.rlwe_degree != rlwe.d
            || metadata.rlwe_modulus != rlwe.q
            || metadata.shard_id != shard_id
            || metadata.query_row_start != query_row_start
            || metadata.rows_sha256 != rows_sha256
        {
            return Err("artifact metadata mismatch".to_string());
        }
        let (_, local_params) = ipir_sp::params_for_simplepir(SHARD_ROWS as u64, ITEM_SIZE_BITS)
            .map_err(|e| e.to_string())?;
        if metadata.db_rows != local_params.db_rows
            || metadata.db_cols != local_params.db_cols
            || metadata.plaintext_modulus != local_params.p
        {
            return Err("persisted artifact parameter mismatch".to_string());
        }
        let raw_db = fs::read(directory.join("database.u16le")).map_err(|e| e.to_string())?;
        if hex::encode(Sha256::digest(&raw_db)) != metadata.database_sha256 {
            return Err("persisted database digest mismatch".to_string());
        }
        let expected_db_bytes = local_params.db_rows * local_params.db_cols * 2;
        if raw_db.len() != expected_db_bytes {
            return Err("persisted database has the wrong size".to_string());
        }
        let coefficients: Vec<u16> = raw_db
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        let server = IPIRServer::<u16>::new_auto_kernel(
            local_params.clone(),
            coefficients.into_iter(),
            true,
            true,
        );
        let hint = fs::read(directory.join("partial-crs.bin")).map_err(|e| e.to_string())?;
        if hex::encode(Sha256::digest(&hint)) != metadata.crs_sha256 {
            return Err("persisted CRS digest mismatch".to_string());
        }
        let crs_blocks = decode_crs_blocks(&hint, local_params.db_cols / rlwe.d, rlwe.d)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            shard_id,
            query_row_start,
            rows_sha256: rows_sha256.to_string(),
            server,
            crs_blocks,
        })
    }

    fn persist(&self, directory: &Path, rlwe: &RlweParams) -> Result<(), std::io::Error> {
        fs::create_dir_all(directory)?;
        let mut database = Vec::with_capacity(self.server.db().len() * 2);
        for coefficient in self.server.db() {
            database.extend_from_slice(&coefficient.to_le_bytes());
        }
        let hint = encode_crs_blocks(&self.crs_blocks);
        write_atomic(directory, "database.u16le", &database)?;
        write_atomic(directory, "partial-crs.bin", &hint)?;
        let metadata = ArtifactMetadata {
            version: ARTIFACT_VERSION,
            rlwe_degree: rlwe.d,
            rlwe_modulus: rlwe.q,
            db_rows: self.server.params().db_rows,
            db_cols: self.server.params().db_cols,
            plaintext_modulus: self.server.params().p,
            shard_id: self.shard_id,
            query_row_start: self.query_row_start,
            rows_sha256: self.rows_sha256.clone(),
            database_sha256: hex::encode(Sha256::digest(&database)),
            crs_sha256: hex::encode(Sha256::digest(&hint)),
        };
        let metadata = serde_json::to_vec_pretty(&metadata).map_err(std::io::Error::other)?;
        write_atomic(directory, "metadata.json", &metadata)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

fn write_atomic(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), std::io::Error> {
    let path = directory.join(name);
    let temporary = directory.join(format!("{name}.tmp"));
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)
}

pub fn global_parameters(
    logical_rows: u64,
) -> Result<(RlweParams, YpirSchemeParams), InspiringError> {
    ipir_sp::params_for_simplepir(logical_rows, ITEM_SIZE_BITS)
}

pub struct RowPlaintextIter<'a> {
    data: &'a [u8],
    row_bytes: usize,
    db_cols: usize,
    plaintext_bits: usize,
    position: usize,
    total: usize,
}

impl<'a> RowPlaintextIter<'a> {
    pub fn new(
        data: &'a [u8],
        row_bytes: usize,
        db_rows: usize,
        db_cols: usize,
        plaintext_bits: usize,
    ) -> Self {
        Self {
            data,
            row_bytes,
            db_cols,
            plaintext_bits,
            position: 0,
            total: db_rows * db_cols,
        }
    }
}

impl Iterator for RowPlaintextIter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.total {
            return None;
        }
        let row = self.position / self.db_cols;
        let column = self.position % self.db_cols;
        self.position += 1;
        let start = row * self.row_bytes;
        let end = start.saturating_add(self.row_bytes).min(self.data.len());
        let bytes = self.data.get(start..end).unwrap_or_default();
        Some(
            ipir_sp::bits::read_bits(bytes, column * self.plaintext_bits, self.plaintext_bits)
                as u16,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memo_rows_use_two_ipir_instances() {
        let (rlwe, params) = global_parameters(SHARD_ROWS as u64).expect("params");
        assert_eq!(rlwe.d, 2_048);
        assert_eq!(params.instances, 2);
        assert_eq!(params.db_cols, 4_096);
    }
}
