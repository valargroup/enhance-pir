use crate::types::{DatabaseId, DatabaseLayout};
use crate::wire::{decode_crs_blocks, encode_crs_blocks};
use inspiring::{InspiringError, RlweParams};
use ipir_sp::server::{CrsBlock, IPIRServer};
use ipir_sp::YpirSchemeParams;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

// Version 2 bound cached shard preprocessing to the domain-separated memo setup seed.
// Version 3 moved to 792-byte action records (6,336-byte rows).
// Version 4 names the table: artifacts live under `{table}/shard-{id}` and
// record which table they belong to, so several tables share one worker.
const ARTIFACT_VERSION: u16 = 4;

#[derive(Serialize, Deserialize)]
struct ArtifactMetadata {
    version: u16,
    table: String,
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
    /// Monotonic load order, so a worker can pick the newest runtime of a shard.
    pub prepared_at: u64,
}

fn next_prepared_at() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Directory holding one shard's artifacts, namespaced by table so several
/// tables can share a worker's artifact root.
pub fn shard_artifact_dir(artifact_root: &Path, table: DatabaseId, shard_id: u64) -> PathBuf {
    artifact_root
        .join(table.as_str())
        .join(format!("shard-{shard_id:08}"))
}

impl ShardRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn load_cached(
        artifact_root: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
    ) -> Result<Self, String> {
        Self::load(
            &shard_artifact_dir(artifact_root, table, shard_id),
            table,
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rlwe,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn load_or_build(
        artifact_root: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
    ) -> Result<(Self, bool), String> {
        let directory = shard_artifact_dir(artifact_root, table, shard_id);
        if let Ok(runtime) = Self::load(
            &directory,
            table,
            layout,
            shard_id,
            query_row_start,
            &rows_sha256,
            rlwe,
        ) {
            return Ok((runtime, false));
        }
        let runtime = Self::build(
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rows,
            rlwe,
            global_setup,
        )
        .map_err(|error| error.to_string())?;
        runtime
            .persist(&directory, table, rlwe)
            .map_err(|error| error.to_string())?;
        Ok((runtime, true))
    }

    pub fn build(
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
    ) -> Result<Self, InspiringError> {
        if rows.len() != layout.shard_bytes() {
            return Err(InspiringError::PreprocessMismatch(format!(
                "shard must be {} bytes, got {}",
                layout.shard_bytes(),
                rows.len()
            )));
        }
        if !query_row_start.is_multiple_of(rlwe.d) {
            return Err(InspiringError::PreprocessMismatch(
                "shard query row is not polynomial aligned".to_string(),
            ));
        }

        let (_, local_params) = shard_parameters(layout)?;
        let coefficients = RowPlaintextIter::new(
            rows,
            layout.row_bytes(),
            local_params.db_rows,
            local_params.db_cols,
            local_params.p.trailing_zeros() as usize,
        );
        let server = IPIRServer::<u16>::new_auto_kernel(local_params, coefficients, false, true);
        let first_poly = query_row_start / rlwe.d;
        let poly_count = layout.shard_rows / rlwe.d;
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
            prepared_at: next_prepared_at(),
        })
    }

    pub fn evaluate(&self, rlwe: &RlweParams, query: &[u64]) -> Result<Vec<u64>, InspiringError> {
        let shard_rows = self.server.params().db_rows;
        if query.len() != shard_rows {
            return Err(InspiringError::LweShape(format!(
                "shard query must contain {shard_rows} coefficients, got {}",
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

    #[allow(clippy::too_many_arguments)]
    fn load(
        directory: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
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
            || metadata.table != table.as_str()
            || metadata.rlwe_degree != rlwe.d
            || metadata.rlwe_modulus != rlwe.q
            || metadata.shard_id != shard_id
            || metadata.query_row_start != query_row_start
            || metadata.rows_sha256 != rows_sha256
        {
            return Err("artifact metadata mismatch".to_string());
        }
        let (_, local_params) = shard_parameters(layout).map_err(|e| e.to_string())?;
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
            prepared_at: next_prepared_at(),
        })
    }

    fn persist(
        &self,
        directory: &Path,
        table: DatabaseId,
        rlwe: &RlweParams,
    ) -> Result<(), std::io::Error> {
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
            table: table.as_str().to_string(),
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

/// iPIR parameters for the global (coordinator-facing) database of a table
/// with `logical_rows` rows.
pub fn global_parameters(
    logical_rows: u64,
    layout: &DatabaseLayout,
) -> Result<(RlweParams, YpirSchemeParams), InspiringError> {
    ipir_sp::params_for_simplepir(logical_rows, layout.item_size_bits())
}

/// iPIR parameters for one shard of a table. Row sharding is sound because the
/// column count derives from the row size, not the row count, so shard and
/// global intermediates have the same width.
pub fn shard_parameters(
    layout: &DatabaseLayout,
) -> Result<(RlweParams, YpirSchemeParams), InspiringError> {
    ipir_sp::params_for_simplepir(layout.shard_rows as u64, layout.item_size_bits())
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

    /// Every served layout, with the instance count its rows need. One instance
    /// carries d * log2(p) = 28,672 plaintext bits.
    const LAYOUTS: &[(&str, DatabaseLayout, usize)] = &[
        ("action", crate::types::ACTION_LAYOUT, 2),
        ("witness", crate::types::WITNESS_LAYOUT, 3),
        ("nullifier", crate::types::NULLIFIER_LAYOUT, 2),
    ];

    #[test]
    fn every_layout_has_the_expected_instance_count() {
        for (name, layout, instances) in LAYOUTS {
            let (rlwe, shard) = shard_parameters(layout).expect("shard params");
            let (global_rlwe, global) = global_parameters(
                layout.logical_rows_for(layout.shard_rows as u64 * 4),
                layout,
            )
            .expect("global params");
            assert_eq!(rlwe.d, 2_048, "{name}");
            assert_eq!(shard.p, 1 << 14, "{name}");
            assert_eq!(shard.instances, *instances, "{name}");
            assert_eq!(shard.db_cols, instances * rlwe.d, "{name}");
            // Shard and global parameters must agree on everything but row count,
            // or partials from shards could not be summed into the global answer.
            assert_eq!((global_rlwe.d, global_rlwe.q), (rlwe.d, rlwe.q), "{name}");
            assert_eq!(global.db_cols, shard.db_cols, "{name}");
            assert!(layout.shard_rows.is_multiple_of(rlwe.d), "{name}");
        }
    }

    #[test]
    fn action_rows_use_two_ipir_instances() {
        // A 6,592-byte row is 52,736 bits, which still fits two instances of
        // 28,672, so the response size is unchanged from the 612-byte memo layout.
        assert_eq!(crate::types::ACTION_LAYOUT.row_bytes(), 6_592);
        let (_, params) = shard_parameters(&crate::types::ACTION_LAYOUT).expect("params");
        assert_eq!(params.instances, 2);
        assert_eq!(params.db_cols, 4_096);
    }
}
