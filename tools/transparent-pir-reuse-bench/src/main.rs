//! Real row-file PIR with bounded four-slot reuse. No network or secret persistence.
use inspiring::TopKeyImages;
use ipir_sp::bits::{read_bits, write_bits};
use ipir_sp::client::reusable::QueryPool;
use ipir_sp::modulus_switch::recover_published_c1;
use ipir_sp::serialize::{
    deserialize_packing_keys, serialize_packing_keys, serialized_packing_keys_len,
};
use ipir_sp::server::{build_pack_preprocessed_blocks, published_c1_rows};
use ipir_sp::{params_for_simplepir, IPIRClient, IPIRServer};
use serde_json::json;
use spiral_rs::poly::PolyMatrix;
use std::{env, fs, time::Instant};

fn rss() -> u64 {
    let mut system = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().expect("pid");
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).expect("process").memory()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 6 {
        return Err("usage: ROW_FILE ROW_BYTES ROWS QUERY_ROWS_CSV ITERATIONS".into());
    }
    let width: usize = args[2].parse()?;
    let rows: usize = args[3].parse()?;
    let queries: Vec<usize> = args[4]
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    let iterations: usize = args[5].parse()?;
    if ![3584, 7168, 10752, 17920].contains(&width)
        || !(2048..=131072).contains(&rows)
        || !rows.is_multiple_of(2048)
        || queries.is_empty()
        || queries.len() > 1000
        || iterations != queries.len()
        || queries.iter().any(|&r| r >= rows)
    {
        return Err("invalid bounded row benchmark".into());
    }
    let source = fs::read(&args[1])?;
    if source.len() != rows * width {
        return Err("source geometry mismatch".into());
    }
    let policy = env::var("PIR_REUSE_MODE").unwrap_or_else(|_| "auto".into());
    let (r, y) = params_for_simplepir(rows as u64, (width * 8) as u64)?;
    let cached_mask: u32 = env::var("PIR_CACHED_PUBLIC_MASK")
        .unwrap_or_else(|_| "0".into())
        .parse()?;
    if cached_mask > 15 {
        return Err("invalid public cache mask".into());
    }
    let key_saving = (queries.len() - queries.len().div_ceil(4)) * serialized_packing_keys_len(&r);
    let extra_sets = 4 - cached_mask.count_ones() as usize - usize::from(cached_mask & 1 == 0);
    // Supported row widths encode 14 plaintext bits per 56-bit public coefficient.
    let extra_public_bytes = extra_sets * width * 4;
    let reuse = match policy.as_str() {
        "auto" => key_saving > extra_public_bytes,
        "fresh" => false,
        "reuse" => true,
        _ => return Err("invalid reuse policy".into()),
    };
    let retry = env::var("PIR_RETRY_FIRST").as_deref() == Ok("1");
    let sets = if reuse { 4 } else { 1 };
    let start = Instant::now();
    let pool = QueryPool::new(IPIRClient::new(&r, &y), [0x72; 32], 4)?;
    let public_matrix_generation_ms = start.elapsed().as_secs_f64() * 1000.;
    let start = Instant::now();
    let values: Vec<u16> = (0..rows)
        .flat_map(|row| {
            let bytes = &source[row * width..(row + 1) * width];
            (0..y.db_cols).map(move |col| read_bits(bytes, col * 14, 14) as u16)
        })
        .collect();
    let server = IPIRServer::new_auto_kernel(y.clone(), values.iter().copied(), false, true);
    let database_build_ms = start.elapsed().as_secs_f64() * 1000.;
    let top = TopKeyImages::build(&r);
    let mut pre = Vec::new();
    let mut c1 = Vec::new();
    let mut prep_ms = Vec::new();
    let mut cache_bytes = 0;
    let mut public_bytes = 0;
    for set in pool.sets().iter().take(sets) {
        let start = Instant::now();
        let offline = server.perform_offline_precomputation_simplepir(&r, set);
        let prepared = build_pack_preprocessed_blocks(&r, &offline.crs_blocks)?;
        let bytes = published_c1_rows(&prepared, r.q);
        if bytes.len() != width * 4 {
            return Err("public byte model no longer matches encoding".into());
        }
        public_bytes += bytes.len();
        cache_bytes += prepared
            .iter()
            .map(|p| {
                p.collapse_a_final_ntt.as_slice().len() * 8
                    + p.digits_ntt
                        .iter()
                        .map(|d| d.as_slice().len() * 8)
                        .sum::<usize>()
            })
            .sum::<usize>();
        c1.push(recover_published_c1(&bytes, r.d, y.db_cols / r.d, r.q));
        pre.push(prepared);
        prep_ms.push(start.elapsed().as_secs_f64() * 1000.);
    }
    let ready_rss = rss();
    eprintln!(
        "{}: {rows} rows x {} columns, {sets} sets",
        if reuse { "reuse" } else { "fresh" },
        y.db_cols
    );
    let mut samples = Vec::new();
    let mut decoded_rows = Vec::new();
    let mut batches = 0;
    let mut key_uploads = 0;
    let mut max_error = 0;
    for chunk in queries.chunks(if reuse { 4 } else { 1 }) {
        let start = Instant::now();
        let mut batch = if reuse {
            Some(pool.start_batch())
        } else {
            None
        };
        let shared_bytes = batch
            .as_ref()
            .map(|b| serialize_packing_keys(&r, b.keys()))
            .transpose()?;
        let key_generation_ms = start.elapsed().as_secs_f64() * 1000.;
        let start = Instant::now();
        let shared_keys = shared_bytes
            .as_ref()
            .map(|bytes| deserialize_packing_keys(&r, bytes))
            .transpose()?;
        let shared_parse_ms = start.elapsed().as_secs_f64() * 1000.;
        for (position, &row) in chunk.iter().enumerate() {
            let start = Instant::now();
            let mut seed = None;
            let mut fresh_bytes = None;
            let (slot, query_bytes) = if let Some(b) = batch.as_mut() {
                let q = b.next_query(row)?;
                (q.slot(), q.bytes().to_vec())
            } else {
                let (q, k, s) = pool
                    .client()
                    .generate_fresh_query_simplepir(&pool.sets()[0], row);
                seed = Some(s);
                fresh_bytes = Some(serialize_packing_keys(&r, &k)?);
                (0, q.to_switched_bytes(r.q, y.query_bits))
            };
            if reuse && slot != position {
                return Err("slot allocation mismatch".into());
            }
            let prepare_ms = start.elapsed().as_secs_f64() * 1000.
                + if position == 0 { key_generation_ms } else { 0. };
            let start = Instant::now();
            let fresh_keys = fresh_bytes
                .as_ref()
                .map(|b| deserialize_packing_keys(&r, b))
                .transpose()?;
            let parse_ms = start.elapsed().as_secs_f64() * 1000.
                + if position == 0 { shared_parse_ms } else { 0. };
            let keys = shared_keys
                .as_ref()
                .or(fresh_keys.as_ref())
                .ok_or("missing keys")?;
            let key_bytes = if reuse {
                if position == 0 {
                    shared_bytes.as_ref().unwrap().len()
                } else {
                    0
                }
            } else {
                fresh_bytes.as_ref().unwrap().len()
            };
            key_uploads += usize::from(key_bytes > 0);
            let attempts = if retry && batches == 0 && position == 0 {
                2
            } else {
                1
            };
            let immutable = query_bytes.clone();
            for attempt in 0..attempts {
                // A retry sends the exact prior bytes, without allocating a new slot.
                assert_eq!(immutable, query_bytes);
                let start = Instant::now();
                let (response, _) = server.perform_full_online_computation_simplepir_measured(
                    &r,
                    &query_bytes,
                    keys,
                    &top,
                    &pre[slot],
                )?;
                let server_ms = start.elapsed().as_secs_f64() * 1000.
                    + if attempt == 0 { parse_ms } else { 0. };
                let start = Instant::now();
                let (decoded, error) = if let Some(b) = &batch {
                    b.decode_with_margin(&c1[slot], &response)
                } else {
                    pool.client().decode_response_simplepir_with_margin(
                        seed.unwrap(),
                        &c1[slot],
                        &response,
                    )
                };
                max_error = max_error.max(error);
                if error >= r.delta / 8
                    || decoded.len() != y.db_cols
                    || !decoded
                        .iter()
                        .zip(&values[row * y.db_cols..(row + 1) * y.db_cols])
                        .all(|(&a, &b)| a == u64::from(b))
                {
                    return Err("decoded row or error margin mismatch".into());
                }
                let decode_ms = start.elapsed().as_secs_f64() * 1000.;
                if attempt + 1 == attempts {
                    let mut bytes = vec![0u8; width];
                    for (col, &v) in decoded.iter().enumerate() {
                        write_bits(&mut bytes, v, col * 14, 14);
                    }
                    decoded_rows.push(json!({"row":row,"bytes":bytes}));
                }
                let generation_ms = if attempt == 0 { prepare_ms } else { 0. };
                samples.push(json!({"row":row,"batch":batches,"slot":slot,"retry":attempt>0,
                    "prepare_ms":generation_ms,"server_ms":server_ms,"decode_ms":decode_ms,
                    "total_ms":generation_ms+server_ms+decode_ms,"query_body_bytes":query_bytes.len(),
                    "key_upload_bytes":if attempt==0{key_bytes}else{0},
                    "upload_bytes":query_bytes.len()+if attempt==0{key_bytes}else{0},"response_bytes":response.len()}));
            }
        }
        if reuse && chunk.len() == 4 && batch.as_mut().unwrap().next_query(chunk[0]).is_ok() {
            return Err("batch did not exhaust".into());
        }
        // Partial batches are dropped permanently; the next process starts fresh.
        batches += 1;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"classification":"row-file four-slot experiment; local IPC, no network",
        "mode":if reuse{"reuse4"}else{"fresh"},"policy":policy,
        "cached_public_mask":cached_mask,"predicted_key_saving_bytes":key_saving,"extra_public_bytes_for_reuse":extra_public_bytes,"rows":rows,"columns":y.db_cols,"row_bytes":width,
        "public_sets":sets,"batches":batches,"key_uploads":key_uploads,"unique_queries":queries.len(),
        "unused_final_batch_slots":if reuse{(4-queries.len()%4)%4}else{0},
        "published_setup_bytes":public_bytes,"public_c1_bytes_per_set":public_bytes/sets,
        "packing_cache_payload_bytes":cache_bytes,"combined_client_server_ready_rss_bytes":ready_rss,
        "combined_client_server_end_rss_bytes":rss(),"database_build_ms":database_build_ms,
        "public_matrix_generation_ms":public_matrix_generation_ms,"preparation_ms_per_set":prep_ms,
        "max_decryption_error":max_error,"decryption_threshold":r.delta/2,
        "samples":samples,"decoded_rows":decoded_rows})
        )?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_lock_preserves_the_demo_public_setup_vector() {
        let (r, y) = params_for_simplepir(2048, 2048 * 14).unwrap();
        let pool = QueryPool::new(IPIRClient::new(&r, &y), [7; 32], 4).unwrap();
        assert_eq!(
            &pool.sets()[0][0][..4],
            &[
                9_164_527_206_802_959,
                5_084_643_010_587_079,
                51_932_877_172_136_113,
                33_393_100_479_081_743,
            ]
        );
    }
}
