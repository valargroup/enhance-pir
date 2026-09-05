//! Research-only serialized IPIR round trips over directory/history row files.
use inspiring::TopKeyImages;
use ipir_sp::bits::read_bits;
use ipir_sp::modulus_switch::recover_published_c1;
use ipir_sp::serialize::{deserialize_packing_keys, serialize_packing_keys};
use ipir_sp::server::{build_pack_preprocessed_blocks, published_c1_rows, IPIRServer};
use ipir_sp::{params_for_simplepir, IPIRClient};
use serde_json::json;
use std::{env, fs, time::Instant};

fn rss() -> u64 {
    let mut system = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().expect("current pid");
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).expect("current process").memory()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        return Err("usage: transparent-pir-bench ROW_FILE ROW_BYTES LOGICAL_ROWS QUERY_ROWS_CSV ITERATIONS".into());
    }
    let source = fs::read(&args[1])?;
    let row_bytes: usize = args[2].parse()?;
    let rows: usize = args[3].parse()?;
    let queries: Vec<usize> = args[4]
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    let iterations: usize = args[5].parse()?;
    if !(3584..=21504).contains(&row_bytes)
        || !(2048..=131072).contains(&rows)
        || source.len() % row_bytes != 0
        || source.len() / row_bytes > rows
        || queries.is_empty()
        || queries.iter().any(|&r| r >= rows)
        || !(1..=10000).contains(&iterations)
    {
        return Err("invalid bounded benchmark inputs".into());
    }
    let (rlwe, params) = params_for_simplepir(rows as u64, (row_bytes * 8) as u64)?;
    let baseline_rss = rss();
    let started = Instant::now();
    let client = IPIRClient::new(&rlwe, &params);
    // Public deterministic CRS seed, not a secret or query randomness.
    let setup = client.generate_public_query_setup_simplepir_from_seed([0x71; 32]);
    let client_setup_ms = started.elapsed().as_secs_f64() * 1000.0;
    let client_setup_rss = rss();

    let started = Instant::now();
    let values: Vec<u16> = (0..params.db_rows)
        .flat_map(|row| {
            let bytes = source
                .get(row * row_bytes..(row + 1) * row_bytes)
                .unwrap_or(&[]);
            (0..params.db_cols).map(move |column| read_bits(bytes, column * 14, 14) as u16)
        })
        .collect();
    let encoding_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    let server = IPIRServer::new_auto_kernel(params.clone(), values.iter().copied(), false, true);
    let offline = server.perform_offline_precomputation_simplepir(&rlwe, &setup);
    let preprocessed = build_pack_preprocessed_blocks(&rlwe, &offline.crs_blocks)?;
    let top_keys = TopKeyImages::build(&rlwe);
    let published = published_c1_rows(&preprocessed, rlwe.q);
    let published_c1 = recover_published_c1(&published, rlwe.d, params.db_cols / rlwe.d, rlwe.q);
    let server_setup_ms = started.elapsed().as_secs_f64() * 1000.0;
    let ready_rss = rss();
    eprintln!(
        "ready: {} rows x {} columns",
        params.db_rows, params.db_cols
    );
    let mut samples = Vec::new();
    let mut decoded_rows = Vec::new();
    for iteration in 0..iterations {
        let row = queries[iteration % queries.len()];
        let begin = Instant::now();
        let (query, keys, seed) = client.generate_fresh_query_simplepir(&setup, row);
        let key_bytes = serialize_packing_keys(&rlwe, &keys)?;
        let query_bytes = query.to_switched_bytes(rlwe.q, params.query_bits);
        let prepare_ms = begin.elapsed().as_secs_f64() * 1000.0;
        let server_start = Instant::now();
        let received_keys = deserialize_packing_keys(&rlwe, &key_bytes)?;
        let (response, _) = server.perform_full_online_computation_simplepir_measured(
            &rlwe,
            &query_bytes,
            &received_keys,
            &top_keys,
            &preprocessed,
        )?;
        let server_ms = server_start.elapsed().as_secs_f64() * 1000.0;
        let decode_start = Instant::now();
        let decoded = client.decode_response_simplepir_raw(seed, &published_c1, &response);
        let expected = &values[row * params.db_cols..(row + 1) * params.db_cols];
        if decoded.len() != expected.len()
            || !decoded
                .iter()
                .zip(expected)
                .all(|(&a, &b)| a == u64::from(b))
        {
            return Err(format!("PIR result mismatch for row {row}").into());
        }
        let decode_ms = decode_start.elapsed().as_secs_f64() * 1000.0;
        let total_ms = begin.elapsed().as_secs_f64() * 1000.0;
        if iteration < queries.len() {
            let mut bytes = vec![0u8; row_bytes];
            for (column, &value) in decoded.iter().enumerate() {
                ipir_sp::bits::write_bits(&mut bytes, value, column * 14, 14);
            }
            decoded_rows.push(json!({"row": row, "bytes": bytes}));
        }
        samples.push(
            json!({"row": row, "prepare_ms": prepare_ms, "server_ms": server_ms,
                            "decode_ms": decode_ms, "total_ms": total_ms,
                            "upload_bytes": key_bytes.len() + query_bytes.len(),
                            "response_bytes": response.len()}),
        );
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "classification": "serialized in-process PIR round trips; no HTTP or mobile runtime",
            "ipir_revision": "e875404cef33661906ab60af236dfb327e6b28b1",
            "source_rows": source.len() / row_bytes, "row_bytes": row_bytes,
            "rows": params.db_rows, "columns": params.db_cols,
            "encoded_database_bytes": values.len() * 2,
            "published_setup_bytes": published.len(), "client_setup_ms": client_setup_ms,
            "encoding_ms": encoding_ms, "server_setup_ms": server_setup_ms,
            "client_setup_rss_increment_bytes": client_setup_rss.saturating_sub(baseline_rss),
            "combined_client_server_ready_rss_bytes": ready_rss,
            "combined_client_server_end_rss_bytes": rss(),
            "samples": samples, "decoded_rows": decoded_rows,
            "padding_note": "Rows beyond source_rows are zero padding used only for geometry scaling"
        }))?
    );
    Ok(())
}
