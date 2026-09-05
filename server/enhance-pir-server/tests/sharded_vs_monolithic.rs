//! The open gate from `docs/vizor_tx_enhancement.md` §11: the row-sharded
//! evaluation must be byte-identical to one monolithic iPIR server over the
//! same database. Offline CRS sums, online partial sums, the packed response,
//! and the decoded row are all compared.

use enhance_pir::types::{ROW_BYTES, SHARD_ROWS};
use enhance_pir_server::coordinator::enhance_setup_seed_bytes;
use enhance_pir_server::ipir::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, deserialize_first_dim_query,
    global_parameters, RowPlaintextIter, ShardRuntime,
};
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::ENHANCE_LAYOUT;
use inspiring::TopKeyImages;
use ipir_sp::modulus_switch::{recover_published_c1, serialize_rlwe_response_bodies};
use ipir_sp::serialize::{deserialize_packing_keys, serialize_packing_keys};
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows, IPIRServer,
};
use ipir_sp::IPIRClient;

fn synthetic_rows(seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..SHARD_ROWS * ROW_BYTES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

#[test]
fn two_shards_equal_one_monolithic_server() {
    let rows0 = synthetic_rows(0x9e37_79b9);
    let rows1 = synthetic_rows(0x7f4a_7c15);
    let logical_rows = 2 * SHARD_ROWS as u64;
    let (rlwe, ypir) = global_parameters(logical_rows, &ENHANCE_LAYOUT).expect("params");
    assert_eq!(ypir.db_rows, logical_rows as usize);
    let client = IPIRClient::new(&rlwe, &ypir);
    let setup = client.generate_public_query_setup_simplepir_from_seed(enhance_setup_seed_bytes());

    // Sharded: each shard preprocesses over its slice of the seeded setup.
    let shard0 = ShardRuntime::build(
        &ENHANCE_LAYOUT,
        0,
        0,
        RecordJournal::rows_digest(&rows0),
        &rows0,
        &rlwe,
        &setup,
    )
    .expect("shard 0");
    let shard1 = ShardRuntime::build(
        &ENHANCE_LAYOUT,
        1,
        SHARD_ROWS,
        RecordJournal::rows_digest(&rows1),
        &rows1,
        &rlwe,
        &setup,
    )
    .expect("shard 1");

    // Monolithic: one server over the concatenated rows.
    let mut all_rows = rows0.clone();
    all_rows.extend_from_slice(&rows1);
    let coefficients = RowPlaintextIter::new(
        &all_rows,
        ROW_BYTES,
        ypir.db_rows,
        ypir.db_cols,
        ypir.p.trailing_zeros() as usize,
    );
    let monolithic = IPIRServer::<u16>::new_auto_kernel(ypir.clone(), coefficients, false, true);
    let crs_mono = monolithic
        .perform_offline_precomputation_simplepir(&rlwe, &setup)
        .crs_blocks;

    let mut crs_sum = shard0.crs_blocks.clone();
    add_crs_blocks_assign_mod(&mut crs_sum, &shard1.crs_blocks, &rlwe).expect("sum crs");
    assert_eq!(crs_sum.len(), crs_mono.len());
    for (summed, mono) in crs_sum.iter().zip(&crs_mono) {
        assert_eq!(summed.rows, mono.rows, "offline CRS blocks differ");
    }

    // One client query, evaluated both ways from the same switched bytes.
    let target_row = SHARD_ROWS + 77;
    let (query, packing_keys, seed) = client.generate_fresh_query_simplepir(&setup, target_row);
    let packing_bytes = serialize_packing_keys(&rlwe, &packing_keys).expect("keys");
    let packing_keys = deserialize_packing_keys(&rlwe, &packing_bytes).expect("keys");
    let switched = query.to_switched_bytes(rlwe.q, ypir.query_bits);
    let global_query = deserialize_first_dim_query(&rlwe, &ypir, &switched).expect("query");

    let mono_partial = monolithic.multiply_query(&rlwe, &global_query);
    let mut sharded_partial = shard0
        .evaluate(&rlwe, &global_query[..SHARD_ROWS])
        .expect("shard 0 partial");
    let partial1 = shard1
        .evaluate(&rlwe, &global_query[SHARD_ROWS..])
        .expect("shard 1 partial");
    add_intermediate_assign_mod(&mut sharded_partial, &partial1, rlwe.q).expect("sum");
    assert_eq!(sharded_partial, mono_partial, "online partials differ");

    // Pack both with their own CRS; the transcripts must match byte for byte.
    let pack = |crs: &[ipir_sp::server::CrsBlock], partial: &[u64]| {
        let preprocessed = build_pack_preprocessed_blocks(&rlwe, crs).expect("preprocess");
        let top = TopKeyImages::build(&rlwe);
        let packed =
            pack_intermediate_blocks(partial, &packing_keys, &top, &preprocessed).expect("pack");
        let body = serialize_rlwe_response_bodies(&packed, ypir.q_prime_1);
        let public = published_c1_rows(&preprocessed, rlwe.q);
        (body, public)
    };
    let (sharded_body, sharded_public) = pack(&crs_sum, &sharded_partial);
    let (mono_body, mono_public) = pack(&crs_mono, &mono_partial);
    assert_eq!(sharded_public, mono_public, "published c1 differs");
    assert_eq!(sharded_body, mono_body, "packed responses differ");

    let blocks = ypir.db_cols / rlwe.d;
    let published_c1 = recover_published_c1(&sharded_public, rlwe.d, blocks, rlwe.q);
    let decoded = client.decode_response_simplepir(seed, &published_c1, &sharded_body);
    assert_eq!(
        &decoded[..ROW_BYTES],
        &all_rows[target_row * ROW_BYTES..(target_row + 1) * ROW_BYTES]
    );
}
