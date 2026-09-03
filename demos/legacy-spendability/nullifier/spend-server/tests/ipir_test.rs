#![cfg(feature = "ipir")]

use ipir_sp::serialize::serialize_packing_keys;
use spend_server::pir_ipir::IpirPirEngine;
use spend_types::{
    hash_to_bucket, PirEngine, YpirScenario, BUCKET_BYTES, ENTRY_BYTES, IPIR_SETUP_SEED,
    NUM_BUCKETS,
};

fn scenario() -> YpirScenario {
    YpirScenario {
        num_items: NUM_BUCKETS as u64,
        item_size_bits: (BUCKET_BYTES * 8) as u64,
    }
}

fn make_nf(seed: u32) -> [u8; 32] {
    let mut nf = [0u8; 32];
    nf[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in nf.iter_mut().enumerate().skip(4) {
        *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
    }
    nf
}

fn build_db_with_nf(nf: &[u8; 32]) -> Vec<u8> {
    let mut db = vec![0u8; NUM_BUCKETS * BUCKET_BYTES];
    let bucket_idx = hash_to_bucket(nf) as usize;
    let offset = bucket_idx * BUCKET_BYTES;
    let entry = spend_types::NullifierEntry {
        nullifier: *nf,
        spend_height: 1,
        first_output_position: 0,
        action_count: 1,
    };
    db[offset..offset + ENTRY_BYTES].copy_from_slice(&entry.to_bytes());
    db
}

fn query_for_bucket(engine: &IpirPirEngine, bucket_idx: usize) -> (Vec<u8>, ipir_sp::IPIRSeed) {
    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
    let offline_query_polys =
        client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);
    assert_eq!(offline_query_polys, engine.offline_query_polys());

    let (query, packing_keys, seed) =
        client.generate_fresh_query_simplepir(&offline_query_polys, bucket_idx);
    let mut query_bytes =
        serialize_packing_keys(client.rlwe_params(), &packing_keys).expect("serialize keys");
    query_bytes
        .extend(query.to_switched_bytes(client.rlwe_params().q, engine.ypir_params().query_bits));
    (query_bytes, seed)
}

/// Decode a server answer the way a real client does.
///
/// The response carries only `c2` behind an epoch tag; `c1` comes from the
/// engine's published parameters, which is what `/public-params` serves.
fn decode(
    engine: &IpirPirEngine,
    state: &<IpirPirEngine as PirEngine>::ServerState,
    seed: ipir_sp::IPIRSeed,
    response: &[u8],
) -> Vec<u8> {
    let published_bytes = engine.public_params(state);
    assert!(
        !published_bytes.is_empty(),
        "engine must publish c1 rows; an empty body means public_params was not forwarded",
    );

    let rlwe = engine.rlwe_params();
    let blocks = engine.ypir_params().db_cols / rlwe.d;
    assert_eq!(
        published_bytes.len(),
        blocks * ipir_sp::modulus_switch::published_c1_len(rlwe.d, rlwe.q),
        "published c1 must be one full-precision row per output block",
    );
    let published_c1 =
        ipir_sp::modulus_switch::recover_published_c1(&published_bytes, rlwe.d, blocks, rlwe.q);

    let (epoch, body) = spend_types::split_epoch(response).expect("response carries an epoch tag");
    assert_eq!(
        epoch,
        spend_types::public_params_epoch(&published_bytes),
        "response epoch must name the parameters it decodes against",
    );

    let client = ipir_sp::IPIRClient::new(rlwe, engine.ypir_params());
    client.decode_response_simplepir(seed, &published_c1, body)
}

#[test]
fn test_ipir_roundtrip_found() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();

    let nf = make_nf(12345);
    let db_bytes = build_db_with_nf(&nf);
    let bucket_idx = hash_to_bucket(&nf) as usize;

    let state = engine.setup(&db_bytes, &sc).unwrap();
    let (query_bytes, seed) = query_for_bucket(&engine, bucket_idx);
    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let decoded = decode(&engine, &state, seed, &response);
    assert!(
        decoded.len() >= BUCKET_BYTES,
        "decoded response too short: {} < {}",
        decoded.len(),
        BUCKET_BYTES,
    );

    let bucket_data = &decoded[..BUCKET_BYTES];
    let found = bucket_data
        .chunks_exact(ENTRY_BYTES)
        .any(|chunk| chunk[..32] == nf[..]);
    assert!(found, "nullifier not found in decoded bucket");
}

#[test]
fn test_ipir_roundtrip_not_found() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();

    let present_nf = make_nf(12345);
    let absent_nf = make_nf(99999);
    let db_bytes = build_db_with_nf(&present_nf);
    let absent_bucket = hash_to_bucket(&absent_nf) as usize;

    let state = engine.setup(&db_bytes, &sc).unwrap();
    let (query_bytes, seed) = query_for_bucket(&engine, absent_bucket);
    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let decoded = decode(&engine, &state, seed, &response);
    let bucket_data = &decoded[..BUCKET_BYTES];

    let found = bucket_data
        .chunks_exact(ENTRY_BYTES)
        .any(|chunk| chunk[..32] == absent_nf[..]);
    assert!(!found, "absent nullifier should not appear in bucket");
}

/// Record what a query actually costs on the wire.
///
/// The point of the upgrade is bandwidth, and the two halves move in opposite
/// directions: the response shrinks because `c1` moved out of it, while `c1`
/// itself becomes a one-time fetch. A response that did not shrink means `c1`
/// is still inline and the migration is incomplete.
#[test]
fn ipir_wire_sizes() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();
    let rlwe = engine.rlwe_params();
    let ypir = engine.ypir_params();

    let db = vec![0u8; NUM_BUCKETS * BUCKET_BYTES];
    let state = engine.setup(&db, &sc).unwrap();
    let (query_bytes, _seed) = query_for_bucket(&engine, 0);
    let response = engine.answer_query(&state, &query_bytes).unwrap();
    let published = engine.public_params(&state);

    let blocks = ypir.db_cols / rlwe.d;
    let keys_len = ipir_sp::serialize::serialized_packing_keys_len(rlwe);
    let body = response.len() - spend_types::PIR_EPOCH_BYTES;

    eprintln!(
        "ipir wire: keys {keys_len} + query {} = {} up, {body} (+{} epoch) down, \
         {} published once over {blocks} blocks",
        query_bytes.len() - keys_len,
        query_bytes.len(),
        spend_types::PIR_EPOCH_BYTES,
        published.len(),
    );

    // The query is switched down from the full modulus width.
    assert!(
        query_bytes.len() - keys_len < (ypir.db_rows * 56).div_ceil(8),
        "query must be transmitted below full `q` precision",
    );
    // The response carries `c2` only.
    assert_eq!(
        body,
        blocks * ipir_sp::modulus_switch::response_body_len(rlwe.d, ypir.q_prime_1),
    );
    assert!(
        body < blocks
            * ipir_sp::modulus_switch::switched_rlwe_response_len(
                rlwe.d,
                ypir.q_prime_1,
                ypir.q_prime_2,
            ),
        "body-only response must beat one that inlines c1",
    );
}
