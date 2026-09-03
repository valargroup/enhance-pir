#![cfg(feature = "ipir")]

use ipir_sp::serialize::serialize_packing_keys;
use pir_types::{PirEngine, YpirScenario, IPIR_SETUP_SEED};
use witness_server::pir_ipir::IpirPirEngine;
use witness_types::{L0_DB_ROWS, SUBSHARD_ROW_BYTES};

fn scenario() -> YpirScenario {
    YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
    }
}

fn query_for_row(engine: &IpirPirEngine, row_idx: usize) -> (Vec<u8>, ipir_sp::IPIRSeed) {
    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
    let offline_query_polys =
        client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);
    assert_eq!(offline_query_polys, engine.offline_query_polys());

    let (query, packing_keys, seed) =
        client.generate_fresh_query_simplepir(&offline_query_polys, row_idx);
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

    let (epoch, body) = pir_types::split_epoch(response).expect("response carries an epoch tag");
    assert_eq!(
        epoch,
        pir_types::public_params_epoch(&published_bytes),
        "response epoch must name the parameters it decodes against",
    );

    let client = ipir_sp::IPIRClient::new(rlwe, engine.ypir_params());
    client.decode_response_simplepir(seed, &published_c1, body)
}

#[test]
fn test_ipir_subshard_row_roundtrip() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();
    let row_idx = 17usize;
    let mut db = vec![0u8; L0_DB_ROWS * SUBSHARD_ROW_BYTES];
    let row_start = row_idx * SUBSHARD_ROW_BYTES;
    for (i, byte) in db[row_start..row_start + 128].iter_mut().enumerate() {
        *byte = (i as u8).wrapping_mul(3).wrapping_add(1);
    }

    let state = engine.setup(&db, &sc).unwrap();
    let (query_bytes, seed) = query_for_row(&engine, row_idx);
    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let decoded = decode(&engine, &state, seed, &response);
    assert!(
        decoded.len() >= SUBSHARD_ROW_BYTES,
        "decoded response too short: {} < {}",
        decoded.len(),
        SUBSHARD_ROW_BYTES,
    );
    assert_eq!(&decoded[..128], &db[row_start..row_start + 128]);
}
