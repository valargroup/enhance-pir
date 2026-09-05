//! End-to-end: store filters, serve a range over HTTP, and run the wallet-side
//! validation and matching path against the response.
//!
//! This exercises the same client code a wallet uses, so a change that made the
//! service and the client disagree fails here rather than in production.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use transparent_filter::{
    build_filter, check_batch, ChainMap, FilterBatch, FilterLimits, RangeRequest, ScriptBytes,
};
use transparent_filter::{BlockHash, FilterServiceInfo};
use transparent_filter_server::service::{router, ServiceState};
use transparent_filter_server::store::FilterStore;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const START: u64 = 3_428_143;

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    bytes[31] = 0x01;
    BlockHash::from_internal_bytes(bytes)
}

fn wallet_script() -> ScriptBytes {
    ScriptBytes::new(vec![0x76, 0xa9, 0x14, 0x42, 0x88, 0xac])
}

/// Blocks START..START+blocks; the wallet's script appears at `active`.
fn state_with(blocks: u64, active: u64) -> (ServiceState, tempfile::TempDir, ChainMap) {
    let dir = tempfile::tempdir().unwrap();
    let mut store =
        FilterStore::open(dir.path(), transparent_filter::PROFILE, GENESIS, START).unwrap();
    let mut chain = ChainMap::new();
    for height in START..START + blocks {
        let elements = if height == active {
            vec![wallet_script(), ScriptBytes::new(vec![0x51])]
        } else {
            vec![ScriptBytes::new(vec![0x52, height as u8])]
        };
        let filter = build_filter(hash_at(height), &elements).unwrap();
        store
            .append(height, hash_at(height), filter.as_slice())
            .unwrap();
        chain.insert(height, hash_at(height));
    }
    store.commit().unwrap();
    (ServiceState::new(store), dir, chain)
}

async fn get(state: &ServiceState, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn info_reports_the_pinned_chain_identity_and_profile() {
    let (state, _dir, _chain) = state_with(4, START + 1);
    let (status, body) = get(&state, "/v1/filters/info").await;
    assert_eq!(status, StatusCode::OK);
    let info: FilterServiceInfo = serde_json::from_slice(&body).unwrap();
    assert_eq!(info.genesis_hash, GENESIS);
    assert_eq!(info.profile, "zcash-transparent-basic-v1");
    assert_eq!(info.network, "main");
    assert_eq!(info.start_height, START);
    assert_eq!(info.covered_through, Some(START + 3));
}

#[tokio::test]
async fn a_range_validates_and_the_active_block_matches() {
    let (state, _dir, chain) = state_with(6, START + 2);
    let stop = hash_at(START + 5);
    let (status, body) = get(
        &state,
        &format!(
            "/v1/filters/range?start_height={START}&stop_block_hash={}",
            stop.to_display_hex()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let batch = FilterBatch::decode(&body).expect("decode");
    let request = RangeRequest {
        genesis: BlockHash::from_display_hex(GENESIS).unwrap(),
        profile: transparent_filter::PROFILE.to_string(),
        start_height: START,
        stop_block_hash: stop,
    };
    let checked = check_batch(&batch, &request, &chain, FilterLimits::default()).expect("check");
    assert_eq!(checked.len(), 6);

    let mut matched = Vec::new();
    for record in &checked {
        let indices = transparent_filter::match_scripts(
            &record.filter,
            record.block_hash,
            &[wallet_script()],
        )
        .unwrap();
        if !indices.is_empty() {
            matched.push(record.height);
        }
    }
    assert!(
        matched.contains(&(START + 2)),
        "the active block did not match"
    );
}

#[tokio::test]
async fn a_wallet_on_a_different_branch_rejects_the_batch() {
    let (state, _dir, mut chain) = state_with(4, START + 1);
    let stop = hash_at(START + 3);
    let (_, body) = get(
        &state,
        &format!(
            "/v1/filters/range?start_height={START}&stop_block_hash={}",
            stop.to_display_hex()
        ),
    )
    .await;
    let batch = FilterBatch::decode(&body).unwrap();
    // The wallet accepted a different block at one of these heights.
    chain.insert(START + 2, BlockHash::from_internal_bytes([0xfe; 32]));
    let request = RangeRequest {
        genesis: BlockHash::from_display_hex(GENESIS).unwrap(),
        profile: transparent_filter::PROFILE.to_string(),
        start_height: START,
        stop_block_hash: stop,
    };
    let error = check_batch(&batch, &request, &chain, FilterLimits::default()).unwrap_err();
    assert!(format!("{error}").contains("different branch"));
}

#[tokio::test]
async fn an_uncovered_stop_hash_is_refused() {
    let (state, _dir, _chain) = state_with(3, START);
    let (status, body) = get(
        &state,
        &format!(
            "/v1/filters/range?start_height={START}&stop_block_hash={}",
            "ab".repeat(32)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("not a covered block"));
}

#[tokio::test]
async fn a_start_below_the_service_start_height_is_refused() {
    let (state, _dir, _chain) = state_with(3, START);
    let stop = hash_at(START + 2);
    let (status, _) = get(
        &state,
        &format!(
            "/v1/filters/range?start_height={}&stop_block_hash={}",
            START - 1,
            stop.to_display_hex()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn the_chain_endpoint_returns_the_covered_prefix_only() {
    let (state, _dir, _chain) = state_with(3, START);
    let (status, body) = get(
        &state,
        &format!("/v1/filters/chain?start_height={START}&count=100"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries: Vec<transparent_filter::ChainEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].height, START);
    assert_eq!(entries[2].block_hash, hash_at(START + 2).to_display_hex());
}

#[tokio::test]
async fn readiness_and_metrics_are_available_on_the_service_port() {
    let (state, _dir, _chain) = state_with(2, START);
    let (status, _) = get(&state, "/ready").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = get(&state, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("transparent_filter_covered_through_height"));
    assert!(text.contains("transparent_filter_filters_stored"));
}

#[tokio::test]
async fn a_service_with_no_coverage_is_not_ready() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilterStore::open(dir.path(), transparent_filter::PROFILE, GENESIS, START).unwrap();
    let state = ServiceState::new(store);
    let (status, _) = get(&state, "/ready").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn a_batch_is_capped_and_the_client_accepts_the_bounded_prefix() {
    let blocks = 1_200u64;
    let (state, _dir, chain) = state_with(blocks, START);
    let stop = hash_at(START + blocks - 1);
    let (status, body) = get(
        &state,
        &format!(
            "/v1/filters/range?start_height={START}&stop_block_hash={}",
            stop.to_display_hex()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let batch = FilterBatch::decode(&body).expect("decode");
    assert_eq!(
        batch.records.len() as u64,
        transparent_filter::MAX_RECORDS_PER_BATCH,
        "the service must bound a batch"
    );
    let request = RangeRequest {
        genesis: BlockHash::from_display_hex(GENESIS).unwrap(),
        profile: transparent_filter::PROFILE.to_string(),
        start_height: START,
        stop_block_hash: stop,
    };
    // The client expects exactly the capped count for a range this long.
    assert!(check_batch(&batch, &request, &chain, FilterLimits::default()).is_ok());
}
