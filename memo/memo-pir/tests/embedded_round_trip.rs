//! In-process coordinator round trip: journal -> publish -> opaque query ->
//! decoded record, with no HTTP. This is the safety net for the coordinator
//! refactor: every assertion here is about bytes a client would see.

use memo_pir::client::{record_in_row, QuerySession};
use memo_pir::coordinator::{router, CoordinatorState, TableSetup, WorkerTarget};
use memo_pir::store::RecordJournal;
use memo_pir::types::{
    ActionRecord, ActionRecordParts, DatabaseId, ACTION_LAYOUT, SHARD_POSITIONS, SHARD_ROWS,
};
use memo_pir::wire::{EvaluateRequest, ShardQuery};
use memo_pir::worker::WorkerState;
use memo_pir::worker::RETAINED_GENERATIONS;
use std::path::Path;

fn record(position: u64) -> ActionRecord {
    let tag = |salt: u8| -> [u8; 32] {
        let mut bytes = [salt; 32];
        bytes[..8].copy_from_slice(&position.to_le_bytes());
        bytes
    };
    let mut enc = [0u8; 580];
    for (index, byte) in enc.iter_mut().enumerate() {
        *byte = (position as usize).wrapping_mul(31).wrapping_add(index) as u8;
    }
    let mut out = [0u8; 80];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = (position as usize).wrapping_mul(7).wrapping_add(index) as u8;
    }
    ActionRecord::from_parts(ActionRecordParts {
        nullifier: tag(1),
        ephemeral_key: tag(2),
        enc_ciphertext: enc,
        cmx: tag(9),
        cv_net: tag(3),
        out_ciphertext: out,
        txid: tag(4),
        height: 3_428_143 + (position / 3) as u32,
    })
}

fn store_with(dir: &Path, count: u64, height: u64) -> RecordJournal {
    let mut store =
        RecordJournal::open(dir, DatabaseId::Action, ACTION_LAYOUT).expect("open journal");
    let records: Vec<_> = (0..count).map(record).collect();
    store
        .append_block(height, format!("{height:064x}"), &records)
        .expect("append");
    store
}

fn coordinator(worker: &WorkerState) -> CoordinatorState {
    CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Action,
        pool: vec![WorkerTarget::Embedded {
            name: "embedded".to_string(),
            state: worker.clone(),
        }],
    }])
    .expect("coordinator")
}

fn session(state: &CoordinatorState) -> QuerySession {
    QuerySession::new(
        state.metadata().expect("metadata"),
        state.params(DatabaseId::Action).expect("params"),
        &state
            .public_params(DatabaseId::Action)
            .expect("public params"),
    )
    .expect("session accepts its own snapshot")
}

async fn fetch(state: &CoordinatorState, session: &QuerySession, position: u64) -> ActionRecord {
    let (query, slot) = session.prepare_position(position).expect("in coverage");
    let response = state
        .answer_query(DatabaseId::Action, query.body())
        .await
        .expect("answered");
    let row = session.decode(query, &response).expect("decodes");
    record_in_row(&row, slot)
}

fn hash(height: u64) -> String {
    format!("{height:064x}")
}

#[tokio::test]
async fn publishes_two_shards_and_answers_boundary_positions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let count = SHARD_POSITIONS as u64 + 300;
    let store = store_with(&dir.path().join("journal"), count, 3_428_143);
    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = coordinator(&worker);
    state
        .publish_from_store(&store, 3_428_143, hash(3_428_143))
        .await
        .expect("publish");

    let metadata = state.metadata().expect("metadata");
    assert_eq!(metadata.shards.len(), 2);
    assert!(metadata.shards[0].sealed);
    assert!(!metadata.shards[1].sealed);
    assert_eq!(metadata.ironwood_tree_size, count);

    let session = session(&state);
    for position in [
        0,
        SHARD_POSITIONS as u64 - 1,
        SHARD_POSITIONS as u64,
        count - 1,
    ] {
        assert_eq!(
            fetch(&state, &session, position).await,
            record(position),
            "position {position}"
        );
    }

    // A cover query is indistinguishable in shape and decodes to some row.
    let dummy = session.prepare_dummy().expect("dummy");
    let response = state
        .answer_query(DatabaseId::Action, dummy.body())
        .await
        .expect("dummy answered");
    session.decode(dummy, &response).expect("dummy decodes");

    // The generation is bound into the request body.
    let (query, _) = session.prepare_position(1).expect("query");
    let mut stale = query.body().to_vec();
    stale[..8].copy_from_slice(&(metadata.generation + 1).to_le_bytes());
    assert!(state
        .answer_query(DatabaseId::Action, &stale)
        .await
        .is_err());

    // A worker only evaluates its complete active assignment.
    let partial = worker
        .evaluate_local(
            DatabaseId::Action,
            EvaluateRequest {
                generation: metadata.generation,
                shards: vec![ShardQuery {
                    shard_id: 0,
                    coefficients: vec![0; SHARD_ROWS],
                }],
            },
        )
        .await
        .expect_err("partial assignment");
    assert!(
        partial.contains("complete active shard assignment"),
        "{partial}"
    );
    let superset = worker
        .evaluate_local(
            DatabaseId::Action,
            EvaluateRequest {
                generation: metadata.generation,
                shards: (0..3)
                    .map(|shard_id| ShardQuery {
                        shard_id,
                        coefficients: vec![0; SHARD_ROWS],
                    })
                    .collect(),
            },
        )
        .await
        .expect_err("superset assignment");
    assert!(
        superset.contains("complete active shard assignment"),
        "{superset}"
    );
}

/// Two generations must be answerable at once so a query built against the
/// previous snapshot survives a publish; a third publish evicts the first on
/// the coordinator and the worker alike.
#[tokio::test]
async fn previous_generation_is_still_answered_after_a_publish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = 300u64;
    let mut store = store_with(&dir.path().join("journal"), first, 3_428_143);
    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = coordinator(&worker);
    state
        .publish_from_store(&store, 3_428_143, hash(3_428_143))
        .await
        .expect("first publish");
    let old_session = session(&state);
    let (old_query, slot) = old_session.prepare_position(5).expect("query");

    let more: Vec<_> = (first..first + 40).map(record).collect();
    store
        .append_block(3_428_144, hash(3_428_144), &more)
        .expect("append");
    state
        .publish_from_store(&store, 3_428_144, hash(3_428_144))
        .await
        .expect("second publish");
    let manifest = state.manifest().expect("manifest");
    assert_eq!(manifest.generation, 3_428_144);
    assert_eq!(manifest.tables[&DatabaseId::Action].positions, first + 40);
    assert!(state.generation(3_428_143).is_some());

    let response = state
        .answer_query(DatabaseId::Action, old_query.body())
        .await
        .expect("previous generation still served");
    let row = old_session.decode(old_query, &response).expect("decodes");
    assert_eq!(record_in_row(&row, slot), record(5));

    // The first generation stays answerable until RETAINED_GENERATIONS newer
    // ones have been published, then its queries are refused.
    let (stale_query, _) = old_session.prepare_position(6).expect("query");
    let mut next_position = first + 40;
    for height in 3_428_145..3_428_143 + RETAINED_GENERATIONS as u64 {
        assert!(state.generation(3_428_143).is_some(), "height {height}");
        let more: Vec<_> = (next_position..next_position + 10).map(record).collect();
        next_position += 10;
        store
            .append_block(height, hash(height), &more)
            .expect("append");
        state
            .publish_from_store(&store, height, hash(height))
            .await
            .expect("publish");
    }
    assert!(state.generation(3_428_144).is_some());
    let evicting = 3_428_143 + RETAINED_GENERATIONS as u64;
    let more: Vec<_> = (next_position..next_position + 10).map(record).collect();
    store
        .append_block(evicting, hash(evicting), &more)
        .expect("append");
    state
        .publish_from_store(&store, evicting, hash(evicting))
        .await
        .expect("evicting publish");
    assert!(state.generation(3_428_143).is_none());
    assert!(state
        .answer_query(DatabaseId::Action, stale_query.body())
        .await
        .is_err());
    // Every retained generation holds one frontier shard.
    assert_eq!(worker.cached_shard_count().await, RETAINED_GENERATIONS);
}

#[tokio::test]
async fn v1_routes_and_legacy_aliases_describe_the_same_generation() {
    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = store_with(&dir.path().join("journal"), 120, 3_428_143);
    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = coordinator(&worker);
    state
        .publish_from_store(&store, 3_428_143, hash(3_428_143))
        .await
        .expect("publish");
    let app = router(state.clone());

    async fn get(app: &axum::Router, path: &str) -> (StatusCode, Vec<u8>) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap();
        (status, body.to_vec())
    }
    async fn post(app: &axum::Router, path: &str, bytes: Vec<u8>) -> (StatusCode, Vec<u8>) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .body(Body::from(bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap();
        (status, body.to_vec())
    }

    let (status, body) = get(&app, "/v1/generation").await;
    assert_eq!(status, StatusCode::OK);
    let manifest: memo_pir::GenerationManifest = serde_json::from_slice(&body).unwrap();
    assert_eq!(manifest.schema_version, 4);
    assert_eq!(manifest.generation, 3_428_143);
    let action = &manifest.tables[&DatabaseId::Action];
    assert_eq!(action.record_bytes, 824);
    assert!(action.parameter_id.contains("-action-"));

    let (_, body) = get(&app, "/memo/metadata").await;
    let legacy: memo_pir::MemoSnapshotMetadata = serde_json::from_slice(&body).unwrap();
    assert_eq!(legacy.schema_version, 3);
    assert_eq!(legacy.generation, manifest.generation);
    assert_eq!(legacy.shards, action.shards);

    assert_eq!(
        get(&app, "/v1/action/params").await,
        get(&app, "/memo/params").await
    );
    assert_eq!(
        get(&app, "/v1/action/public-params").await,
        get(&app, "/memo/public-params").await
    );
    assert_eq!(get(&app, "/v1/health").await.0, StatusCode::OK);

    // A known table this coordinator does not serve is unavailable; an
    // unknown name does not exist.
    assert_eq!(
        get(&app, "/v1/witness/params").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(get(&app, "/v1/memo/params").await.0, StatusCode::NOT_FOUND);

    // One query, answered identically through both routes.
    let session = session(&state);
    let (query, slot) = session.prepare_position(7).expect("query");
    let (status, v1) = post(&app, "/v1/action/query", query.body().to_vec()).await;
    assert_eq!(status, StatusCode::OK);
    let (_, legacy) = post(&app, "/memo/query", query.body().to_vec()).await;
    assert_eq!(v1, legacy);
    let row = session.decode(query, &v1).expect("decodes");
    assert_eq!(record_in_row(&row, slot), record(7));
}
