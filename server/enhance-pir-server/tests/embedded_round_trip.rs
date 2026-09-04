//! In-process coordinator round trip: journal -> publish -> opaque query ->
//! decoded record, with no HTTP. This is the safety net for the coordinator
//! refactor: every assertion here is about bytes a client would see.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use enhance_pir::client::{record_in_row, QuerySession};
use enhance_pir::types::{
    EnhanceRecord, EnhanceRecordParts, EnhanceSession, SHARD_POSITIONS, SHARD_ROWS,
};
use enhance_pir_server::coordinator::{
    router, CoordinatorState, TableSetup, WorkerGroup, WorkerTarget,
};
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::{DatabaseId, ENHANCE_LAYOUT};
use enhance_pir_server::wire::{EvaluateRequest, ShardQuery};
use enhance_pir_server::worker::router as worker_router;
use enhance_pir_server::worker::WorkerState;
use enhance_pir_server::worker::RETAINED_GENERATIONS;
use std::path::Path;

fn record(position: u64) -> EnhanceRecord {
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
    EnhanceRecord::from_parts(EnhanceRecordParts {
        ephemeral_key: tag(2),
        enc_ciphertext: enc,
        cv_net: tag(3),
        out_ciphertext: out,
        has_transparent_bundle: position.is_multiple_of(2),
    })
}

fn store_with(dir: &Path, count: u64, height: u64) -> RecordJournal {
    let mut store =
        RecordJournal::open(dir, DatabaseId::Enhance, ENHANCE_LAYOUT).expect("open journal");
    let records: Vec<_> = (0..count).map(record).collect();
    store
        .append_block(height, format!("{height:064x}"), &records)
        .expect("append");
    store
}

fn coordinator(worker: &WorkerState) -> CoordinatorState {
    CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: vec![WorkerGroup {
            name: "embedded-group".to_string(),
            replicas: vec![WorkerTarget::Embedded {
                name: "embedded".to_string(),
                state: worker.clone(),
            }],
        }],
    }])
    .expect("coordinator")
}

fn replicated_coordinator(replicas: Vec<WorkerTarget>) -> CoordinatorState {
    CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: vec![WorkerGroup {
            name: "group-1".to_string(),
            replicas,
        }],
    }])
    .expect("replicated coordinator")
}

fn session(state: &CoordinatorState) -> QuerySession {
    QuerySession::from_session(state.session().expect("session"))
        .expect("session accepts its own snapshot")
}

async fn fetch(state: &CoordinatorState, session: &QuerySession, position: u64) -> EnhanceRecord {
    let (query, slot) = session.prepare_position(position).expect("in coverage");
    let response = state
        .answer_query(DatabaseId::Enhance, query.body())
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

    let metadata = state.session().expect("session").generation;
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
        .answer_query(DatabaseId::Enhance, dummy.body())
        .await
        .expect("dummy answered");
    session.decode(dummy, &response).expect("dummy decodes");

    // The generation is bound into the request body.
    let (query, _) = session.prepare_position(1).expect("query");
    let mut stale = query.body().to_vec();
    stale[..8].copy_from_slice(&(metadata.generation + 1).to_le_bytes());
    assert!(state
        .answer_query(DatabaseId::Enhance, &stale)
        .await
        .is_err());

    // A worker only evaluates its complete active assignment.
    let partial = worker
        .evaluate_local(
            DatabaseId::Enhance,
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
            DatabaseId::Enhance,
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

#[tokio::test]
async fn replicas_hold_the_same_assignment_and_produce_one_logical_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store_with(&dir.path().join("journal"), 120, 3_428_143);
    let replica_a = WorkerState::new(dir.path().join("replica-a")).expect("replica a");
    let replica_b = WorkerState::new(dir.path().join("replica-b")).expect("replica b");
    let state = replicated_coordinator(vec![
        WorkerTarget::Embedded {
            name: "replica-a".to_string(),
            state: replica_a.clone(),
        },
        WorkerTarget::Embedded {
            name: "replica-b".to_string(),
            state: replica_b.clone(),
        },
    ]);
    state
        .publish_from_store(&store, 3_428_143, hash(3_428_143))
        .await
        .expect("publish with both replicas");

    let metadata = state.session().expect("session").generation;
    assert_eq!(metadata.shards[0].worker, "group-1");
    assert_eq!(replica_a.cached_shard_count().await, 1);
    assert_eq!(replica_b.cached_shard_count().await, 1);
    let request = EvaluateRequest {
        generation: metadata.generation,
        shards: vec![ShardQuery {
            shard_id: 0,
            coefficients: vec![0; SHARD_ROWS],
        }],
    };
    assert_eq!(
        replica_a
            .evaluate_local(DatabaseId::Enhance, request.clone())
            .await
            .expect("replica a evaluates"),
        replica_b
            .evaluate_local(DatabaseId::Enhance, request)
            .await
            .expect("replica b evaluates")
    );

    let session = session(&state);
    assert_eq!(fetch(&state, &session, 42).await, record(42));
    assert_eq!(fetch(&state, &session, 43).await, record(43));
}

#[tokio::test]
async fn one_replica_is_a_publish_and_query_quorum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store_with(&dir.path().join("journal"), 120, 3_428_143);
    let healthy = WorkerState::new(dir.path().join("healthy")).expect("healthy replica");
    let state = replicated_coordinator(vec![
        WorkerTarget::Remote {
            name: "offline".to_string(),
            base_url: "http://127.0.0.1:9".to_string(),
        },
        WorkerTarget::Embedded {
            name: "healthy".to_string(),
            state: healthy.clone(),
        },
    ]);
    state
        .publish_from_store(&store, 3_428_143, hash(3_428_143))
        .await
        .expect("one replica satisfies the group quorum");

    assert_eq!(healthy.cached_shard_count().await, 1);
    let session = session(&state);
    assert_eq!(fetch(&state, &session, 7).await, record(7));
}

#[tokio::test]
async fn query_retries_the_peer_when_the_selected_replica_goes_offline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = store_with(&dir.path().join("journal"), 120, 3_428_143);
    let remote = WorkerState::new(dir.path().join("remote")).expect("remote replica");
    let peer = WorkerState::new(dir.path().join("peer")).expect("peer replica");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind worker");
    let address = listener.local_addr().expect("worker address");
    let server = tokio::spawn(async move {
        axum::serve(listener, worker_router(remote))
            .await
            .expect("worker server");
    });
    let state = replicated_coordinator(vec![
        WorkerTarget::Remote {
            name: "remote".to_string(),
            base_url: format!("http://{address}"),
        },
        WorkerTarget::Embedded {
            name: "peer".to_string(),
            state: peer,
        },
    ]);
    state
        .publish_from_store(&store, 3_428_143, hash(3_428_143))
        .await
        .expect("both replicas publish");

    server.abort();
    let _ = server.await;
    let session = session(&state);
    assert_eq!(fetch(&state, &session, 11).await, record(11));
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
    assert_eq!(manifest.tables[&DatabaseId::Enhance].positions, first + 40);
    assert!(state.generation(3_428_143).is_some());

    let response = state
        .answer_query(DatabaseId::Enhance, old_query.body())
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
        .answer_query(DatabaseId::Enhance, stale_query.body())
        .await
        .is_err());
    // Every retained generation holds one frontier shard.
    assert_eq!(worker.cached_shard_count().await, RETAINED_GENERATIONS);
}

#[tokio::test]
async fn enhance_v1_routes_expose_only_the_current_generation() {
    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = store_with(&dir.path().join("journal"), 120, 3_428_143);
    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = coordinator(&worker);
    let unavailable = router(state.clone())
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/enhance/init")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
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

    let expected_session = state.session().expect("session");
    let expected_public_params = BASE64_STANDARD
        .decode(&expected_session.public_params_base64)
        .unwrap();
    let (status, body) = get(&app, "/v1/enhance/init").await;
    assert_eq!(status, StatusCode::OK);
    let wire: EnhanceSession = serde_json::from_slice(&body).unwrap();
    let manifest = &wire.generation;
    assert_eq!(manifest.schema_version, 6);
    assert_eq!(manifest.generation, 3_428_143);
    assert_eq!(manifest.record_bytes, 725);
    assert!(manifest.parameter_id.contains("-enhance-"));
    assert_eq!(wire.params, expected_session.params);
    let public_params = BASE64_STANDARD.decode(&wire.public_params_base64).unwrap();
    assert_eq!(public_params.len(), 28_672);
    assert_eq!(public_params, expected_public_params);
    assert_eq!(get(&app, "/v1/health").await.0, StatusCode::OK);

    for removed in [
        "/v1/enhance/session",
        "/v1/enhance/generation",
        "/v1/enhance/params",
        "/v1/enhance/public-params",
        "/v1/generation",
        "/memo/metadata",
        "/v1/action/params",
        "/v1/witness/params",
    ] {
        assert_eq!(
            get(&app, removed).await.0,
            StatusCode::NOT_FOUND,
            "{removed}"
        );
    }

    // One query through the only public PIR route.
    let session = session(&state);
    let (query, slot) = session.prepare_position(7).expect("query");
    let (status, v1) = post(&app, "/v1/enhance/query", query.body().to_vec()).await;
    assert_eq!(status, StatusCode::OK);
    let row = session.decode(query, &v1).expect("decodes");
    assert_eq!(record_in_row(&row, slot), record(7));
}
