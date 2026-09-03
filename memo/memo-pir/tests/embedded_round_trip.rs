//! In-process coordinator round trip: journal -> publish -> opaque query ->
//! decoded record, with no HTTP. This is the safety net for the coordinator
//! refactor: every assertion here is about bytes a client would see.

use memo_pir::client::{record_in_row, QuerySession};
use memo_pir::coordinator::{CoordinatorState, WorkerTarget};
use memo_pir::store::MemoStore;
use memo_pir::types::{ActionRecord, ActionRecordParts, Coverage, SHARD_POSITIONS, SHARD_ROWS};
use memo_pir::wire::{EvaluateRequest, ShardQuery};
use memo_pir::worker::WorkerState;
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
        cv_net: tag(3),
        out_ciphertext: out,
        txid: tag(4),
        height: 3_428_143 + (position / 3) as u32,
    })
}

fn store_with(dir: &Path, count: u64, height: u64) -> MemoStore {
    let mut store = MemoStore::open(dir, 0).expect("open journal");
    let records: Vec<_> = (0..count).map(record).collect();
    store
        .append_block(height, format!("{height:064x}"), &records)
        .expect("append");
    store
}

fn coordinator(worker: &WorkerState) -> CoordinatorState {
    CoordinatorState::new(vec![WorkerTarget::Embedded {
        name: "embedded".to_string(),
        state: worker.clone(),
    }])
    .expect("coordinator")
}

fn session(state: &CoordinatorState) -> QuerySession {
    QuerySession::new(
        state.metadata().expect("metadata"),
        state.params().expect("params"),
        &state.public_params().expect("public params"),
    )
    .expect("session accepts its own snapshot")
}

async fn fetch(state: &CoordinatorState, session: &QuerySession, position: u64) -> ActionRecord {
    let (query, slot) = session.prepare_position(position).expect("in coverage");
    let response = state.answer_query(query.body()).await.expect("answered");
    let row = session.decode(query, &response).expect("decodes");
    record_in_row(&row, slot)
}

#[tokio::test]
async fn publishes_two_shards_and_answers_boundary_positions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let count = SHARD_POSITIONS as u64 + 300;
    let store = store_with(&dir.path().join("journal"), count, 3_428_143);
    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = coordinator(&worker);
    state
        .publish_from_store(
            &store,
            Coverage::Full {
                covered_position_start: 0,
            },
            3_428_143,
            format!("{:064x}", 3_428_143),
        )
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
        .answer_query(dummy.body())
        .await
        .expect("dummy answered");
    session.decode(dummy, &response).expect("dummy decodes");

    // The generation is bound into the request body.
    let (query, _) = session.prepare_position(1).expect("query");
    let mut stale = query.body().to_vec();
    stale[..8].copy_from_slice(&(metadata.generation + 1).to_le_bytes());
    assert!(state.answer_query(&stale).await.is_err());

    // A worker only evaluates its complete active assignment.
    let partial = worker
        .evaluate_local(EvaluateRequest {
            generation: metadata.generation,
            shards: vec![ShardQuery {
                shard_id: 0,
                coefficients: vec![0; SHARD_ROWS],
            }],
        })
        .await
        .expect_err("partial assignment");
    assert!(
        partial.contains("complete active shard assignment"),
        "{partial}"
    );
    let superset = worker
        .evaluate_local(EvaluateRequest {
            generation: metadata.generation,
            shards: (0..3)
                .map(|shard_id| ShardQuery {
                    shard_id,
                    coefficients: vec![0; SHARD_ROWS],
                })
                .collect(),
        })
        .await
        .expect_err("superset assignment");
    assert!(
        superset.contains("complete active shard assignment"),
        "{superset}"
    );
}

/// Two generations must be answerable at once so a query built against the
/// previous snapshot survives a publish. The coordinator keeps one today.
#[tokio::test]
#[ignore = "two-generation retention lands in Phase 2 Step 4"]
async fn previous_generation_is_still_answered_after_a_publish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = 300u64;
    let mut store = store_with(&dir.path().join("journal"), first, 3_428_143);
    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = coordinator(&worker);
    let coverage = Coverage::Full {
        covered_position_start: 0,
    };
    state
        .publish_from_store(
            &store,
            coverage.clone(),
            3_428_143,
            format!("{:064x}", 3_428_143),
        )
        .await
        .expect("first publish");
    let old_session = session(&state);
    let (old_query, slot) = old_session.prepare_position(5).expect("query");

    let more: Vec<_> = (first..first + 40).map(record).collect();
    store
        .append_block(3_428_144, format!("{:064x}", 3_428_144), &more)
        .expect("append");
    state
        .publish_from_store(&store, coverage, 3_428_144, format!("{:064x}", 3_428_144))
        .await
        .expect("second publish");
    assert_eq!(state.metadata().expect("metadata").generation, 3_428_144);

    let response = state
        .answer_query(old_query.body())
        .await
        .expect("previous generation still served");
    let row = old_session.decode(old_query, &response).expect("decodes");
    assert_eq!(record_in_row(&row, slot), record(5));
}
