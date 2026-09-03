//! The one-to-two-worker migration. Production runs a single worker, which
//! `worker_index_for_shard` treats as owning every shard. Appending a second
//! worker is the one step that moves published shards: everything at or
//! beyond `SHARDS_PER_WORKER` goes to the new worker and is rebuilt there.
//! This test is the rehearsal for `docs/enhance-pir-deploy.md`, "Adding the
//! second worker": the coordinator restarts with a two-entry inventory,
//! republishes from the same journal, and every client-visible byte is
//! unchanged while ownership moves exactly as documented.

use enhance_pir::client::{record_in_row, QuerySession};
use enhance_pir_server::coordinator::{CoordinatorState, TableSetup, WorkerTarget};
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::{
    worker_index_for_shard, DatabaseId, EnhanceRecord, EnhanceRecordParts, ENHANCE_LAYOUT,
    SHARDS_PER_WORKER, SHARD_POSITIONS,
};
use enhance_pir_server::worker::WorkerState;
use std::path::Path;

fn record(position: u64) -> EnhanceRecord {
    let tag = |salt: u8| -> [u8; 32] {
        let mut bytes = [salt; 32];
        bytes[..8].copy_from_slice(&position.to_le_bytes());
        bytes
    };
    let mut enc = [0u8; 580];
    for (index, byte) in enc.iter_mut().enumerate() {
        *byte = (position as usize).wrapping_mul(29).wrapping_add(index) as u8;
    }
    EnhanceRecord::from_parts(EnhanceRecordParts {
        ephemeral_key: tag(2),
        enc_ciphertext: enc,
        cv_net: tag(3),
        out_ciphertext: [5; 80],
    })
}

fn hash(height: u64) -> String {
    format!("{height:064x}")
}

fn store_with(dir: &Path, count: u64, height: u64) -> RecordJournal {
    let mut store =
        RecordJournal::open(dir, DatabaseId::Enhance, ENHANCE_LAYOUT).expect("open journal");
    let records: Vec<_> = (0..count).map(record).collect();
    store
        .append_block(height, hash(height), &records)
        .expect("append");
    store
}

fn coordinator(pool: &[(&str, &WorkerState)]) -> CoordinatorState {
    CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        pool: pool
            .iter()
            .map(|(name, state)| WorkerTarget::Embedded {
                name: (*name).to_string(),
                state: (*state).clone(),
            })
            .collect(),
    }])
    .expect("coordinator")
}

fn session(state: &CoordinatorState) -> QuerySession {
    QuerySession::new(
        state.metadata().expect("metadata"),
        state.params(DatabaseId::Enhance).expect("params"),
        &state
            .public_params(DatabaseId::Enhance)
            .expect("public params"),
    )
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

#[test]
fn placement_rule_for_the_migration_is_as_documented() {
    // One worker owns everything, however many shards exist.
    for shard in 0..10 {
        assert_eq!(worker_index_for_shard(shard, 1), Some(0));
    }
    // Two workers: shards below the quantum stay, the rest move to the new one
    // and shards beyond its range are unowned until a third worker is appended.
    for shard in 0..SHARDS_PER_WORKER {
        assert_eq!(worker_index_for_shard(shard, 2), Some(0));
    }
    for shard in SHARDS_PER_WORKER..2 * SHARDS_PER_WORKER {
        assert_eq!(worker_index_for_shard(shard, 2), Some(1));
    }
    assert_eq!(worker_index_for_shard(2 * SHARDS_PER_WORKER, 2), None);
    // Every append after the first moves nothing.
    for shard in 0..2 * SHARDS_PER_WORKER {
        assert_eq!(
            worker_index_for_shard(shard, 2),
            worker_index_for_shard(shard, 3)
        );
    }
}

/// One worker serving three shards, then a coordinator restart with a second
/// worker appended. Shards below `SHARDS_PER_WORKER` keep their digest and
/// owner; the rest move to the new worker; every answer is byte-identical.
#[tokio::test]
async fn second_worker_takes_the_upper_shards_without_changing_answers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let height = 3_428_143;
    let count = 2 * SHARD_POSITIONS as u64 + 300; // shards 0 and 1 sealed, 2 frontier
    let mut store = store_with(&dir.path().join("journal"), count, height);
    let worker_1 = WorkerState::new(dir.path().join("worker-1")).expect("worker 1");
    let worker_2 = WorkerState::new(dir.path().join("worker-2")).expect("worker 2");
    let probes = [
        0,
        SHARD_POSITIONS as u64 - 1,
        SHARD_POSITIONS as u64,
        2 * SHARD_POSITIONS as u64 - 1,
        2 * SHARD_POSITIONS as u64,
        count - 1,
    ];

    // Before: the production shape, one worker owns every shard.
    let before = coordinator(&[("worker-1", &worker_1)]);
    before
        .publish_from_store(&store, height, hash(height))
        .await
        .expect("publish with one worker");
    let manifest_before = before.manifest().expect("manifest").tables[&DatabaseId::Enhance].clone();
    assert_eq!(manifest_before.shards.len(), 3);
    assert!(manifest_before
        .shards
        .iter()
        .all(|shard| shard.worker == "worker-1"));
    let session_before = session(&before);
    let answers_before: Vec<EnhanceRecord> = {
        let mut answers = Vec::new();
        for &position in &probes {
            let answer = fetch(&before, &session_before, position).await;
            assert_eq!(answer, record(position), "position {position} before");
            answers.push(answer);
        }
        answers
    };

    // The migration: the inventory gains worker-2, the coordinator restarts and
    // publishes the next finalized block from the same journal.
    let next = height + 1;
    store
        .append_block(next, hash(next), &[record(count)])
        .expect("append one more block");
    let after = coordinator(&[("worker-1", &worker_1), ("worker-2", &worker_2)]);
    after
        .publish_from_store(&store, next, hash(next))
        .await
        .expect("publish with two workers");
    let manifest_after = after.manifest().expect("manifest").tables[&DatabaseId::Enhance].clone();
    assert_eq!(manifest_after.shards.len(), 3);
    for (old, new) in manifest_before.shards.iter().zip(&manifest_after.shards) {
        assert_eq!(old.shard_id, new.shard_id);
        let expected_owner = if new.shard_id < SHARDS_PER_WORKER {
            "worker-1"
        } else {
            "worker-2"
        };
        assert_eq!(
            new.worker, expected_owner,
            "owner of shard {}",
            new.shard_id
        );
        if old.sealed {
            assert!(new.sealed);
            assert_eq!(
                old.rows_sha256, new.rows_sha256,
                "sealed shard {} was rebuilt with different rows",
                new.shard_id
            );
        }
    }

    // Every client-visible byte is unchanged for positions the old pool served.
    let session_after = session(&after);
    for (&position, expected) in probes.iter().zip(&answers_before) {
        assert_eq!(
            &fetch(&after, &session_after, position).await,
            expected,
            "position {position} after"
        );
    }
    assert_eq!(fetch(&after, &session_after, count).await, record(count));

    // The moved shard now lives on worker-2 and is gone from worker-1's active
    // assignment: a query that reaches worker-1 for shard 2 is refused.
    let generation = after.metadata().expect("metadata").generation;
    let refused = worker_1
        .evaluate_local(
            DatabaseId::Enhance,
            enhance_pir_server::wire::EvaluateRequest {
                generation,
                shards: (0..3)
                    .map(|shard_id| enhance_pir_server::wire::ShardQuery {
                        shard_id,
                        coefficients: vec![0; ENHANCE_LAYOUT.shard_rows],
                    })
                    .collect(),
            },
        )
        .await
        .expect_err("worker-1 no longer owns shard 2");
    assert!(
        refused.contains("complete active shard assignment"),
        "{refused}"
    );
}
