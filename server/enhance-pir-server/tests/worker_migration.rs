//! Worker-group migration and placement. Replica membership may change without
//! moving shards; appending a group only claims shards at or beyond the stable
//! six-shard capacity boundary.

use enhance_pir::client::{record_in_row, QuerySession};
use enhance_pir_server::coordinator::{CoordinatorState, TableSetup, WorkerGroup, WorkerTarget};
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::{
    group_index_for_shard, DatabaseId, EnhanceRecord, EnhanceRecordParts, ENHANCE_LAYOUT,
    SHARDS_PER_GROUP, SHARD_POSITIONS,
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
        has_transparent_inputs: false,
        has_transparent_outputs: false,
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

fn coordinator(groups: &[(&str, &WorkerState)]) -> CoordinatorState {
    CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: groups
            .iter()
            .map(|(name, state)| WorkerGroup {
                name: (*name).to_string(),
                replicas: vec![WorkerTarget::Embedded {
                    name: format!("{name}-replica"),
                    state: (*state).clone(),
                }],
            })
            .collect(),
    }])
    .expect("coordinator")
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

#[test]
fn placement_rule_for_the_migration_is_as_documented() {
    // One logical group owns everything, however many shards exist.
    for shard in 0..10 {
        assert_eq!(group_index_for_shard(shard, 1), Some(0));
    }
    // Two groups: shards below the quantum stay, the rest move to the new one
    // and shards beyond its range are unowned until a third worker is appended.
    for shard in 0..SHARDS_PER_GROUP {
        assert_eq!(group_index_for_shard(shard, 2), Some(0));
    }
    for shard in SHARDS_PER_GROUP..2 * SHARDS_PER_GROUP {
        assert_eq!(group_index_for_shard(shard, 2), Some(1));
    }
    assert_eq!(group_index_for_shard(2 * SHARDS_PER_GROUP, 2), None);
    // Every append after the first moves nothing.
    for shard in 0..2 * SHARDS_PER_GROUP {
        assert_eq!(
            group_index_for_shard(shard, 2),
            group_index_for_shard(shard, 3)
        );
    }
}

/// One group serving three shards, then a coordinator restart with a second
/// group appended. All shards remain below the first group's capacity and
/// every answer and digest remains byte-identical.
#[tokio::test]
async fn appending_a_group_does_not_move_existing_in_range_shards() {
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

    // Before: the original production shape, one group owns every shard.
    let before = coordinator(&[("worker-1", &worker_1)]);
    before
        .publish_from_store(&store, height, hash(height))
        .await
        .expect("publish with one group");
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
        assert_eq!(new.worker, "worker-1", "owner of shard {}", new.shard_id);
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

    // The unused second group is neither prepared nor activated.
    assert_eq!(worker_2.cached_shard_count().await, 0);
    let generation = after.session().expect("session").generation.generation;
    let refused = worker_2
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
        .expect_err("worker-2 has no active assignment");
    assert!(refused.contains("generation mismatch"), "{refused}");
}
