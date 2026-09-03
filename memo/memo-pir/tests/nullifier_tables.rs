//! Cold and warm nullifier tables built from the spend log, published beside
//! the other tables, and answered through the same opaque query path.

use memo_pir::client::TableSession;
use memo_pir::coordinator::{Anchor, CoordinatorState, TableJournal, TableSetup, WorkerTarget};
use memo_pir::ingest::{Journals, NullifierEntry};
use memo_pir::nullifier::{hash_to_bucket, scan_bucket, NullifierTables};
use memo_pir::types::{ActionRecord, ActionRecordParts, DatabaseId};
use memo_pir::worker::WorkerState;
use memo_pir::zakura::{CanonicalBlock, CanonicalTx};

fn block(height: u64, first_position: u64, actions: usize) -> CanonicalBlock {
    let mut records = Vec::new();
    let mut nullifiers = Vec::new();
    let mut cmxs = Vec::new();
    for i in 0..actions as u64 {
        let position = first_position + i;
        let mut cmx = [0u8; 32];
        cmx[..8].copy_from_slice(&(position + 1).to_le_bytes());
        let mut nf = [0u8; 32];
        nf[..8].copy_from_slice(&(height * 1_000 + i).to_le_bytes());
        nf[8..16].copy_from_slice(&(position * 7 + 3).to_le_bytes());
        records.push(ActionRecord::from_parts(ActionRecordParts {
            nullifier: nf,
            ephemeral_key: [2; 32],
            enc_ciphertext: [3; 580],
            cmx,
            cv_net: [4; 32],
            out_ciphertext: [5; 80],
            txid: [height as u8; 32],
            height: height as u32,
        }));
        nullifiers.push(nf);
        cmxs.push(cmx);
    }
    CanonicalBlock {
        height,
        hash: format!("{height:064x}"),
        records,
        transactions: vec![CanonicalTx {
            txid: [height as u8; 32],
            first_action_index: 0,
            nullifiers,
            cmxs,
        }],
        tree_size: first_position + actions as u64,
    }
}

#[tokio::test]
async fn cold_and_warm_tables_answer_spends_on_either_side_of_the_checkpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut journals = Journals::open(dir.path()).expect("journals");
    let mut position = 0;
    for height in 3_428_143..3_428_143 + 40 {
        journals
            .append_block(&block(height, position, 5))
            .expect("append");
        position += 5;
    }
    let checkpoint = 3_428_143 + 19;
    let tables = NullifierTables::build(&journals.nullifiers, checkpoint).expect("build");
    assert_eq!(tables.cold.entries(), 100);
    assert_eq!(tables.warm.entries(), 100);

    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = CoordinatorState::new(
        [DatabaseId::Action, DatabaseId::NfCold, DatabaseId::NfWarm]
            .into_iter()
            .map(|table| TableSetup {
                table,
                pool: vec![WorkerTarget::Embedded {
                    name: "embedded".to_string(),
                    state: worker.clone(),
                }],
            })
            .collect(),
    )
    .expect("coordinator");
    let action = TableJournal::new(DatabaseId::Action, &journals.action).unwrap();
    state
        .publish(
            &[&action, &tables.cold, &tables.warm],
            Anchor {
                height: 3_428_143 + 39,
                hash: "aa".repeat(32),
                cold_checkpoint_height: checkpoint,
                ..Anchor::default()
            },
        )
        .await
        .expect("publish");
    let manifest = state.manifest().expect("manifest");
    assert_eq!(manifest.cold_checkpoint_height, checkpoint);
    assert_eq!(
        manifest.tables[&DatabaseId::NfCold].positions,
        tables.cold.num_buckets() as u64
    );

    // A spend before the checkpoint is in cold, one after it in warm; each
    // is found by one opaque row query against its bucket and absent from
    // the other table.
    let cold_spend = block(3_428_143 + 3, 15, 5).transactions[0].nullifiers[2];
    let warm_spend = block(3_428_143 + 30, 150, 5).transactions[0].nullifiers[1];
    let sessions = [DatabaseId::NfCold, DatabaseId::NfWarm].map(|table| {
        TableSession::new(
            manifest.clone(),
            table,
            state.params(table).unwrap(),
            &state.public_params(table).unwrap(),
        )
        .expect("session")
    });
    for (nf, expect_cold, expect_height) in [
        (cold_spend, true, 3_428_143 + 3),
        (warm_spend, false, 3_428_143 + 30),
    ] {
        for session in &sessions {
            let buckets = session.positions() as usize;
            let query = session
                .prepare_row(hash_to_bucket(&nf, buckets))
                .expect("query");
            let response = state
                .answer_query(session.table(), query.body())
                .await
                .expect("answered");
            let row = session.decode(query, &response).expect("decodes");
            let found = scan_bucket(&row, &nf);
            let is_cold = session.table() == DatabaseId::NfCold;
            if is_cold == expect_cold {
                let entry: NullifierEntry = found.expect("present in its table");
                assert_eq!(entry.spend_height, expect_height as u32);
                assert_eq!(entry.action_count, 5);
            } else {
                assert!(found.is_none(), "found in the wrong table");
            }
        }
    }
}
