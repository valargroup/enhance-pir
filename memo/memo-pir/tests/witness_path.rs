//! A witness for any position is two opaque row queries plus the public cap,
//! and it reaches the anchor's tree root.

use memo_pir::client::TableSession;
use memo_pir::coordinator::{Anchor, CoordinatorState, TableJournal, TableSetup, WorkerTarget};
use memo_pir::ingest::Journals;
use memo_pir::types::{ActionRecord, ActionRecordParts, DatabaseId};
use memo_pir::witness::{apply_frontier_update, decode_frontier, decompose, reconstruct};
use memo_pir::worker::WorkerState;
use memo_pir::zakura::{CanonicalBlock, CanonicalTx};

/// A block of `actions` actions split into transactions of at most 200.
fn block(height: u64, first_position: u64, actions: usize) -> CanonicalBlock {
    let mut records = Vec::new();
    let mut transactions = Vec::new();
    let mut nullifiers = Vec::new();
    let mut cmxs = Vec::new();
    for i in 0..actions as u64 {
        let position = first_position + i;
        let mut cmx = [0u8; 32];
        cmx[..8].copy_from_slice(&(position + 1).to_le_bytes());
        let mut nf = [0xAAu8; 32];
        nf[..8].copy_from_slice(&position.to_le_bytes());
        records.push(ActionRecord::from_parts(ActionRecordParts {
            nullifier: nf,
            ephemeral_key: [2; 32],
            enc_ciphertext: [3; 580],
            cmx,
            cv_net: [4; 32],
            out_ciphertext: [5; 80],
            txid: [1; 32],
            height: height as u32,
        }));
        nullifiers.push(nf);
        cmxs.push(cmx);
        if cmxs.len() == 200 || i + 1 == actions as u64 {
            transactions.push(CanonicalTx {
                txid: [transactions.len() as u8; 32],
                first_action_index: records.len() - cmxs.len(),
                nullifiers: std::mem::take(&mut nullifiers),
                cmxs: std::mem::take(&mut cmxs),
            });
        }
    }
    CanonicalBlock {
        height,
        hash: format!("{height:064x}"),
        records,
        transactions,
        tree_size: first_position + actions as u64,
    }
}

#[tokio::test]
async fn two_queries_and_the_cap_reach_the_anchor_root_across_a_shard_boundary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut journals = Journals::open(dir.path()).expect("journals");
    // 65,536 + 300 leaves over three blocks: shard 0 seals, shard 1 is frontier.
    journals.append_block(&block(3_428_143, 0, 60_000)).unwrap();
    journals
        .append_block(&block(3_428_144, 60_000, 5_836))
        .unwrap();
    let anchor = 3_428_144;
    let cap = journals.witness_cap(anchor).expect("cap");
    assert_eq!(cap.tree_size, 65_836);
    assert_eq!(cap.shard_roots.len(), 2);
    assert!(cap.frontier_subshard_root.is_some());

    let worker = WorkerState::new(dir.path().join("worker")).expect("worker");
    let state = CoordinatorState::new(
        [DatabaseId::Witness, DatabaseId::WitnessRoots]
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
    let leaves = TableJournal::new(DatabaseId::Witness, &journals.witness).unwrap();
    let roots = TableJournal::new(DatabaseId::WitnessRoots, &journals.witness_roots).unwrap();
    state
        .publish(
            &[&leaves, &roots],
            Anchor {
                height: anchor,
                hash: "aa".repeat(32),
                witness_cap: Some(cap.clone()),
                frontier: journals.frontier_updates(anchor - 1, anchor).unwrap(),
                ..Anchor::default()
            },
        )
        .await
        .expect("publish");
    let manifest = state.manifest().expect("manifest");
    assert_eq!(manifest.anchor_tree_root, cap.tree_root);
    let session = |table| {
        TableSession::new(
            manifest.clone(),
            table,
            state.params(table).unwrap(),
            &state.public_params(table).unwrap(),
        )
        .expect("session")
    };
    let leaves_session = session(DatabaseId::Witness);
    let roots_session = session(DatabaseId::WitnessRoots);

    let mut witnesses = Vec::new();
    for position in [0u64, 255, 256, 65_535, 65_536, 65_835] {
        let (shard, subshard, _) = decompose(position);
        let q = leaves_session.prepare_row(subshard as usize).unwrap();
        let r = state
            .answer_query(DatabaseId::Witness, q.body())
            .await
            .unwrap();
        let leaves_row = leaves_session.decode(q, &r).unwrap();
        let q = roots_session.prepare_row(shard as usize).unwrap();
        let r = state
            .answer_query(DatabaseId::WitnessRoots, q.body())
            .await
            .unwrap();
        let roots_row = roots_session.decode(q, &r).unwrap();
        let witness = reconstruct(position, &leaves_row, &roots_row, &cap)
            .unwrap_or_else(|e| panic!("position {position}: {e}"));
        assert_eq!(hex::encode(witness.root), cap.tree_root);
        witnesses.push(witness);
    }

    // A later block: sealed-shard witnesses move to the new root by the
    // frontier update alone; the one in the frontier sub-shard must re-fetch.
    journals
        .append_block(&block(3_428_145, 65_836, 40))
        .unwrap();
    let new_cap = journals.witness_cap(3_428_145).unwrap();
    let update = journals
        .frontier_updates(3_428_145, 3_428_145)
        .unwrap()
        .remove(0);
    let nodes: Vec<[u8; 32]> = update
        .rightmost_nodes
        .iter()
        .map(|h| hex::decode(h).unwrap().try_into().unwrap())
        .collect();
    let nodes: [[u8; 32]; 32] = nodes.try_into().unwrap();
    assert!(decode_frontier(&nodes.concat()).is_some());
    for witness in &mut witnesses {
        let result = apply_frontier_update(witness, &nodes, update.tree_size, update.height);
        if witness.position >= 65_536 + 256 {
            assert!(result.is_err(), "frontier sub-shard witness must re-fetch");
        } else {
            result.unwrap();
            assert_eq!(
                hex::encode(witness.root),
                new_cap.tree_root,
                "position {}",
                witness.position
            );
        }
    }
}
