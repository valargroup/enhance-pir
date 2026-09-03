//! The 792-byte action record is the one layout that can never change cheaply:
//! widening it later rebuilds every sealed shard. Pin every offset here, and
//! pin that a record survives the journal and the padded shard read unchanged.

use memo_pir::store::RecordJournal;
use memo_pir::types::{
    ActionRecord, ActionRecordParts, DatabaseId, ACTION_LAYOUT, RECORDS_PER_ROW, RECORD_BYTES,
    ROW_BYTES, SHARD_ROWS,
};

fn sample(seed: u8) -> ActionRecord {
    ActionRecord::from_parts(ActionRecordParts {
        nullifier: [seed; 32],
        ephemeral_key: [seed.wrapping_add(1); 32],
        enc_ciphertext: [seed.wrapping_add(2); 580],
        cv_net: [seed.wrapping_add(3); 32],
        out_ciphertext: [seed.wrapping_add(4); 80],
        txid: [seed.wrapping_add(5); 32],
        height: 3_428_143 + u32::from(seed),
    })
}

#[test]
fn field_offsets_are_pinned() {
    let record = sample(10);
    let bytes = record.as_bytes();
    assert_eq!(bytes.len(), 792);
    assert_eq!(&bytes[0..32], &[10; 32]);
    assert_eq!(&bytes[32..64], &[11; 32]);
    assert_eq!(&bytes[64..644], &[12; 580][..]);
    assert_eq!(&bytes[644..676], &[13; 32]);
    assert_eq!(&bytes[676..756], &[14; 80][..]);
    assert_eq!(&bytes[756..788], &[15; 32]);
    assert_eq!(&bytes[788..792], &(3_428_153u32).to_le_bytes());
    assert_eq!(record.height(), 3_428_153);
}

#[test]
fn records_survive_the_journal_and_padded_shard_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecordJournal::open(dir.path(), DatabaseId::Action, ACTION_LAYOUT).expect("open");
    let records: Vec<ActionRecord> = (0..(RECORDS_PER_ROW as u8 + 3)).map(sample).collect();
    store
        .append_block(3_428_143, "hash".to_string(), &records)
        .expect("append");
    drop(store);

    let store = RecordJournal::open(dir.path(), DatabaseId::Action, ACTION_LAYOUT).expect("reopen");
    let shard = store.read_shard_rows(0).expect("shard");
    assert_eq!(shard.len(), SHARD_ROWS * ROW_BYTES);
    for (position, expected) in records.iter().enumerate() {
        let row = position / RECORDS_PER_ROW;
        let slot = position % RECORDS_PER_ROW;
        let start = row * ROW_BYTES + slot * RECORD_BYTES;
        assert_eq!(&shard[start..start + RECORD_BYTES], expected.as_bytes());
    }
    let padding_start = records.len() * RECORD_BYTES;
    assert!(shard[padding_start..].iter().all(|byte| *byte == 0));
}
