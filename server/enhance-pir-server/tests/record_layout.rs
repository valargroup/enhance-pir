//! The 724-byte Enhance record is the one layout that can never change cheaply:
//! widening it later rebuilds every sealed shard. Pin every offset here, and
//! pin that a record survives the journal and the padded shard read unchanged.

use enhance_pir::types::{
    EnhanceRecord, EnhanceRecordParts, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES, SHARD_ROWS,
};
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::{DatabaseId, ENHANCE_LAYOUT};

fn sample(seed: u8) -> EnhanceRecord {
    EnhanceRecord::from_parts(EnhanceRecordParts {
        ephemeral_key: [seed.wrapping_add(1); 32],
        enc_ciphertext: [seed.wrapping_add(2); 580],
        cv_net: [seed.wrapping_add(3); 32],
        out_ciphertext: [seed.wrapping_add(4); 80],
    })
}

#[test]
fn field_offsets_are_pinned() {
    let record = sample(10);
    let bytes = record.as_bytes();
    assert_eq!(bytes.len(), 724);
    assert_eq!(&bytes[0..32], &[11; 32]);
    assert_eq!(&bytes[32..612], &[12; 580][..]);
    assert_eq!(&bytes[612..644], &[13; 32]);
    assert_eq!(&bytes[644..724], &[14; 80][..]);
}

#[test]
fn records_survive_the_journal_and_padded_shard_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).expect("open");
    let records: Vec<EnhanceRecord> = (0..(RECORDS_PER_ROW as u8 + 3)).map(sample).collect();
    store
        .append_block(3_428_143, "hash".to_string(), &records)
        .expect("append");
    drop(store);

    let store =
        RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).expect("reopen");
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
