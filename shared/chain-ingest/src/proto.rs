//! Re-exported protobuf types generated from `compact_formats.proto` and `service.proto`.

pub mod compact_formats {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");
}

pub use compact_formats::*;

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    /// Ironwood data rides on field numbers that are not adjacent to Orchard's
    /// and are easy to get wrong when resyncing `proto/` against upstream
    /// lightwallet-protocol. These tests decode bytes hand-built with the
    /// expected tags, so a renumbering fails here rather than silently
    /// producing an empty PIR database.
    ///
    /// Reference: lightwallet-protocol v0.5.0.
    fn len_delimited(tag: u32, body: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, &mut buf);
        prost::encoding::encode_varint(body.len() as u64, &mut buf);
        buf.extend_from_slice(body);
        buf
    }

    #[test]
    fn compact_tx_decodes_ironwood_actions_at_tag_nine() {
        // One CompactOrchardAction { nullifier: [0xAB; 32] }.
        let action = len_delimited(1, &[0xAB; 32]);

        let orchard_field = len_delimited(6, &action);
        let ironwood_field = len_delimited(9, &action);

        let tx = CompactTx::decode(ironwood_field.as_slice()).unwrap();
        assert_eq!(
            tx.ironwood_actions.len(),
            1,
            "tag 9 must be ironwoodActions"
        );
        assert!(tx.actions.is_empty());
        assert_eq!(tx.ironwood_actions[0].nullifier, vec![0xAB; 32]);

        // Tag 6 must remain Orchard, so the two pools cannot be conflated.
        let tx = CompactTx::decode(orchard_field.as_slice()).unwrap();
        assert_eq!(tx.actions.len(), 1, "tag 6 must stay Orchard actions");
        assert!(tx.ironwood_actions.is_empty());
    }

    #[test]
    fn chain_metadata_decodes_ironwood_tree_size_at_tag_three() {
        let mut buf = Vec::new();
        prost::encoding::encode_key(3, prost::encoding::WireType::Varint, &mut buf);
        prost::encoding::encode_varint(134_545, &mut buf);

        let meta = ChainMetadata::decode(buf.as_slice()).unwrap();
        assert_eq!(meta.ironwood_commitment_tree_size, 134_545);
        assert_eq!(meta.orchard_commitment_tree_size, 0);
    }

    #[test]
    fn tree_state_decodes_ironwood_tree_at_tag_seven() {
        let state = TreeState::decode(len_delimited(7, b"iw").as_slice()).unwrap();
        assert_eq!(state.ironwood_tree, "iw");
        assert!(state.orchard_tree.is_empty());
    }

    #[test]
    fn shielded_protocol_ironwood_is_two() {
        // `get_subtree_roots` is called with this raw i32.
        assert_eq!(ShieldedProtocol::Ironwood as i32, 2);
        assert_eq!(ShieldedProtocol::Orchard as i32, 1);
        assert_eq!(PoolType::Ironwood as i32, 4);
    }
}
