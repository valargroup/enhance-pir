//! Turning an accepted Zcash block into its filter element set.
//!
//! The rules, from the profile:
//!
//! 1. every nonempty transparent output script, including coinbase outputs,
//!    except a script whose *first* opcode byte is `OP_RETURN`;
//! 2. the nonempty previous-output **locking** script of every transparent
//!    input, excluding the coinbase input — not the unlocking script, not the
//!    transaction id, not the outpoint, and not any address text;
//! 3. deduplication by raw script bytes across the whole block;
//! 4. an unresolvable previous output is a construction error.
//!
//! Elements come from the raw parsed block, not from any helper that filters to
//! the script types some other component happens to support. A shared public
//! filter that silently omitted unusual scripts would report "no activity" to a
//! wallet that does have activity.

use std::sync::Arc;
use transparent_filter::ScriptBytes;
use zakura_chain::transaction::Transaction;
use zakura_chain::transparent::{Input, OutPoint};

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("previous output {0} is unavailable; refusing to build a partial filter")]
    MissingPreviousOutput(String),
    #[error("previous output lookup failed for {outpoint}: {source}")]
    Lookup {
        outpoint: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Renders an outpoint for diagnostics. Display order, as a person would look
/// it up.
pub fn outpoint_label(outpoint: &OutPoint) -> String {
    let mut bytes = outpoint.hash.0;
    bytes.reverse();
    format!("{}:{}", hex::encode(bytes), outpoint.index)
}

/// Supplies the locking script of an output created before this block.
pub trait PreviousOutputs {
    /// Returns the locking script of `outpoint`, or `None` if it is not known.
    ///
    /// `None` aborts the block. It must never be treated as an empty script:
    /// that would publish a filter missing a real spend, and a wallet checking
    /// it would be told it had no activity.
    fn lock_script(
        &mut self,
        outpoint: &OutPoint,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
}

/// The element set for one block.
///
/// Same-block spends resolve from this block's own outputs before the resolver
/// is consulted, so a transaction spending an output created earlier in the
/// same block needs no lookup.
///
/// Takes the transaction list rather than the whole block: the block hash is
/// not an element and is applied separately when the filter is encoded, so
/// passing the header here would suggest a dependency that does not exist.
pub fn extract_elements(
    transactions: &[Arc<Transaction>],
    previous: &mut impl PreviousOutputs,
) -> Result<Vec<ScriptBytes>, ExtractError> {
    // Every output this block creates, keyed by outpoint, for same-block spends.
    let mut created: std::collections::HashMap<OutPoint, Vec<u8>> =
        std::collections::HashMap::new();
    for transaction in transactions {
        let txid = transaction.hash();
        for (index, output) in transaction.outputs().iter().enumerate() {
            created.insert(
                OutPoint {
                    hash: txid,
                    index: index as u32,
                },
                output.lock_script.as_raw_bytes().to_vec(),
            );
        }
    }

    let mut elements: Vec<ScriptBytes> = Vec::new();

    for transaction in transactions {
        // Outputs. Coinbase outputs are included; a leading OP_RETURN is not.
        for output in transaction.outputs() {
            let script = ScriptBytes::new(output.lock_script.as_raw_bytes().to_vec());
            if script.is_filter_element() {
                elements.push(script);
            }
        }
        // Inputs. The coinbase input spends nothing and is skipped explicitly.
        for input in transaction.inputs() {
            let outpoint = match input {
                Input::Coinbase { .. } => continue,
                Input::PrevOut { outpoint, .. } => outpoint,
            };
            let bytes = match created.get(outpoint) {
                Some(bytes) => bytes.clone(),
                None => previous
                    .lock_script(outpoint)
                    .map_err(|source| ExtractError::Lookup {
                        outpoint: outpoint_label(outpoint),
                        source,
                    })?
                    .ok_or_else(|| ExtractError::MissingPreviousOutput(outpoint_label(outpoint)))?,
            };
            // A previous output can legitimately have an empty script; it is
            // then not an element. It cannot legitimately be OP_RETURN, since
            // such an output is unspendable, but the same rule is applied
            // rather than assuming well-formed history.
            let script = ScriptBytes::new(bytes);
            if script.is_filter_element() {
                elements.push(script);
            }
        }
    }

    // Deduplicate by raw bytes, preserving nothing about order: the encoder
    // sorts by mapped value anyway.
    elements.sort();
    elements.dedup();
    Ok(elements)
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A resolver backed by a fixed map, for tests.
    #[derive(Default)]
    pub struct MapPreviousOutputs {
        pub scripts: std::collections::HashMap<OutPoint, Vec<u8>>,
        pub lookups: usize,
    }

    impl PreviousOutputs for MapPreviousOutputs {
        fn lock_script(
            &mut self,
            outpoint: &OutPoint,
        ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
            self.lookups += 1;
            Ok(self.scripts.get(outpoint).cloned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::MapPreviousOutputs;
    use super::*;
    use zakura_chain::amount::Amount;
    use zakura_chain::serialization::ZcashDeserialize;
    use zakura_chain::transparent::{Output, Script};

    fn script(bytes: Vec<u8>) -> Script {
        Script::new(&bytes)
    }

    fn p2pkh(tag: u8) -> Vec<u8> {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&[tag; 20]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        bytes
    }

    fn output(bytes: Vec<u8>) -> Output {
        Output {
            value: Amount::try_from(1000).unwrap(),
            lock_script: script(bytes),
        }
    }

    fn transaction(inputs: Vec<Input>, outputs: Vec<Output>) -> Arc<Transaction> {
        Arc::new(Transaction::V1 {
            inputs,
            outputs,
            lock_time: zakura_chain::transaction::LockTime::unlocked(),
        })
    }

    /// A coinbase input, built by deserializing its consensus encoding.
    ///
    /// `CoinbaseData`'s constructor is test-gated inside zebra-chain, so the
    /// variant cannot be built directly from here. Going through the real
    /// parser is better anyway: it is the same path production takes, so a
    /// change in how coinbase inputs are recognised would show up in these
    /// tests rather than only in production.
    fn coinbase_input() -> Input {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0u8; 32]);
        bytes.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        // BIP34-style height for 3,428,143: opcode 0x03 then three LE bytes,
        // followed by arbitrary miner data.
        let data = [0x03u8, 0x2f, 0x4f, 0x34, 0xaa, 0xbb];
        bytes.push(data.len() as u8);
        bytes.extend_from_slice(&data);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let input = Input::zcash_deserialize(bytes.as_slice()).expect("coinbase input");
        assert!(
            matches!(input, Input::Coinbase { .. }),
            "not parsed as coinbase"
        );
        input
    }

    fn prevout_input(outpoint: OutPoint) -> Input {
        Input::PrevOut {
            outpoint,
            unlock_script: script(vec![0x47, 0x30]),
            sequence: 0,
        }
    }

    fn elements_of(
        transactions: &[Arc<Transaction>],
        previous: &mut MapPreviousOutputs,
    ) -> Vec<Vec<u8>> {
        extract_elements(transactions, previous)
            .expect("extract")
            .into_iter()
            .map(|script| script.0)
            .collect()
    }

    fn contains(elements: &[Vec<u8>], script: &[u8]) -> bool {
        elements.iter().any(|element| element == script)
    }

    #[test]
    fn an_empty_block_has_no_elements() {
        let mut previous = MapPreviousOutputs::default();
        assert!(elements_of(&[], &mut previous).is_empty());
    }

    #[test]
    fn a_receive_only_block_yields_its_output_scripts() {
        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![output(p2pkh(1)), output(p2pkh(2))],
        )];
        let mut previous = MapPreviousOutputs::default();
        let elements = elements_of(&transactions, &mut previous);
        assert_eq!(elements.len(), 2);
        assert!(contains(&elements, &p2pkh(1)));
        // A coinbase receive is included like any other output.
        assert!(contains(&elements, &p2pkh(2)));
        assert_eq!(previous.lookups, 0, "coinbase input must not be looked up");
    }

    #[test]
    fn a_spend_with_no_matching_new_output_still_yields_the_spent_script() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 0,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            // Change goes to an unrelated script.
            vec![output(p2pkh(50))],
        )];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(spent, p2pkh(7));
        let elements = elements_of(&transactions, &mut previous);
        assert!(contains(&elements, &p2pkh(7)), "spent script missing");
        assert!(contains(&elements, &p2pkh(50)));
        assert_eq!(previous.lookups, 1);
    }

    #[test]
    fn a_same_block_spend_resolves_without_a_lookup() {
        let funding = transaction(vec![coinbase_input()], vec![output(p2pkh(11))]);
        let funding_id = funding.hash();
        let spending = transaction(
            vec![prevout_input(OutPoint {
                hash: funding_id,
                index: 0,
            })],
            vec![output(p2pkh(12))],
        );
        let mut previous = MapPreviousOutputs::default();
        let elements = elements_of(&[funding, spending], &mut previous);
        assert_eq!(
            previous.lookups, 0,
            "same-block spend must not need a lookup"
        );
        assert!(contains(&elements, &p2pkh(11)));
        assert!(contains(&elements, &p2pkh(12)));
    }

    #[test]
    fn op_return_outputs_are_excluded_but_a_later_0x6a_byte_is_not() {
        // A script whose payload happens to contain 0x6a is a normal script.
        let mut embedded = vec![0x76, 0xa9, 0x14];
        embedded.extend_from_slice(&[0x6a; 20]);
        embedded.extend_from_slice(&[0x88, 0xac]);

        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![
                output(vec![0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]),
                output(embedded.clone()),
                output(p2pkh(3)),
            ],
        )];
        let mut previous = MapPreviousOutputs::default();
        let elements = elements_of(&transactions, &mut previous);
        assert!(!elements
            .iter()
            .any(|element| element.first() == Some(&0x6a)));
        assert!(
            contains(&elements, &embedded),
            "embedded 0x6a wrongly excluded"
        );
        assert!(contains(&elements, &p2pkh(3)));
        assert_eq!(elements.len(), 2);
    }

    #[test]
    fn empty_scripts_are_not_elements() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([4; 32]),
            index: 1,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            vec![output(vec![]), output(p2pkh(6))],
        )];
        let mut previous = MapPreviousOutputs::default();
        // An empty previous-output script is resolvable and simply not an element.
        previous.scripts.insert(spent, vec![]);
        let elements = elements_of(&transactions, &mut previous);
        assert_eq!(elements, vec![p2pkh(6)]);
    }

    #[test]
    fn a_repeated_script_appears_once() {
        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![output(p2pkh(8)), output(p2pkh(8)), output(p2pkh(8))],
        )];
        let mut previous = MapPreviousOutputs::default();
        assert_eq!(elements_of(&transactions, &mut previous), vec![p2pkh(8)]);
    }

    #[test]
    fn a_nonstandard_script_is_still_an_element() {
        // Not any recognised template, and not address-decodable.
        let odd = vec![0x51, 0x52, 0x53, 0xae, 0xff, 0x00, 0x01];
        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![output(odd.clone())],
        )];
        let mut previous = MapPreviousOutputs::default();
        assert_eq!(elements_of(&transactions, &mut previous), vec![odd]);
    }

    #[test]
    fn a_missing_previous_output_blocks_publication() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([5; 32]),
            index: 2,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            vec![output(p2pkh(9))],
        )];
        let mut previous = MapPreviousOutputs::default();
        let error = extract_elements(&transactions, &mut previous).unwrap_err();
        assert!(
            matches!(error, ExtractError::MissingPreviousOutput(_)),
            "an unresolved previous output must abort the block, not be treated as empty"
        );
    }

    #[test]
    fn the_element_set_is_order_independent() {
        let a = transaction(vec![coinbase_input()], vec![output(p2pkh(1))]);
        let b = transaction(vec![coinbase_input()], vec![output(p2pkh(2))]);
        let mut previous = MapPreviousOutputs::default();
        let forward = elements_of(&[a.clone(), b.clone()], &mut previous);
        let backward = elements_of(&[b, a], &mut previous);
        assert_eq!(forward, backward);
    }
}
