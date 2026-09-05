//! Resolving the locking script of a spent output.
//!
//! Most spends are recent, so a bounded cache of the outputs created by the
//! blocks just ingested answers the large majority of lookups without any RPC.
//! The rest go to `getrawtransaction`, which an archive node answers from its
//! transaction index. This is the strategy the Python collector already
//! validated against this node over a full mainnet day
//! (`tools/transparent_pir_collect.py`).
//!
//! A lookup that cannot be answered is an error. It is never an empty script:
//! a filter missing a real spend would tell a wallet it had no activity.

use crate::extract::{outpoint_label, PreviousOutputs};
use crate::zakura::ZakuraClient;
use std::collections::{HashMap, VecDeque};
use zakura_chain::transaction::Transaction;
use zakura_chain::transparent::{Input, OutPoint};

/// Previous transactions requested per JSON-RPC batch.
///
/// Matches the batch size the Python collector uses against this node.
pub const PREVOUT_BATCH: usize = 16;

/// Number of recently seen transactions whose outputs are retained.
///
/// Sized so that ordinary spends of recent outputs hit the cache during
/// backfill. It bounds memory rather than guaranteeing a hit rate: a miss is
/// correct, just slower.
pub const DEFAULT_CACHE_TRANSACTIONS: usize = 200_000;

/// Outputs of transactions seen recently, with insertion-order eviction.
pub struct OutputCache {
    scripts: HashMap<OutPoint, Vec<u8>>,
    /// Transaction ids in insertion order, for eviction.
    order: VecDeque<(zakura_chain::transaction::Hash, u32)>,
    capacity: usize,
}

impl OutputCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            scripts: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Records every output of a transaction.
    pub fn insert_transaction(&mut self, transaction: &Transaction) {
        let txid = transaction.hash();
        let outputs = transaction.outputs();
        for (index, output) in outputs.iter().enumerate() {
            self.scripts.insert(
                OutPoint {
                    hash: txid,
                    index: index as u32,
                },
                output.lock_script.as_raw_bytes().to_vec(),
            );
        }
        if !outputs.is_empty() {
            self.order.push_back((txid, outputs.len() as u32));
            while self.order.len() > self.capacity {
                if let Some((evicted, count)) = self.order.pop_front() {
                    for index in 0..count {
                        self.scripts.remove(&OutPoint {
                            hash: evicted,
                            index,
                        });
                    }
                }
            }
        }
    }

    pub fn get(&self, outpoint: &OutPoint) -> Option<&Vec<u8>> {
        self.scripts.get(outpoint)
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }
    pub fn is_empty(&self) -> bool {
        self.scripts.is_empty()
    }
}

/// Resolver backed by the cache first and Zakura second.
///
/// Holds a Tokio runtime handle because `extract_elements` is synchronous: the
/// element set for a block is built in one pass, and threading async through it
/// would buy nothing when the node is on loopback.
pub struct ZakuraPreviousOutputs<'a> {
    client: &'a ZakuraClient,
    runtime: tokio::runtime::Handle,
    cache: &'a mut OutputCache,
    /// Lookups that had to go to the node.
    pub rpc_lookups: u64,
    /// Lookups answered from the cache.
    pub cache_hits: u64,
}

impl<'a> ZakuraPreviousOutputs<'a> {
    pub fn new(
        client: &'a ZakuraClient,
        runtime: tokio::runtime::Handle,
        cache: &'a mut OutputCache,
    ) -> Self {
        Self {
            client,
            runtime,
            cache,
            rpc_lookups: 0,
            cache_hits: 0,
        }
    }
}

impl PreviousOutputs for ZakuraPreviousOutputs<'_> {
    fn lock_script(
        &mut self,
        outpoint: &OutPoint,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(script) = self.cache.get(outpoint) {
            self.cache_hits += 1;
            return Ok(Some(script.clone()));
        }
        self.rpc_lookups += 1;
        let mut txid = outpoint.hash.0;
        txid.reverse();
        let txid = hex::encode(txid);
        let transaction = self
            .runtime
            .block_on(self.client.transaction(&txid))
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        let Some(transaction) = transaction else {
            return Ok(None);
        };
        // Cache the whole transaction: a block that spends one of its outputs
        // often spends several.
        self.cache.insert_transaction(&transaction);
        let script = transaction
            .outputs()
            .get(outpoint.index as usize)
            .map(|output| output.lock_script.as_raw_bytes().to_vec());
        if script.is_none() {
            tracing::warn!(
                outpoint = %outpoint_label(outpoint),
                outputs = transaction.outputs().len(),
                "previous transaction has no output at this index"
            );
        }
        Ok(script)
    }
}

/// Resolves, in batches, every previous output this block will need.
///
/// A pre-pass rather than lazy lookups: resolving during extraction costs one
/// round trip per missing transaction, which dominates ingest as soon as the
/// node is not on loopback. After this returns, extraction finds everything in
/// the cache and issues no further requests.
///
/// Outputs created earlier in this same block are already in the cache and are
/// not requested. A transaction the node cannot supply is left absent, so
/// extraction still fails closed rather than treating it as an empty script.
pub async fn prefetch_previous_outputs(
    zakura: &ZakuraClient,
    cache: &mut OutputCache,
    transactions: &[std::sync::Arc<Transaction>],
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut wanted: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for transaction in transactions {
        for input in transaction.inputs() {
            let Input::PrevOut { outpoint, .. } = input else {
                continue;
            };
            if cache.get(outpoint).is_some() {
                continue;
            }
            if !seen.insert(outpoint.hash) {
                continue;
            }
            let mut txid = outpoint.hash.0;
            txid.reverse();
            wanted.push(hex::encode(txid));
        }
    }
    let mut fetched = 0u64;
    for chunk in wanted.chunks(PREVOUT_BATCH) {
        for transaction in zakura.transactions(chunk).await?.into_iter().flatten() {
            cache.insert_transaction(&transaction);
            fetched += 1;
        }
    }
    Ok(fetched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outpoint(seed: u8, index: u32) -> OutPoint {
        OutPoint {
            hash: zakura_chain::transaction::Hash([seed; 32]),
            index,
        }
    }

    #[test]
    fn an_empty_cache_answers_nothing() {
        let cache = OutputCache::new(4);
        assert!(cache.is_empty());
        assert!(cache.get(&outpoint(1, 0)).is_none());
    }

    #[test]
    fn eviction_bounds_the_cache_without_losing_recent_entries() {
        use zakura_chain::amount::Amount;
        use zakura_chain::transparent::{Output, Script};

        let make = |tag: u8| {
            std::sync::Arc::new(Transaction::V1 {
                inputs: vec![],
                outputs: vec![Output {
                    value: Amount::try_from(i64::from(tag) + 1).unwrap(),
                    lock_script: Script::new(&[0x76, 0xa9, tag]),
                }],
                lock_time: zakura_chain::transaction::LockTime::unlocked(),
            })
        };

        let mut cache = OutputCache::new(2);
        let first = make(1);
        let second = make(2);
        let third = make(3);
        cache.insert_transaction(&first);
        cache.insert_transaction(&second);
        cache.insert_transaction(&third);

        // Capacity is in transactions, and the oldest was evicted.
        assert_eq!(cache.len(), 2);
        let gone = OutPoint {
            hash: first.hash(),
            index: 0,
        };
        let kept = OutPoint {
            hash: third.hash(),
            index: 0,
        };
        assert!(cache.get(&gone).is_none(), "oldest entry should be evicted");
        assert_eq!(cache.get(&kept), Some(&vec![0x76, 0xa9, 3]));
    }
}
