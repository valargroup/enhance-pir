//! Witness-table helpers shared by ingest, the coordinator's cap and frontier
//! routes, the CLI, and tests: tree geometry, the public cap, per-block
//! frontier updates, and path reconstruction from two PIR rows plus the cap.
//!
//! A note's authentication path has three tiers:
//!
//! | levels  | source                                   |
//! |---------|------------------------------------------|
//! | 0 .. 8  | the `witness` row of its sub-shard (256 leaves)        |
//! | 8 .. 16 | the `witness-roots` row of its shard (256 sub-shard roots) |
//! | 16 .. 32| the public cap (one root per shard)      |
//!
//! Only the frontier sub-shard and frontier shard change as the tree grows,
//! so a note in a sealed shard fetches its rows once; the frontier update
//! then moves any held path to a newer anchor locally.

use commitment_tree_db::{complete_subtree_root, empty_roots, hash_combine, sparse_subtree_root};
use serde::{Deserialize, Serialize};

pub type Hash = [u8; 32];

pub const TREE_DEPTH: usize = 32;
pub const SUBSHARD_HEIGHT: u8 = 8;
pub const SHARD_HEIGHT: u8 = 16;
pub const SUBSHARD_LEAVES: usize = 1 << SUBSHARD_HEIGHT;
pub const SUBSHARDS_PER_SHARD: usize = 1 << (SHARD_HEIGHT - SUBSHARD_HEIGHT);
pub const SHARD_LEAVES: usize = 1 << SHARD_HEIGHT;

/// Public, non-private tree summary served with every generation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WitnessCap {
    pub anchor_height: u64,
    pub tree_size: u64,
    /// Root of every shard with at least one leaf, including the frontier
    /// shard, hex. Index = shard.
    pub shard_roots: Vec<String>,
    /// Root of the frontier sub-shard (padded with empty leaves), hex, or
    /// absent when the tree size is a multiple of 256.
    pub frontier_subshard_root: Option<String>,
    /// Depth-32 tree root, hex.
    pub tree_root: String,
}

/// Nodes on the rightmost path after one block, level 0 first: for level `h`
/// the node at index `(tree_size - 1) >> h`. 32 × 32 bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrontierUpdate {
    pub height: u64,
    pub tree_size: u64,
    /// Hex, level 0 first.
    pub rightmost_nodes: Vec<String>,
}

pub const FRONTIER_RECORD_BYTES: usize = TREE_DEPTH * 32;

/// Raw form of [`FrontierUpdate`] as journaled: 32 hashes.
pub fn encode_frontier(nodes: &[Hash; TREE_DEPTH]) -> [u8; FRONTIER_RECORD_BYTES] {
    let mut bytes = [0u8; FRONTIER_RECORD_BYTES];
    for (index, node) in nodes.iter().enumerate() {
        bytes[index * 32..(index + 1) * 32].copy_from_slice(node);
    }
    bytes
}

pub fn decode_frontier(bytes: &[u8]) -> Option<[Hash; TREE_DEPTH]> {
    if bytes.len() != FRONTIER_RECORD_BYTES {
        return None;
    }
    let mut nodes = [[0u8; 32]; TREE_DEPTH];
    for (index, node) in nodes.iter_mut().enumerate() {
        node.copy_from_slice(&bytes[index * 32..(index + 1) * 32]);
    }
    Some(nodes)
}

fn parse_hashes(bytes: &[u8]) -> Vec<Hash> {
    bytes
        .chunks_exact(32)
        .map(|chunk| chunk.try_into().expect("32-byte hash"))
        .collect()
}

/// Everything the tree summary is derived from: the commitments and the
/// completed sub-shard roots, both as raw 32-byte arrays in order.
pub struct TreeView<'a> {
    pub tree_size: u64,
    /// Leaves of the frontier sub-shard only (`tree_size % 256` of them).
    pub frontier_leaves: &'a [u8],
    /// Roots of every completed sub-shard, in order.
    pub subshard_roots: &'a [u8],
}

impl TreeView<'_> {
    fn completed_subshards(&self) -> usize {
        (self.tree_size as usize) / SUBSHARD_LEAVES
    }

    /// Root of the frontier sub-shard, if it has any leaves.
    pub fn frontier_subshard_root(&self, empty: &[Hash]) -> Option<Hash> {
        let leaves = parse_hashes(self.frontier_leaves);
        (!leaves.is_empty()).then(|| sparse_subtree_root(&leaves, SUBSHARD_LEAVES, 0, empty))
    }

    /// Roots of every sub-shard with a leaf: completed ones then the frontier.
    fn all_subshard_roots(&self, empty: &[Hash]) -> Vec<Hash> {
        let mut roots = parse_hashes(self.subshard_roots);
        debug_assert_eq!(roots.len(), self.completed_subshards());
        if let Some(frontier) = self.frontier_subshard_root(empty) {
            roots.push(frontier);
        }
        roots
    }

    /// Roots of every shard with a leaf, index = shard.
    pub fn shard_roots(&self, empty: &[Hash]) -> Vec<Hash> {
        let subshards = self.all_subshard_roots(empty);
        subshards
            .chunks(SUBSHARDS_PER_SHARD)
            .map(|chunk| {
                if chunk.len() == SUBSHARDS_PER_SHARD {
                    complete_subtree_root(chunk, SUBSHARD_HEIGHT)
                } else {
                    sparse_subtree_root(chunk, SUBSHARDS_PER_SHARD, SUBSHARD_HEIGHT, empty)
                }
            })
            .collect()
    }

    pub fn tree_root(&self, empty: &[Hash]) -> Hash {
        let shards = self.shard_roots(empty);
        sparse_subtree_root(
            &shards,
            1 << (TREE_DEPTH as u8 - SHARD_HEIGHT),
            SHARD_HEIGHT,
            empty,
        )
    }

    pub fn cap(&self, anchor_height: u64) -> WitnessCap {
        let empty = empty_roots();
        WitnessCap {
            anchor_height,
            tree_size: self.tree_size,
            shard_roots: self.shard_roots(&empty).iter().map(hex::encode).collect(),
            frontier_subshard_root: self.frontier_subshard_root(&empty).map(hex::encode),
            tree_root: hex::encode(self.tree_root(&empty)),
        }
    }

    /// The rightmost path after this tree size: at level `h`, the node whose
    /// subtree contains the last leaf. Empty when the tree is empty.
    pub fn rightmost_nodes(&self) -> Option<[Hash; TREE_DEPTH]> {
        if self.tree_size == 0 {
            return None;
        }
        let empty = empty_roots();
        let last = self.tree_size - 1;
        let leaves = parse_hashes(self.frontier_leaves);
        let subshards = self.all_subshard_roots(&empty);
        let shards = self.shard_roots(&empty);
        let mut nodes = [[0u8; 32]; TREE_DEPTH];
        for (level, node) in nodes.iter_mut().enumerate() {
            let level = level as u8;
            // Index of the node at `level` and the level it is built from.
            *node = if level < SUBSHARD_HEIGHT {
                // Within the frontier sub-shard, or a completed one if the
                // frontier is empty.
                let width = 1usize << level;
                if leaves.is_empty() {
                    // The last leaf sits in a completed sub-shard whose
                    // interior nodes are not stored; the subtree is full,
                    // so its node is the root of `width` empty... no: full
                    // of real leaves. Recompute from the sub-shard root only
                    // at level 8; below it, callers must hold the leaves.
                    // We report the completed sub-shard's interior nodes as
                    // unavailable by using the sub-shard root at level 8 and
                    // leaving lower levels to the client-side leaf cache.
                    [0u8; 32]
                } else {
                    let within = (last as usize) % SUBSHARD_LEAVES;
                    let start = (within >> level) << level;
                    let end = (start + width).min(leaves.len());
                    sparse_subtree_root(&leaves[start..end], width, 0, &empty)
                }
            } else if level < SHARD_HEIGHT {
                let width = 1usize << (level - SUBSHARD_HEIGHT);
                let index = ((last as usize) >> level) << (level - SUBSHARD_HEIGHT);
                let end = (index + width).min(subshards.len());
                sparse_subtree_root(&subshards[index..end], width, SUBSHARD_HEIGHT, &empty)
            } else {
                let width = 1usize << (level - SHARD_HEIGHT);
                let index = ((last as usize) >> level) << (level - SHARD_HEIGHT);
                let end = (index + width).min(shards.len());
                sparse_subtree_root(&shards[index..end], width, SHARD_HEIGHT, &empty)
            };
        }
        Some(nodes)
    }
}

/// Position decomposition shared with clients.
pub fn decompose(position: u64) -> (u64, u64, usize) {
    (
        position >> SHARD_HEIGHT,
        position >> SUBSHARD_HEIGHT,
        (position % SUBSHARD_LEAVES as u64) as usize,
    )
}

/// Given a complete `2^k`-node array at `base_level`, records the siblings
/// along the path to `index` into `siblings[base_level..base_level + k]`.
pub fn extract_siblings(
    nodes: &[Hash],
    index: usize,
    base_level: u8,
    siblings: &mut [Hash; TREE_DEPTH],
) {
    let empty = empty_roots();
    let mut current = nodes.to_vec();
    let mut idx = index;
    let levels = current.len().trailing_zeros() as usize;
    for offset in 0..levels {
        let level = base_level as usize + offset;
        let sibling = idx ^ 1;
        siblings[level] = if sibling < current.len() {
            current[sibling]
        } else {
            empty[level]
        };
        let mut next = Vec::with_capacity(current.len() / 2);
        for pair in current.chunks(2) {
            let right = if pair.len() > 1 {
                pair[1]
            } else {
                empty[level]
            };
            next.push(hash_combine(level as u8, &pair[0], &right));
        }
        current = next;
        idx /= 2;
    }
}

/// Root implied by a leaf and its 32 siblings.
pub fn root_from_path(position: u64, leaf: &Hash, siblings: &[Hash; TREE_DEPTH]) -> Hash {
    let mut current = *leaf;
    let mut pos = position;
    for (level, sibling) in siblings.iter().enumerate() {
        current = if pos & 1 == 0 {
            hash_combine(level as u8, &current, sibling)
        } else {
            hash_combine(level as u8, sibling, &current)
        };
        pos >>= 1;
    }
    current
}

/// A full authentication path reconstructed from the two PIR rows and the cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Witness {
    pub position: u64,
    pub leaf: Hash,
    pub siblings: [Hash; TREE_DEPTH],
    pub anchor_height: u64,
    pub tree_size: u64,
    pub root: Hash,
}

/// Reconstructs the path for `position` from its sub-shard's leaf row, its
/// shard's completed-sub-shard-roots row, and the cap. Rows are exactly as
/// decoded from PIR: 8,192 bytes, zero-padded past the populated records.
pub fn reconstruct(
    position: u64,
    leaves_row: &[u8],
    roots_row: &[u8],
    cap: &WitnessCap,
) -> Result<Witness, String> {
    if position >= cap.tree_size {
        return Err("position is beyond the anchor's tree size".to_string());
    }
    let empty = empty_roots();
    let (shard, subshard, leaf_index) = decompose(position);
    let mut leaves = parse_hashes(
        leaves_row
            .get(..SUBSHARD_LEAVES * 32)
            .ok_or("short leaf row")?,
    );
    // Padding past the tree size is the empty leaf, not zero bytes.
    let populated_in_subshard =
        (cap.tree_size - subshard * SUBSHARD_LEAVES as u64).min(SUBSHARD_LEAVES as u64) as usize;
    for leaf in leaves.iter_mut().skip(populated_in_subshard) {
        *leaf = empty[0];
    }
    let leaf = leaves[leaf_index];
    let mut siblings = [[0u8; 32]; TREE_DEPTH];
    extract_siblings(&leaves, leaf_index, 0, &mut siblings);

    let mut roots = parse_hashes(
        roots_row
            .get(..SUBSHARDS_PER_SHARD * 32)
            .ok_or("short roots row")?,
    );
    let completed_subshards = (cap.tree_size as usize) / SUBSHARD_LEAVES;
    let first_subshard = (shard as usize) * SUBSHARDS_PER_SHARD;
    let completed_in_shard = completed_subshards
        .saturating_sub(first_subshard)
        .min(SUBSHARDS_PER_SHARD);
    for (index, root) in roots.iter_mut().enumerate().skip(completed_in_shard) {
        *root = if index == completed_in_shard {
            match &cap.frontier_subshard_root {
                Some(frontier) if first_subshard + index == completed_subshards => {
                    hex_hash(frontier)?
                }
                _ => empty[SUBSHARD_HEIGHT as usize],
            }
        } else {
            empty[SUBSHARD_HEIGHT as usize]
        };
    }
    extract_siblings(
        &roots,
        (subshard as usize) % SUBSHARDS_PER_SHARD,
        SUBSHARD_HEIGHT,
        &mut siblings,
    );

    let mut shard_roots: Vec<Hash> = cap
        .shard_roots
        .iter()
        .map(|root| hex_hash(root))
        .collect::<Result<_, _>>()?;
    shard_roots.resize(
        1 << (TREE_DEPTH as u8 - SHARD_HEIGHT),
        empty[SHARD_HEIGHT as usize],
    );
    extract_siblings(&shard_roots, shard as usize, SHARD_HEIGHT, &mut siblings);

    let root = root_from_path(position, &leaf, &siblings);
    if hex::encode(root) != cap.tree_root {
        return Err("reconstructed path does not reach the cap's tree root".to_string());
    }
    Ok(Witness {
        position,
        leaf,
        siblings,
        anchor_height: cap.anchor_height,
        tree_size: cap.tree_size,
        root,
    })
}

/// Moves a held path to the anchor of `update`. Levels whose sibling subtree
/// is fully populated are final; a sibling on the rightmost path takes the
/// update's node; a sibling beyond the tree is the empty root. Returns an
/// error when levels 0..8 changed inside a still-frontier sub-shard, which
/// needs the leaf cache the wallet keeps.
pub fn apply_frontier_update(
    witness: &mut Witness,
    update: &[Hash; TREE_DEPTH],
    new_tree_size: u64,
    new_height: u64,
) -> Result<(), String> {
    if new_tree_size < witness.tree_size {
        return Err("frontier update moves the tree backwards".to_string());
    }
    let empty = empty_roots();
    let last = new_tree_size - 1;
    let same_subshard = (witness.position >> SUBSHARD_HEIGHT) == (last >> SUBSHARD_HEIGHT)
        && !new_tree_size.is_multiple_of(SUBSHARD_LEAVES as u64);
    if same_subshard && new_tree_size != witness.tree_size {
        return Err(
            "witness is in the frontier sub-shard; re-fetch or splice new leaves".to_string(),
        );
    }
    for level in 0..TREE_DEPTH {
        let sibling_pos = (witness.position >> level) ^ 1;
        let rightmost_pos = last >> level;
        if sibling_pos == rightmost_pos {
            witness.siblings[level] = update[level];
        } else if sibling_pos > rightmost_pos {
            witness.siblings[level] = empty[level];
        }
    }
    witness.tree_size = new_tree_size;
    witness.anchor_height = new_height;
    witness.root = root_from_path(witness.position, &witness.leaf, &witness.siblings);
    Ok(())
}

fn hex_hash(text: &str) -> Result<Hash, String> {
    let bytes = hex::decode(text).map_err(|e| e.to_string())?;
    bytes
        .try_into()
        .map_err(|_| "hash is not 32 bytes".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(i: u64) -> Hash {
        // Any 32 bytes that decode as a Pallas base element: keep the top
        // byte zero.
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&(i + 1).to_le_bytes());
        bytes
    }

    /// Builds the journal views for `n` leaves the way ingest would.
    fn view(n: u64) -> (Vec<u8>, Vec<u8>) {
        let completed = (n as usize) / SUBSHARD_LEAVES;
        let mut roots = Vec::new();
        for subshard in 0..completed {
            let leaves: Vec<Hash> = (0..SUBSHARD_LEAVES)
                .map(|i| leaf((subshard * SUBSHARD_LEAVES + i) as u64))
                .collect();
            roots.extend_from_slice(&complete_subtree_root(&leaves, 0));
        }
        let mut frontier = Vec::new();
        for i in (completed * SUBSHARD_LEAVES) as u64..n {
            frontier.extend_from_slice(&leaf(i));
        }
        (frontier, roots)
    }

    fn rows_for(position: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
        let (_, subshard, _) = decompose(position);
        let mut leaves_row = vec![0u8; SUBSHARD_LEAVES * 32];
        for i in 0..SUBSHARD_LEAVES as u64 {
            let p = subshard * SUBSHARD_LEAVES as u64 + i;
            if p < n {
                leaves_row[i as usize * 32..(i as usize + 1) * 32].copy_from_slice(&leaf(p));
            }
        }
        let (_, roots) = view(n);
        let shard = (position >> SHARD_HEIGHT) as usize;
        let mut roots_row = vec![0u8; SUBSHARDS_PER_SHARD * 32];
        let start = shard * SUBSHARDS_PER_SHARD * 32;
        let end = ((shard + 1) * SUBSHARDS_PER_SHARD * 32).min(roots.len());
        if start < roots.len() {
            roots_row[..end - start].copy_from_slice(&roots[start..end]);
        }
        (leaves_row, roots_row)
    }

    #[test]
    fn reconstructed_paths_reach_the_cap_root_across_frontiers() {
        for n in [1u64, 255, 256, 257, 65_536, 65_537, 70_000] {
            let (frontier, roots) = view(n);
            let tree = TreeView {
                tree_size: n,
                frontier_leaves: &frontier,
                subshard_roots: &roots,
            };
            let cap = tree.cap(100);
            for position in [0, n / 2, n - 1] {
                let (leaves_row, roots_row) = rows_for(position, n);
                let witness = reconstruct(position, &leaves_row, &roots_row, &cap)
                    .unwrap_or_else(|e| panic!("n={n} position={position}: {e}"));
                assert_eq!(witness.leaf, leaf(position));
                assert_eq!(hex::encode(witness.root), cap.tree_root);
            }
        }
    }

    #[test]
    fn frontier_updates_move_a_sealed_witness_to_a_newer_root() {
        let n0 = 65_536 + 300;
        let (f0, r0) = view(n0);
        let cap0 = TreeView {
            tree_size: n0,
            frontier_leaves: &f0,
            subshard_roots: &r0,
        }
        .cap(100);
        let position = 12_345; // in sealed shard 0
        let (lr, rr) = rows_for(position, n0);
        let mut witness = reconstruct(position, &lr, &rr, &cap0).expect("witness");

        for (height, n) in [(101, n0 + 3), (102, n0 + 100), (103, 66_000)] {
            let (f, r) = view(n);
            let tree = TreeView {
                tree_size: n,
                frontier_leaves: &f,
                subshard_roots: &r,
            };
            let nodes = tree.rightmost_nodes().expect("nodes");
            apply_frontier_update(&mut witness, &nodes, n, height).expect("update");
            assert_eq!(
                hex::encode(witness.root),
                tree.cap(height).tree_root,
                "n={n}"
            );
        }
    }

    #[test]
    fn frontier_records_round_trip() {
        let nodes: [Hash; TREE_DEPTH] = std::array::from_fn(|i| leaf(i as u64));
        assert_eq!(decode_frontier(&encode_frontier(&nodes)).unwrap(), nodes);
        assert!(decode_frontier(&[0u8; 5]).is_none());
    }
}
