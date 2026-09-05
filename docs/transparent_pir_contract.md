# Transparent PIR recovery and privacy contract

Date: 2026-09-04. Status: proposed v1 evaluation contract, not a deployed protocol.

This defines the behavior to evaluate for the
[transparent PIR proposal](transparent_pir_design.md). The companion
[feasibility assessment](transparent_pir_evaluation.md) records evidence,
acceptance gates, and the next experiment. Numerical gates are proposed defaults;
they are not measured performance promises or authorization to deploy.

The intended interface is reusable by Zcash wallets. **Vizor is the first
integration and benchmark target**, not a protocol dependency. Keep network
transport, wallet storage, derivation policy, and user-interface behavior behind
wallet-owned adapters; measure their cost explicitly in the Vizor integration.

## Recovery goal

Recover every confirmed transparent receive and spend belonging to a wallet's
supported scripts within a declared chain range, including zero-balance history
and outputs received and spent while the wallet was offline. Derive current
UTXOs from that history. An empty current UTXO set is not evidence of no history.

For this evaluation, support standard P2PKH and P2SH output scripts controlled
by the wallet. Use exact script bytes as the logical identity, independently of
address text encoding. The indexer must resolve the previous output of every
non-coinbase input so spends are indexed under the consumed output's script.
Unsupported script classes must be reported as outside coverage, never as absent.

Preserve the wallet's existing account, derivation-scope, gap-limit, and imported
key rules. Do not introduce a separate PIR gap limit. A bounded derivation search
cannot discover arbitrary gaps or unknown imported keys. For fresh restoration,
the directory must cover genesis through its accepted anchor for historical
address-use decisions; an optimized birthday-limited search requires a separately
justified discovery rule. Routine sync starts after persisted complete coverage.

The result is a confirmed transparent ledger. Mempool detection, broadcast,
pending-transaction status, and full transaction display are separate capabilities.
Coinbase status, confirmation depth, and existing wallet spendability checks still
apply. Do not make an immature or otherwise ineligible output spendable merely
because its receive was recovered.

## Completeness and trust

The evaluation assumes a correct, complete chain indexer. The PIR service must
not learn which scripts or history pages a query selects. These are separate
assumptions: PIR query privacy does not prove that the database is complete.

A negative activity filter can advance coverage only under the complete-indexer
assumption. A block hash accepted by the wallet identifies an anchor; it does not
authenticate all entries or omissions in a server-created index. Inclusion proofs
for returned transactions do not prove absence of other matching transactions.
Production use requires an explicit wallet-owner decision that this trust model
is acceptable for balance and spendability, or a different completeness mechanism.
Until that decision, evaluate the new ledger alongside the existing sync result.

Call work complete only when all of the following hold:

- The chain, generation, anchor height/hash, range, and supported script set are
  explicit and consistent with wallet-accepted chain state.
- Every script's required discovery/recovery range is covered. Newly derived or
  imported scripts are checked over their earlier required range too.
- Required directory results and all history pages are applied, including exact
  key checks and rejection of false filter matches and dictionary collisions.
- Receives and spends are deduplicated by stable outpoint and spending-transaction
  identities, with ordering and references checked. An unexplained missing receive
  is unresolved work, not a fabricated output or permission to omit a spend.
- There are no missing filters, unresolved locators, malformed records, or
  exhausted resource budgets affecting that coverage.

Persist coverage by script/discovery scope and chain range, separately from
shielded scan progress. An incomplete result may expose known history as partial;
it must not assert a fully synchronized balance. Keep the current spend-verification
path until the replacement's coverage/trust contract is explicitly accepted.

## Observable behavior and failure handling

The selected script, address-derived partition, bucket, outpoint, transaction ID,
and history-page locator must not appear in plaintext requests for protected
recovery. Query directory buckets and overflow pages privately. Public filter
downloads depend only on public chain intervals, not on wallet-script partitions.

V1 evaluation accepts service contact, network identity on a direct connection,
generation/coverage selection, timing, and query-count leakage. No cover traffic
or anonymity against public-chain correlation is claimed. Filter-triggered contact
can reveal activity; unusually large retrievals can narrow likely scripts.
Tor can change network-identity exposure but does not erase count/timing leakage.
An actively malicious service and error/retry side channels require composition
review before claiming privacy beyond this stated baseline.

Individual history pages have a fixed serving size. Large histories require more
private requests, with locally bounded work and durable resumption. Someone else
can create a large history by sending to a wallet; do not assume large histories
are voluntary. Reaching a budget leaves work incomplete.

Timeouts, stale generations, unsupported schemas, authentication failures, and
service outages must not trigger public address, outpoint, or transaction-ID
retrieval for protected recovery. Retain pending work and expose its status.
Any explicitly selected alternative sync method is a separate user policy.

On reorg, rewind affected events and coverage to the accepted common ancestor;
discard affected in-flight results and invalidate affected filters/pages. Cached
historical pages are reusable only if their chain validity is established.
Publication and tail sealing must not mix directory locators with incompatible
pages or lose/duplicate events. Retrying a completed response is idempotent.

## Wallet integration boundary

These are required capabilities, not a new wire schema or a request to change the
wallet libraries in this deliverable:

| Capability | Required information or behavior |
|---|---|
| Generation acceptance | Chain, anchor, declared historical coverage, locally chosen setup/work limits |
| Discovery | Exact script match or explicit absence within declared coverage; advance existing derivation rules only on real activity |
| Receive application | Outpoint, value, script, block/transaction location, coinbase status |
| Spend application | Consumed outpoint, spending txid and block/transaction location |
| Progress | Persist unresolved private work and per-script coverage; rewind atomically with ledger state |
| Routing | Suppress public retrieval only for work this private path can completely fulfill |

Before claiming mixed-pool privacy, trace each wallet requirement independently:

| Scenario | Transparent history supplies | Separate requirement to resolve |
|---|---|---|
| Transparent receive, later spend | Owned output and its confirmed spender | Confirmation/maturity policy and any pending status |
| Transparent-to-shielded or shielded-to-transparent | Matching transparent legs | Shielded scanning and enhancement; complete transaction fee/display inputs |
| Transaction with another party's inputs/outputs | Wallet-script events only | Other inputs' values and other outputs if required for fees or display |
| Restored outgoing transaction | Spends of known wallet outputs | External recipients, locally stored intent, and other details absent from script history |

Enhance PIR remains independently shippable. Neither this ledger nor Enhance alone
justifies retiring all ordinary mixed-pool enhancement or transaction-status work.

## Required correctness checks

Compare the recovered ledger against a separately derived archive scan at an
identical anchor. Require exact equality, not merely matching final balances.
Exercise: unused scripts and false positives; zero-balance history; offline
receive-then-spend; self-transfers; multiple owned scripts; coinbase maturity;
gap advancement and imported scripts; colliding keys and full buckets; inline/page
boundaries; retries and duplicate delivery; interrupted pagination; tail sealing;
reorgs affecting recent and cached history; unavailable/malformed data; and work
limits. Capture network requests to check routing and locator privacy under
success, failure, retry, and reorg paths.
