#!/usr/bin/env python3
"""Read a bounded, anchored transparent block sample from a local archive RPC.

Run on the archive host through SSH stdin. Output is JSONL containing public
chain data only; authentication is read locally and never included in output.
"""

import argparse
import base64
import datetime
import json
from pathlib import Path
import sys
import time
import tomllib
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--anchor", type=int, required=True)
    parser.add_argument("--blocks", type=int, default=1024)
    parser.add_argument("--max-rps", type=float, default=10)
    parser.add_argument("--resume", type=Path, help="resume a complete JSONL block prefix")
    args = parser.parse_args()
    if not 1 <= args.blocks <= 10000 or args.anchor < args.blocks - 1:
        parser.error("sample must contain 1..10000 blocks at nonnegative heights")
    if not 0 < args.max_rps <= 100:
        parser.error("max-rps must be in (0, 100]")
    config = tomllib.loads(Path("/etc/zakura/zakura.toml").read_text())
    headers = {"Content-Type": "application/json"}
    if config.get("rpc", {}).get("enable_cookie_auth", True):
        cookie_dir = config.get("rpc", {}).get("cookie_dir")
        candidates = ([Path(cookie_dir) / ".cookie"] if cookie_dir else []) + [
            Path("/root/.cache/zakura/.cookie"), Path("/root/.cache/zebra/.cookie")]
        cookie = next((p for p in candidates if p.exists()), None)
        if cookie is None:
            raise RuntimeError("RPC authentication cookie not found")
        headers["Authorization"] = "Basic " + base64.b64encode(cookie.read_bytes().strip()).decode()
    rpc_count = 0
    last_request = 0.0

    def rpc_many(method, parameters):
        nonlocal rpc_count, last_request
        if not parameters or len(parameters) > 16:
            raise ValueError("RPC batches must contain 1..16 methods")
        time.sleep(max(0, last_request - time.monotonic()))
        last_request = time.monotonic() + len(parameters) / args.max_rps
        ids = list(range(rpc_count + 1, rpc_count + len(parameters) + 1))
        rpc_count += len(parameters)
        request = urllib.request.Request(
            "http://127.0.0.1:8232",
            json.dumps([{"jsonrpc": "2.0", "id": identifier,
                         "method": method, "params": params}
                        for identifier, params in zip(ids, parameters)]).encode(), headers)
        limit = (32 if len(parameters) > 1 else 128) * 1024 * 1024
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read(limit + 1)
        if len(raw) > limit:
            if len(parameters) == 1:
                raise RuntimeError("single RPC response exceeded sample collection limit")
            middle = len(parameters) // 2
            return rpc_many(method, parameters[:middle]) + rpc_many(method, parameters[middle:])
        results = json.loads(raw)
        if not isinstance(results, list) or len(results) != len(ids):
            raise RuntimeError("RPC batch result count mismatch")
        by_id = {r["id"]: r for r in results}
        if set(by_id) != set(ids):
            raise RuntimeError("RPC batch identity mismatch")
        ordered = [by_id[i] for i in ids]
        if any(r.get("error") or "result" not in r for r in ordered):
            raise RuntimeError(f"RPC {method} failed")
        return [r["result"] for r in ordered]

    def rpc(method, params):
        return rpc_many(method, [params])[0]

    def emit(value):
        print(json.dumps(value, separators=(",", ":")), flush=True)

    def outputs(transaction):
        return [{"n": v["n"], "value_zat": v["valueZat"],
                 "script": v["scriptPubKey"]["hex"]} for v in transaction["vout"]]

    info = rpc("getblockchaininfo", [])
    if info["chain"] != "main" or info["pruned"] or info["blocks"] < args.anchor:
        raise RuntimeError("a Mainnet archive covering the anchor is required")
    anchor_hash = rpc("getblockhash", [args.anchor])
    start = args.anchor - args.blocks + 1
    previous = rpc("getblockhash", [start - 1]) if start else None
    resumed = []
    if args.resume:
        prior = [json.loads(line) for line in args.resume.read_text().splitlines()]
        old = prior[0]
        if old["start_height"] != start or old["anchor_hash"] != anchor_hash:
            raise RuntimeError("resume range or anchor mismatch")
        for block in prior[1:]:
            if block.get("type") != "block" or block["height"] != start + len(resumed) or block["prev_hash"] != previous:
                raise RuntimeError("invalid resume prefix")
            resumed.append(block)
            previous = block["hash"]
        if resumed and rpc("getblockhash", [resumed[-1]["height"]]) != previous:
            raise RuntimeError("resume prefix no longer canonical")
    emit({"type": "manifest", "collected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
          "network": info["chain"], "genesis_hash": rpc("getblockhash", [0]),
          "start_height": start, "anchor_height": args.anchor, "anchor_hash": anchor_hash,
          "node_tip_at_start": info["blocks"], "pruned": info["pruned"],
          "max_rpc_requests_per_second": args.max_rps,
          "resumed_blocks": len(resumed),
          "classification": "bounded public chain sample, not historical census"})
    cache = {}
    for block in resumed:
        for transaction in block["transactions"]:
            cache[transaction["txid"]] = transaction["vout"]
        emit(block)
    started = time.monotonic()
    external_transactions = 0
    def prefetched_blocks():
        nonlocal external_transactions
        for batch_start in range(start + len(resumed), args.anchor + 1, 4):
            heights = list(range(batch_start, min(batch_start + 4, args.anchor + 1)))
            group = rpc_many("getblock", [[str(h), 2] for h in heights])
            for expected_height, block in zip(heights, group):
                if block["height"] != expected_height:
                    raise RuntimeError("RPC block height mismatch")
                for transaction in block["tx"]:
                    cache[transaction["txid"]] = outputs(transaction)
            missing = sorted({v["txid"] for block in group for t in block["tx"]
                              for v in t["vin"] if "coinbase" not in v and v["txid"] not in cache})
            for offset in range(0, len(missing), 16):
                parent_ids = missing[offset:offset + 16]
                parents = rpc_many("getrawtransaction", [[p, 1] for p in parent_ids])
                for parent_id, parent in zip(parent_ids, parents):
                    if parent["txid"] != parent_id or parent.get("in_active_chain") is False:
                        raise RuntimeError("invalid previous transaction identity or chain")
                    cache[parent_id] = outputs(parent)
                    external_transactions += 1
            yield from group

    for block in prefetched_blocks():
        height = block["height"]
        if block["height"] != height or (previous and block["previousblockhash"] != previous):
            raise RuntimeError("non-contiguous sample or reorganization")
        previous = block["hash"]
        compact = []
        for index, transaction in enumerate(block["tx"]):
            txid = transaction["txid"]
            out = outputs(transaction)
            cache[txid] = out
            ins = []
            for v in transaction["vin"]:
                if "coinbase" in v:
                    continue
                parent_id = v["txid"]
                if parent_id not in cache:
                    parent = rpc("getrawtransaction", [parent_id, 1])
                    if parent["txid"] != parent_id or parent.get("in_active_chain") is False:
                        raise RuntimeError("invalid previous transaction identity or chain")
                    cache[parent_id] = outputs(parent)
                    external_transactions += 1
                matching = [o for o in cache[parent_id] if o["n"] == v["vout"]]
                if len(matching) != 1:
                    raise RuntimeError("unresolved previous output")
                ins.append({"txid": parent_id, "n": v["vout"],
                            "script": matching[0]["script"],
                            "value_zat": matching[0]["value_zat"]})
            if out or ins:
                compact.append({"index": index, "txid": txid, "vin": ins, "vout": out})
        emit({"type": "block", "height": height, "hash": block["hash"],
              "prev_hash": block.get("previousblockhash"), "time": block["time"],
              "transactions": compact})
        if (height - start + 1) % 128 == 0:
            print(f"collected {height - start + 1}/{args.blocks} blocks; {rpc_count} RPC calls",
                  file=sys.stderr, flush=True)
    if previous != anchor_hash or rpc("getblockhash", [args.anchor]) != anchor_hash:
        raise RuntimeError("anchor changed during collection")
    emit({"type": "complete", "blocks": args.blocks, "anchor_hash": anchor_hash,
          "unresolved_prevouts": 0, "rpc_calls": rpc_count,
          "external_previous_transactions": external_transactions,
          "collection_elapsed_seconds": time.monotonic() - started})


if __name__ == "__main__":
    main()
