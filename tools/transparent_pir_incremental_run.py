#!/usr/bin/env python3
"""Execute adaptive filter -> PIR directory -> PIR pages on the fixed public day.

Checkpoint balances and wallet groupings are synthetic. Archive events remain
real. The oracle uses transaction dictionaries independently of client decoding.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import platform
import time

from transparent_pir_incremental import (
    EVENT,
    load_sample,
    source_events,
    build_generation,
    new_state,
    save,
    sync,
    PirTransport,
    ChargedFailure,
)


def oracle(blocks, scripts, checkpoint):
    scripts = set(scripts)
    receives = {
        (tx["txid"], v["n"])
        for b in blocks
        for tx in b["transactions"]
        for v in tx["vout"]
    }
    utxos = {}
    for b in blocks:
        for tx in b["transactions"]:
            for v in tx["vin"]:
                if v["script"] in scripts and (v["txid"], v["n"]) not in receives:
                    utxos[f"{v['txid']}:{v['n']}"] = {
                        "script": v["script"],
                        "value": v["value_zat"],
                        "height": None,
                        "coinbase": None,
                    }
    starting = dict(utxos)
    expected = []
    for b in blocks:
        for tx in b["transactions"]:
            for v in tx["vout"]:
                if v["script"] not in scripts:
                    continue
                coinbase = tx["index"] == 0 and not tx["vin"]
                utxos[f"{tx['txid']}:{v['n']}"] = {
                    "script": v["script"],
                    "value": v["value_zat"],
                    "height": b["height"],
                    "coinbase": coinbase,
                }
                if b["height"] > checkpoint:
                    expected.append(
                        (
                            v["script"],
                            3 if coinbase else 1,
                            b["height"],
                            tx["index"],
                            tx["txid"],
                            v["n"],
                            v["value_zat"],
                            "00" * 32,
                        )
                    )
            for v in tx["vin"]:
                if v["script"] not in scripts:
                    continue
                prior = utxos.pop(f"{v['txid']}:{v['n']}")
                if prior["value"] != v["value_zat"] or prior["script"] != v["script"]:
                    raise ValueError("invalid oracle prevout")
                if b["height"] > checkpoint:
                    expected.append(
                        (
                            v["script"],
                            2,
                            b["height"],
                            tx["index"],
                            v["txid"],
                            v["n"],
                            v["value_zat"],
                            tx["txid"],
                        )
                    )
        if b["height"] == checkpoint:
            starting = dict(utxos)
    return starting, Counter(expected), utxos


def verify(state, expected, utxos, end):
    decoded = []
    for script, event in state["events"]:
        kind, height, index, txid, n, value, spender = EVENT.unpack(
            bytes.fromhex(event)
        )
        decoded.append(
            (script, kind, height, index, txid.hex(), n, value, spender.hex())
        )
    if (
        state["height"] != end
        or state["pending"] is not None
        or Counter(decoded) != expected
        or state["utxos"] != utxos
    ):
        raise ValueError(
            "incremental ledger/checkpoint differs from independent archive oracle"
        )


def run(args):
    metadata, blocks = load_sample(args.sample)
    accepted = {
        blocks[0]["height"] - 1: blocks[0]["prev_hash"],
        **{b["height"]: b["hash"] for b in blocks},
    }
    folder = build_generation(
        metadata["network"],
        blocks,
        args.output / "generations",
        page_bytes=args.page_bytes,
    )
    events = source_events(blocks)
    ranked = sorted(events, key=lambda s: (len(events[s]), s))
    absent = [
        "76a914"
        + hashlib.sha256(f"incremental-absent-{i}".encode()).digest()[:20].hex()
        + "88ac"
        for i in range(100)
    ]
    if any(s in events for s in absent):
        raise ValueError("unexpected absent-fixture collision")
    start = blocks[0]["height"] - 1
    cases = [
        ("unused_100", absent, start),
        ("sparse_1", [ranked[len(ranked) // 2]], start),
        ("median_10", ranked[len(ranked) // 2 : len(ranked) // 2 + 10], start),
        ("large_1", [ranked[-1]], start),
        ("large_half_day", [ranked[-1]], blocks[len(blocks) // 2 - 1]["height"]),
    ]
    result = {
        "classification": "integrated adaptive client with real serialized in-process PIR; no network or device sync",
        "network": metadata["network"],
        "start": start + 1,
        "end": blocks[-1]["height"],
        "anchor": blocks[-1]["hash"],
        "generation": folder.name,
        "manifest": json.loads((folder / "manifest.json").read_text()),
        "machine": platform.platform(),
        "row_page_bytes": args.page_bytes,
        "reference_day_incremental_application_bytes": 2888097,
        "trust": "complete indexer, accepted chain supplied by wallet; no authentication/composition claim",
        "checkpoint_fixture": "only prior outputs referenced by these chain events; not full real wallet balances",
        "runs": [],
    }
    public_caches = {}
    result["public_cache_policy"] = (
        "retain public decoding data per workload across repetitions"
        if args.public_cache
        else "charge every batch initialization"
    )
    for repetition in range(args.runs):
        for name, scripts, checkpoint in cases:
            out = args.output / f"run-{repetition}" / name
            out.mkdir(parents=True, exist_ok=True)
            initial, expected, utxos = oracle(blocks, scripts, checkpoint)
            state = new_state(
                metadata["network"], scripts, checkpoint, accepted[checkpoint], initial
            )
            path = out / "state.json"
            save(path, state)
            transport = PirTransport(
                args.binary,
                out / "pir",
                public_caches.setdefault(name, set()) if args.public_cache else None,
            )
            begin = time.perf_counter()
            state = sync(path, folder, accepted, transport)
            elapsed = time.perf_counter() - begin
            verify(state, expected, utxos, blocks[-1]["height"])
            cost = state["cost"]
            total = sum(
                cost[k]
                for k in [
                    "public_download_bytes",
                    "upload_bytes",
                    "response_bytes",
                    "setup_download_bytes",
                ]
            )
            result["runs"].append(
                {
                    "name": name,
                    "repeat": repetition,
                    "script_count": len(scripts),
                    "checkpoint": checkpoint,
                    "verified_events": sum(expected.values()),
                    "cost": cost,
                    "application_bytes": total,
                    "harness_wall_seconds_including_server_precomputation_and_files": elapsed,
                    "day_50_percent_byte_screen": "pass"
                    if total <= 2888097 / 2
                    else "fail",
                    "screen_applicable": checkpoint == start,
                    "private_batches": [
                        {"table": t, "queries": len(rows)}
                        for t, rows in transport.calls
                    ],
                }
            )
            print(
                name, repetition, total, "bytes", cost["queries"], "queries", flush=True
            )
    # Real lost-response and budget-resume runs charge every completed crypto batch.
    for mode in ["budget_resume", "lost_response"]:
        out = args.output / mode
        out.mkdir(exist_ok=True)
        scripts = (
            [ranked[-1]] if mode == "budget_resume" else [ranked[len(ranked) // 2]]
        )
        initial, expected, utxos = oracle(blocks, scripts, start)
        path = out / "state.json"
        save(
            path,
            new_state(metadata["network"], scripts, start, accepted[start], initial),
        )
        transport = PirTransport(
            args.binary, out / "pir", set() if args.public_cache else None
        )
        pauses = 0
        if mode == "lost_response":
            transport.drop_once = True
            try:
                sync(path, folder, accepted, transport)
            except ChargedFailure:
                pauses += 1
            else:
                raise ValueError("lost-response injection did not execute")
            if json.loads(path.read_text())["height"] != start:
                raise ValueError("lost response advanced coverage")
        while True:
            state = sync(
                path,
                folder,
                accepted,
                transport,
                budget=10 if mode == "budget_resume" else 1000,
            )
            if state["pending"] is None:
                break
            if state["height"] != start or state["events"]:
                raise ValueError("budget pause advanced committed ledger")
            pauses += 1
        verify(state, expected, utxos, blocks[-1]["height"])
        result[mode] = {
            "pauses": pauses,
            "cost": state["cost"],
            "verified_events": sum(expected.values()),
            "completed": True,
        }
    (args.output / "results.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sample", type=Path, required=True)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--public-cache", action="store_true")
    p.add_argument("--page-bytes", type=int, default=17920)
    args = p.parse_args()
    if not 1 <= args.runs <= 10:
        raise ValueError("invalid repeat count")
    args.output.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
