#!/usr/bin/env python3
"""Compare retrieve-all with private binary search on a fixed mainnet snapshot."""

import argparse
import json
from pathlib import Path
from transparent_pir_incremental import (
    load_sample,
    source_events,
    build_generation,
    new_state,
    save,
    sync,
    PirTransport,
)
from transparent_pir_incremental_run import oracle, verify


def run(args):
    metadata, blocks = load_sample(args.sample)
    accepted = {
        blocks[0]["height"] - 1: blocks[0]["prev_hash"],
        **{b["height"]: b["hash"] for b in blocks},
    }
    folder = build_generation(metadata["network"], blocks, args.output / "generations")
    events = source_events(blocks)
    largest = max(events, key=lambda s: (len(events[s]), s))
    baseline = json.loads(args.baseline.read_text())
    assert (
        baseline["start"] == blocks[0]["height"]
        and baseline["end"] == blocks[-1]["height"]
    )
    result = {
        "generation": folder.name,
        "classification": "real in-process PIR serialized payloads; synthetic wallet checkpoints",
        "runs": [],
    }
    for fraction in [0, 0.5, 0.75, 0.9, 0.99]:
        prefix = int(len(blocks) * fraction)
        checkpoint = blocks[0]["height"] - 1 + prefix
        initial, expected, utxos = oracle(blocks, [largest], checkpoint)
        for navigation in [False, True]:
            cache = set()
            for repeat in range(2):
                name = f"prefix-{prefix}-nav-{int(navigation)}-run-{repeat}"
                out = args.output / name
                out.mkdir(parents=True, exist_ok=True)
                path = out / "state.json"
                save(
                    path,
                    new_state(
                        metadata["network"],
                        [largest],
                        checkpoint,
                        accepted[checkpoint],
                        initial,
                    ),
                )
                transport = PirTransport(args.binary, out / "pir", cache)
                state = sync(path, folder, accepted, transport, navigation=navigation)
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
                row = {
                    "name": name,
                    "checkpoint": checkpoint,
                    "prefix_blocks": prefix,
                    "navigation": navigation,
                    "cache": "cold" if repeat == 0 else "warm",
                    "verified_events": sum(expected.values()),
                    "cost": cost,
                    "application_bytes": total,
                    "compact_suffix_bytes": sum(baseline["bytes_per_height"][prefix:]),
                    "batches": [
                        {"table": t, "queries": len(rows)}
                        for t, rows in transport.calls
                    ],
                }
                result["runs"].append(row)
                save(args.output / "results.json", result)
                print(name, total, cost["queries"], "verified", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ["sample", "baseline", "binary", "output"]:
        p.add_argument("--" + name, type=Path, required=True)
    run(p.parse_args())
