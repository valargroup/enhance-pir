#!/usr/bin/env python3
"""Exercise partial batches, immutable live retries and process restart on real rows."""

import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--generation", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((args.generation / "manifest.json").read_text())
    selected = [0, 2047, 2048, manifest["directory"]["rows"] - 1, 2]
    results = []
    for retry in [True, False]:
        target = args.output / ("retry.json" if retry else "restart.json")
        environment = dict(
            os.environ, PIR_REUSE_MODE="reuse", PIR_RETRY_FIRST="1" if retry else "0"
        )
        with target.open("w") as out:
            subprocess.run(
                [
                    str(args.binary),
                    str(args.generation / "directory.bin"),
                    "3584",
                    str(manifest["directory"]["rows"]),
                    ",".join(map(str, selected)),
                    str(len(selected)),
                ],
                env=environment,
                stdout=out,
                check=True,
            )
        data = json.loads(target.read_text())
        if (
            data["batches"] != 2
            or data["key_uploads"] != 2
            or data["unused_final_batch_slots"] != 3
        ):
            raise ValueError("partial batch mismatch")
        expected = (
            [(0, 0, False)]
            + ([(0, 0, True)] if retry else [])
            + [(0, 1, False), (0, 2, False), (0, 3, False), (1, 0, False)]
        )
        if [(s["batch"], s["slot"], s["retry"]) for s in data["samples"]] != expected:
            raise ValueError("slot or retry mismatch")
        if sum(s["key_upload_bytes"] for s in data["samples"]) != 2 * 86016:
            raise ValueError("key amortization mismatch")
        if {r["row"] for r in data["decoded_rows"]} != set(selected):
            raise ValueError("row coverage mismatch")
        results.append(data.pop("decoded_rows"))
        target.write_text(json.dumps(data, indent=2) + "\n")
    if results[0] != results[1]:
        raise ValueError("fresh restart decoded different rows")
    policy_results = []
    for name, table, mask, rows, expected in [
        ("single_directory", "directory", 0, [0], "fresh"),
        ("two_cold_pages", "pages", 0, [0, 1], "fresh"),
        ("two_warm_pages", "pages", 15, [0, 1], "reuse4"),
    ]:
        target = args.output / f"{name}.json"
        geometry = manifest[table]
        environment = dict(
            os.environ,
            PIR_REUSE_MODE="auto",
            PIR_RETRY_FIRST="0",
            PIR_CACHED_PUBLIC_MASK=str(mask),
        )
        with target.open("w") as out:
            subprocess.run(
                [
                    str(args.binary),
                    str(args.generation / f"{table}.bin"),
                    str(geometry["row_bytes"]),
                    str(geometry["rows"]),
                    ",".join(map(str, rows)),
                    str(len(rows)),
                ],
                env=environment,
                stdout=out,
                check=True,
            )
        data = json.loads(target.read_text())
        if data["mode"] != expected:
            raise ValueError("cold/warm policy selected the wrong mode")
        data.pop("decoded_rows")
        target.write_text(json.dumps(data, indent=2) + "\n")
        policy_results.append({"case": name, "mode": expected, "result": "pass"})
    (args.output / "probe-result.json").write_text(
        json.dumps(
            {
                "result": "pass",
                "policy_checks": policy_results,
                "source": "actual directory rows",
                "queries_per_process": 5,
                "batches_per_process": 2,
                "last_batch_unused_slots": 3,
                "live_retry": "same immutable bytes, same slot; no extra key upload",
                "restart": "new process creates new batches through OS-random start_batch; same expected rows recovered",
                "key_uploads_each_process": 2,
            },
            indent=2,
        )
        + "\n"
    )
    print("PASS: real-row partial batches, immutable live retry, fresh process restart")


if __name__ == "__main__":
    main()
