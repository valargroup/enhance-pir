#!/usr/bin/env python3
"""Reproduce sample byte gates using the compact increment for identical coverage."""

import argparse
import json
from pathlib import Path


def summarize(root):
    evidence = json.loads((root / "results.json").read_text())
    baseline = json.loads((root / "baseline.json").read_text())
    if baseline["start"] != evidence["start"] or baseline["end"] != evidence["end"]:
        raise ValueError("baseline coverage differs")
    if len(baseline["bytes_per_height"]) != baseline["end"] - baseline["start"] + 1:
        raise ValueError("incomplete baseline")
    output = {
        "classification": "sample application-byte screen; not product/device gates",
        "workloads": {},
    }
    names = sorted({r["name"] for r in evidence["runs"]})
    for name in names:
        runs = [r for r in evidence["runs"] if r["name"] == name]
        totals = {r["application_bytes"] for r in runs}
        checkpoints = {r["checkpoint"] for r in runs}
        if len(totals) != 1 or len(checkpoints) != 1:
            raise ValueError("inconsistent workload repeats")
        checkpoint = checkpoints.pop()
        compact = sum(
            baseline["bytes_per_height"][checkpoint - baseline["start"] + 1 :]
        )
        amount = totals.pop()
        for r in runs:
            if amount != sum(
                r["cost"][k]
                for k in [
                    "public_download_bytes",
                    "upload_bytes",
                    "response_bytes",
                    "setup_download_bytes",
                ]
            ):
                raise ValueError("inconsistent byte total")
        output["workloads"][name] = {
            "runs": len(runs),
            "checkpoint": checkpoint,
            "verified_events": runs[0]["verified_events"],
            "application_bytes": amount,
            "compact_increment_bytes_same_coverage": compact,
            "ratio_to_compact": amount / compact,
            "savings_percent": 100 * (1 - amount / compact),
            "sample_50_percent_byte_screen": "pass"
            if 2 * amount <= compact
            else "fail",
            "queries": runs[0]["cost"]["queries"],
            "harness_wall_seconds_range": [
                min(
                    r["harness_wall_seconds_including_server_precomputation_and_files"]
                    for r in runs
                ),
                max(
                    r["harness_wall_seconds_including_server_precomputation_and_files"]
                    for r in runs
                ),
            ],
        }
    for name in ["budget_resume", "lost_response"]:
        r = evidence[name]
        output[name] = {
            **r,
            "application_bytes": sum(
                r["cost"][k]
                for k in [
                    "public_download_bytes",
                    "upload_bytes",
                    "response_bytes",
                    "setup_download_bytes",
                ]
            ),
        }
    output["product_gates"] = {
        "representative_wallet_population": "unmeasured",
        "mobile_latency_and_peak_memory": "unmeasured",
        "network_privacy_and_transport": "unmeasured",
        "30_and_180_day_catchup": "unmeasured",
        "full_restoration": "unmeasured",
        "concurrent_publication_and_load": "unmeasured",
    }
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--evidence",
        type=Path,
        default=Path("docs/transparent-pir-evaluation/incremental"),
    )
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    encoded = json.dumps(summarize(args.evidence), indent=2, sort_keys=True) + "\n"
    path = args.evidence / "summary.json"
    if args.check:
        if path.read_text() != encoded:
            raise ValueError("stale summary")
        print("PASS: integrated byte accounting and coverage-matched gates reproduce")
    else:
        path.write_text(encoded)


if __name__ == "__main__":
    main()
