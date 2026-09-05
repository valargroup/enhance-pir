#!/usr/bin/env python3
"""Validate the measured navigation evidence and print checkpoint comparisons."""

import argparse
import json
from pathlib import Path


def summarize(root):
    report = json.loads((root / "results.json").read_text())
    baseline = json.loads((root / "baseline.json").read_text())
    runs = report["runs"]
    if len(runs) != 20:
        raise ValueError("expected five checkpoints, two strategies, cold and warm")
    output = []
    for prefix in sorted({r["prefix_blocks"] for r in runs}):
        group = [r for r in runs if r["prefix_blocks"] == prefix]
        if len({(r["navigation"], r["cache"]) for r in group}) != 4:
            raise ValueError("missing strategy/cache pair")
        if len({r["verified_events"] for r in group}) != 1:
            raise ValueError("strategies recovered different event counts")
        for r in group:
            raw = [
                json.loads(p.read_text())
                for p in sorted((root / r["name"] / "pir").glob("*.json"))
            ]
            if len(raw) != len(r["batches"]):
                raise ValueError("missing raw batch evidence")
            seen = set()
            for batch, details in zip(raw, r["batches"]):
                if len(batch["samples"]) != details["queries"]:
                    raise ValueError("batch query count mismatch")
                for sample in batch["samples"]:
                    identity = (details["table"], sample["row"])
                    if identity in seen:
                        raise ValueError("navigation fetched a row twice")
                    seen.add(identity)
            for key, value in {
                "queries": sum(len(b["samples"]) for b in raw),
                "upload_bytes": sum(
                    s["upload_bytes"] for b in raw for s in b["samples"]
                ),
                "response_bytes": sum(
                    s["response_bytes"] for b in raw for s in b["samples"]
                ),
                "setup_download_bytes": sum(
                    b["charged_public_download_bytes"] for b in raw
                ),
            }.items():
                if r["cost"][key] != value:
                    raise ValueError(f"{r['name']}: {key} mismatch")
            if r["application_bytes"] != sum(
                r["cost"][k]
                for k in [
                    "public_download_bytes",
                    "setup_download_bytes",
                    "upload_bytes",
                    "response_bytes",
                ]
            ):
                raise ValueError("total mismatch")
            if r["compact_suffix_bytes"] != sum(baseline["bytes_per_height"][prefix:]):
                raise ValueError("compact suffix mismatch")
        row = {
            "prefix_blocks": prefix,
            "remaining_blocks": len(baseline["bytes_per_height"]) - prefix,
            "verified_events": group[0]["verified_events"],
            "compact_suffix_bytes": group[0]["compact_suffix_bytes"],
        }
        for cache in ["cold", "warm"]:
            a = next(r for r in group if not r["navigation"] and r["cache"] == cache)
            b = next(r for r in group if r["navigation"] and r["cache"] == cache)
            row[cache] = {
                "all_bytes": a["application_bytes"],
                "navigation_bytes": b["application_bytes"],
                "reduction_percent": 100
                * (1 - b["application_bytes"] / a["application_bytes"]),
                "all_queries": a["cost"]["queries"],
                "navigation_queries": b["cost"]["queries"],
                "navigation_batches": b["batches"],
            }
        output.append(row)
    return output


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    args = p.parse_args()
    print(json.dumps(summarize(args.root), indent=2))
