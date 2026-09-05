#!/usr/bin/env python3
"""Reproduce byte, batching and cache comparisons for identical wallet fixtures."""

import argparse
import json
import math
from pathlib import Path


def total(cost):
    return sum(
        cost[k]
        for k in [
            "public_download_bytes",
            "setup_download_bytes",
            "upload_bytes",
            "response_bytes",
        ]
    )


def summarize(root):
    baseline = json.loads((root / "baseline.json").read_text())
    reports = {
        mode: json.loads((root / mode / "results.json").read_text())
        for mode in ["fresh", "reuse4"]
    }
    result = {
        "classification": "measured application payloads; warm means public c1 cached, not secret-key reuse across sessions",
        "workloads": {},
        "geometries": {},
        "recovery": {},
    }
    for mode, report in reports.items():
        if report["start"] != baseline["start"] or report["end"] != baseline["end"]:
            raise ValueError("range mismatch")
        for name in ["budget_resume", "lost_response"]:
            result["recovery"][f"{mode}_{name}"] = {
                **report[name],
                "application_bytes": total(report[name]["cost"]),
            }
    names = sorted({r["name"] for r in reports["fresh"]["runs"]})
    for name in names:
        out = {}
        for mode, report in reports.items():
            runs = sorted(
                (r for r in report["runs"] if r["name"] == name),
                key=lambda r: r["repeat"],
            )
            if len(runs) != 3:
                raise ValueError("expected cold + two warm repetitions")
            for r in runs:
                if r["application_bytes"] != total(r["cost"]):
                    raise ValueError("invalid traffic accounting")
            if runs[1]["application_bytes"] != runs[2]["application_bytes"]:
                raise ValueError("warm sizes differ")
            compact = sum(
                baseline["bytes_per_height"][
                    runs[0]["checkpoint"] - baseline["start"] + 1 :
                ]
            )
            out[mode] = {
                "cold_bytes": runs[0]["application_bytes"],
                "warm_bytes": runs[1]["application_bytes"],
                "verified_events": runs[0]["verified_events"],
                "queries": runs[0]["cost"]["queries"],
                "cold_setup_bytes": runs[0]["cost"]["setup_download_bytes"],
                "warm_setup_bytes": runs[1]["cost"]["setup_download_bytes"],
                "cold_byte_gate": "pass"
                if 2 * runs[0]["application_bytes"] <= compact
                else "fail",
                "warm_byte_gate": "pass"
                if 2 * runs[1]["application_bytes"] <= compact
                else "fail",
            }
            out["compact_increment_same_coverage"] = compact
        for state in ["cold", "warm"]:
            out[f"{state}_reduction_vs_fresh_percent"] = 100 * (
                1 - out["reuse4"][f"{state}_bytes"] / out["fresh"][f"{state}_bytes"]
            )
        result["workloads"][name] = out
    for mode in ["fresh", "reuse4"]:
        for path in sorted((root / mode).glob("run-*/*/pir/*.json")):
            r = json.loads(path.read_text())
            samples = r["samples"]
            if sum(s["key_upload_bytes"] > 0 for s in samples) != r["key_uploads"]:
                raise ValueError("key upload mismatch")
            expected = (
                math.ceil(r["unique_queries"] / 4)
                if r["mode"] == "reuse4"
                else r["unique_queries"]
            )
            if r["key_uploads"] != expected:
                raise ValueError("partial batch accounting mismatch")
            used = set()
            for s in samples:
                identity = (s["batch"], s["slot"])
                if identity in used and not s["retry"]:
                    raise ValueError("slot reused without immutable retry")
                used.add(identity)
                if s["upload_bytes"] != s["key_upload_bytes"] + s["query_body_bytes"]:
                    raise ValueError("upload decomposition mismatch")
            key = f"{r['mode']}-{r['rows']}x{r['columns']}"
            entry = result["geometries"].setdefault(
                key,
                {
                    "mode": r["mode"],
                    "rows": r["rows"],
                    "columns": r["columns"],
                    "public_sets": r["public_sets"],
                    "public_c1_bytes_per_set": r["public_c1_bytes_per_set"],
                    "packing_cache_payload_bytes": r["packing_cache_payload_bytes"],
                    "rss_samples": [],
                    "preparation_ms_samples": [],
                    "generation_ms_sum": 0.0,
                    "query_samples": 0,
                    "max_decryption_error": 0,
                    "decryption_threshold": r["decryption_threshold"],
                },
            )
            entry["rss_samples"].append(r["combined_client_server_ready_rss_bytes"])
            entry["preparation_ms_samples"].append(sum(r["preparation_ms_per_set"]))
            entry["generation_ms_sum"] += sum(s["prepare_ms"] for s in samples)
            entry["query_samples"] += len(samples)
            entry["max_decryption_error"] = max(
                entry["max_decryption_error"], r["max_decryption_error"]
            )
    for entry in result["geometries"].values():
        entry["mean_generation_ms_per_query"] = (
            entry.pop("generation_ms_sum") / entry["query_samples"]
        )
        rss = entry.pop("rss_samples")
        prep = entry.pop("preparation_ms_samples")
        entry["sampled_combined_rss_range_bytes"] = [min(rss), max(rss)]
        entry["preparation_ms_range"] = [min(prep), max(prep)]
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--evidence", type=Path, default=Path("docs/transparent-pir-evaluation/reuse4")
    )
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    encoded = json.dumps(summarize(args.evidence), indent=2, sort_keys=True) + "\n"
    path = args.evidence / "summary.json"
    if args.check:
        if path.read_text() != encoded:
            raise ValueError("stale reuse summary")
        print("PASS: traffic, partial batches, slot use and cache summary reproduce")
    else:
        path.write_text(encoded)


if __name__ == "__main__":
    main()
