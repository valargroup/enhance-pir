#!/usr/bin/env python3
"""Reproduce transparent-PIR sensitivity arithmetic; this is not a benchmark."""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "transparent-pir-evaluation"


def evaluate(evidence):
    raw = evidence["enhance_load_test"]["original_json"]
    digest = hashlib.sha256(raw.encode()).hexdigest()
    if digest != evidence["enhance_load_test"]["sha256"]:
        raise ValueError("preserved load-test bytes do not match their digest")
    run = json.loads(raw)
    if run["completed"] != run["succeeded"] + run["errors"]:
        raise ValueError("load-test outcome totals disagree")
    rate = run["completed"] / run["duration_s"]
    if abs(rate - run["requests_per_second"]) > 1e-9:
        raise ValueError("load-test rate disagrees with count/duration")
    if abs(run["error_rate"] - run["errors"] / run["completed"]) > 1e-9:
        raise ValueError("load-test error rate disagrees with outcomes")

    geometries = evidence["recorded_wire_geometries"]
    scenarios = []
    for label, scripts in [
        ("direct_20_candidates", 20),
        ("direct_100_candidates", 100),
        ("direct_1000_candidates", 1000),
        ("hybrid_1_changed_script", 1),
        ("hybrid_10_changed_scripts", 10),
    ]:
        # Two bucket queries is a sensitivity input, not a dictionary decision.
        requests = 2 * scripts
        cases = []
        for geometry in geometries:
            upload = requests * geometry["upload_bytes"]
            download = requests * geometry["response_bytes"]
            cases.append({
                "reference_geometry": geometry["name"],
                "upload_bytes": upload,
                "download_bytes": download,
                "total_bytes": upload + download,
                "total_mib": (upload + download) / 2**20,
            })
        scenarios.append({"scenario": label, "physical_requests": requests,
                          "cases": cases})

    reference = next(g for g in geometries if g["name"] == "ipir_sp_16_instances")
    query_bytes = reference["upload_bytes"] + reference["response_bytes"]
    false_positives = []
    for intervals in [1, 24, 1152]:
        for probability in [1e-4, 1e-5, 1e-6]:
            expected_matches = 100 * intervals * probability
            false_positives.append({
                "absent_scripts": 100,
                "intervals": intervals,
                "false_positive_probability": probability,
                "expected_matches_before_coalescing": expected_matches,
                "expected_bytes_before_coalescing": expected_matches * 2 * query_bytes,
                "expected_mib_before_coalescing": expected_matches * 2 * query_bytes / 2**20,
            })

    peak_syncs = 10000 * 4 * 10 / 86400
    return {
        "classification": "arithmetic sensitivity, not a transparent PIR benchmark",
        "exclusions": ["setup", "filters", "history pages", "HTTP/TLS",
                       "retries", "wallet processing", "generation refresh"],
        "assumed_bucket_requests_per_lookup": 2,
        "scenarios": scenarios,
        "false_positive_sensitivity": false_positives,
        "capacity_assumptions": {
            "wallets": 10000, "syncs_per_day": 4, "peak_factor": 10,
            "headroom_factor": 2, "peak_syncs_per_second": peak_syncs,
            "required_syncs_per_second": peak_syncs * 2,
            "required_queries_per_second_at_2_queries_per_sync": peak_syncs * 2 * 2,
            "required_queries_per_second_at_40_queries_per_sync": peak_syncs * 2 * 40,
        },
        "source_enhance_requests_per_second": rate,
        "decision": "HOLD: no end-to-end transparent PIR benchmark",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="check the saved result without writing files")
    args = parser.parse_args()
    evidence = json.loads((DATA / "evidence.json").read_text())
    result = json.dumps(evaluate(evidence), indent=2, sort_keys=True) + "\n"
    output = DATA / "sensitivity.json"
    if args.check:
        if not output.exists() or output.read_text() != result:
            raise SystemExit("sensitivity.json is missing or stale; rerun without --check")
        print("PASS: evidence integrity, load-test totals, and sensitivity output")
    else:
        output.write_text(result)
        print(output.relative_to(ROOT))


if __name__ == "__main__":
    main()
